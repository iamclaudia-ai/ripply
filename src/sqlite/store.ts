/**
 * SQLite Store — entries + per-index MATERIALIZED tally tables.
 *
 * Each index's reduced output IS a real table, `ripply_<indexName>`, with
 * the groupBy fields and aggregate outputs as plain columns:
 *
 *   SELECT TaskType, Count FROM ripply_TaskCompletionsByTechDate
 *   WHERE HotelId = 'hotels/32546' ORDER BY TaskCompletedDate DESC;
 *
 * Any SQL client can read it with zero Ripply code; `IndexDefinition.indexes`
 * declares ordinary SQL indexes on it; and because it's a real table,
 * Ripply's own trigger capture works on it — enabling cascading indexes
 * (an index whose collection is another index's tally, RavenDB 4's
 * OutputReduceToCollection).
 *
 * Column projection rules:
 *   - `avg` → derived column (sum/count, NULL when empty) PLUS `<out>_sum`
 *     and `<out>_count` component columns so downstream rollups stay exact
 *   - `distinct` → JSON array text
 *   - everything else → the value itself (objects/arrays as JSON text)
 *
 * `group_key` (canonical JSON) is the primary key and — with the `vals`
 * internal column — the source of truth for reconstruction; projected
 * columns are a queryable denormalization maintained in the same
 * transaction.
 *
 * Transactions are real BEGIN IMMEDIATE / COMMIT / ROLLBACK on the shared
 * connection: cursor advance and index writes commit together, giving
 * exactly-once processing when the Source lives in the same database file.
 */

import type { Database } from 'bun:sqlite';
import { groupOfKey, pkKeyOf, tallyTableOf } from '../core/canonical';
import { RipplyError } from '../core/errors';
import type {
  Cursor,
  IndexMeta,
  IndexSchema,
  PkValue,
  ReducedRow,
  Store,
  StoredEntry,
  StoreTx,
} from '../core/types';

export interface SqliteStoreOptions {
  db: Database;
}

const IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;

function ident(name: string): string {
  if (!IDENTIFIER.test(name)) {
    throw new RipplyError(`sqlite: invalid identifier "${name}"`);
  }
  return `"${name}"`;
}

/** One aggregate output expanded to its physical tally columns. */
interface ProjectedAggregate {
  out: string;
  fn: IndexSchema['aggregates'][number]['fn'];
  columns: string[]; // [out] or [out, out_sum, out_count] for avg
}

interface TallyInfo {
  table: string;
  groupBy: string[];
  aggregates: ProjectedAggregate[];
  /** Full column list in table order. */
  columns: string[];
}

/** Column-type strings go into DDL verbatim — keep them boring. */
const TYPE_STRING = /^[A-Za-z_][A-Za-z0-9_ ]*(\(\s*\d+(\s*,\s*\d+)?\s*\))?$/;

function columnType(name: string, column: string, type: string | undefined): string {
  if (type === undefined) return '';
  if (!TYPE_STRING.test(type)) {
    throw new RipplyError(
      `index "${name}": invalid column type "${type}" for tally column "${column}"`,
    );
  }
  return ` ${type}`;
}

function projectSchema(name: string, schema: IndexSchema): TallyInfo {
  const aggregates: ProjectedAggregate[] = schema.aggregates.map(({ out, fn }) => ({
    out,
    fn,
    columns: fn === 'avg' ? [out, `${out}_sum`, `${out}_count`] : [out],
  }));
  const columns = [
    'group_key',
    ...schema.groupBy,
    ...aggregates.flatMap((agg) => agg.columns),
    'entry_count',
    'vals',
  ];
  const seen = new Set<string>();
  for (const column of columns) {
    ident(column); // identifier-safe (throws otherwise)
    if (seen.has(column)) {
      throw new RipplyError(
        `index "${name}": tally column "${column}" would be defined twice ` +
          `(avg outputs also claim "<name>_sum"/"<name>_count")`,
      );
    }
    seen.add(column);
  }
  return { table: tallyTableOf(name), groupBy: [...schema.groupBy], aggregates, columns };
}

/** Scalars pass through (booleans as 0/1); objects/arrays become JSON text. */
function projected(value: unknown): string | number | null {
  if (value === null || value === undefined) return null;
  if (typeof value === 'boolean') return value ? 1 : 0;
  if (typeof value === 'string' || typeof value === 'number') return value;
  return JSON.stringify(value);
}

export class SqliteStore implements Store {
  private readonly db: Database;
  private readonly tallies = new Map<string, TallyInfo>();
  private inTransaction = false;

  constructor(options: SqliteStoreOptions) {
    this.db = options.db;
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS _ripply_entries (
        idx        TEXT NOT NULL,
        pk         TEXT NOT NULL,
        ord        INTEGER NOT NULL,
        group_key  TEXT NOT NULL,
        vals       TEXT NOT NULL,
        PRIMARY KEY (idx, pk, ord)
      );
      CREATE INDEX IF NOT EXISTS _ripply_entries_by_group
        ON _ripply_entries (idx, group_key);
      CREATE TABLE IF NOT EXISTS _ripply_indexes (
        idx         TEXT PRIMARY KEY,
        cursor      TEXT,
        map_version TEXT
      );
    `);
  }

  /** Materialize (or refresh) the index's real tally table. Idempotent. */
  async ensureIndex(name: string, schema: IndexSchema): Promise<void> {
    ident(name); // index names become table names — must be identifier-safe
    const info = projectSchema(name, schema);
    const table = ident(info.table);

    const existing = (
      this.db.query(`PRAGMA table_info(${table})`).all() as Array<{ name: string }>
    ).map((column) => column.name);

    const shapeChanged =
      existing.length > 0 &&
      (existing.length !== info.columns.length ||
        existing.some((column, i) => column !== info.columns[i]));
    if (shapeChanged) {
      // definition changed → map version changed → a rebuild repopulates
      this.db.exec(`DROP TABLE ${table}`);
    }
    if (existing.length === 0 || shapeChanged) {
      const columnDefs = info.columns
        .map((column) => {
          if (column === 'group_key') return `"group_key" TEXT PRIMARY KEY`;
          if (column === 'entry_count') return `"entry_count" INTEGER NOT NULL`;
          if (column === 'vals') return `"vals" TEXT NOT NULL`;
          // declared columnTypes become type affinities (SQLite is dynamic)
          return `${ident(column)}${columnType(name, column, schema.columnTypes[column])}`;
        })
        .join(', ');
      this.db.exec(`CREATE TABLE ${table} (${columnDefs})`);
    }

    for (const sqlIndex of schema.sqlIndexes) {
      const columns = sqlIndex.map((column) => {
        if (!info.columns.includes(column)) {
          throw new RipplyError(
            `index "${name}": SQL index column "${column}" is not a tally column`,
          );
        }
        return ident(column);
      });
      const indexName = ident(`${info.table}__${sqlIndex.join('_')}`);
      this.db.exec(`CREATE INDEX IF NOT EXISTS ${indexName} ON ${table} (${columns.join(', ')})`);
    }

    this.tallies.set(name, info);
  }

  async transaction<T>(fn: (tx: StoreTx) => Promise<T>): Promise<T> {
    if (this.inTransaction) {
      throw new RipplyError('sqlite store does not support nested transactions');
    }
    this.db.exec('BEGIN IMMEDIATE');
    this.inTransaction = true;
    try {
      const result = await fn(new SqliteStoreTx(this.db, this.tallies));
      this.db.exec('COMMIT');
      return result;
    } catch (error) {
      this.db.exec('ROLLBACK');
      throw error;
    } finally {
      this.inTransaction = false;
    }
  }
}

interface EntryRecord {
  pk: string;
  ord: number;
  group_key: string;
  vals: string;
}

interface TallyRecord {
  group_key: string;
  vals: string;
  entry_count: number;
}

class SqliteStoreTx implements StoreTx {
  constructor(
    private readonly db: Database,
    private readonly tallies: Map<string, TallyInfo>,
  ) {}

  private tally(index: string): TallyInfo {
    const info = this.tallies.get(index);
    if (!info) {
      throw new RipplyError(
        `sqlite store: index "${index}" has no materialized table — ensureIndex was not called`,
      );
    }
    return info;
  }

  // -------------------------------------------------------------------- entries

  async readEntries(index: string, pk: PkValue): Promise<StoredEntry[]> {
    const records = this.db
      .query(
        `SELECT pk, ord, group_key, vals FROM _ripply_entries
         WHERE idx = ?1 AND pk = ?2 ORDER BY ord`,
      )
      .all(index, pkKeyOf(pk)) as EntryRecord[];
    return records.map(toStoredEntry);
  }

  async replaceEntries(index: string, pk: PkValue, entries: StoredEntry[]): Promise<void> {
    const key = pkKeyOf(pk);
    this.db.query(`DELETE FROM _ripply_entries WHERE idx = ?1 AND pk = ?2`).run(index, key);
    const insert = this.db.query(
      `INSERT INTO _ripply_entries (idx, pk, ord, group_key, vals)
       VALUES (?1, ?2, ?3, ?4, ?5)`,
    );
    for (const entry of entries) {
      insert.run(index, key, entry.ord, entry.groupKey, JSON.stringify(entry.values));
    }
  }

  async readGroupEntries(index: string, groupKey: string): Promise<StoredEntry[]> {
    const records = this.db
      .query(
        `SELECT pk, ord, group_key, vals FROM _ripply_entries
         WHERE idx = ?1 AND group_key = ?2 ORDER BY pk, ord`,
      )
      .all(index, groupKey) as EntryRecord[];
    return records.map(toStoredEntry);
  }

  async allEntries(index: string): Promise<StoredEntry[]> {
    const records = this.db
      .query(
        `SELECT pk, ord, group_key, vals FROM _ripply_entries
         WHERE idx = ?1 ORDER BY pk, ord`,
      )
      .all(index) as EntryRecord[];
    return records.map(toStoredEntry);
  }

  // -------------------------------------------------------------------- reduced

  async getReduced(index: string, groupKey: string): Promise<ReducedRow | null> {
    const info = this.tally(index);
    const record = this.db
      .query(
        `SELECT group_key, vals, entry_count FROM ${ident(info.table)}
         WHERE group_key = ?1`,
      )
      .get(groupKey) as TallyRecord | null;
    return record ? toReducedRow(record) : null;
  }

  async putReduced(index: string, row: ReducedRow): Promise<void> {
    const info = this.tally(index);
    const values: Array<string | number | null> = [row.groupKey];
    for (const field of info.groupBy) {
      values.push(projected(row.group[field]));
    }
    for (const agg of info.aggregates) {
      const value = row.values[agg.out];
      if (agg.fn === 'avg') {
        const components = (value ?? { sum: 0, count: 0 }) as {
          sum: number;
          count: number;
        };
        values.push(components.count > 0 ? components.sum / components.count : null);
        values.push(components.sum, components.count);
      } else {
        values.push(projected(value));
      }
    }
    values.push(row.entryCount, JSON.stringify(row.values));

    const columnList = info.columns.map(ident).join(', ');
    const placeholders = info.columns.map((_, i) => `?${i + 1}`).join(', ');
    const updates = info.columns
      .filter((column) => column !== 'group_key')
      .map((column) => `${ident(column)} = excluded.${ident(column)}`)
      .join(', ');
    this.db
      .query(
        `INSERT INTO ${ident(info.table)} (${columnList}) VALUES (${placeholders})
         ON CONFLICT (group_key) DO UPDATE SET ${updates}`,
      )
      .run(...values);
  }

  async deleteReduced(index: string, groupKey: string): Promise<void> {
    const info = this.tally(index);
    this.db.query(`DELETE FROM ${ident(info.table)} WHERE group_key = ?1`).run(groupKey);
  }

  async allReduced(index: string): Promise<ReducedRow[]> {
    const info = this.tally(index);
    const records = this.db
      .query(
        `SELECT group_key, vals, entry_count FROM ${ident(info.table)}
         ORDER BY group_key`,
      )
      .all() as TallyRecord[];
    return records.map(toReducedRow);
  }

  async truncateIndex(index: string): Promise<void> {
    const info = this.tally(index);
    this.db.query(`DELETE FROM _ripply_entries WHERE idx = ?1`).run(index);
    this.db.exec(`DELETE FROM ${ident(info.table)}`);
  }

  // -------------------------------------------------------------- cursors/meta

  async getCursor(index: string): Promise<Cursor> {
    const record = this.db
      .query(`SELECT cursor FROM _ripply_indexes WHERE idx = ?1`)
      .get(index) as { cursor: string | null } | null;
    return record?.cursor == null ? null : (JSON.parse(record.cursor) as Cursor);
  }

  async setCursor(index: string, cursor: Cursor): Promise<void> {
    this.db
      .query(
        `INSERT INTO _ripply_indexes (idx, cursor) VALUES (?1, ?2)
         ON CONFLICT (idx) DO UPDATE SET cursor = excluded.cursor`,
      )
      .run(index, JSON.stringify(cursor));
  }

  async getIndexMeta(index: string): Promise<IndexMeta | null> {
    const record = this.db
      .query(`SELECT map_version FROM _ripply_indexes WHERE idx = ?1`)
      .get(index) as { map_version: string | null } | null;
    return record?.map_version == null ? null : { mapVersion: record.map_version };
  }

  async setIndexMeta(index: string, meta: IndexMeta): Promise<void> {
    this.db
      .query(
        `INSERT INTO _ripply_indexes (idx, map_version) VALUES (?1, ?2)
         ON CONFLICT (idx) DO UPDATE SET map_version = excluded.map_version`,
      )
      .run(index, meta.mapVersion);
  }
}

function toStoredEntry(record: EntryRecord): StoredEntry {
  return {
    pk: JSON.parse(record.pk) as PkValue,
    ord: record.ord,
    groupKey: record.group_key,
    values: JSON.parse(record.vals) as StoredEntry['values'],
  };
}

function toReducedRow(record: TallyRecord): ReducedRow {
  return {
    groupKey: record.group_key,
    group: groupOfKey(record.group_key),
    values: JSON.parse(record.vals) as ReducedRow['values'],
    entryCount: record.entry_count,
  };
}

export function sqliteStore(options: SqliteStoreOptions): SqliteStore {
  return new SqliteStore(options);
}
