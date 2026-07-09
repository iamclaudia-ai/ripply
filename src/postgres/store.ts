/**
 * Postgres Store — entries + per-index MATERIALIZED tally tables.
 *
 * Each index's reduced output IS a real table, `ripply_<indexName>`, with
 * the groupBy fields and aggregate outputs as typed columns:
 *
 *   SELECT wins, losses, high_score FROM "ripply_playerStats"
 *   WHERE player_id = 42;
 *
 * Any SQL client can read it with zero Ripply code; `IndexDefinition.indexes`
 * declares ordinary SQL indexes on it; and because it's a real table with
 * the capture trigger attached, cascading indexes work — `putReduced` is an
 * `INSERT ... ON CONFLICT (group_key) DO UPDATE`, so downstream capture
 * fires in the SAME transaction as the upstream tally write.
 *
 * Postgres is strictly typed, so tally columns get real types. Defaults:
 *
 *   groupBy, first/last → text        sum, count, avg, min/max → double precision
 *   distinct            → jsonb
 *
 * Override any of them with `IndexDefinition.columnTypes` — e.g.
 * `{ player_id: 'bigint' }` for a numeric group key, or `{ hi: 'text' }`
 * for a min/max over strings. `avg` additionally gets `<out>_sum` /
 * `<out>_count` component columns so downstream rollups stay exact.
 *
 * `group_key` (canonical JSON) is the primary key and — with the `vals`
 * jsonb column — the source of truth for reconstruction; projected columns
 * are a queryable denormalization maintained in the same transaction.
 * Shape comparison is by column NAME: changing only a columnType does not
 * recreate the table (drop it yourself, or change the index definition).
 *
 * Postgres folds unquoted identifiers to lowercase, and Ripply creates
 * tally columns QUOTED — prefer snake_case index/aggregate names on
 * Postgres, or quote them (`"avgRevenue"`) in your queries.
 *
 * Transactions are `sql.begin(...)`: cursor advance and index writes commit
 * together, giving exactly-once processing when the Source shares the
 * database (the default and the Neon deployment shape).
 */

import type { SQL } from 'bun';
import { groupOfKey, pkKeyOf, tallyTableOf } from '../core/canonical';
import { RipplyError } from '../core/errors';
import type {
  AggregateFn,
  Cursor,
  IndexMeta,
  IndexSchema,
  PkValue,
  ReducedRow,
  Store,
  StoredEntry,
  StoreTx,
} from '../core/types';

export interface PostgresStoreOptions {
  sql: SQL;
}

const IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;

function ident(name: string): string {
  if (!IDENTIFIER.test(name)) {
    throw new RipplyError(`postgres: invalid identifier "${name}"`);
  }
  return `"${name}"`;
}

/** Column-type strings go into DDL verbatim — keep them boring. */
const TYPE_STRING = /^[A-Za-z_][A-Za-z0-9_ ]*(\(\s*\d+(\s*,\s*\d+)?\s*\))?$/;

/**
 * Numeric aggregates default to double precision — NOT bigint — on
 * purpose: Bun returns int8 as a string, which would make a rebuild's
 * scan of a tally table (cascading indexes) see '3' where the trigger
 * after-image says 3. Double precision is exact to 2^53 and comes back
 * as a JS number both ways. Override via columnTypes if you truly need
 * int8 semantics (and normalize in your downstream map).
 */
const DEFAULT_TYPE: Record<AggregateFn, string> = {
  sum: 'double precision',
  count: 'double precision',
  avg: 'double precision',
  min: 'double precision',
  max: 'double precision',
  first: 'text',
  last: 'text',
  distinct: 'jsonb',
};

/** One aggregate output expanded to its physical tally columns. */
interface ProjectedAggregate {
  out: string;
  fn: AggregateFn;
  columns: string[]; // [out] or [out, out_sum, out_count] for avg
}

interface TallyInfo {
  table: string;
  groupBy: string[];
  aggregates: ProjectedAggregate[];
  /** Full column list in table order. */
  columns: string[];
  /** SQL type per projected column (groupBy + aggregate columns). */
  types: Record<string, string>;
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

  const types: Record<string, string> = {};
  for (const field of schema.groupBy) types[field] = 'text';
  for (const agg of aggregates) {
    types[agg.out] = DEFAULT_TYPE[agg.fn];
    if (agg.fn === 'avg') {
      types[`${agg.out}_sum`] = 'double precision';
      types[`${agg.out}_count`] = 'double precision';
    }
  }
  for (const [column, type] of Object.entries(schema.columnTypes)) {
    if (!(column in types)) {
      throw new RipplyError(`index "${name}": columnTypes key "${column}" is not a tally column`);
    }
    if (!TYPE_STRING.test(type)) {
      throw new RipplyError(
        `index "${name}": invalid column type "${type}" for tally column "${column}"`,
      );
    }
    types[column] = type;
  }

  return { table: tallyTableOf(name), groupBy: [...schema.groupBy], aggregates, columns, types };
}

/** Convert a JS value for a typed tally column. */
function projected(value: unknown, type: string): string | number | boolean | null {
  if (value === null || value === undefined) return null;
  const t = type.toLowerCase();
  if (t.includes('json')) return JSON.stringify(value);
  if (typeof value === 'object') return JSON.stringify(value);
  if (t.startsWith('text') || t.startsWith('varchar') || t.startsWith('char')) {
    return typeof value === 'string' ? value : String(value);
  }
  if (typeof value === 'boolean' && !t.startsWith('bool')) return value ? 1 : 0;
  return value as string | number | boolean;
}

/** The subset of Bun's SQL/TransactionSQL surface the store uses. */
interface SqlLike {
  unsafe(query: string, params?: unknown[]): Promise<unknown>;
}

export class PostgresStore implements Store {
  private readonly sql: SQL;
  private readonly tallies = new Map<string, TallyInfo>();
  private ready: Promise<void> | null = null;

  constructor(options: PostgresStoreOptions) {
    this.sql = options.sql;
  }

  /** Materialize (or refresh) the index's real tally table. Idempotent. */
  async ensureIndex(name: string, schema: IndexSchema): Promise<void> {
    await this.ensureReady();
    ident(name); // index names become table names — must be identifier-safe
    const info = projectSchema(name, schema);
    const table = ident(info.table);

    const existing = (
      (await this.sql.unsafe(
        `SELECT column_name FROM information_schema.columns
         WHERE table_schema = current_schema() AND table_name = $1
         ORDER BY ordinal_position`,
        [info.table],
      )) as Array<{ column_name: string }>
    ).map((record) => record.column_name);

    const shapeChanged =
      existing.length > 0 &&
      (existing.length !== info.columns.length ||
        existing.some((column, i) => column !== info.columns[i]));
    if (shapeChanged) {
      // definition changed → map version changed → a rebuild repopulates
      await this.sql.unsafe(`DROP TABLE ${table}`);
    }
    if (existing.length === 0 || shapeChanged) {
      const columnDefs = info.columns
        .map((column) => {
          if (column === 'group_key') return `"group_key" TEXT PRIMARY KEY`;
          if (column === 'entry_count') return `"entry_count" INTEGER NOT NULL`;
          if (column === 'vals') return `"vals" JSONB NOT NULL`;
          return `${ident(column)} ${info.types[column]!}`;
        })
        .join(', ');
      await this.sql.unsafe(`CREATE TABLE ${table} (${columnDefs})`);
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
      await this.sql.unsafe(
        `CREATE INDEX IF NOT EXISTS ${indexName} ON ${table} (${columns.join(', ')})`,
      );
    }

    this.tallies.set(name, info);
  }

  async transaction<T>(fn: (tx: StoreTx) => Promise<T>): Promise<T> {
    await this.ensureReady();
    return this.sql.begin(async (tx) =>
      fn(new PostgresStoreTx(tx as unknown as SqlLike, this.tallies)),
    ) as Promise<T>;
  }

  private ensureReady(): Promise<void> {
    this.ready ??= (async () => {
      await this.sql.unsafe(`
        CREATE TABLE IF NOT EXISTS _ripply_entries (
          idx        TEXT NOT NULL,
          pk         TEXT NOT NULL,
          ord        INTEGER NOT NULL,
          group_key  TEXT NOT NULL,
          vals       JSONB NOT NULL,
          PRIMARY KEY (idx, pk, ord)
        )`);
      await this.sql.unsafe(`
        CREATE INDEX IF NOT EXISTS _ripply_entries_by_group
          ON _ripply_entries (idx, group_key)`);
      await this.sql.unsafe(`
        CREATE TABLE IF NOT EXISTS _ripply_indexes (
          idx         TEXT PRIMARY KEY,
          cursor      TEXT,
          map_version TEXT
        )`);
    })();
    return this.ready;
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

class PostgresStoreTx implements StoreTx {
  constructor(
    private readonly sql: SqlLike,
    private readonly tallies: Map<string, TallyInfo>,
  ) {}

  private tally(index: string): TallyInfo {
    const info = this.tallies.get(index);
    if (!info) {
      throw new RipplyError(
        `postgres store: index "${index}" has no materialized table — ensureIndex was not called`,
      );
    }
    return info;
  }

  // -------------------------------------------------------------------- entries

  async readEntries(index: string, pk: PkValue): Promise<StoredEntry[]> {
    const records = (await this.sql.unsafe(
      `SELECT pk, ord, group_key, vals::text AS vals FROM _ripply_entries
       WHERE idx = $1 AND pk = $2 ORDER BY ord`,
      [index, pkKeyOf(pk)],
    )) as EntryRecord[];
    return records.map(toStoredEntry);
  }

  async replaceEntries(index: string, pk: PkValue, entries: StoredEntry[]): Promise<void> {
    await this.sql.unsafe(`DELETE FROM _ripply_entries WHERE idx = $1 AND pk = $2`, [
      index,
      pkKeyOf(pk),
    ]);
    await this.insertEntries(index, entries);
  }

  /** Bulk insert (rebuild fast path — also the tail of replaceEntries). */
  async insertEntries(index: string, entries: StoredEntry[]): Promise<void> {
    // chunked multi-row inserts: 5 params per row, well under PG's 65535
    for (let at = 0; at < entries.length; at += 1000) {
      const chunk = entries.slice(at, at + 1000);
      const params: unknown[] = [];
      const tuples = chunk.map((entry) => {
        const p = params.length;
        params.push(
          index,
          pkKeyOf(entry.pk),
          entry.ord,
          entry.groupKey,
          JSON.stringify(entry.values),
        );
        // ::text::jsonb — the param is explicitly text and the SERVER parses
        // it; Bun would otherwise JSON-encode a pre-stringified param twice
        return `($${p + 1}, $${p + 2}, $${p + 3}, $${p + 4}, $${p + 5}::text::jsonb)`;
      });
      await this.sql.unsafe(
        `INSERT INTO _ripply_entries (idx, pk, ord, group_key, vals)
         VALUES ${tuples.join(', ')}`,
        params,
      );
    }
  }

  async readGroupEntries(index: string, groupKey: string): Promise<StoredEntry[]> {
    const records = (await this.sql.unsafe(
      `SELECT pk, ord, group_key, vals::text AS vals FROM _ripply_entries
       WHERE idx = $1 AND group_key = $2 ORDER BY pk, ord`,
      [index, groupKey],
    )) as EntryRecord[];
    return records.map(toStoredEntry);
  }

  async allEntries(index: string): Promise<StoredEntry[]> {
    const records = (await this.sql.unsafe(
      `SELECT pk, ord, group_key, vals::text AS vals FROM _ripply_entries
       WHERE idx = $1 ORDER BY pk, ord`,
      [index],
    )) as EntryRecord[];
    return records.map(toStoredEntry);
  }

  // -------------------------------------------------------------------- reduced

  async getReduced(index: string, groupKey: string): Promise<ReducedRow | null> {
    const info = this.tally(index);
    const records = (await this.sql.unsafe(
      `SELECT group_key, vals::text AS vals, entry_count FROM ${ident(info.table)}
       WHERE group_key = $1`,
      [groupKey],
    )) as TallyRecord[];
    return records.length ? toReducedRow(records[0]!) : null;
  }

  async putReduced(index: string, row: ReducedRow): Promise<void> {
    await this.putReducedMany(index, [row]);
  }

  /** Bulk upsert (rebuild fast path). Group keys must be distinct. */
  async putReducedMany(index: string, rows: ReducedRow[]): Promise<void> {
    const info = this.tally(index);
    const width = info.columns.length;
    const rowsPerChunk = Math.max(1, Math.floor(20_000 / width));
    const updates = info.columns
      .filter((column) => column !== 'group_key')
      .map((column) => `${ident(column)} = EXCLUDED.${ident(column)}`)
      .join(', ');
    const columnList = info.columns.map(ident).join(', ');

    for (let at = 0; at < rows.length; at += rowsPerChunk) {
      const chunk = rows.slice(at, at + rowsPerChunk);
      const params: unknown[] = [];
      const tuples = chunk.map((row) => {
        const base = params.length;
        params.push(...projectedValues(info, row));
        const placeholders = info.columns.map((column, i) => {
          const type = column === 'vals' ? 'jsonb' : info.types[column];
          // ::text::jsonb, not ::jsonb — see insertEntries
          return type?.toLowerCase().includes('json')
            ? `$${base + i + 1}::text::jsonb`
            : `$${base + i + 1}`;
        });
        return `(${placeholders.join(', ')})`;
      });
      // upsert on purpose: fires the capture trigger's INSERT/UPDATE in this
      // same transaction, which is what makes cascading indexes atomic
      await this.sql.unsafe(
        `INSERT INTO ${ident(info.table)} (${columnList}) VALUES ${tuples.join(', ')}
         ON CONFLICT (group_key) DO UPDATE SET ${updates}`,
        params,
      );
    }
  }

  async deleteReduced(index: string, groupKey: string): Promise<void> {
    const info = this.tally(index);
    await this.sql.unsafe(`DELETE FROM ${ident(info.table)} WHERE group_key = $1`, [groupKey]);
  }

  async allReduced(index: string): Promise<ReducedRow[]> {
    const info = this.tally(index);
    const records = (await this.sql.unsafe(
      `SELECT group_key, vals::text AS vals, entry_count FROM ${ident(info.table)}
       ORDER BY group_key`,
    )) as TallyRecord[];
    return records.map(toReducedRow);
  }

  async truncateIndex(index: string): Promise<void> {
    const info = this.tally(index);
    await this.sql.unsafe(`DELETE FROM _ripply_entries WHERE idx = $1`, [index]);
    await this.sql.unsafe(`DELETE FROM ${ident(info.table)}`);
  }

  // -------------------------------------------------------------- cursors/meta

  async getCursor(index: string): Promise<Cursor> {
    const records = (await this.sql.unsafe(`SELECT cursor FROM _ripply_indexes WHERE idx = $1`, [
      index,
    ])) as Array<{ cursor: string | null }>;
    const cursor = records[0]?.cursor;
    return cursor == null ? null : (JSON.parse(cursor) as Cursor);
  }

  async setCursor(index: string, cursor: Cursor): Promise<void> {
    await this.sql.unsafe(
      `INSERT INTO _ripply_indexes (idx, cursor) VALUES ($1, $2)
       ON CONFLICT (idx) DO UPDATE SET cursor = EXCLUDED.cursor`,
      [index, JSON.stringify(cursor)],
    );
  }

  async getIndexMeta(index: string): Promise<IndexMeta | null> {
    const records = (await this.sql.unsafe(
      `SELECT map_version FROM _ripply_indexes WHERE idx = $1`,
      [index],
    )) as Array<{ map_version: string | null }>;
    const version = records[0]?.map_version;
    return version == null ? null : { mapVersion: version };
  }

  async setIndexMeta(index: string, meta: IndexMeta): Promise<void> {
    await this.sql.unsafe(
      `INSERT INTO _ripply_indexes (idx, map_version) VALUES ($1, $2)
       ON CONFLICT (idx) DO UPDATE SET map_version = EXCLUDED.map_version`,
      [index, meta.mapVersion],
    );
  }
}

/** One reduced row projected to its tally-column values, in column order. */
function projectedValues(info: TallyInfo, row: ReducedRow): unknown[] {
  const values: unknown[] = [row.groupKey];
  for (const field of info.groupBy) {
    values.push(projected(row.group[field], info.types[field]!));
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
      values.push(projected(value, info.types[agg.out]!));
    }
  }
  values.push(row.entryCount, JSON.stringify(row.values));
  return values;
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
    entryCount: Number(record.entry_count),
  };
}

export function postgresStore(options: PostgresStoreOptions): PostgresStore {
  return new PostgresStore(options);
}
