/**
 * SQLite Source — change capture via generated triggers (DESIGN.md §3).
 *
 * SQLite has no logical replication, so triggers are the only option.
 * `install()` introspects the table (`PRAGMA table_info`) and generates
 * INSERT/UPDATE/DELETE triggers that append `(pk, op, after)` to an
 * append-only `_ripply_changelog` table. Single-writer SQLite means the
 * AUTOINCREMENT seq IS commit order — the Postgres out-of-order gotcha
 * does not exist here, so cursor-based polling is strictly safe.
 *
 * The changelog is read-only to consumers (per-index cursors; multiple
 * indexes may share it) and cleaned by `prune()` once every index on a
 * collection has advanced past a change.
 *
 * Note: Bun's sqlite driver does not expose `update_hook`, so there are no
 * wakeups — the Ripply poll fallback (default 250ms) provides freshness.
 * v1 limits: collection == table name, ≤ ~50 columns (SQLite's
 * json_object arg cap), no BLOB columns in captured tables.
 */

import type { Database } from 'bun:sqlite';
import { RipplyError } from '../core/errors';
import type {
  Change,
  ChangeBatch,
  Cursor,
  PkValue,
  Row,
  Scalar,
  Source,
} from '../core/types';

export interface SqliteSourceOptions {
  db: Database;
  collections: Record<string, { pk: string[] }>;
}

const IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;

function ident(name: string): string {
  if (!IDENTIFIER.test(name)) {
    throw new RipplyError(`sqlite: invalid identifier "${name}"`);
  }
  return `"${name}"`;
}

function literal(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

export class SqliteSource implements Source {
  private readonly db: Database;
  private readonly collections = new Map<string, { pk: string[] }>();

  constructor(options: SqliteSourceOptions) {
    this.db = options.db;
    for (const [name, config] of Object.entries(options.collections)) {
      if (!Array.isArray(config.pk) || config.pk.length === 0) {
        throw new RipplyError(`collection "${name}": pk must name at least one column`);
      }
      this.collections.set(name, { pk: [...config.pk] });
    }
  }

  async install(collection: string): Promise<void> {
    const config = this.mustCollection(collection);
    this.ensureChangelog();

    const table = ident(collection);
    const columns = (
      this.db.query(`PRAGMA table_info(${table})`).all() as Array<{ name: string }>
    ).map((column) => column.name);
    if (columns.length === 0) {
      throw new RipplyError(`sqlite source: table "${collection}" does not exist`);
    }
    for (const pkColumn of config.pk) {
      if (!columns.includes(pkColumn)) {
        throw new RipplyError(
          `sqlite source: table "${collection}" has no pk column "${pkColumn}"`,
        );
      }
    }

    const afterJson = `json_object(${columns
      .map((column) => `${literal(column)}, NEW.${ident(column)}`)
      .join(', ')})`;
    const pkArray = (prefix: 'NEW' | 'OLD') =>
      `json_array(${config.pk.map((column) => `${prefix}.${ident(column)}`).join(', ')})`;
    const trigger = (suffix: string) => ident(`_ripply_${collection}_${suffix}`);
    const insertChange = (pk: string, op: string, after: string) =>
      `INSERT INTO _ripply_changelog (collection, pk, op, after)
       VALUES (${literal(collection)}, ${pk}, ${literal(op)}, ${after});`;

    // drop + recreate so schema changes (new columns) refresh the capture
    this.db.exec(`
      DROP TRIGGER IF EXISTS ${trigger('ai')};
      CREATE TRIGGER ${trigger('ai')} AFTER INSERT ON ${table} BEGIN
        ${insertChange(pkArray('NEW'), 'insert', afterJson)}
      END;
      DROP TRIGGER IF EXISTS ${trigger('au')};
      CREATE TRIGGER ${trigger('au')} AFTER UPDATE ON ${table} BEGIN
        ${insertChange(pkArray('NEW'), 'update', afterJson)}
      END;
      DROP TRIGGER IF EXISTS ${trigger('ad')};
      CREATE TRIGGER ${trigger('ad')} AFTER DELETE ON ${table} BEGIN
        ${insertChange(pkArray('OLD'), 'delete', 'NULL')}
      END;
    `);
  }

  async poll(collection: string, cursor: Cursor, limit: number): Promise<ChangeBatch> {
    this.mustCollection(collection);
    this.ensureChangelog();
    const from = cursor === null ? 0 : asSeq(cursor);
    const records = this.db
      .query(
        `SELECT seq, pk, op, after FROM _ripply_changelog
         WHERE collection = ?1 AND seq > ?2 ORDER BY seq LIMIT ?3`,
      )
      .all(collection, from, limit) as Array<{
      seq: number;
      pk: string;
      op: Change['op'];
      after: string | null;
    }>;
    return {
      changes: records.map((record) => ({
        pk: JSON.parse(record.pk) as PkValue,
        op: record.op,
        after: record.after === null ? null : (JSON.parse(record.after) as Row),
        seq: record.seq,
      })),
      nextCursor: records.length ? records[records.length - 1]!.seq : cursor,
    };
  }

  async scan(
    collection: string,
    onRow: (pk: PkValue, row: Row) => void | Promise<void>,
  ): Promise<void> {
    const config = this.mustCollection(collection);
    const rows = this.db.query(`SELECT * FROM ${ident(collection)}`).all() as Row[];
    for (const row of rows) {
      await onRow(config.pk.map((column) => row[column] as Scalar), row);
    }
  }

  async currentCursor(collection: string): Promise<Cursor> {
    this.mustCollection(collection);
    this.ensureChangelog();
    const row = this.db
      .query(`SELECT MAX(seq) AS seq FROM _ripply_changelog`)
      .get() as { seq: number | null };
    return row.seq ?? null;
  }

  async prune(collection: string, cursors: Cursor[]): Promise<number> {
    this.mustCollection(collection);
    this.ensureChangelog();
    if (cursors.length === 0 || cursors.some((cursor) => cursor === null)) return 0;
    const upTo = Math.min(...cursors.map(asSeq));
    const result = this.db
      .query(`DELETE FROM _ripply_changelog WHERE collection = ?1 AND seq <= ?2`)
      .run(collection, upTo);
    return Number(result.changes);
  }

  // ---------------------------------------------------------------------------

  private ensureChangelog(): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS _ripply_changelog (
        seq        INTEGER PRIMARY KEY AUTOINCREMENT,
        collection TEXT NOT NULL,
        pk         TEXT NOT NULL,
        op         TEXT NOT NULL,
        after      TEXT
      );
      CREATE INDEX IF NOT EXISTS _ripply_changelog_by_collection
        ON _ripply_changelog (collection, seq);
    `);
  }

  private mustCollection(collection: string): { pk: string[] } {
    const config = this.collections.get(collection);
    if (!config) throw new RipplyError(`unknown collection "${collection}"`);
    return config;
  }
}

function asSeq(cursor: Cursor): number {
  if (typeof cursor !== 'number') {
    throw new RipplyError(`sqlite source cursors are numbers, got ${JSON.stringify(cursor)}`);
  }
  return cursor;
}

export function sqliteSource(options: SqliteSourceOptions): SqliteSource {
  return new SqliteSource(options);
}
