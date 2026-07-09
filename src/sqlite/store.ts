/**
 * SQLite Store — entries + reduced + cursors in three tables.
 *
 * Transactions are real BEGIN IMMEDIATE / COMMIT / ROLLBACK on the shared
 * connection: cursor advance and index writes commit together, giving
 * exactly-once processing when the Source lives in the same database file
 * (DESIGN.md §4). The engine serializes its own work, so one connection
 * and one transaction at a time is exactly the contract.
 */

import type { Database } from 'bun:sqlite';
import { pkKeyOf } from '../core/canonical';
import { RipplyError } from '../core/errors';
import type {
  Cursor,
  IndexMeta,
  PkValue,
  ReducedRow,
  Store,
  StoredEntry,
  StoreTx,
} from '../core/types';

export interface SqliteStoreOptions {
  db: Database;
}

export class SqliteStore implements Store {
  private readonly db: Database;
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
      CREATE TABLE IF NOT EXISTS _ripply_reduced (
        idx         TEXT NOT NULL,
        group_key   TEXT NOT NULL,
        grp         TEXT NOT NULL,
        vals        TEXT NOT NULL,
        entry_count INTEGER NOT NULL,
        PRIMARY KEY (idx, group_key)
      );
      CREATE TABLE IF NOT EXISTS _ripply_indexes (
        idx         TEXT PRIMARY KEY,
        cursor      TEXT,
        map_version TEXT
      );
    `);
  }

  async transaction<T>(fn: (tx: StoreTx) => Promise<T>): Promise<T> {
    if (this.inTransaction) {
      throw new RipplyError('sqlite store does not support nested transactions');
    }
    this.db.exec('BEGIN IMMEDIATE');
    this.inTransaction = true;
    try {
      const result = await fn(new SqliteStoreTx(this.db));
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

interface ReducedRecord {
  group_key: string;
  grp: string;
  vals: string;
  entry_count: number;
}

class SqliteStoreTx implements StoreTx {
  constructor(private readonly db: Database) {}

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
    this.db
      .query(`DELETE FROM _ripply_entries WHERE idx = ?1 AND pk = ?2`)
      .run(index, key);
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

  async getReduced(index: string, groupKey: string): Promise<ReducedRow | null> {
    const record = this.db
      .query(
        `SELECT group_key, grp, vals, entry_count FROM _ripply_reduced
         WHERE idx = ?1 AND group_key = ?2`,
      )
      .get(index, groupKey) as ReducedRecord | null;
    return record ? toReducedRow(record) : null;
  }

  async putReduced(index: string, row: ReducedRow): Promise<void> {
    this.db
      .query(
        `INSERT INTO _ripply_reduced (idx, group_key, grp, vals, entry_count)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT (idx, group_key) DO UPDATE SET
           grp = excluded.grp, vals = excluded.vals, entry_count = excluded.entry_count`,
      )
      .run(
        index,
        row.groupKey,
        JSON.stringify(row.group),
        JSON.stringify(row.values),
        row.entryCount,
      );
  }

  async deleteReduced(index: string, groupKey: string): Promise<void> {
    this.db
      .query(`DELETE FROM _ripply_reduced WHERE idx = ?1 AND group_key = ?2`)
      .run(index, groupKey);
  }

  async allReduced(index: string): Promise<ReducedRow[]> {
    const records = this.db
      .query(
        `SELECT group_key, grp, vals, entry_count FROM _ripply_reduced
         WHERE idx = ?1 ORDER BY group_key`,
      )
      .all(index) as ReducedRecord[];
    return records.map(toReducedRow);
  }

  async truncateIndex(index: string): Promise<void> {
    this.db.query(`DELETE FROM _ripply_entries WHERE idx = ?1`).run(index);
    this.db.query(`DELETE FROM _ripply_reduced WHERE idx = ?1`).run(index);
  }

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

function toReducedRow(record: ReducedRecord): ReducedRow {
  return {
    groupKey: record.group_key,
    group: JSON.parse(record.grp) as ReducedRow['group'],
    values: JSON.parse(record.vals) as ReducedRow['values'],
    entryCount: record.entry_count,
  };
}

export function sqliteStore(options: SqliteStoreOptions): SqliteStore {
  return new SqliteStore(options);
}
