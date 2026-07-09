/**
 * Adapter matrix, Postgres column: the IDENTICAL invariant suite from
 * Phase 0, now running against a real Postgres — trigger-outbox capture,
 * snapshot-windowed polling, transactional store. Same tests, same
 * oracle, third backend.
 *
 * The factory contract is synchronous but Postgres setup is not, so the
 * returned Source/Store gate every call on a shared init promise (schema
 * reset + table + capture install). Tests run sequentially, so resetting
 * one shared database's schema per backend is race-free.
 */

import { SQL } from 'bun';
import { afterAll, setDefaultTimeout } from 'bun:test';
import {
  runInvariantSuite,
  WORK_ORDER_COLUMNS,
  type SuiteBackend,
} from '../../core/__tests__/suite';
import type { Row, Source, Store } from '../../core/types';
import { postgresSource } from '../source';
import { postgresStore } from '../store';
import { resetSchema, testDatabaseUrl } from './helpers';

setDefaultTimeout(120_000); // real network round-trips; the king test is chatty

const url = await testDatabaseUrl('ripply_test_invariants');
const pools: SQL[] = [];
afterAll(async () => {
  for (const pool of pools) await pool.close();
});

export function postgresBackend(): SuiteBackend {
  const sql = new SQL({ url, max: 4 });
  pools.push(sql);

  const rawSource = postgresSource({
    sql,
    collections: { work_orders: { pk: ['id'] } },
  });
  const rawStore = postgresStore({ sql });

  // Fresh schema + capture BEFORE test writes, gated behind one promise.
  const init = (async () => {
    await resetSchema(sql);
    await sql.unsafe(`
      CREATE TABLE work_orders (
        id            TEXT PRIMARY KEY,
        status        TEXT,
        technician_id TEXT,
        revenue       INTEGER,
        tags          JSONB,
        a             TEXT,
        b             TEXT,
        flip          BOOLEAN
      )`);
    await rawSource.install('work_orders');
  })();

  const source: Source = {
    install: async (collection) => {
      await init;
      await rawSource.install(collection);
    },
    poll: async (collection, cursor, limit) => {
      await init;
      return rawSource.poll(collection, cursor, limit);
    },
    scan: async (collection, onRow) => {
      await init;
      return rawSource.scan(collection, onRow);
    },
    currentCursor: async (collection) => {
      await init;
      return rawSource.currentCursor(collection);
    },
    prune: async (collection, cursors) => {
      await init;
      return rawSource.prune(collection, cursors);
    },
  };
  const store: Store = {
    transaction: async (fn) => {
      await init;
      return rawStore.transaction(fn);
    },
    ensureIndex: async (name, schema) => {
      await init;
      return rawStore.ensureIndex(name, schema);
    },
  };

  const columns = [...WORK_ORDER_COLUMNS];
  const toPg = (value: unknown): unknown => {
    if (value === undefined || value === null) return null;
    if (typeof value === 'object') return JSON.stringify(value);
    return value;
  };
  const placeholder = (column: string, i: number) => (column === 'tags' ? `$${i}::jsonb` : `$${i}`);

  const insertSql = `INSERT INTO work_orders (${columns.join(', ')})
     VALUES (${columns.map((column, i) => placeholder(column, i + 1)).join(', ')})`;
  const updateSql = `UPDATE work_orders SET ${columns
    .slice(1)
    .map((column, i) => `${column} = ${placeholder(column, i + 2)}`)
    .join(', ')} WHERE id = $1`;

  return {
    source,
    store,
    insert: async (row: Row) => {
      await init;
      await sql.unsafe(
        insertSql,
        columns.map((column) => toPg(row[column])),
      );
    },
    update: async (row: Row) => {
      await init;
      await sql.unsafe(
        updateSql,
        columns.map((column) => toPg(row[column])),
      );
    },
    remove: async (id: string) => {
      await init;
      await sql.unsafe(`DELETE FROM work_orders WHERE id = $1`, [id]);
    },
    snapshot: async () => {
      await init;
      const rows = (await sql.unsafe(`SELECT * FROM work_orders ORDER BY id`)) as Row[];
      return rows.map((row) => ({ pk: [row.id as string], row }));
    },
  };
}

runInvariantSuite('postgres', postgresBackend);
