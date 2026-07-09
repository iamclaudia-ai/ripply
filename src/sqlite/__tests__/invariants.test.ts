/**
 * Adapter matrix, SQLite column: the IDENTICAL invariant suite from
 * Phase 0, now running against a real SQLite database — generated
 * triggers, changelog polling, transactional store. Same tests, same
 * oracle, new backend. That's the whole point.
 */

import { Database } from 'bun:sqlite';
import {
  runInvariantSuite,
  WORK_ORDER_COLUMNS,
  type SuiteBackend,
} from '../../core/__tests__/suite';
import type { Row } from '../../core/types';
import { sqliteSource } from '../source';
import { sqliteStore } from '../store';

export function sqliteBackend(): SuiteBackend {
  const db = new Database(':memory:');
  db.exec(`
    CREATE TABLE work_orders (
      id            TEXT PRIMARY KEY,
      status        TEXT,
      technician_id TEXT,
      revenue       INTEGER,
      tags          TEXT,
      a             TEXT,
      b             TEXT,
      flip          INTEGER
    );
  `);

  const source = sqliteSource({ db, collections: { work_orders: { pk: ['id'] } } });
  const store = sqliteStore({ db });
  // Install capture BEFORE test writes so the changelog sees everything
  // (in an app, engine.start() does this; install() is effectively sync).
  void source.install('work_orders');

  const columns = [...WORK_ORDER_COLUMNS];
  const toSql = (value: unknown): string | number | null => {
    if (value === undefined || value === null) return null;
    if (typeof value === 'boolean') return value ? 1 : 0;
    if (typeof value === 'object') return JSON.stringify(value);
    return value as string | number;
  };

  const insertStmt = db.query(
    `INSERT INTO work_orders (${columns.join(', ')})
     VALUES (${columns.map((_, i) => `?${i + 1}`).join(', ')})`,
  );
  const updateStmt = db.query(
    `UPDATE work_orders SET ${columns
      .slice(1)
      .map((column, i) => `${column} = ?${i + 2}`)
      .join(', ')} WHERE id = ?1`,
  );

  return {
    source,
    store,
    insert: (row: Row) => {
      insertStmt.run(...columns.map((column) => toSql(row[column])));
    },
    update: (row: Row) => {
      updateStmt.run(...columns.map((column) => toSql(row[column])));
    },
    remove: (id: string) => {
      db.query(`DELETE FROM work_orders WHERE id = ?1`).run(id);
    },
    snapshot: () =>
      (db.query(`SELECT * FROM work_orders ORDER BY id`).all() as Row[]).map((row) => ({
        pk: [row.id as string],
        row,
      })),
  };
}

runInvariantSuite('sqlite', sqliteBackend);
