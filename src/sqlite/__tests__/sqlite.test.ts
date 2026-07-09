/**
 * SQLite-specific behavior beyond the shared invariant matrix:
 * durability across process restarts, trigger codegen hygiene, and
 * changelog pruning.
 */

import { Database } from 'bun:sqlite';
import { afterEach, expect, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createEngine, createRipply, type IndexDefinition } from '../../index';
import { sqliteSource, sqliteStore } from '../index';

const countByStatus: IndexDefinition = {
  collection: 'work_orders',
  map: (wo) => ({ status: wo.status, n: 1 }),
  reduce: { groupBy: ['status'], aggregate: { count: { sum: 'n' } } },
};

let tempDirs: string[] = [];
afterEach(() => {
  for (const dir of tempDirs.splice(0)) rmSync(dir, { recursive: true, force: true });
});

function tempDbPath(): string {
  const dir = mkdtempSync(join(tmpdir(), 'ripply-sqlite-'));
  tempDirs.push(dir);
  return join(dir, 'app.db');
}

function openAdapters(path: string) {
  const db = new Database(path, { create: true });
  db.exec(`
    CREATE TABLE IF NOT EXISTS work_orders (
      id     TEXT PRIMARY KEY,
      status TEXT
    );
  `);
  const source = sqliteSource({ db, collections: { work_orders: { pk: ['id'] } } });
  const store = sqliteStore({ db });
  return { db, source, store };
}

test('durability — index, cursor, and map version survive close + reopen', async () => {
  const path = tempDbPath();

  // session 1: build, process, close
  {
    const { db, source, store } = openAdapters(path);
    const engine = createEngine({ source, store });
    engine.defineIndex('countByStatus', countByStatus);
    await engine.start();
    db.query(`INSERT INTO work_orders (id, status) VALUES ('a', 'pending')`).run();
    db.query(`INSERT INTO work_orders (id, status) VALUES ('b', 'pending')`).run();
    await engine.drain();
    db.close();
  }

  // session 2: same definition — no rebuild, state is already there
  {
    const { db, source, store } = openAdapters(path);
    let scans = 0;
    const originalScan = source.scan.bind(source);
    source.scan = (collection, onRow) => {
      scans++;
      return originalScan(collection, onRow);
    };
    const engine = createEngine({ source, store });
    engine.defineIndex('countByStatus', countByStatus);
    await engine.start();
    expect(scans).toBe(0); // map version matched — the stored index was trusted

    const rows = await store.transaction((tx) => tx.allReduced('countByStatus'));
    expect(rows).toMatchObject([{ group: { status: 'pending' }, values: { count: 2 } }]);

    // and processing picks up right where the cursor left off
    db.query(`INSERT INTO work_orders (id, status) VALUES ('c', 'completed')`).run();
    await engine.drain();
    const after = await store.transaction((tx) => tx.allReduced('countByStatus'));
    expect(after).toHaveLength(2);
    db.close();
  }
});

test('install is idempotent and refreshes triggers after schema changes', async () => {
  const { db, source } = openAdapters(tempDbPath());

  await source.install('work_orders');
  await source.install('work_orders'); // double install must not double-capture

  db.query(`INSERT INTO work_orders (id, status) VALUES ('a', 'pending')`).run();
  let batch = await source.poll('work_orders', null, 100);
  expect(batch.changes).toHaveLength(1);
  expect(batch.changes[0]!.after).toEqual({ id: 'a', status: 'pending' });

  // schema evolves — re-install regenerates triggers with the new column
  db.exec(`ALTER TABLE work_orders ADD COLUMN priority TEXT`);
  await source.install('work_orders');
  db.query(
    `INSERT INTO work_orders (id, status, priority) VALUES ('b', 'pending', 'high')`,
  ).run();
  batch = await source.poll('work_orders', batch.nextCursor, 100);
  expect(batch.changes[0]!.after).toEqual({ id: 'b', status: 'pending', priority: 'high' });

  db.close();
});

test('unknown tables, missing pk columns, and hostile identifiers are rejected', async () => {
  const { db, source } = openAdapters(tempDbPath());

  await expect(
    sqliteSource({ db, collections: { missing: { pk: ['id'] } } }).install('missing'),
  ).rejects.toThrow('does not exist');
  await expect(
    sqliteSource({ db, collections: { work_orders: { pk: ['nope'] } } }).install(
      'work_orders',
    ),
  ).rejects.toThrow('no pk column');
  await expect(
    sqliteSource({
      db,
      collections: { 'work_orders"; DROP TABLE x; --': { pk: ['id'] } },
    }).install('work_orders"; DROP TABLE x; --'),
  ).rejects.toThrow('invalid identifier');

  expect(source).toBeDefined();
  db.close();
});

test('ripply auto-prunes the changelog once every index has caught up', async () => {
  const { db, source, store } = openAdapters(tempDbPath());
  const ripply = createRipply({ source, store, pollInterval: 60_000 });
  ripply.defineIndex('countByStatus', countByStatus);
  await ripply.start();

  db.query(`INSERT INTO work_orders (id, status) VALUES ('a', 'pending')`).run();
  db.query(`INSERT INTO work_orders (id, status) VALUES ('b', 'completed')`).run();
  await ripply.drain();

  // applied + pruned: the changelog is empty, the index is correct
  const changelog = db.query(`SELECT COUNT(*) AS n FROM _ripply_changelog`).get() as {
    n: number;
  };
  expect(changelog.n).toBe(0);
  expect(await ripply.index('countByStatus').where({ status: 'pending' }).value('count')).toBe(1);

  // pruning never breaks idempotent replay guarantees going FORWARD
  db.query(`UPDATE work_orders SET status = 'completed' WHERE id = 'a'`).run();
  await ripply.drain();
  expect(await ripply.index('countByStatus').where({ status: 'completed' }).value('count')).toBe(2);

  await ripply.stop();
  db.close();
});
