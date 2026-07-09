/**
 * Materialized tally tables + cascading indexes.
 *
 * The reduced output of an index is a REAL table (`ripply_<name>`) with
 * groupBy fields and aggregate outputs as plain columns — queryable by any
 * SQL client with no Ripply code, indexable with ordinary SQL indexes, and
 * (because it's a real table) a valid Source collection for a SECOND index:
 * RavenDB 4's OutputReduceToCollection, incremental all the way down.
 */

import { Database } from 'bun:sqlite';
import { expect, test } from 'bun:test';
import { createRipply, RipplyError, createEngine } from '../../index';
import { sqliteSource, sqliteStore } from '../index';

function setupDb() {
  const db = new Database(':memory:');
  db.exec(`
    CREATE TABLE work_orders (
      id            TEXT PRIMARY KEY,
      technician_id TEXT,
      revenue       INTEGER,
      completed_day TEXT
    );
  `);
  return db;
}

const insertWorkOrder = (
  db: Database,
  id: string,
  tech: string,
  revenue: number,
  day: string,
) =>
  db
    .query(
      `INSERT INTO work_orders (id, technician_id, revenue, completed_day)
       VALUES (?1, ?2, ?3, ?4)`,
    )
    .run(id, tech, revenue, day);

test('the tally is a real table: plain SQL, declared indexes, avg components', async () => {
  const db = setupDb();
  const ripply = createRipply({
    source: sqliteSource({ db, collections: { work_orders: { pk: ['id'] } } }),
    store: sqliteStore({ db }),
    pollInterval: 60_000,
  });

  ripply.defineIndex('revenueByTechDay', {
    collection: 'work_orders',
    map: (wo) => ({
      tech: wo.technician_id,
      day: wo.completed_day,
      revenue: wo.revenue as number,
    }),
    reduce: {
      groupBy: ['tech', 'day'],
      aggregate: { revenue: 'sum', jobs: 'count', avgRevenue: { avg: 'revenue' } },
    },
    indexes: [['tech'], ['day', 'tech']],
  });

  insertWorkOrder(db, 'a', 't1', 100, '2026-07-01');
  insertWorkOrder(db, 'b', 't1', 300, '2026-07-01');
  insertWorkOrder(db, 'c', 't2', 50, '2026-07-02');
  await ripply.start();

  // ── the app queries the tally with PLAIN SQL — no Ripply, no json_extract
  const rows = db
    .query(
      `SELECT tech, day, revenue, jobs, avgRevenue
       FROM ripply_revenueByTechDay
       WHERE tech = 't1'
       ORDER BY day`,
    )
    .all();
  expect(rows).toEqual([
    { tech: 't1', day: '2026-07-01', revenue: 400, jobs: 2, avgRevenue: 200 },
  ]);

  // avg components are real columns too — downstream rollups stay exact
  const components = db
    .query(
      `SELECT avgRevenue_sum, avgRevenue_count FROM ripply_revenueByTechDay
       WHERE tech = 't1' AND day = '2026-07-01'`,
    )
    .get();
  expect(components).toEqual({ avgRevenue_sum: 400, avgRevenue_count: 2 });

  // the declared SQL indexes exist
  const indexNames = (
    db.query(`PRAGMA index_list(ripply_revenueByTechDay)`).all() as Array<{
      name: string;
    }>
  ).map((row) => row.name);
  expect(indexNames).toContain('ripply_revenueByTechDay__tech');
  expect(indexNames).toContain('ripply_revenueByTechDay__day_tech');

  // and it stays fresh: delete a row, the tally row updates in place
  db.query(`DELETE FROM work_orders WHERE id = 'b'`).run();
  await ripply.drain();
  expect(
    db
      .query(
        `SELECT revenue, jobs, avgRevenue FROM ripply_revenueByTechDay WHERE tech = 't1'`,
      )
      .get(),
  ).toEqual({ revenue: 100, jobs: 1, avgRevenue: 100 });

  await ripply.stop();
  db.close();
});

test('cascade: day tally → month tally, incremental all the way down', async () => {
  const db = setupDb();
  const ripply = createRipply({
    source: sqliteSource({
      db,
      collections: {
        work_orders: { pk: ['id'] },
        // an index's tally table is just another collection
        ripply_revenueByDay: { pk: ['group_key'] },
      },
    }),
    store: sqliteStore({ db }),
    pollInterval: 60_000,
  });

  ripply.defineIndex('revenueByDay', {
    collection: 'work_orders',
    map: (wo) => ({ day: wo.completed_day, revenue: wo.revenue as number, n: 1 }),
    reduce: {
      groupBy: ['day'],
      aggregate: { revenue: 'sum', jobs: { sum: 'n' } },
    },
  });

  // rolls up the DAY TALLY — note: monthly jobs = SUM of daily job counts
  const byMonth = ripply.defineIndex('revenueByMonth', {
    collection: 'ripply_revenueByDay',
    map: (day) => ({
      month: (day.day as string).slice(0, 7),
      revenue: day.revenue as number,
      jobs: day.jobs as number,
    }),
    reduce: {
      groupBy: ['month'],
      aggregate: { revenue: 'sum', jobs: 'sum', peakDay: { max: 'revenue' } },
    },
  });

  insertWorkOrder(db, 'a', 't1', 100, '2026-07-01');
  insertWorkOrder(db, 'b', 't1', 300, '2026-07-02');
  insertWorkOrder(db, 'c', 't2', 50, '2026-07-15');
  insertWorkOrder(db, 'd', 't2', 500, '2026-06-30');
  await ripply.start();

  // ONE drain settles the whole cascade (rows → day tally → month tally)
  expect(await byMonth.where({ month: '2026-07' }).one()).toEqual({
    month: '2026-07',
    revenue: 450,
    jobs: 3,
    peakDay: 300,
  });
  expect(await byMonth.where({ month: '2026-06' }).value('revenue')).toBe(500);

  // …and the month tally is ALSO a real table
  expect(
    db
      .query(`SELECT revenue, jobs FROM ripply_revenueByMonth WHERE month = '2026-07'`)
      .get(),
  ).toEqual({ revenue: 450, jobs: 3 });

  // live: a delete retracts through BOTH levels in one drain
  db.query(`DELETE FROM work_orders WHERE id = 'b'`).run();
  await ripply.drain();
  expect(await byMonth.where({ month: '2026-07' }).one()).toEqual({
    month: '2026-07',
    revenue: 150,
    jobs: 2,
    peakDay: 100,
  });

  // a whole month emptying deletes the month group (zero-group deletion cascades)
  db.query(`DELETE FROM work_orders WHERE id = 'd'`).run();
  await ripply.drain();
  expect(await byMonth.where({ month: '2026-06' }).one()).toBeNull();

  await ripply.stop();
  db.close();
});

test('cascade cycles are rejected at start()', async () => {
  const db = setupDb();
  const engine = createEngine({
    source: sqliteSource({
      db,
      collections: {
        ripply_a: { pk: ['group_key'] },
        ripply_b: { pk: ['group_key'] },
      },
    }),
    store: sqliteStore({ db }),
  });

  engine.defineIndex('a', {
    collection: 'ripply_b',
    map: (row) => ({ k: row.k, n: 1 }),
    reduce: { groupBy: ['k'], aggregate: { n: 'sum' } },
  });
  engine.defineIndex('b', {
    collection: 'ripply_a',
    map: (row) => ({ k: row.k, n: 1 }),
    reduce: { groupBy: ['k'], aggregate: { n: 'sum' } },
  });

  await expect(engine.start()).rejects.toThrow(RipplyError);
  await expect(engine.start()).rejects.toThrow('cascading index cycle');
  db.close();
});

test('reserved and colliding tally columns are rejected at definition time', () => {
  const db = setupDb();
  const engine = createEngine({
    source: sqliteSource({ db, collections: { work_orders: { pk: ['id'] } } }),
    store: sqliteStore({ db }),
  });

  expect(() =>
    engine.defineIndex('bad1', {
      collection: 'work_orders',
      map: (wo) => ({ tech: wo.technician_id, n: 1 }),
      reduce: { groupBy: ['tech'], aggregate: { tech: { sum: 'n' } } },
    }),
  ).toThrow('collides with a groupBy field');

  expect(() =>
    engine.defineIndex('bad2', {
      collection: 'work_orders',
      map: (wo) => ({ entry_count: wo.technician_id, n: 1 }),
      reduce: { groupBy: ['entry_count'], aggregate: { n: 'sum' } },
    }),
  ).toThrow('reserved column name');

  expect(() =>
    engine.defineIndex('bad3', {
      collection: 'work_orders',
      map: (wo) => ({ tech: wo.technician_id, n: 1 }),
      reduce: { groupBy: ['tech'], aggregate: { n: 'sum' } },
      indexes: [['nope']],
    }),
  ).toThrow('not a groupBy field or aggregate output');

  db.close();
});
