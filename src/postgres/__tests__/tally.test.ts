/**
 * Materialized tally tables + cascading indexes on Postgres.
 *
 * The reduced output of an index is a REAL, TYPED Postgres table
 * (`ripply_<name>`) — plain-SQL queryable, indexable, `columnTypes`
 * overridable — and a valid Source collection for a second index
 * (RavenDB 4's OutputReduceToCollection), incremental all the way down.
 * Shaped like the NutWords use case on purpose: player stats by id,
 * day → month revenue rollups.
 */

import { SQL } from 'bun';
import { afterAll, expect, test, setDefaultTimeout } from 'bun:test';
import { createRipply } from '../../index';
import { postgresSource, postgresStore } from '../index';
import { resetSchema, testDatabaseUrl } from './helpers';

setDefaultTimeout(120_000);

const url = await testDatabaseUrl('ripply_test_tally');
const sql = new SQL({ url, max: 4 });
afterAll(() => sql.close());

test('the tally is a real typed table: plain SQL, columnTypes, declared indexes, avg components', async () => {
  await resetSchema(sql);
  await sql.unsafe(`
    CREATE TABLE games (
      id        TEXT PRIMARY KEY,
      player_id BIGINT NOT NULL,
      score     INTEGER NOT NULL,
      won       BOOLEAN NOT NULL
    )`);

  const ripply = createRipply({
    source: postgresSource({ sql, collections: { games: { pk: ['id'] } } }),
    store: postgresStore({ sql }),
    pollInterval: 60_000,
  });
  try {
  ripply.defineIndex('player_stats', {
    collection: 'games',
    map: (game) => ({
      player_id: game.player_id as number,
      wins: game.won ? 1 : 0,
      score: game.score as number,
    }),
    reduce: {
      groupBy: ['player_id'],
      aggregate: {
        games: 'count',
        wins: { sum: 'wins' },
        high_score: { max: 'score' },
        avg_score: { avg: 'score' },
      },
    },
    indexes: [['high_score']],
    // the group key is numeric — say so, and the tally column is queryable
    // as a number instead of the text default
    columnTypes: { player_id: 'bigint' },
  });

  await sql`INSERT INTO games VALUES ('g1', 42, 310, true)`;
  await sql`INSERT INTO games VALUES ('g2', 42, 250, false)`;
  await sql`INSERT INTO games VALUES ('g3', 7, 495, true)`;
  await ripply.start();

  // ── the app queries the tally with PLAIN SQL — numeric where-clause, no quotes
  const rows = (await sql.unsafe(
    `SELECT player_id::float8 AS player_id, games, wins, high_score, avg_score
     FROM ripply_player_stats WHERE player_id = 42`,
  )) as Array<Record<string, unknown>>;
  expect(rows).toEqual([
    { player_id: 42, games: 2, wins: 1, high_score: 310, avg_score: 280 },
  ]);

  // declared column type stuck
  const types = (await sql.unsafe(
    `SELECT data_type FROM information_schema.columns
     WHERE table_name = 'ripply_player_stats' AND column_name = 'player_id'`,
  )) as Array<{ data_type: string }>;
  expect(types[0]!.data_type).toBe('bigint');

  // avg components are real columns too — downstream rollups stay exact
  const components = (await sql.unsafe(
    `SELECT avg_score_sum, avg_score_count FROM ripply_player_stats WHERE player_id = 42`,
  )) as Array<Record<string, unknown>>;
  expect(components).toEqual([{ avg_score_sum: 560, avg_score_count: 2 }]);

  // the declared SQL index exists
  const indexes = (await sql.unsafe(
    `SELECT indexname FROM pg_indexes WHERE tablename = 'ripply_player_stats'`,
  )) as Array<{ indexname: string }>;
  expect(indexes.map((record) => record.indexname)).toContain(
    'ripply_player_stats__high_score',
  );

  // and it stays fresh: losing the high score re-reduces from entries
  await sql`DELETE FROM games WHERE id = 'g1'`;
  await ripply.drain();
  const fresh = (await sql.unsafe(
    `SELECT games, wins, high_score FROM ripply_player_stats WHERE player_id = 42`,
  )) as Array<Record<string, unknown>>;
  expect(fresh).toEqual([{ games: 1, wins: 0, high_score: 250 }]);
  } finally {
    await ripply.stop();
  }
});

test('cascade on Postgres: day tally → month tally, retraction through both levels', async () => {
  await resetSchema(sql);
  await sql.unsafe(`
    CREATE TABLE work_orders (
      id            TEXT PRIMARY KEY,
      revenue       INTEGER NOT NULL,
      completed_day TEXT NOT NULL
    )`);

  const ripply = createRipply({
    source: postgresSource({
      sql,
      collections: {
        work_orders: { pk: ['id'] },
        // an index's tally table is just another collection
        ripply_revenue_by_day: { pk: ['group_key'] },
      },
    }),
    store: postgresStore({ sql }),
    pollInterval: 60_000,
  });
  try {
  ripply.defineIndex('revenue_by_day', {
    collection: 'work_orders',
    map: (wo) => ({ day: wo.completed_day, revenue: wo.revenue as number, n: 1 }),
    reduce: { groupBy: ['day'], aggregate: { revenue: 'sum', jobs: { sum: 'n' } } },
  });

  // rolls up the DAY TALLY — monthly jobs = SUM of daily job counts
  const byMonth = ripply.defineIndex('revenue_by_month', {
    collection: 'ripply_revenue_by_day',
    map: (day) => ({
      month: (day.day as string).slice(0, 7),
      revenue: day.revenue as number,
      jobs: day.jobs as number,
    }),
    reduce: {
      groupBy: ['month'],
      aggregate: { revenue: 'sum', jobs: 'sum', peak_day: { max: 'revenue' } },
    },
  });

  await sql`INSERT INTO work_orders VALUES ('a', 100, '2026-07-01')`;
  await sql`INSERT INTO work_orders VALUES ('b', 300, '2026-07-02')`;
  await sql`INSERT INTO work_orders VALUES ('c', 50, '2026-07-15')`;
  await sql`INSERT INTO work_orders VALUES ('d', 500, '2026-06-30')`;
  await ripply.start();

  // ONE start settles the whole cascade (rows → day tally → month tally)
  expect(await byMonth.where({ month: '2026-07' }).one()).toEqual({
    month: '2026-07',
    revenue: 450,
    jobs: 3,
    peak_day: 300,
  });
  expect(await byMonth.where({ month: '2026-06' }).value('revenue')).toBe(500);

  // …and the month tally is ALSO a real table
  const monthRows = (await sql.unsafe(
    `SELECT revenue, jobs FROM ripply_revenue_by_month WHERE month = '2026-07'`,
  )) as Array<Record<string, unknown>>;
  expect(monthRows).toEqual([{ revenue: 450, jobs: 3 }]);

  // live: a delete retracts through BOTH levels in one drain
  await sql`DELETE FROM work_orders WHERE id = 'b'`;
  await ripply.drain();
  expect(await byMonth.where({ month: '2026-07' }).one()).toEqual({
    month: '2026-07',
    revenue: 150,
    jobs: 2,
    peak_day: 100,
  });

  // a whole month emptying deletes the month group (zero-group deletion cascades)
  await sql`DELETE FROM work_orders WHERE id = 'd'`;
  await ripply.drain();
  expect(await byMonth.where({ month: '2026-06' }).one()).toBeNull();
  } finally {
    await ripply.stop();
  }
});
