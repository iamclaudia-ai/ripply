/**
 * Keyset-paged scan (issue #3).
 *
 * `scan()` no longer reads the whole table into the client — it pages by
 * primary key. Two things have to hold that a single-page fixture would
 * never catch:
 *
 *  1. The watermark round-trips through TEXT and is cast back to the pk's
 *     own SQL type. On a BIGINT pk, comparing as text would order 1000
 *     before 999 and silently drop most of the table.
 *  2. Composite keys page as a row value, not column-by-column.
 *
 * SCAN_PAGE is 1000 in the adapter, so these fixtures cross it on purpose.
 */

import { SQL } from 'bun';
import { afterAll, expect, setDefaultTimeout, test } from 'bun:test';
import { createEngine } from '../../index';
import { postgresSource, postgresStore } from '../index';
import { resetSchema, testDatabaseUrl } from './helpers';

setDefaultTimeout(120_000);

const byKey = (rows: Array<{ groupKey: string; values: Record<string, unknown> }>) =>
  Object.fromEntries(rows.map((row) => [row.groupKey, row.values]));

const url = await testDatabaseUrl('ripply_test_scan');
const sql = new SQL({ url, max: 4 });
afterAll(() => sql.close());

const ROWS = 2_500;

test('a bigint pk pages across scan boundaries without losing or repeating rows', async () => {
  await resetSchema(sql);
  await sql.unsafe(`
    CREATE TABLE plays (
      id        BIGINT PRIMARY KEY,
      player_id TEXT NOT NULL,
      score     INTEGER NOT NULL
    )`);
  // ids 1..2500: as text these interleave badly (1000 < 999), which is the
  // whole point — the keyset cast has to restore numeric order.
  await sql.unsafe(
    `INSERT INTO plays (id, player_id, score)
     SELECT g, 'p' || (g % 7), g % 100 FROM generate_series(1, ${ROWS}) AS g`,
  );

  const engine = createEngine({
    source: postgresSource({ sql, collections: { plays: { pk: ['id'] } } }),
    store: postgresStore({ sql }),
  });
  engine.defineIndex('play_stats', {
    collection: 'plays',
    map: (play) => ({ player_id: play.player_id as string, score: play.score as number }),
    reduce: {
      groupBy: ['player_id'],
      // a non-linear aggregate on purpose: the streaming fold's forward
      // pass is what makes this rebuildable without holding the entries
      aggregate: {
        plays: 'count',
        total: { sum: 'score' },
        high: { max: 'score' },
        low: { min: 'score' },
      },
    },
  });

  await engine.start();

  const expected = (await sql.unsafe(
    `SELECT player_id, count(*)::int AS plays, sum(score)::int AS total,
            max(score)::int AS high, min(score)::int AS low
     FROM plays GROUP BY player_id ORDER BY player_id`,
  )) as Array<Record<string, string | number>>;

  const actual = await engine.store.transaction((tx) => tx.allReduced('play_stats'));
  expect(actual).toHaveLength(expected.length);

  const byGroup = new Map(actual.map((row) => [row.group.player_id as string, row]));
  let totalEntries = 0;
  for (const row of expected) {
    const got = byGroup.get(row.player_id as string);
    expect(got).toBeDefined();
    expect(got!.values).toEqual({
      plays: Number(row.plays),
      total: Number(row.total),
      high: Number(row.high),
      low: Number(row.low),
    });
    totalEntries += got!.entryCount;
  }
  // every source row contributed exactly once — no page-boundary drops,
  // and no double-scan (which the entries PK would have rejected anyway)
  expect(totalEntries).toBe(ROWS);

  expect((await engine.verify('play_stats')).ok).toBe(true);
});

test('a composite pk pages as a row value', async () => {
  await resetSchema(sql);
  await sql.unsafe(`
    CREATE TABLE game_play (
      game_id BIGINT NOT NULL,
      turn    INTEGER NOT NULL,
      word    TEXT NOT NULL,
      points  INTEGER NOT NULL,
      PRIMARY KEY (game_id, turn)
    )`);
  // 250 games x 10 turns = 2500 rows, so pages land mid-game and the
  // watermark has to carry both columns
  await sql.unsafe(
    `INSERT INTO game_play (game_id, turn, word, points)
     SELECT g, t, 'w' || (g % 13), (g * t) % 50
     FROM generate_series(1, 250) AS g, generate_series(1, 10) AS t`,
  );

  const engine = createEngine({
    source: postgresSource({
      sql,
      collections: { game_play: { pk: ['game_id', 'turn'] } },
    }),
    store: postgresStore({ sql }),
  });
  engine.defineIndex('word_stats', {
    collection: 'game_play',
    map: (play) => ({ word: play.word as string, points: play.points as number }),
    reduce: {
      groupBy: ['word'],
      aggregate: { plays: 'count', points: { sum: 'points' }, best: { max: 'points' } },
    },
  });

  await engine.start();

  const expected = (await sql.unsafe(
    `SELECT word, count(*)::int AS plays, sum(points)::int AS points, max(points)::int AS best
     FROM game_play GROUP BY word ORDER BY word`,
  )) as Array<Record<string, string | number>>;

  const actual = await engine.store.transaction((tx) => tx.allReduced('word_stats'));
  expect(actual).toHaveLength(expected.length);

  const byGroup = new Map(actual.map((row) => [row.group.word as string, row]));
  let totalEntries = 0;
  for (const row of expected) {
    const got = byGroup.get(row.word as string);
    expect(got).toBeDefined();
    expect(got!.values).toEqual({
      plays: Number(row.plays),
      points: Number(row.points),
      best: Number(row.best),
    });
    totalEntries += got!.entryCount;
  }
  expect(totalEntries).toBe(2_500);

  expect((await engine.verify('word_stats')).ok).toBe(true);
});

test('a small page size still converges — the rebuild commits in chunks', async () => {
  await resetSchema(sql);
  await sql.unsafe(`
    CREATE TABLE plays (
      id     BIGINT PRIMARY KEY,
      player TEXT NOT NULL,
      score  INTEGER NOT NULL
    )`);
  await sql.unsafe(
    `INSERT INTO plays (id, player, score)
     SELECT g, 'p' || (g % 5), g % 30 FROM generate_series(1, 300) AS g`,
  );

  const engine = createEngine({
    source: postgresSource({ sql, collections: { plays: { pk: ['id'] } } }),
    store: postgresStore({ sql }),
  });
  engine.defineIndex('stats', {
    collection: 'plays',
    map: (play) => ({ player: play.player as string, score: play.score as number }),
    reduce: {
      groupBy: ['player'],
      aggregate: { plays: 'count', high: { max: 'score' }, first_score: { first: 'score' } },
    },
  });

  await engine.start();
  const baseline = await engine.store.transaction((tx) => tx.allReduced('stats'));

  // 7 rows per transaction: ~43 entry commits instead of one
  await engine.rebuild('stats', { pageSize: 7 });
  const paged = await engine.store.transaction((tx) => tx.allReduced('stats'));

  expect(byKey(paged)).toEqual(byKey(baseline));
  expect((await engine.verify('stats')).ok).toBe(true);
});
