/**
 * Invariant 9 (DESIGN.md §8) — out-of-order commit convergence. The
 * Postgres-only invariant, and the reason the adapter's cursor is a
 * snapshot window instead of a seq.
 *
 * BIGSERIAL seq is assigned at INSERT time; commits happen in any order.
 * A naive `seq > cursor` poll that observes seq 2 while seq 1's
 * transaction is still open would advance past 1 and NEVER see it. The
 * deterministic test constructs exactly that interleaving with a held-open
 * transaction; the stress test lets 8 concurrent writers race with a
 * concurrent drainer and demands full convergence with an independent
 * oracle at the end.
 */

import { SQL } from 'bun';
import { afterAll, expect, test, setDefaultTimeout } from 'bun:test';
import { int, mulberry32, oracleReduce, pick } from '../../core/__tests__/helpers';
import { createEngine } from '../../index';
import type { IndexDefinition, Store } from '../../core/types';
import { postgresSource, postgresStore } from '../index';
import { resetSchema, testDatabaseUrl } from './helpers';

setDefaultTimeout(120_000);

const url = await testDatabaseUrl('ripply_test_ordering');
const sql = new SQL({ url, max: 12 });
afterAll(() => sql.close());

const countByStatus: IndexDefinition = {
  collection: 'games',
  map: (game) => ({ status: game.status, n: 1 }),
  reduce: { groupBy: ['status'], aggregate: { count: { sum: 'n' } } },
};

const statsByStatus: IndexDefinition = {
  collection: 'games',
  map: (game) => ({ status: game.status, score: game.score }),
  reduce: {
    groupBy: ['status'],
    aggregate: { hi: { max: 'score' }, total: { sum: 'score' }, n: 'count' },
  },
};

async function reducedState(store: Store, name: string) {
  const rows = await store.transaction((tx) => tx.allReduced(name));
  return Object.fromEntries(rows.map((row) => [row.groupKey, row.values]));
}

async function setup(defs: Record<string, IndexDefinition>) {
  await resetSchema(sql);
  await sql.unsafe(`CREATE TABLE games (id TEXT PRIMARY KEY, status TEXT, score INTEGER)`);
  const source = postgresSource({ sql, collections: { games: { pk: ['id'] } } });
  const store = postgresStore({ sql });
  const engine = createEngine({ source, store, batchSize: 7 });
  for (const [name, def] of Object.entries(defs)) engine.defineIndex(name, def);
  await engine.start();
  return { source, store, engine };
}

test('invariant 9 — deterministic: an early-seq, late-commit transaction is never skipped', async () => {
  const { source, store, engine } = await setup({ countByStatus });

  // Transaction A allocates the EARLIER changelog seq… and stays open.
  const held = await sql.reserve();
  await held`BEGIN`;
  await held`INSERT INTO games (id, status) VALUES ('early_seq_late_commit', 'pending')`;

  // Transaction B allocates the LATER seq and commits immediately.
  await sql`INSERT INTO games (id, status) VALUES ('late_seq_early_commit', 'completed')`;

  // Prove the trap is real: the uncommitted change holds the smaller seq.
  const seqs = (await sql.unsafe(
    `SELECT seq::float8 AS seq, pk::text AS pk FROM _ripply_changelog ORDER BY seq`,
  )) as Array<{ seq: number; pk: string }>;
  expect(seqs).toHaveLength(1); // A's row isn't even visible yet
  expect(JSON.parse(seqs[0]!.pk)).toEqual(['late_seq_early_commit']);

  // Drain now: only B is visible, and the cursor advances past B's seq.
  await engine.drain();
  expect(await reducedState(store, 'countByStatus')).toEqual({
    '{"status":"completed"}': { count: 1 },
  });
  const cursorAfterB = await store.transaction((tx) => tx.getCursor('countByStatus'));
  expect(cursorAfterB).not.toBeNull(); // a naive seq cursor is now past seq 1

  // A finally commits — with a seq BELOW everything already applied.
  await held`COMMIT`;
  held.release();

  // The frozen-window cursor picks it up in the next window. No skip.
  await engine.drain();
  expect(await reducedState(store, 'countByStatus')).toEqual({
    '{"status":"completed"}': { count: 1 },
    '{"status":"pending"}': { count: 1 },
  });
  expect(await engine.verify('countByStatus')).toMatchObject({ ok: true });

  // And once both indexes have applied everything, prune drains the outbox.
  const cursor = await store.transaction((tx) => tx.getCursor('countByStatus'));
  const pruned = await source.prune('games', [cursor]);
  expect(pruned).toBe(2);
  const left = (await sql.unsafe(`SELECT count(*)::int AS n FROM _ripply_changelog`)) as Array<{
    n: number;
  }>;
  expect(left[0]!.n).toBe(0);
});

test('invariant 9 — stress: 8 concurrent out-of-order committers + live drainer converge', async () => {
  const { source, store, engine } = await setup({ countByStatus, statsByStatus });
  const STATUSES = ['pending', 'active', 'completed'] as const;

  let writing = true;
  const drainer = (async () => {
    while (writing) {
      await engine.drain();
      await Bun.sleep(5);
    }
  })();

  const writers = Array.from({ length: 8 }, (_, w) =>
    (async () => {
      const rng = mulberry32(1000 + w);
      for (let i = 0; i < 15; i++) {
        const id = `g_${w}_${i}`;
        // hold each write's transaction open a random beat → commits land
        // in a different order than their changelog seqs
        await sql.begin(async (tx) => {
          await tx`INSERT INTO games (id, status, score)
                   VALUES (${id}, ${pick(rng, STATUSES)}, ${int(rng, 500)})`;
          await Bun.sleep(int(rng, 8));
        });
        if (rng() < 0.4) {
          await sql`UPDATE games SET status = ${pick(rng, STATUSES)}, score = ${int(rng, 500)}
                    WHERE id = ${id}`;
        }
        if (rng() < 0.15) {
          await sql`DELETE FROM games WHERE id = ${id}`;
        }
      }
    })(),
  );

  await Promise.all(writers);
  writing = false;
  await drainer;
  await engine.drain();

  // converge with an INDEPENDENT oracle over the real table state
  const rows = (
    (await sql.unsafe(`SELECT * FROM games ORDER BY id`)) as Array<Record<string, unknown>>
  ).map((row) => ({ pk: [row.id as string | number | boolean | null], row }));
  for (const [name, def] of Object.entries({ countByStatus, statsByStatus })) {
    const expected = Object.fromEntries(
      [...oracleReduce(rows, def).entries()].map(([groupKey, group]) => [groupKey, group.values]),
    );
    expect(await reducedState(store, name)).toEqual(expected);
    expect(await engine.verify(name)).toMatchObject({ ok: true });
  }

  // every change applied by both indexes → prune empties the outbox
  const cursors = await store.transaction(async (tx) => [
    await tx.getCursor('countByStatus'),
    await tx.getCursor('statsByStatus'),
  ]);
  await source.prune('games', cursors);
  const left = (await sql.unsafe(`SELECT count(*)::int AS n FROM _ripply_changelog`)) as Array<{
    n: number;
  }>;
  expect(left[0]!.n).toBe(0);
});
