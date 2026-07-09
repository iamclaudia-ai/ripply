/**
 * The ten correctness invariants (DESIGN.md §8), written against the
 * in-memory reference adapters. These tests ARE the spec — when design and
 * test disagree, stop and discuss.
 *
 * Invariant 9 (out-of-order commit) is a Postgres concern and lives with the
 * Phase 2 adapter.
 */

import { describe, expect, test } from 'bun:test';
import type { IndexDefinition, Store, StoreTx } from '../../index';
import { createEngine } from '../../index';
import { memorySource, memoryStore, type MemorySource, type MemoryStore } from '../../memory/index';
import { int, mulberry32, oracleReduce, pick } from './helpers';

// ---------------------------------------------------------------------------
// Fixture: a work_orders collection and four index shapes that between them
// cover linear, avg, non-linear (min/max/distinct/first), filtered maps
// (null), and multi-emit maps.
// ---------------------------------------------------------------------------

const STATUSES = ['pending', 'active', 'completed'] as const;
const TECHS = ['t1', 't2', 't3', 't4', 't5'] as const;
const TAGS = ['alpha', 'beta', 'gamma', 'delta', 'epsilon'] as const;

interface WorkOrder {
  id: string;
  status: string;
  technician_id: string;
  revenue: number | null;
  tags: string[];
  [key: string]: unknown;
}

const INDEXES: Record<string, IndexDefinition> = {
  // linear sum, group transitions
  countByStatus: {
    collection: 'work_orders',
    map: (wo) => ({ status: wo.status, n: 1 }),
    reduce: { groupBy: ['status'], aggregate: { count: { sum: 'n' } } },
  },
  // filtered map (null) + sum/count/avg
  revenueByTech: {
    collection: 'work_orders',
    map: (wo) =>
      wo.status === 'completed'
        ? { techId: wo.technician_id, revenue: wo.revenue }
        : null,
    reduce: {
      groupBy: ['techId'],
      aggregate: {
        revenue: 'sum',
        jobs: 'count',
        avgRevenue: { avg: 'revenue' },
      },
    },
  },
  // non-linear: min/max/distinct/first
  statsByStatus: {
    collection: 'work_orders',
    map: (wo) => ({
      status: wo.status,
      revenue: wo.revenue,
      techId: wo.technician_id,
    }),
    reduce: {
      groupBy: ['status'],
      aggregate: {
        lo: { min: 'revenue' },
        hi: { max: 'revenue' },
        techs: { distinct: 'techId' },
        firstRevenue: { first: 'revenue' },
      },
    },
  },
  // multi-emit (one entry per tag; duplicate tags allowed on purpose)
  byTag: {
    collection: 'work_orders',
    map: (wo) =>
      (wo.tags as string[]).map((tag) => ({ tag, n: 1, revenue: wo.revenue })),
    reduce: {
      groupBy: ['tag'],
      aggregate: { count: { sum: 'n' }, revenue: 'sum' },
    },
  },
};

function setup(
  defs: Record<string, IndexDefinition> = INDEXES,
  options: { batchSize?: number } = {},
) {
  const source = memorySource({ collections: { work_orders: { pk: ['id'] } } });
  const store = memoryStore();
  const engine = createEngine({ source, store, ...options });
  for (const [name, def] of Object.entries(defs)) engine.defineIndex(name, def);
  return { source, store, engine };
}

function randomWorkOrder(rng: () => number, id: string): WorkOrder {
  return {
    id,
    status: pick(rng, STATUSES),
    technician_id: pick(rng, TECHS),
    revenue: rng() < 0.1 ? null : int(rng, 1000),
    // duplicates allowed — stresses multi-emit ord handling
    tags: Array.from({ length: int(rng, 4) }, () => pick(rng, TAGS)),
  };
}

async function reducedState(store: Store, name: string) {
  const rows = await store.transaction((tx: StoreTx) => tx.allReduced(name));
  return Object.fromEntries(
    rows.map((r) => [r.groupKey, { values: r.values, entryCount: r.entryCount }]),
  );
}

async function expectMatchesOracle(
  source: MemorySource,
  store: MemoryStore,
  defs: Record<string, IndexDefinition>,
) {
  const rows = source.snapshot('work_orders');
  for (const [name, def] of Object.entries(defs)) {
    const expected = Object.fromEntries(
      [...oracleReduce(rows, def).entries()].map(([groupKey, group]) => [
        groupKey,
        { values: group.values, entryCount: group.entryCount },
      ]),
    );
    expect(await reducedState(store, name)).toEqual(expected);
  }
}

// ---------------------------------------------------------------------------
// Smoke test — a deterministic happy path, useful when the king test breaks
// ---------------------------------------------------------------------------

test('smoke — counts by status through insert/update/delete', async () => {
  const { source, store, engine } = setup({ countByStatus: INDEXES.countByStatus! });

  source.insert('work_orders', { id: 'a', status: 'pending' });
  source.insert('work_orders', { id: 'b', status: 'pending' });
  source.insert('work_orders', { id: 'c', status: 'completed' });
  await engine.drain();

  expect(await reducedState(store, 'countByStatus')).toEqual({
    '{"status":"completed"}': { values: { count: 1 }, entryCount: 1 },
    '{"status":"pending"}': { values: { count: 2 }, entryCount: 2 },
  });

  source.update('work_orders', { id: 'a', status: 'completed' });
  source.delete('work_orders', ['b']);
  await engine.drain();

  expect(await reducedState(store, 'countByStatus')).toEqual({
    '{"status":"completed"}': { values: { count: 2 }, entryCount: 2 },
  });
});

// ---------------------------------------------------------------------------
// Invariant 1 — THE KING TEST
// ---------------------------------------------------------------------------

describe('invariant 1 — incremental == full rebuild over random op sequences', () => {
  for (const seed of [11, 23, 37, 53, 71]) {
    test(`seed ${seed}`, async () => {
      const rng = mulberry32(seed);
      // small batch size on purpose: forces multi-batch drains
      const { source, store, engine } = setup(INDEXES, { batchSize: 7 });

      let nextId = 1;
      const live: string[] = [];
      const rowsById = new Map<string, WorkOrder>();

      for (let i = 0; i < 250; i++) {
        const roll = rng();
        if (live.length === 0 || roll < 0.45) {
          const id = `wo_${nextId++}`;
          const row = randomWorkOrder(rng, id);
          source.insert('work_orders', row);
          rowsById.set(id, row);
          live.push(id);
        } else if (roll < 0.8) {
          const id = pick(rng, live);
          // 15% of updates are no-ops (same row) — must apply a zero delta
          const row = rng() < 0.15 ? rowsById.get(id)! : randomWorkOrder(rng, id);
          source.update('work_orders', row);
          rowsById.set(id, row);
        } else {
          const id = live.splice(int(rng, live.length), 1)[0]!;
          source.delete('work_orders', [id]);
          rowsById.delete(id);
        }
        // interleave processing with writes
        if (rng() < 0.2) await engine.drain();
      }

      await engine.drain();
      await expectMatchesOracle(source, store, INDEXES);

      // and the engine's own verify() agrees with itself
      for (const name of Object.keys(INDEXES)) {
        const report = await engine.verify(name);
        expect(report).toMatchObject({ ok: true });
      }
    });
  }
});

// ---------------------------------------------------------------------------
// Invariant 2 — idempotent replay
// ---------------------------------------------------------------------------

test('invariant 2 — replaying the same changes twice equals once', async () => {
  const { source, store, engine } = setup();
  const rng = mulberry32(99);
  for (let i = 1; i <= 20; i++) {
    source.insert('work_orders', randomWorkOrder(rng, `wo_${i}`));
  }
  source.update('work_orders', randomWorkOrder(rng, 'wo_3'));
  source.delete('work_orders', ['wo_7']);
  await engine.drain();

  const before: Record<string, unknown> = {};
  for (const name of Object.keys(INDEXES)) {
    before[name] = await reducedState(store, name);
  }

  // rewind every cursor to the very beginning and reprocess the entire feed
  for (const name of Object.keys(INDEXES)) {
    await store.transaction(async (tx) => tx.setCursor(name, null));
  }
  await engine.drain();

  for (const name of Object.keys(INDEXES)) {
    expect(await reducedState(store, name)).toEqual(before[name] as never);
  }
});

// ---------------------------------------------------------------------------
// Invariant 3 — crash safety
// ---------------------------------------------------------------------------

test('invariant 3 — crash between apply and cursor commit leaves no drift', async () => {
  const source = memorySource({ collections: { work_orders: { pk: ['id'] } } });
  const store = memoryStore();

  let crashesLeft = 1;
  const crashingStore: Store = {
    transaction: (fn) =>
      store.transaction((tx) => {
        const wrapped = new Proxy(tx, {
          get(target, prop, receiver) {
            if (prop === 'setCursor' && crashesLeft > 0) {
              return async () => {
                crashesLeft--;
                throw new Error('simulated crash before cursor commit');
              };
            }
            const value = Reflect.get(target, prop, receiver);
            return typeof value === 'function' ? value.bind(target) : value;
          },
        });
        return fn(wrapped);
      }),
  };

  const engine = createEngine({ source, store: crashingStore });
  engine.defineIndex('countByStatus', INDEXES.countByStatus!);

  source.insert('work_orders', { id: 'a', status: 'pending' });
  source.insert('work_orders', { id: 'b', status: 'completed' });

  // the crash propagates…
  await expect(engine.drain()).rejects.toThrow('simulated crash');

  // …and the transaction rolled back WHOLE: no partial apply, cursor unmoved
  expect(await reducedState(store, 'countByStatus')).toEqual({});
  expect(await store.transaction((tx) => tx.getCursor('countByStatus'))).toBeNull();

  // reprocessing after the "restart" converges with zero drift
  await engine.drain();
  await expectMatchesOracle(source, store, { countByStatus: INDEXES.countByStatus! });
});

// ---------------------------------------------------------------------------
// Invariant 4 — group transitions + zero-group deletion
// ---------------------------------------------------------------------------

test('invariant 4 — updates move rows between groups; empty groups are deleted', async () => {
  const { source, store, engine } = setup({ countByStatus: INDEXES.countByStatus! });

  source.insert('work_orders', { id: 'a', status: 'pending' });
  source.insert('work_orders', { id: 'b', status: 'pending' });
  await engine.drain();

  source.update('work_orders', { id: 'a', status: 'completed' });
  await engine.drain();
  expect(await reducedState(store, 'countByStatus')).toEqual({
    '{"status":"completed"}': { values: { count: 1 }, entryCount: 1 },
    '{"status":"pending"}': { values: { count: 1 }, entryCount: 1 },
  });

  // the last pending row leaves: the group must be DELETED, not left at 0
  source.update('work_orders', { id: 'b', status: 'completed' });
  await engine.drain();
  expect(await reducedState(store, 'countByStatus')).toEqual({
    '{"status":"completed"}': { values: { count: 2 }, entryCount: 2 },
  });
});

// ---------------------------------------------------------------------------
// Invariant 5 — non-linear re-reduce (remove the min)
// ---------------------------------------------------------------------------

test('invariant 5 — removing the current min/max re-reduces from entries', async () => {
  const { source, store, engine } = setup({ statsByStatus: INDEXES.statsByStatus! });

  source.insert('work_orders', { id: 'a', status: 'active', revenue: 3, technician_id: 't1' });
  source.insert('work_orders', { id: 'b', status: 'active', revenue: 5, technician_id: 't2' });
  source.insert('work_orders', { id: 'c', status: 'active', revenue: 9, technician_id: 't1' });
  await engine.drain();

  const key = '{"status":"active"}';
  let state = await reducedState(store, 'statsByStatus');
  expect(state[key]!.values).toMatchObject({ lo: 3, hi: 9, techs: ['t1', 't2'] });

  // delete the min — a reduced scalar alone could never recover lo=5
  source.delete('work_orders', ['a']);
  await engine.drain();
  state = await reducedState(store, 'statsByStatus');
  expect(state[key]!.values).toMatchObject({ lo: 5, hi: 9 });

  // delete the max too
  source.delete('work_orders', ['c']);
  await engine.drain();
  state = await reducedState(store, 'statsByStatus');
  expect(state[key]!.values).toMatchObject({ lo: 5, hi: 5, techs: ['t2'] });
});

// ---------------------------------------------------------------------------
// Invariant 6 — multi-emit retraction (k entries → k−1)
// ---------------------------------------------------------------------------

test('invariant 6 — a map emitting fewer entries retracts exactly the difference', async () => {
  const { source, store, engine } = setup({ byTag: INDEXES.byTag! });

  source.insert('work_orders', {
    id: 'a',
    revenue: 100,
    tags: ['alpha', 'beta', 'gamma'],
  });
  await engine.drain();
  expect(Object.keys(await reducedState(store, 'byTag'))).toHaveLength(3);

  source.update('work_orders', { id: 'a', revenue: 100, tags: ['alpha', 'gamma'] });
  await engine.drain();

  const state = await reducedState(store, 'byTag');
  expect(Object.keys(state).sort()).toEqual(['{"tag":"alpha"}', '{"tag":"gamma"}']);
  expect(state['{"tag":"alpha"}']).toEqual({
    values: { count: 1, revenue: 100 },
    entryCount: 1,
  });

  // the entries table holds exactly the row's current contribution
  const entries = await store.transaction((tx) => tx.readEntries('byTag', ['a']));
  expect(entries.map((e) => e.values.tag)).toEqual(['alpha', 'gamma']);
});

// ---------------------------------------------------------------------------
// Invariant 7 — canonical group keys
// ---------------------------------------------------------------------------

test('invariant 7 — group objects differing in key order or undefined-ness collapse', async () => {
  const flipping: IndexDefinition = {
    collection: 'work_orders',
    // same logical group, different key insertion order per row
    map: (wo) =>
      wo.flip ? { b: wo.b, a: wo.a, n: 1 } : { a: wo.a, b: wo.b, n: 1 },
    reduce: { groupBy: ['a', 'b'], aggregate: { count: { sum: 'n' } } },
  };
  const { source, store, engine } = setup({ flipping });

  source.insert('work_orders', { id: '1', a: 'x', b: 'y', flip: false });
  source.insert('work_orders', { id: '2', a: 'x', b: 'y', flip: true });
  // b absent (undefined) and b:null must land in the SAME group
  source.insert('work_orders', { id: '3', a: 'x', flip: false });
  source.insert('work_orders', { id: '4', a: 'x', b: null, flip: true });
  await engine.drain();

  expect(await reducedState(store, 'flipping')).toEqual({
    '{"a":"x","b":"y"}': { values: { count: 2 }, entryCount: 2 },
    '{"a":"x","b":null}': { values: { count: 2 }, entryCount: 2 },
  });
});

// ---------------------------------------------------------------------------
// Invariant 8 — delete retraction
// ---------------------------------------------------------------------------

test('invariant 8 — deleting a source row removes all its contributions', async () => {
  const { source, store, engine } = setup({
    countByStatus: INDEXES.countByStatus!,
    byTag: INDEXES.byTag!,
  });

  source.insert('work_orders', { id: 'a', status: 'pending', tags: ['alpha', 'beta'] });
  source.insert('work_orders', { id: 'b', status: 'pending', tags: ['alpha'] });
  await engine.drain();

  source.delete('work_orders', ['a']);
  await engine.drain();

  expect(await reducedState(store, 'countByStatus')).toEqual({
    '{"status":"pending"}': { values: { count: 1 }, entryCount: 1 },
  });
  expect(await reducedState(store, 'byTag')).toEqual({
    '{"tag":"alpha"}': { values: { count: 1, revenue: 0 }, entryCount: 1 },
  });

  source.delete('work_orders', ['b']);
  await engine.drain();
  expect(await reducedState(store, 'countByStatus')).toEqual({});
  expect(await store.transaction((tx) => tx.allEntries('countByStatus'))).toEqual([]);
});

// ---------------------------------------------------------------------------
// Invariant 10 — map versioning
// ---------------------------------------------------------------------------

test('invariant 10 — changed map rebuilds at start(); unchanged map does not', async () => {
  const source = memorySource({ collections: { work_orders: { pk: ['id'] } } });
  const store = memoryStore();

  // rows exist BEFORE Ripply ever sees the collection
  source.insert('work_orders', { id: 'a', status: 'pending' });
  source.insert('work_orders', { id: 'b', status: 'pending' });
  source.insert('work_orders', { id: 'c', status: 'completed' });

  let scans = 0;
  const originalScan = source.scan.bind(source);
  source.scan = (collection, onRow) => {
    scans++;
    return originalScan(collection, onRow);
  };

  const v1: IndexDefinition = {
    collection: 'work_orders',
    map: (wo) => ({ status: wo.status, n: 1 }),
    reduce: { groupBy: ['status'], aggregate: { count: { sum: 'n' } } },
  };

  // fresh index (no meta) → initial build via scan, picks up pre-existing rows
  const engine1 = createEngine({ source, store });
  engine1.defineIndex('idx', v1);
  await engine1.start();
  expect(scans).toBe(1);
  expect(await reducedState(store, 'idx')).toEqual({
    '{"status":"completed"}': { values: { count: 1 }, entryCount: 1 },
    '{"status":"pending"}': { values: { count: 2 }, entryCount: 2 },
  });

  // live changes after start are incremental
  source.insert('work_orders', { id: 'd', status: 'pending' });
  await engine1.drain();
  expect((await reducedState(store, 'idx'))['{"status":"pending"}']).toEqual({
    values: { count: 3 },
    entryCount: 3,
  });

  // same definition, new process ("restart") → NO rebuild
  const engine2 = createEngine({ source, store });
  engine2.defineIndex('idx', v1);
  await engine2.start();
  expect(scans).toBe(1);

  // changed map → version mismatch → automatic rebuild, stale index never served
  const engine3 = createEngine({ source, store });
  engine3.defineIndex('idx', {
    ...v1,
    map: (wo) => ({ status: wo.status, n: 2 }), // counts double now
  });
  await engine3.start();
  expect(scans).toBe(2);
  expect(await reducedState(store, 'idx')).toEqual({
    '{"status":"completed"}': { values: { count: 2 }, entryCount: 1 },
    '{"status":"pending"}': { values: { count: 6 }, entryCount: 3 },
  });
});

// ---------------------------------------------------------------------------
// rebuild() + verify() — §7 maintenance surface
// ---------------------------------------------------------------------------

test('rebuild converges to the same state; verify() detects corruption', async () => {
  const { source, store, engine } = setup();
  const rng = mulberry32(7);
  for (let i = 1; i <= 15; i++) {
    source.insert('work_orders', randomWorkOrder(rng, `wo_${i}`));
  }
  await engine.drain();

  await engine.rebuild('statsByStatus');
  await expectMatchesOracle(source, store, INDEXES);

  // corrupt one reduced row behind the engine's back
  const victim = await store.transaction(async (tx) => {
    const rows = await tx.allReduced('countByStatus');
    const row = rows[0]!;
    row.values.count = 999_999;
    await tx.putReduced('countByStatus', row);
    return row.groupKey;
  });

  const report = await engine.verify('countByStatus');
  expect(report.ok).toBe(false);
  expect(report.mismatchedGroups).toContain(victim);

  // rebuild repairs it
  await engine.rebuild('countByStatus');
  expect((await engine.verify('countByStatus')).ok).toBe(true);
});
