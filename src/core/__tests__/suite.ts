/**
 * The ten correctness invariants (DESIGN.md §8) as a reusable suite —
 * every adapter pair must pass the IDENTICAL tests (the adapter matrix).
 * Backends supply real writes (memory maps, SQL statements…); the suite
 * only speaks Source/Store through the engine.
 *
 * These tests ARE the spec — when design and test disagree, stop and
 * discuss. Invariant 9 (out-of-order commit) is Postgres-specific and
 * lives with the Phase 2 adapter.
 */

import { describe, expect, test } from 'bun:test';
import type {
  IndexDefinition,
  PkValue,
  Row,
  Source,
  Store,
  StoreTx,
} from '../../index';
import { createEngine } from '../../index';
import { int, mulberry32, oracleReduce, pick } from './helpers';

// ---------------------------------------------------------------------------
// Backend contract
// ---------------------------------------------------------------------------

/**
 * One isolated environment: a `work_orders` collection (pk: id) whose rows
 * may use the columns id/status/technician_id/revenue/tags/a/b/flip.
 * `tags` is an array; SQL backends may surface it as a JSON string (the
 * fixture maps normalize). `update` replaces the whole row.
 */
export interface SuiteBackend {
  source: Source;
  store: Store;
  insert(row: Row): void | Promise<void>;
  update(row: Row): void | Promise<void>;
  remove(id: string): void | Promise<void>;
  snapshot(): Array<{ pk: PkValue; row: Row }> | Promise<Array<{ pk: PkValue; row: Row }>>;
}

export type SuiteBackendFactory = () => SuiteBackend;

export const WORK_ORDER_COLUMNS = [
  'id',
  'status',
  'technician_id',
  'revenue',
  'tags',
  'a',
  'b',
  'flip',
] as const;

// ---------------------------------------------------------------------------
// Fixture: four index shapes covering linear, avg, non-linear
// (min/max/distinct/first), filtered maps (null), and multi-emit.
// ---------------------------------------------------------------------------

const STATUSES = ['pending', 'active', 'completed'] as const;
const TECHS = ['t1', 't2', 't3', 't4', 't5'] as const;
const TAGS = ['alpha', 'beta', 'gamma', 'delta', 'epsilon'] as const;

/** Backends may surface array columns as JSON text — normalize. */
const tagsOf = (value: unknown): string[] =>
  typeof value === 'string'
    ? (JSON.parse(value) as string[])
    : ((value as string[] | null | undefined) ?? []);

export const INDEXES: Record<string, IndexDefinition> = {
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
  // non-linear (min/max/distinct/first) mixed with a linear sum
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
        total: { sum: 'revenue' },
      },
    },
  },
  // multi-emit (one entry per tag; duplicate tags allowed on purpose)
  byTag: {
    collection: 'work_orders',
    map: (wo) => tagsOf(wo.tags).map((tag) => ({ tag, n: 1, revenue: wo.revenue })),
    reduce: {
      groupBy: ['tag'],
      aggregate: { count: { sum: 'n' }, revenue: 'sum' },
    },
  },
};

function randomWorkOrder(rng: () => number, id: string): Row {
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
  backend: SuiteBackend,
  defs: Record<string, IndexDefinition>,
) {
  const rows = await backend.snapshot();
  for (const [name, def] of Object.entries(defs)) {
    const expected = Object.fromEntries(
      [...oracleReduce(rows, def).entries()].map(([groupKey, group]) => [
        groupKey,
        { values: group.values, entryCount: group.entryCount },
      ]),
    );
    expect(await reducedState(backend.store, name)).toEqual(expected);
  }
}

// ---------------------------------------------------------------------------
// The suite
// ---------------------------------------------------------------------------

export function runInvariantSuite(
  backendName: string,
  createBackend: SuiteBackendFactory,
): void {
  function setup(
    defs: Record<string, IndexDefinition> = INDEXES,
    options: { batchSize?: number } = {},
  ) {
    const backend = createBackend();
    const engine = createEngine({
      source: backend.source,
      store: backend.store,
      ...options,
    });
    for (const [name, def] of Object.entries(defs)) engine.defineIndex(name, def);
    return { backend, engine };
  }

  describe(`invariants [${backendName}]`, () => {
    test('smoke — counts by status through insert/update/delete', async () => {
      const { backend, engine } = setup({ countByStatus: INDEXES.countByStatus! });

      await backend.insert({ id: 'a', status: 'pending' });
      await backend.insert({ id: 'b', status: 'pending' });
      await backend.insert({ id: 'c', status: 'completed' });
      await engine.drain();

      expect(await reducedState(backend.store, 'countByStatus')).toEqual({
        '{"status":"completed"}': { values: { count: 1 }, entryCount: 1 },
        '{"status":"pending"}': { values: { count: 2 }, entryCount: 2 },
      });

      await backend.update({ id: 'a', status: 'completed' });
      await backend.remove('b');
      await engine.drain();

      expect(await reducedState(backend.store, 'countByStatus')).toEqual({
        '{"status":"completed"}': { values: { count: 2 }, entryCount: 2 },
      });
    });

    describe('invariant 1 — incremental == full rebuild over random op sequences', () => {
      for (const seed of [11, 23, 37, 53, 71]) {
        test(`seed ${seed}`, async () => {
          const rng = mulberry32(seed);
          // small batch size on purpose: forces multi-batch drains
          const { backend, engine } = setup(INDEXES, { batchSize: 7 });

          let nextId = 1;
          const live: string[] = [];
          const rowsById = new Map<string, Row>();

          for (let i = 0; i < 250; i++) {
            const roll = rng();
            if (live.length === 0 || roll < 0.45) {
              const id = `wo_${nextId++}`;
              const row = randomWorkOrder(rng, id);
              await backend.insert(row);
              rowsById.set(id, row);
              live.push(id);
            } else if (roll < 0.8) {
              const id = pick(rng, live);
              // 15% of updates are no-ops (same row) — must apply a zero delta
              const row = rng() < 0.15 ? rowsById.get(id)! : randomWorkOrder(rng, id);
              await backend.update(row);
              rowsById.set(id, row);
            } else {
              const id = live.splice(int(rng, live.length), 1)[0]!;
              await backend.remove(id);
              rowsById.delete(id);
            }
            // interleave processing with writes
            if (rng() < 0.2) await engine.drain();
          }

          await engine.drain();
          await expectMatchesOracle(backend, INDEXES);

          // and the engine's own verify() agrees with itself
          for (const name of Object.keys(INDEXES)) {
            const report = await engine.verify(name);
            expect(report).toMatchObject({ ok: true });
          }
        });
      }
    });

    test('invariant 2 — replaying the same changes twice equals once', async () => {
      const { backend, engine } = setup();
      const rng = mulberry32(99);
      for (let i = 1; i <= 20; i++) {
        await backend.insert(randomWorkOrder(rng, `wo_${i}`));
      }
      await backend.update(randomWorkOrder(rng, 'wo_3'));
      await backend.remove('wo_7');
      await engine.drain();

      const before: Record<string, unknown> = {};
      for (const name of Object.keys(INDEXES)) {
        before[name] = await reducedState(backend.store, name);
      }

      // rewind every cursor to the very beginning and reprocess the entire feed
      for (const name of Object.keys(INDEXES)) {
        await backend.store.transaction(async (tx) => tx.setCursor(name, null));
      }
      await engine.drain();

      for (const name of Object.keys(INDEXES)) {
        expect(await reducedState(backend.store, name)).toEqual(before[name] as never);
      }
    });

    test('invariant 3 — crash between apply and cursor commit leaves no drift', async () => {
      const backend = createBackend();

      let crashesLeft = 1;
      const crashingStore: Store = {
        transaction: (fn) =>
          backend.store.transaction((tx) => {
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

      const engine = createEngine({ source: backend.source, store: crashingStore });
      engine.defineIndex('countByStatus', INDEXES.countByStatus!);

      await backend.insert({ id: 'a', status: 'pending' });
      await backend.insert({ id: 'b', status: 'completed' });

      // the crash propagates…
      await expect(engine.drain()).rejects.toThrow('simulated crash');

      // …and the transaction rolled back WHOLE: no partial apply, cursor unmoved
      expect(await reducedState(backend.store, 'countByStatus')).toEqual({});
      expect(
        await backend.store.transaction((tx) => tx.getCursor('countByStatus')),
      ).toBeNull();

      // reprocessing after the "restart" converges with zero drift
      await engine.drain();
      await expectMatchesOracle(backend, { countByStatus: INDEXES.countByStatus! });
    });

    test('invariant 4 — updates move rows between groups; empty groups are deleted', async () => {
      const { backend, engine } = setup({ countByStatus: INDEXES.countByStatus! });

      await backend.insert({ id: 'a', status: 'pending' });
      await backend.insert({ id: 'b', status: 'pending' });
      await engine.drain();

      await backend.update({ id: 'a', status: 'completed' });
      await engine.drain();
      expect(await reducedState(backend.store, 'countByStatus')).toEqual({
        '{"status":"completed"}': { values: { count: 1 }, entryCount: 1 },
        '{"status":"pending"}': { values: { count: 1 }, entryCount: 1 },
      });

      // the last pending row leaves: the group must be DELETED, not left at 0
      await backend.update({ id: 'b', status: 'completed' });
      await engine.drain();
      expect(await reducedState(backend.store, 'countByStatus')).toEqual({
        '{"status":"completed"}': { values: { count: 2 }, entryCount: 2 },
      });
    });

    test('invariant 5 — removing the current min/max re-reduces from entries', async () => {
      const { backend, engine } = setup({ statsByStatus: INDEXES.statsByStatus! });

      await backend.insert({ id: 'a', status: 'active', revenue: 3, technician_id: 't1' });
      await backend.insert({ id: 'b', status: 'active', revenue: 5, technician_id: 't2' });
      await backend.insert({ id: 'c', status: 'active', revenue: 9, technician_id: 't1' });
      await engine.drain();

      const key = '{"status":"active"}';
      let state = await reducedState(backend.store, 'statsByStatus');
      expect(state[key]!.values).toMatchObject({ lo: 3, hi: 9, total: 17, techs: ['t1', 't2'] });

      // delete the min — a reduced scalar alone could never recover lo=5
      await backend.remove('a');
      await engine.drain();
      state = await reducedState(backend.store, 'statsByStatus');
      expect(state[key]!.values).toMatchObject({ lo: 5, hi: 9, total: 14 });

      // delete the max too
      await backend.remove('c');
      await engine.drain();
      state = await reducedState(backend.store, 'statsByStatus');
      expect(state[key]!.values).toMatchObject({ lo: 5, hi: 5, total: 5, techs: ['t2'] });
    });

    test('invariant 6 — a map emitting fewer entries retracts exactly the difference', async () => {
      const { backend, engine } = setup({ byTag: INDEXES.byTag! });

      await backend.insert({ id: 'a', revenue: 100, tags: ['alpha', 'beta', 'gamma'] });
      await engine.drain();
      expect(Object.keys(await reducedState(backend.store, 'byTag'))).toHaveLength(3);

      await backend.update({ id: 'a', revenue: 100, tags: ['alpha', 'gamma'] });
      await engine.drain();

      const state = await reducedState(backend.store, 'byTag');
      expect(Object.keys(state).sort()).toEqual(['{"tag":"alpha"}', '{"tag":"gamma"}']);
      expect(state['{"tag":"alpha"}']).toEqual({
        values: { count: 1, revenue: 100 },
        entryCount: 1,
      });

      // the entries table holds exactly the row's current contribution
      const entries = await backend.store.transaction((tx) =>
        tx.readEntries('byTag', ['a']),
      );
      expect(entries.map((e) => e.values.tag)).toEqual(['alpha', 'gamma']);
    });

    test('invariant 7 — group objects differing in key order or undefined-ness collapse', async () => {
      const flipping: IndexDefinition = {
        collection: 'work_orders',
        // same logical group, different key insertion order per row
        map: (wo) =>
          wo.flip ? { b: wo.b, a: wo.a, n: 1 } : { a: wo.a, b: wo.b, n: 1 },
        reduce: { groupBy: ['a', 'b'], aggregate: { count: { sum: 'n' } } },
      };
      const { backend, engine } = setup({ flipping });

      await backend.insert({ id: '1', a: 'x', b: 'y', flip: false });
      await backend.insert({ id: '2', a: 'x', b: 'y', flip: true });
      // b absent/null must land in the SAME group either way
      await backend.insert({ id: '3', a: 'x', flip: false });
      await backend.insert({ id: '4', a: 'x', b: null, flip: true });
      await engine.drain();

      expect(await reducedState(backend.store, 'flipping')).toEqual({
        '{"a":"x","b":"y"}': { values: { count: 2 }, entryCount: 2 },
        '{"a":"x","b":null}': { values: { count: 2 }, entryCount: 2 },
      });
    });

    test('invariant 8 — deleting a source row removes all its contributions', async () => {
      const { backend, engine } = setup({
        countByStatus: INDEXES.countByStatus!,
        byTag: INDEXES.byTag!,
      });

      await backend.insert({ id: 'a', status: 'pending', tags: ['alpha', 'beta'] });
      await backend.insert({ id: 'b', status: 'pending', tags: ['alpha'] });
      await engine.drain();

      await backend.remove('a');
      await engine.drain();

      expect(await reducedState(backend.store, 'countByStatus')).toEqual({
        '{"status":"pending"}': { values: { count: 1 }, entryCount: 1 },
      });
      expect(await reducedState(backend.store, 'byTag')).toEqual({
        '{"tag":"alpha"}': { values: { count: 1, revenue: 0 }, entryCount: 1 },
      });

      await backend.remove('b');
      await engine.drain();
      expect(await reducedState(backend.store, 'countByStatus')).toEqual({});
      expect(
        await backend.store.transaction((tx) => tx.allEntries('countByStatus')),
      ).toEqual([]);
    });

    test('invariant 10 — changed map rebuilds at start(); unchanged map does not', async () => {
      const backend = createBackend();

      // rows exist BEFORE Ripply ever sees the collection
      await backend.insert({ id: 'a', status: 'pending' });
      await backend.insert({ id: 'b', status: 'pending' });
      await backend.insert({ id: 'c', status: 'completed' });

      let scans = 0;
      const originalScan = backend.source.scan.bind(backend.source);
      backend.source.scan = (collection, onRow) => {
        scans++;
        return originalScan(collection, onRow);
      };

      const v1: IndexDefinition = {
        collection: 'work_orders',
        map: (wo) => ({ status: wo.status, n: 1 }),
        reduce: { groupBy: ['status'], aggregate: { count: { sum: 'n' } } },
      };

      // fresh index (no meta) → initial build via scan, picks up pre-existing rows
      const engine1 = createEngine({ source: backend.source, store: backend.store });
      engine1.defineIndex('idx', v1);
      await engine1.start();
      expect(scans).toBe(1);
      expect(await reducedState(backend.store, 'idx')).toEqual({
        '{"status":"completed"}': { values: { count: 1 }, entryCount: 1 },
        '{"status":"pending"}': { values: { count: 2 }, entryCount: 2 },
      });

      // live changes after start are incremental
      await backend.insert({ id: 'd', status: 'pending' });
      await engine1.drain();
      expect((await reducedState(backend.store, 'idx'))['{"status":"pending"}']).toEqual({
        values: { count: 3 },
        entryCount: 3,
      });

      // same definition, new process ("restart") → NO rebuild
      const engine2 = createEngine({ source: backend.source, store: backend.store });
      engine2.defineIndex('idx', v1);
      await engine2.start();
      expect(scans).toBe(1);

      // changed map → version mismatch → automatic rebuild, stale index never served
      const engine3 = createEngine({ source: backend.source, store: backend.store });
      engine3.defineIndex('idx', {
        ...v1,
        map: (wo) => ({ status: wo.status, n: 2 }), // counts double now
      });
      await engine3.start();
      expect(scans).toBe(2);
      expect(await reducedState(backend.store, 'idx')).toEqual({
        '{"status":"completed"}': { values: { count: 2 }, entryCount: 1 },
        '{"status":"pending"}': { values: { count: 6 }, entryCount: 3 },
      });
    });

    test('rebuild converges to the same state; verify() detects corruption', async () => {
      const { backend, engine } = setup();
      const rng = mulberry32(7);
      for (let i = 1; i <= 15; i++) {
        await backend.insert(randomWorkOrder(rng, `wo_${i}`));
      }
      await engine.drain();

      await engine.rebuild('statsByStatus');
      await expectMatchesOracle(backend, INDEXES);

      // corrupt one reduced row behind the engine's back
      const victim = await backend.store.transaction(async (tx) => {
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
  });
}
