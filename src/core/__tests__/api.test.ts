/**
 * The public API surface (DESIGN.md §6) — Phase 0 exit criterion:
 * the documented API compiles against the real types and behaves.
 */

import { expect, test } from 'bun:test';
import { createRipply } from '../../index';
import { memorySource, memoryStore } from '../../memory/index';
import { eventually } from './helpers';

function setup() {
  const source = memorySource({ collections: { work_orders: { pk: ['id'] } } });
  const store = memoryStore();
  const ripply = createRipply({ source, store, pollInterval: 25 });
  return { source, store, ripply };
}

test('§6 end-to-end: define → start → live queries → drill-down → maintenance', async () => {
  const { source, ripply } = setup();

  const countByStatus = ripply.defineIndex('countByStatus', {
    collection: 'work_orders',
    map: (wo) => ({ status: wo.status, count: 1 }),
    reduce: { groupBy: ['status'], aggregate: { count: 'sum' } },
  });

  const revenueByTech = ripply.defineIndex('revenueByTech', {
    collection: 'work_orders',
    map: (wo) =>
      wo.status === 'completed'
        ? { techId: wo.technician_id, revenue: wo.revenue as number, count: 1 }
        : null,
    reduce: {
      groupBy: ['techId'],
      aggregate: {
        revenue: 'sum',
        count: 'sum',
        top: { max: 'revenue' },
        avgRevenue: { avg: 'revenue' },
      },
    },
  });

  // rows that exist BEFORE Ripply ever starts are picked up by the initial build
  source.insert('work_orders', {
    id: '1',
    status: 'pending',
    technician_id: 't1',
    revenue: 50,
  });

  await ripply.start();
  expect(await countByStatus.where({ status: 'pending' }).value('count')).toBe(1);

  // live writes flow through wakeups — NO manual drain
  source.insert('work_orders', {
    id: '2',
    status: 'completed',
    technician_id: 't1',
    revenue: 300,
  });
  source.insert('work_orders', {
    id: '3',
    status: 'completed',
    technician_id: 't1',
    revenue: 100,
  });

  await eventually(async () => {
    expect(await revenueByTech.where({ techId: 't1' }).value('revenue')).toBe(400);
  });

  // one(): group fields + derived aggregates (avg divided at query time)
  expect(await revenueByTech.where({ techId: 't1' }).one()).toEqual({
    techId: 't1',
    revenue: 400,
    count: 2,
    top: 300,
    avgRevenue: 200,
  });

  // drill-down 🤯 — not just "400", but WHICH rows produced it
  const entries = await revenueByTech.where({ techId: 't1' }).entries();
  expect(entries.map((e) => e.pk).sort()).toEqual([['2'], ['3']]);

  // all(): one object per group
  expect(await countByStatus.all()).toEqual([
    { status: 'completed', count: 2 },
    { status: 'pending', count: 1 },
  ]);

  // updates retract: tech's revenue drops when a job un-completes
  source.update('work_orders', {
    id: '2',
    status: 'pending',
    technician_id: 't1',
    revenue: 300,
  });
  await eventually(async () => {
    expect(await revenueByTech.where({ techId: 't1' }).one()).toEqual({
      techId: 't1',
      revenue: 100,
      count: 1,
      top: 100,
      avgRevenue: 100,
    });
  });

  // maintenance passthroughs share the work queue (no tx collisions)
  await ripply.rebuild('revenueByTech');
  expect((await ripply.verify('revenueByTech')).ok).toBe(true);
  expect((await ripply.verify('countByStatus')).ok).toBe(true);

  await ripply.stop();
});

test('query semantics: one() null vs ambiguous, value() undefined, ripply.index()', async () => {
  const { source, ripply } = setup();

  ripply.defineIndex('countByStatus', {
    collection: 'work_orders',
    map: (wo) => ({ status: wo.status, count: 1 }),
    reduce: { groupBy: ['status'], aggregate: { count: 'sum' } },
  });

  source.insert('work_orders', { id: 'a', status: 'pending' });
  source.insert('work_orders', { id: 'b', status: 'completed' });
  await ripply.start();

  // handle re-acquired by name (post-hoc, untyped generics default)
  const handle = ripply.index('countByStatus');

  expect(await handle.where({ status: 'nope' }).one()).toBeNull();
  expect(await handle.where({ status: 'nope' }).value('count')).toBeUndefined();
  // two groups match the empty where() — ambiguous single-value reads throw
  await expect(handle.value('count')).rejects.toThrow('narrow the where()');
  expect(() => ripply.index('missing')).toThrow('unknown index');

  await ripply.stop();
});

test('typed surface: aggregate names and entry fields are inferred', async () => {
  const { source, ripply } = setup();

  const revenueByTech = ripply.defineIndex('revenueByTech', {
    collection: 'work_orders',
    map: (wo) => ({ techId: wo.technician_id, revenue: wo.revenue as number }),
    reduce: {
      groupBy: ['techId'],
      aggregate: { revenue: 'sum', top: { max: 'revenue' } },
    },
  });

  source.insert('work_orders', { id: '1', technician_id: 't1', revenue: 10 });
  await ripply.start();

  // valid aggregate names compile
  expect(await revenueByTech.where({ techId: 't1' }).value('revenue')).toBe(10);
  expect(await revenueByTech.where({ techId: 't1' }).value('top')).toBe(10);

  // @ts-expect-error — 'nope' is not an aggregate output of this index
  const bad = revenueByTech.value('nope');
  await bad.catch(() => {});

  // entries() carries the map's entry type through
  const entries = await revenueByTech.where({ techId: 't1' }).entries();
  const revenue: number = entries[0]!.entry.revenue; // typed as number
  expect(revenue).toBe(10);

  await ripply.stop();
});
