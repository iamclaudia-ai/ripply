/**
 * The streaming group fold (issue #3).
 *
 * `rebuild` and `verify` page the source and carry only per-group
 * accumulators, so the fold sees entries in SCAN order — which is not the
 * (pk, ord) order `reduceFull` reduces in. The property that makes that
 * safe is order-independence: folding a group's entries in any order must
 * produce exactly what `reduceFull` produces.
 *
 * `first`/`last` are the trap. v1 semantics are "first/last in pk order",
 * and pk order is lexicographic over canonical JSON — `["wo_10"]` sorts
 * before `["wo_2"]` — so a fold that trusted arrival order would look
 * correct on single-digit fixtures and silently diverge at scale.
 */

import { describe, expect, test } from 'bun:test';
import { createGroupFold, parseReduceSpec, reduceFull, toStoredEntries } from '../aggregates';
import type { ReduceSpec, StoredEntry } from '../types';
import { int, mulberry32, pick } from './helpers';

const SPEC: ReduceSpec = {
  groupBy: ['bucket'],
  aggregate: {
    total: { sum: 'revenue' },
    jobs: 'count',
    rated: { count: 'rating' },
    avgRevenue: { avg: 'revenue' },
    lo: { min: 'revenue' },
    hi: { max: 'revenue' },
    firstTech: { first: 'techId' },
    lastTech: { last: 'techId' },
    techs: { distinct: 'techId' },
  },
};

const AGGREGATES = parseReduceSpec(SPEC);
const TECHS = ['t1', 't2', 't3', 't4'] as const;

function shuffle<T>(rng: () => number, items: T[]): T[] {
  const out = [...items];
  for (let i = out.length - 1; i > 0; i--) {
    const j = Math.floor(rng() * (i + 1));
    [out[i], out[j]] = [out[j]!, out[i]!];
  }
  return out;
}

/** Entries for one group, with numeric-ish ids whose pk order != scan order. */
function groupEntries(rng: () => number, count: number): StoredEntry[] {
  const entries: StoredEntry[] = [];
  for (let i = 1; i <= count; i++) {
    // multi-emit: some rows contribute two entries, exercising `ord`
    const emits = rng() < 0.3 ? 2 : 1;
    const values = Array.from({ length: emits }, () => ({
      bucket: 'b',
      revenue: rng() < 0.15 ? null : int(rng, 500),
      rating: rng() < 0.4 ? null : int(rng, 5),
      techId: pick(rng, TECHS),
    }));
    entries.push(...toStoredEntries(SPEC.groupBy, [`wo_${i}`], values));
  }
  return entries;
}

function fold(entries: StoredEntry[]) {
  const acc = createGroupFold(AGGREGATES, '{"bucket":"b"}');
  for (const entry of entries) acc.add(entry);
  return acc.finish();
}

describe('createGroupFold', () => {
  test('any fold order == reduceFull, over random groups', () => {
    for (let seed = 1; seed <= 40; seed++) {
      const rng = mulberry32(seed);
      const entries = groupEntries(rng, 1 + int(rng, 30));
      const expected = reduceFull(AGGREGATES, '{"bucket":"b"}', entries);

      for (let shuffled = 0; shuffled < 5; shuffled++) {
        expect(fold(shuffle(rng, entries))).toEqual(expected);
      }
    }
  });

  test('first/last follow pk order, not arrival order', () => {
    // 12 ids: scan order is 1..12, pk order is wo_1, wo_10, wo_11, wo_12,
    // wo_2, ... — the two disagree about both ends of the group.
    const entries = Array.from({ length: 12 }, (_, i) =>
      toStoredEntries(
        SPEC.groupBy,
        [`wo_${i + 1}`],
        [{ bucket: 'b', revenue: i, rating: 1, techId: `tech_${i + 1}` }],
      ),
    ).flat();

    const folded = fold(entries);
    expect(folded.values.firstTech).toBe('tech_1');
    expect(folded.values.lastTech).toBe('tech_9');
    expect(folded).toEqual(reduceFull(AGGREGATES, '{"bucket":"b"}', entries));
    expect(fold(entries.toReversed())).toEqual(folded);
  });

  test('an empty group folds to the same zeros reduceFull produces', () => {
    expect(fold([])).toEqual(reduceFull(AGGREGATES, '{"bucket":"b"}', []));
    expect(fold([]).values).toEqual({
      total: 0,
      jobs: 0,
      rated: 0,
      avgRevenue: { sum: 0, count: 0 },
      lo: null,
      hi: null,
      firstTech: null,
      lastTech: null,
      techs: [],
    });
  });

  test('min/max still reject mixed comparable types', () => {
    const aggregates = parseReduceSpec({
      groupBy: ['bucket'],
      aggregate: { hi: { max: 'score' } },
    });
    const entries = toStoredEntries(
      ['bucket'],
      ['wo_1'],
      [
        { bucket: 'b', score: 10 },
        { bucket: 'b', score: 'ten' },
      ],
    );
    const acc = createGroupFold(aggregates, '{"bucket":"b"}');
    expect(() => {
      for (const entry of entries) acc.add(entry);
    }).toThrow(/cannot compare/);
  });
});
