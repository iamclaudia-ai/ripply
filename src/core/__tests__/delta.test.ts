/**
 * `applyDelta` (issue #7): maintaining a group from its reduced row alone.
 *
 * The contract is one-sided, and both sides are tested here:
 *
 * - **Sound:** whenever it answers, the answer equals `reduceFull` over the
 *   group's post-change entries. A wrong answer is a silently wrong tally.
 * - **Useful:** it only gives up (`null` ⇒ re-reduce) when it must. Inserts
 *   into a min/max + linear group must never give up, or the fast path is
 *   decoration.
 *
 * The engine-level suite proves the same thing end to end per backend, but
 * random op sequences rarely hit "the max leaves and a LOWER value replaces
 * it while a runner-up sits between them". This test aims straight at it.
 */

import { describe, expect, test } from 'bun:test';
import { applyDelta, parseReduceSpec, reduceFull, toStoredEntries } from '../aggregates';
import type { Entry, ReduceSpec, StoredEntry } from '../types';
import { int, mulberry32, pick } from './helpers';

const BOARD: ReduceSpec = {
  groupBy: ['g'],
  aggregate: {
    games: { sum: 'n' },
    points: { sum: 'score' },
    avgScore: { avg: 'score' },
    lo: { min: 'score' },
    hi: { max: 'score' },
    hiTier: { max: 'tier' },
    loName: { min: 'name' },
  },
};
const AGGREGATES = parseReduceSpec(BOARD);
const GROUP = '{"g":1}';
const NAMES = ['ash', 'birch', 'cedar', 'elm'] as const;

function randomEntry(rng: () => number): Entry {
  const score = rng() < 0.1 ? null : int(rng, 12); // small range ⇒ frequent ties
  return {
    g: 1,
    n: 1,
    score,
    tier: score === null ? null : score % 3,
    name: rng() < 0.1 ? null : pick(rng, NAMES),
  };
}

function rowEntries(pk: number, values: Entry[]): StoredEntry[] {
  return toStoredEntries(['g'], [pk], values);
}

describe('applyDelta', () => {
  test('agrees with reduceFull whenever it answers (random retract + insert)', () => {
    const rng = mulberry32(7);
    let answered = 0;
    let insertsAnswered = 0;
    let inserts = 0;

    for (let trial = 0; trial < 5000; trial++) {
      // a group of 1–8 rows, each emitting 1–3 entries
      const rows = new Map<number, StoredEntry[]>();
      const rowCount = 1 + int(rng, 8);
      for (let pk = 0; pk < rowCount; pk++) {
        rows.set(
          pk,
          rowEntries(
            pk,
            Array.from({ length: 1 + int(rng, 3) }, () => randomEntry(rng)),
          ),
        );
      }
      const current = reduceFull(AGGREGATES, GROUP, [...rows.values()].flat());

      // change one row: an existing pk (update/delete) or a fresh one (insert)
      const isInsert = rng() < 0.3;
      const pk = isInsert ? rowCount : int(rng, rowCount);
      const oldEntries = rows.get(pk) ?? [];
      const newEntries = rowEntries(
        pk,
        Array.from({ length: int(rng, 3) + (isInsert ? 1 : 0) }, () => randomEntry(rng)),
      );

      const after = new Map(rows);
      if (newEntries.length) after.set(pk, newEntries);
      else after.delete(pk);
      const remaining = [...after.values()].flat();

      const next = applyDelta(AGGREGATES, GROUP, current, oldEntries, newEntries);
      if (isInsert) inserts++;
      if (next === null) continue;
      answered++;
      if (isInsert) insertsAnswered++;

      expect(next.entryCount).toBe(remaining.length);
      if (remaining.length > 0) {
        expect(next).toEqual(reduceFull(AGGREGATES, GROUP, remaining));
      }
    }

    // sound AND useful: every insert answered, and retractions that leave
    // every extreme in place answered too (~900 of ~3500 with this fixture's
    // four extremes over a 12-value range — most retractions here hit one)
    expect(insertsAnswered).toBe(inserts);
    expect(answered - insertsAnswered).toBeGreaterThan(500);
  });

  test('retracting the max with a lower replacement rescans when a runner-up exists', () => {
    // 9 is the max, 7 the runner-up; the 9 becomes a 3. The answer is 7, which
    // only the group knows.
    const current = reduceFull(AGGREGATES, GROUP, [
      ...rowEntries(1, [{ g: 1, n: 1, score: 9, tier: 0, name: 'elm' }]),
      ...rowEntries(2, [{ g: 1, n: 1, score: 7, tier: 1, name: 'elm' }]),
    ]);
    const next = applyDelta(
      AGGREGATES,
      GROUP,
      current,
      rowEntries(1, [{ g: 1, n: 1, score: 9, tier: 0, name: 'elm' }]),
      rowEntries(1, [{ g: 1, n: 1, score: 3, tier: 0, name: 'elm' }]),
    );
    expect(next).toBeNull();
  });

  test('retracting one of two tied maxima rescans (the twin is invisible from the row)', () => {
    const entries = [
      ...rowEntries(1, [{ g: 1, n: 1, score: 9, tier: 0, name: 'elm' }]),
      ...rowEntries(2, [{ g: 1, n: 1, score: 9, tier: 0, name: 'elm' }]),
    ];
    const current = reduceFull(AGGREGATES, GROUP, entries);
    expect(applyDelta(AGGREGATES, GROUP, current, entries.slice(0, 1), [])).toBeNull();
  });

  test('a new group folds from its own entries, for every aggregate', () => {
    const spec = parseReduceSpec({
      groupBy: ['g'],
      aggregate: { first: { first: 'v' }, last: { last: 'v' }, vs: { distinct: 'v' } },
    });
    const fresh = rowEntries(4, [
      { g: 1, v: 'b' },
      { g: 1, v: 'a' },
    ]);
    expect(applyDelta(spec, GROUP, null, [], fresh)).toEqual(reduceFull(spec, GROUP, fresh));
  });

  test('first/last/distinct on an existing group always defer to the re-reduce', () => {
    const spec = parseReduceSpec({ groupBy: ['g'], aggregate: { vs: { distinct: 'v' } } });
    const current = reduceFull(spec, GROUP, rowEntries(1, [{ g: 1, v: 'a' }]));
    expect(applyDelta(spec, GROUP, current, [], rowEntries(2, [{ g: 1, v: 'b' }]))).toBeNull();
  });

  test('a stored row that disagrees with the entries rescans instead of guessing', () => {
    // retracting from a group with no row
    expect(
      applyDelta(AGGREGATES, GROUP, null, rowEntries(1, [randomEntry(mulberry32(1))]), []),
    ).toBeNull();
    // retracting a value beyond the stored max
    const current = reduceFull(AGGREGATES, GROUP, rowEntries(1, [{ g: 1, n: 1, score: 5 }]));
    expect(
      applyDelta(AGGREGATES, GROUP, current, rowEntries(2, [{ g: 1, n: 1, score: 8 }]), []),
    ).toBeNull();
  });

  test('a value changing type defers to the scan, which knows whether the old type is gone', () => {
    const spec = parseReduceSpec({ groupBy: ['g'], aggregate: { hi: { max: 'v' } } });
    const old = rowEntries(1, [{ g: 1, v: 'z' }]);
    const current = reduceFull(spec, GROUP, old);
    const next = applyDelta(spec, GROUP, current, old, rowEntries(1, [{ g: 1, v: 5 }]));
    expect(next).toBeNull();
    // and the scan does the right thing: the only string left, so 5 is legal
    expect(reduceFull(spec, GROUP, rowEntries(1, [{ g: 1, v: 5 }])).values).toEqual({ hi: 5 });
  });
});
