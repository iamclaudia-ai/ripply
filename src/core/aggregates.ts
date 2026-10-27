/**
 * Aggregate registry, classified by invertibility (DESIGN.md §5).
 *
 * - Linear (`sum`, `count`, `avg` as sum+count): accept signed deltas, O(1)
 *   per change against the reduced row.
 * - Non-linear (`min`, `max`, `first`, `last`, `distinct`): cannot always be
 *   maintained from the reduced row. `applyDelta` decides per change: `min`/
 *   `max` stay O(1) unless their current extreme is retracted; otherwise the
 *   group is marked dirty and re-reduced from its stored entries only.
 *
 * Everything here is pure — no store, no source, no engine state.
 */

import { canonicalJson, groupKeyOf, groupOfKey, pkKeyOf } from './canonical';
import { RipplyError } from './errors';
import type { AggregateFn, Entry, PkValue, ReduceSpec, ReducedRow, StoredEntry } from './types';

export interface ParsedAggregate {
  /** Output field name on the reduced row. */
  out: string;
  fn: AggregateFn;
  /** Entry field to aggregate over; null for bare `count`. */
  field: string | null;
}

const AGGREGATE_FNS = new Set<AggregateFn>([
  'sum',
  'count',
  'avg',
  'min',
  'max',
  'first',
  'last',
  'distinct',
]);

/** Validate a reduce spec and flatten it into parsed aggregates. */
export function parseReduceSpec(spec: ReduceSpec): ParsedAggregate[] {
  if (!Array.isArray(spec.groupBy) || spec.groupBy.length === 0) {
    throw new RipplyError('reduce.groupBy must name at least one entry field');
  }
  const outs = Object.entries(spec.aggregate ?? {});
  if (outs.length === 0) {
    throw new RipplyError('reduce.aggregate must define at least one aggregate');
  }
  return outs.map(([out, aggSpec]) => {
    if (typeof aggSpec === 'string') {
      assertAggregateFn(out, aggSpec);
      return { out, fn: aggSpec, field: aggSpec === 'count' ? null : out };
    }
    const pairs = Object.entries(aggSpec);
    if (pairs.length !== 1) {
      throw new RipplyError(`aggregate "${out}": expected exactly one { fn: field } pair`);
    }
    const [fn, field] = pairs[0]!;
    assertAggregateFn(out, fn);
    if (typeof field !== 'string' || field.length === 0) {
      throw new RipplyError(`aggregate "${out}": field name must be a string`);
    }
    return { out, fn, field };
  });
}

function assertAggregateFn(out: string, fn: string): asserts fn is AggregateFn {
  if (!AGGREGATE_FNS.has(fn as AggregateFn)) {
    throw new RipplyError(`aggregate "${out}": unknown aggregate fn "${fn}"`);
  }
}

// ---------------------------------------------------------------------------
// Map output handling
// ---------------------------------------------------------------------------

/** Normalize a map return value to Entry[] (multi-emit, null = no entries). */
export function normalizeMapOutput(
  index: string,
  output: Entry | Entry[] | null | undefined,
): Entry[] {
  if (output === null || output === undefined) return [];
  const entries = Array.isArray(output) ? output : [output];
  for (const entry of entries) {
    if (typeof entry !== 'object' || entry === null || Array.isArray(entry)) {
      throw new RipplyError(
        `index "${index}": map must return an entry object, an array of them, or null`,
      );
    }
  }
  return entries;
}

/** Attach pk/ord/groupKey to raw map output, ready for the entries table. */
export function toStoredEntries(
  groupBy: readonly string[],
  pk: PkValue,
  entries: Entry[],
): StoredEntry[] {
  return entries.map((values, ord) => ({
    pk,
    ord,
    groupKey: groupKeyOf(groupBy, values),
    values,
  }));
}

// ---------------------------------------------------------------------------
// Full reduce (rebuild, verify, dirty-group re-reduce)
// ---------------------------------------------------------------------------

/**
 * Streaming fold for ONE group.
 *
 * Every aggregate has forward-pass state that is O(1) per entry (`distinct`
 * excepted — its state is the distinct set, bounded by cardinality). This is
 * what lets `rebuild` and `verify` page a large source instead of holding it:
 * a rebuild only ever inserts, and non-linearity is a statement about
 * RETRACTION, not accumulation. Nothing here can retract; `applyDelta`
 * is still the only signed-delta path.
 *
 * Entry order does not affect the result. `first`/`last` carry the winning
 * (pk, ord) position rather than trusting arrival order, so a scan that
 * yields rows in any order still agrees with `reduceFull` exactly. (Float
 * summation is the one exception — addition is not associative, so a `sum`
 * folded in a different order can differ in its last bits. The incremental
 * delta path already accumulates in change order, so this is a tolerance the
 * engine has always had, not a new one.)
 */
export interface GroupFold {
  add(entry: StoredEntry): void;
  finish(): ReducedRow;
}

/** Winning (pk, ord) position for a `first`/`last` aggregate. */
interface EdgeState {
  pkKey: string;
  ord: number;
  value: unknown;
}

interface FoldState {
  sum: number;
  count: number;
  /** min/max running extremum. */
  best: number | string | null;
  edge: EdgeState | null;
  distinct: Map<string, unknown> | null;
}

export function createGroupFold(aggregates: ParsedAggregate[], groupKey: string): GroupFold {
  // pkKeyOf is not free — only pay for it when first/last actually need it
  const needsPosition = aggregates.some((agg) => agg.fn === 'first' || agg.fn === 'last');
  const states: FoldState[] = aggregates.map(() => ({
    sum: 0,
    count: 0,
    best: null,
    edge: null,
    distinct: null,
  }));
  let entryCount = 0;

  return {
    add(entry: StoredEntry): void {
      entryCount++;
      const pkKey = needsPosition ? pkKeyOf(entry.pk) : '';

      for (let i = 0; i < aggregates.length; i++) {
        const agg = aggregates[i]!;
        const state = states[i]!;
        switch (agg.fn) {
          case 'sum': {
            state.sum += numeric(agg, entry.values[agg.field!]) ?? 0;
            break;
          }
          case 'count': {
            if (agg.field === null) {
              state.count++;
            } else {
              const value = entry.values[agg.field];
              if (value !== null && value !== undefined) state.count++;
            }
            break;
          }
          case 'avg': {
            const value = numeric(agg, entry.values[agg.field!]);
            if (value !== null) {
              state.sum += value;
              state.count++;
            }
            break;
          }
          case 'min':
          case 'max': {
            const value = comparable(agg, entry.values[agg.field!]);
            if (value === null) break;
            if (state.best === null) {
              state.best = value;
            } else if (typeof value !== typeof state.best) {
              throw new RipplyError(
                `aggregate "${agg.out}": cannot compare ${typeof state.best} with ${typeof value}`,
              );
            } else if (agg.fn === 'min' ? value < state.best : value > state.best) {
              state.best = value;
            }
            break;
          }
          case 'first':
          case 'last': {
            // NOT arrival order: v1 semantics are first/last in (pk, ord)
            // order, so the fold must compare positions explicitly.
            const candidate: EdgeState = {
              pkKey,
              ord: entry.ord,
              value: entry.values[agg.field!] ?? null,
            };
            const cmp = state.edge === null ? 0 : comparePosition(candidate, state.edge);
            if (state.edge === null || (agg.fn === 'first' ? cmp < 0 : cmp > 0)) {
              state.edge = candidate;
            }
            break;
          }
          case 'distinct': {
            const value = entry.values[agg.field!];
            if (value === null || value === undefined) break;
            state.distinct ??= new Map<string, unknown>();
            const key = canonicalJson(value);
            if (!state.distinct.has(key)) state.distinct.set(key, value);
            break;
          }
        }
      }
    },

    finish(): ReducedRow {
      const values: Record<string, unknown> = {};
      for (let i = 0; i < aggregates.length; i++) {
        const agg = aggregates[i]!;
        const state = states[i]!;
        switch (agg.fn) {
          case 'sum':
            values[agg.out] = state.sum;
            break;
          case 'count':
            values[agg.out] = state.count;
            break;
          case 'avg':
            values[agg.out] = { sum: state.sum, count: state.count };
            break;
          case 'min':
          case 'max':
            values[agg.out] = state.best;
            break;
          case 'first':
          case 'last':
            values[agg.out] = state.edge ? state.edge.value : null;
            break;
          case 'distinct':
            values[agg.out] = state.distinct
              ? [...state.distinct.entries()]
                  .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
                  .map(([, value]) => value)
              : [];
            break;
        }
      }
      return { groupKey, group: groupOfKey(groupKey), values, entryCount };
    },
  };
}

function comparePosition(a: EdgeState, b: EdgeState): number {
  if (a.pkKey !== b.pkKey) return a.pkKey < b.pkKey ? -1 : 1;
  return a.ord - b.ord;
}

/**
 * Reduce a group from scratch, from its entries. Entries are ordered
 * canonically by (pk, ord) so `first`/`last` are deterministic — v1
 * semantics are "first/last in pk order", not insertion time.
 *
 * This is `createGroupFold` over the sorted entries: one implementation, so
 * the streaming rebuild and the dirty-group re-reduce cannot drift apart.
 */
export function reduceFull(
  aggregates: ParsedAggregate[],
  groupKey: string,
  entries: StoredEntry[],
): ReducedRow {
  const fold = createGroupFold(aggregates, groupKey);
  for (const entry of [...entries].sort(compareEntries)) fold.add(entry);
  return fold.finish();
}

function compareEntries(a: StoredEntry, b: StoredEntry): number {
  const ka = pkKeyOf(a.pk);
  const kb = pkKeyOf(b.pk);
  if (ka !== kb) return ka < kb ? -1 : 1;
  return a.ord - b.ord;
}

// ---------------------------------------------------------------------------
// Incremental delta application (issue #7)
// ---------------------------------------------------------------------------

/**
 * Fold one row's change (retract `oldEntries`, add `newEntries`) into one
 * group's reduced row WITHOUT reading the group's other entries, when that is
 * provably exact. Returns the next row, or `null` when the group must be
 * re-reduced from its entries (`reReduceInTx`). The caller deletes the row
 * when `entryCount` reaches 0.
 *
 * The decision is per aggregate, not per index — one `max` among four sums no
 * longer sends the sums through a full-group scan:
 *
 * - `sum` / `count` / `avg`: signed deltas, always O(1).
 * - `min` / `max`: retraction only needs the group when the value leaving IS
 *   the current extreme and nothing arriving reaches it. Inserts never need
 *   the group — the extreme can only improve. So an append-only workload is
 *   O(1) per change for good.
 * - `first` / `last` / `distinct`: retraction always re-reduces. The reduced
 *   row stores the edge VALUE but not its (pk, ord) position, and `distinct`
 *   has no per-value refcounts, so neither can tell whether a retraction
 *   moved it.
 *
 * A brand-new group (no reduced row, nothing retracted) is exactly the fold
 * of the new entries, for every aggregate — the same `createGroupFold` the
 * rebuild uses, so the paths cannot drift.
 */
export function applyDelta(
  aggregates: ParsedAggregate[],
  groupKey: string,
  current: ReducedRow | null,
  oldEntries: StoredEntry[],
  newEntries: StoredEntry[],
): ReducedRow | null {
  if (current === null) {
    // Retracting from a group that has no row means the stored state is not
    // what we think it is — let the scan sort it out rather than guess.
    if (oldEntries.length > 0) return null;
    return reduceFull(aggregates, groupKey, newEntries);
  }

  const oldValues = oldEntries.map((e) => e.values);
  const newValues = newEntries.map((e) => e.values);
  const values: Record<string, unknown> = { ...current.values };

  for (const agg of aggregates) {
    switch (agg.fn) {
      case 'sum': {
        const cur = values[agg.out] as number;
        values[agg.out] = cur + sumOf(agg, newValues) - sumOf(agg, oldValues);
        break;
      }
      case 'count': {
        const cur = values[agg.out] as number;
        values[agg.out] = cur + countOf(agg, newValues) - countOf(agg, oldValues);
        break;
      }
      case 'avg': {
        const cur = values[agg.out] as { sum: number; count: number };
        const add = avgOf(agg, newValues);
        const sub = avgOf(agg, oldValues);
        values[agg.out] = {
          sum: cur.sum + add.sum - sub.sum,
          count: cur.count + add.count - sub.count,
        };
        break;
      }
      case 'min':
      case 'max': {
        const next = extremeDelta(
          agg,
          values[agg.out] as number | string | null,
          oldValues,
          newValues,
        );
        if (next === RESCAN) return null;
        values[agg.out] = next;
        break;
      }
      case 'first':
      case 'last':
      case 'distinct':
        // Even an insert into an existing group needs the group: an edge
        // needs the current winner's (pk, ord) position and `distinct` needs
        // the full value set, and neither is in the reduced row.
        return null;
    }
  }

  return {
    groupKey,
    group: current.group,
    values,
    entryCount: current.entryCount + newEntries.length - oldEntries.length,
  };
}

/** Sentinel: this min/max cannot be maintained from the reduced row alone. */
const RESCAN = Symbol('rescan');

/**
 * Next min/max after retracting `oldValues` and adding `newValues`, or
 * `RESCAN` when the answer depends on entries we have not read.
 *
 * With `best` the current extreme (for `max`; `min` mirrors it):
 *   - a retracted value strictly below `best` cannot have been the extreme;
 *   - a retracted value equal to `best` might have been the only one — the
 *     runner-up is unknown — UNLESS a new value reaches `best` too, in which
 *     case the result is the new extreme regardless of the runner-up;
 *   - anything else (a retraction from an all-null group, a value beyond
 *     `best`, a type that can't compare) means the stored row disagrees with
 *     the entries, and only a scan is safe.
 */
function extremeDelta(
  agg: ParsedAggregate,
  best: number | string | null,
  oldValues: Entry[],
  newValues: Entry[],
): number | string | null | typeof RESCAN {
  const isMax = agg.fn === 'max';
  const beats = (a: number | string, b: number | string): boolean => (isMax ? a > b : a < b);

  // Fold the arrivals first; the retraction test needs to know how far they reach.
  let incoming: number | string | null = null;
  for (const entry of newValues) {
    const value = comparable(agg, entry[agg.field!]);
    if (value === null) continue;
    if (incoming !== null && typeof value !== typeof incoming) {
      throw new RipplyError(
        `aggregate "${agg.out}": cannot compare ${typeof incoming} with ${typeof value}`,
      );
    }
    if (incoming === null || beats(value, incoming)) incoming = value;
  }

  let extremeLeaving = false;
  for (const entry of oldValues) {
    const value = comparable(agg, entry[agg.field!]);
    if (value === null) continue;
    if (best === null || typeof value !== typeof best) return RESCAN;
    if (beats(value, best)) return RESCAN;
    if (value === best) extremeLeaving = true;
  }

  // A new type against the stored extreme: the old type may be leaving
  // entirely (a legal transition), or the group is genuinely mixed (an
  // error). Only the scan can tell, and it raises the error if there is one.
  if (best !== null && incoming !== null && typeof incoming !== typeof best) return RESCAN;

  if (extremeLeaving) {
    // `best` left. Only a new value at least as extreme pins the answer.
    if (incoming === null || beats(best!, incoming)) return RESCAN;
    return incoming;
  }
  if (best === null) return incoming;
  if (incoming === null) return best;
  return beats(incoming, best) ? incoming : best;
}

// ---------------------------------------------------------------------------
// Field helpers
// ---------------------------------------------------------------------------

/** null/undefined are skipped; any other non-number is a hard error. */
function numeric(agg: ParsedAggregate, value: unknown): number | null {
  if (value === null || value === undefined) return null;
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    throw new RipplyError(
      `aggregate "${agg.out}": expected a finite number for field "${agg.field}", got ${JSON.stringify(value)}`,
    );
  }
  return value;
}

function comparable(agg: ParsedAggregate, value: unknown): number | string | null {
  if (value === null || value === undefined) return null;
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value === 'string') return value;
  throw new RipplyError(
    `aggregate "${agg.out}": min/max need number or string values for field "${agg.field}", got ${JSON.stringify(value)}`,
  );
}

function sumOf(agg: ParsedAggregate, entries: Entry[]): number {
  let total = 0;
  for (const entry of entries) {
    total += numeric(agg, entry[agg.field!]) ?? 0;
  }
  return total;
}

function countOf(agg: ParsedAggregate, entries: Entry[]): number {
  if (agg.field === null) return entries.length;
  let count = 0;
  for (const entry of entries) {
    const value = entry[agg.field];
    if (value !== null && value !== undefined) count++;
  }
  return count;
}

/** avg components: sum and count over non-null numeric values. */
function avgOf(agg: ParsedAggregate, entries: Entry[]): { sum: number; count: number } {
  let sum = 0;
  let count = 0;
  for (const entry of entries) {
    const value = numeric(agg, entry[agg.field!]);
    if (value === null) continue;
    sum += value;
    count++;
  }
  return { sum, count };
}
