/**
 * Aggregate registry, classified by invertibility (DESIGN.md §5).
 *
 * - Linear (`sum`, `count`, `avg` as sum+count): accept signed deltas, O(1)
 *   per change against the reduced row.
 * - Non-linear (`min`, `max`, `first`, `last`, `distinct`): the group is
 *   marked dirty and re-reduced from that group's stored entries only.
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

const LINEAR_FNS = new Set<AggregateFn>(['sum', 'count', 'avg']);

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

export function isLinear(aggregates: ParsedAggregate[]): boolean {
  return aggregates.every((agg) => LINEAR_FNS.has(agg.fn));
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
 * RETRACTION, not accumulation. Nothing here can retract; `applyLinearDelta`
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
// Linear delta application (O(1) path)
// ---------------------------------------------------------------------------

/**
 * Apply a signed delta to one group's reduced row: subtract the row's old
 * contribution, add its new one. Only valid when every aggregate is linear.
 * Returns the next row; the caller deletes it when `entryCount` hits 0.
 */
export function applyLinearDelta(
  aggregates: ParsedAggregate[],
  groupKey: string,
  current: ReducedRow | null,
  oldEntries: Entry[],
  newEntries: Entry[],
): ReducedRow {
  const values: Record<string, unknown> = current ? { ...current.values } : initValues(aggregates);

  for (const agg of aggregates) {
    switch (agg.fn) {
      case 'sum': {
        const cur = values[agg.out] as number;
        values[agg.out] = cur + sumOf(agg, newEntries) - sumOf(agg, oldEntries);
        break;
      }
      case 'count': {
        const cur = values[agg.out] as number;
        values[agg.out] = cur + countOf(agg, newEntries) - countOf(agg, oldEntries);
        break;
      }
      case 'avg': {
        const cur = values[agg.out] as { sum: number; count: number };
        const add = avgOf(agg, newEntries);
        const sub = avgOf(agg, oldEntries);
        values[agg.out] = {
          sum: cur.sum + add.sum - sub.sum,
          count: cur.count + add.count - sub.count,
        };
        break;
      }
      default:
        throw new RipplyError(
          `aggregate "${agg.out}": "${agg.fn}" is non-linear and cannot take deltas`,
        );
    }
  }

  return {
    groupKey,
    group: current?.group ?? groupOfKey(groupKey),
    values,
    entryCount: (current?.entryCount ?? 0) + newEntries.length - oldEntries.length,
  };
}

function initValues(aggregates: ParsedAggregate[]): Record<string, unknown> {
  const values: Record<string, unknown> = {};
  for (const agg of aggregates) {
    values[agg.out] = agg.fn === 'avg' ? { sum: 0, count: 0 } : 0;
  }
  return values;
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
