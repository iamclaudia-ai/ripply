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
import type {
  AggregateFn,
  Entry,
  PkValue,
  ReduceSpec,
  ReducedRow,
  StoredEntry,
} from './types';

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
      throw new RipplyError(
        `aggregate "${out}": expected exactly one { fn: field } pair`,
      );
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
 * Reduce a group from scratch, from its entries. Entries are ordered
 * canonically by (pk, ord) so `first`/`last` are deterministic — v1
 * semantics are "first/last in pk order", not insertion time.
 */
export function reduceFull(
  aggregates: ParsedAggregate[],
  groupKey: string,
  entries: StoredEntry[],
): ReducedRow {
  const ordered = [...entries].sort(compareEntries);
  const values: Record<string, unknown> = {};
  for (const agg of aggregates) {
    values[agg.out] = reduceOne(agg, ordered);
  }
  return {
    groupKey,
    group: groupOfKey(groupKey),
    values,
    entryCount: entries.length,
  };
}

function compareEntries(a: StoredEntry, b: StoredEntry): number {
  const ka = pkKeyOf(a.pk);
  const kb = pkKeyOf(b.pk);
  if (ka !== kb) return ka < kb ? -1 : 1;
  return a.ord - b.ord;
}

function reduceOne(agg: ParsedAggregate, ordered: StoredEntry[]): unknown {
  switch (agg.fn) {
    case 'sum':
      return sumOf(agg, valuesOf(ordered));
    case 'count':
      return countOf(agg, valuesOf(ordered));
    case 'avg':
      return avgOf(agg, valuesOf(ordered));
    case 'min':
    case 'max': {
      let best: number | string | null = null;
      for (const entry of ordered) {
        const value = comparable(agg, entry.values[agg.field!]);
        if (value === null) continue;
        if (best === null) best = value;
        else if (typeof value !== typeof best) {
          throw new RipplyError(
            `aggregate "${agg.out}": cannot compare ${typeof best} with ${typeof value}`,
          );
        } else if (agg.fn === 'min' ? value < best : value > best) {
          best = value;
        }
      }
      return best;
    }
    case 'first':
      return ordered.length ? (ordered[0]!.values[agg.field!] ?? null) : null;
    case 'last':
      return ordered.length
        ? (ordered[ordered.length - 1]!.values[agg.field!] ?? null)
        : null;
    case 'distinct': {
      const seen = new Map<string, unknown>();
      for (const entry of ordered) {
        const value = entry.values[agg.field!];
        if (value === null || value === undefined) continue;
        const key = canonicalJson(value);
        if (!seen.has(key)) seen.set(key, value);
      }
      return [...seen.entries()]
        .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
        .map(([, value]) => value);
    }
  }
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
  const values: Record<string, unknown> = current
    ? { ...current.values }
    : initValues(aggregates);

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

function valuesOf(entries: StoredEntry[]): Entry[] {
  return entries.map((entry) => entry.values);
}

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
