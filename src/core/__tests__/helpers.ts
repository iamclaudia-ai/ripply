/**
 * Test helpers: a seeded PRNG and an INDEPENDENT oracle.
 *
 * The oracle deliberately shares no code with `src/core` — it has its own
 * sorted-key JSON and its own naive aggregation, so a bug in the engine's
 * canonicalization or aggregate math cannot hide inside the oracle too.
 */

import type { Entry, IndexDefinition, PkValue, Row } from '../types';

// ---------------------------------------------------------------------------
// Seeded PRNG (mulberry32) — deterministic property tests
// ---------------------------------------------------------------------------

export function mulberry32(seed: number): () => number {
  let state = seed;
  return () => {
    state = (state + 0x6d2b79f5) | 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

export function pick<T>(rng: () => number, items: readonly T[]): T {
  return items[Math.floor(rng() * items.length)]!;
}

export function int(rng: () => number, maxExclusive: number): number {
  return Math.floor(rng() * maxExclusive);
}

// ---------------------------------------------------------------------------
// Independent oracle: from-scratch map + group + reduce over current rows
// ---------------------------------------------------------------------------

/** Independent sorted-key JSON (undefined → null), separate from core. */
export function oracleCanonical(value: unknown): string {
  const walk = (v: unknown): unknown => {
    if (v === undefined) return null;
    if (v === null || typeof v !== 'object') return v;
    if (Array.isArray(v)) return v.map(walk);
    const obj = v as Record<string, unknown>;
    const sorted: Record<string, unknown> = {};
    for (const k of Object.keys(obj).sort()) sorted[k] = walk(obj[k]);
    return sorted;
  };
  return JSON.stringify(walk(value));
}

export interface OracleGroup {
  values: Record<string, unknown>;
  entryCount: number;
}

/**
 * Compute the expected reduced state of an index from the source's current
 * rows, completely from scratch. Mirrors Ripply's documented semantics:
 * avg as {sum,count}, first/last in (pk, ord) order, distinct sorted by
 * canonical value, null/undefined skipped by sum/count(field)/avg/min/max.
 */
export function oracleReduce(
  rows: Array<{ pk: PkValue; row: Row }>,
  def: IndexDefinition,
): Map<string, OracleGroup> {
  // 1. map + normalize + group
  type OracleEntry = { pkKey: string; ord: number; values: Entry };
  const groups = new Map<string, OracleEntry[]>();
  for (const { pk, row } of rows) {
    const output = def.map(row);
    const entries =
      output === null || output === undefined
        ? []
        : Array.isArray(output)
          ? output
          : [output];
    entries.forEach((values, ord) => {
      const groupObj: Record<string, unknown> = {};
      for (const field of def.reduce.groupBy) groupObj[field] = values[field];
      const groupKey = oracleCanonical(groupObj);
      const list = groups.get(groupKey) ?? [];
      list.push({ pkKey: JSON.stringify(pk), ord, values });
      groups.set(groupKey, list);
    });
  }

  // 2. reduce each group naively
  const result = new Map<string, OracleGroup>();
  for (const [groupKey, entries] of groups) {
    entries.sort((a, b) =>
      a.pkKey !== b.pkKey ? (a.pkKey < b.pkKey ? -1 : 1) : a.ord - b.ord,
    );
    const values: Record<string, unknown> = {};
    for (const [out, spec] of Object.entries(def.reduce.aggregate)) {
      const fn = typeof spec === 'string' ? spec : Object.keys(spec)[0]!;
      const field =
        typeof spec === 'string'
          ? fn === 'count'
            ? null
            : out
          : Object.values(spec)[0]!;
      const raw = entries.map((e) => (field === null ? undefined : e.values[field]));
      const present = raw.filter((v) => v !== null && v !== undefined);
      const numbers = present as number[];
      switch (fn) {
        case 'sum':
          values[out] = numbers.reduce((a, b) => a + b, 0);
          break;
        case 'count':
          values[out] = field === null ? entries.length : present.length;
          break;
        case 'avg':
          values[out] = {
            sum: numbers.reduce((a, b) => a + b, 0),
            count: numbers.length,
          };
          break;
        case 'min':
          values[out] = present.length
            ? (present as (number | string)[]).reduce((a, b) => (b < a ? b : a))
            : null;
          break;
        case 'max':
          values[out] = present.length
            ? (present as (number | string)[]).reduce((a, b) => (b > a ? b : a))
            : null;
          break;
        case 'first':
          values[out] = entries.length ? (entries[0]!.values[field!] ?? null) : null;
          break;
        case 'last':
          values[out] = entries.length
            ? (entries[entries.length - 1]!.values[field!] ?? null)
            : null;
          break;
        case 'distinct': {
          const seen = new Map<string, unknown>();
          for (const v of present) {
            const key = oracleCanonical(v);
            if (!seen.has(key)) seen.set(key, v);
          }
          values[out] = [...seen.entries()]
            .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
            .map(([, v]) => v);
          break;
        }
        default:
          throw new Error(`oracle: unknown aggregate fn "${fn}"`);
      }
    }
    result.set(groupKey, { values, entryCount: entries.length });
  }
  return result;
}
