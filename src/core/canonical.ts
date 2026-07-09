/**
 * Canonical serialization.
 *
 * Group keys MUST be canonical (object keys sorted, recursively) or group
 * objects that differ only in key insertion order become phantom groups
 * (DESIGN.md §8 invariant 7). Non-negotiable.
 */

import type { Entry, PkValue } from './types';

/**
 * JSON.stringify with object keys sorted recursively. `undefined` values
 * are treated as `null` so that "field absent" and "field null" collapse to
 * the same group rather than producing two.
 */
export function canonicalJson(value: unknown): string {
  return JSON.stringify(sortValue(value));
}

function sortValue(value: unknown): unknown {
  if (value === undefined) return null;
  if (value === null || typeof value !== 'object') return value;
  if (Array.isArray(value)) return value.map(sortValue);
  const source = value as Record<string, unknown>;
  const out: Record<string, unknown> = {};
  for (const key of Object.keys(source).sort()) {
    out[key] = sortValue(source[key]);
  }
  return out;
}

/** Canonical group key: the groupBy fields of an entry, sorted-key JSON. */
export function groupKeyOf(groupBy: readonly string[], entry: Entry): string {
  const group: Record<string, unknown> = {};
  for (const field of groupBy) {
    group[field] = entry[field];
  }
  return canonicalJson(group);
}

/** Parse a canonical group key back into its groupBy fields. */
export function groupOfKey(groupKey: string): Record<string, unknown> {
  return JSON.parse(groupKey) as Record<string, unknown>;
}

/** Canonical primary-key serialization (pk values are ordered arrays). */
export function pkKeyOf(pk: PkValue): string {
  return JSON.stringify(pk);
}
