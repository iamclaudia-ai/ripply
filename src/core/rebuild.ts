/**
 * Rebuild & verify (DESIGN.md §7).
 *
 * Rebuild: capture the feed position FIRST, then truncate + full scan +
 * resume from that position. Changes committed during the scan may be
 * replayed afterwards — reconcile-from-entries makes the replay converge,
 * so the pre-scan cursor is safe (never skips, only repeats).
 *
 * Verify: an independent from-scratch reduce over the source, diffed
 * against the maintained index. Ships as a dev/CI assertion; run it while
 * writes are quiesced or expect false positives from in-flight changes.
 */

import {
  normalizeMapOutput,
  reduceFull,
  toStoredEntries,
} from './aggregates';
import { canonicalJson, pkKeyOf } from './canonical';
import type { Engine, IndexRuntime } from './engine';
import type { ReducedRow, StoredEntry } from './types';
import { mapVersionOf } from './version';

export async function rebuildIndex(engine: Engine, name: string): Promise<void> {
  const runtime = engine.runtimeOf(name);
  const collection = runtime.def.collection;

  // BEFORE the scan — replayed overlap is idempotent, gaps would not be.
  const resumeCursor = await engine.source.currentCursor(collection);

  // Fold the whole source IN MEMORY (map → group → reduceFull) — a scan
  // never repeats a pk, so there is nothing to reconcile against. The
  // store then gets bulk writes instead of per-row query chatter, which is
  // the difference between seconds and tens of minutes against a remote
  // database (Neon RTTs × rows × queries-per-row adds up fast).
  const { entries, byGroup } = await foldSource(engine, runtime);
  const reduced = [...byGroup.entries()].map(([groupKey, groupEntries]) =>
    reduceFull(runtime.aggregates, groupKey, groupEntries),
  );

  await engine.store.transaction(async (tx) => {
    await tx.truncateIndex(name);
    if (tx.insertEntries) {
      await tx.insertEntries(name, entries);
    } else {
      // fallback: one replaceEntries per source row (post-truncate, the
      // delete inside replaceEntries is a cheap no-op)
      const byPk = new Map<string, StoredEntry[]>();
      for (const entry of entries) {
        const key = pkKeyOf(entry.pk);
        byPk.set(key, [...(byPk.get(key) ?? []), entry]);
      }
      for (const rowEntries of byPk.values()) {
        await tx.replaceEntries(name, rowEntries[0]!.pk, rowEntries);
      }
    }
    if (tx.putReducedMany) {
      await tx.putReducedMany(name, reduced);
    } else {
      for (const row of reduced) await tx.putReduced(name, row);
    }
    await tx.setCursor(name, resumeCursor);
    await tx.setIndexMeta(name, { mapVersion: mapVersionOf(runtime.def) });
  });
}

export interface VerifyResult {
  ok: boolean;
  /** Groups the from-scratch reduce expects but the index lacks. */
  missingGroups: string[];
  /** Groups the index has but the from-scratch reduce does not. */
  extraGroups: string[];
  /** Groups present in both whose values or entryCount differ. */
  mismatchedGroups: string[];
}

export async function verifyIndex(
  engine: Engine,
  name: string,
): Promise<VerifyResult> {
  const runtime = engine.runtimeOf(name);

  const expected = await expectedReduced(engine, runtime);
  const actual = new Map<string, ReducedRow>();
  for (const row of await engine.store.transaction((tx) => tx.allReduced(name))) {
    actual.set(row.groupKey, row);
  }

  const missingGroups: string[] = [];
  const extraGroups: string[] = [];
  const mismatchedGroups: string[] = [];

  for (const [groupKey, expectedRow] of expected) {
    const actualRow = actual.get(groupKey);
    if (!actualRow) {
      missingGroups.push(groupKey);
    } else if (
      canonicalJson(actualRow.values) !== canonicalJson(expectedRow.values) ||
      actualRow.entryCount !== expectedRow.entryCount
    ) {
      mismatchedGroups.push(groupKey);
    }
  }
  for (const groupKey of actual.keys()) {
    if (!expected.has(groupKey)) extraGroups.push(groupKey);
  }

  return {
    ok:
      missingGroups.length === 0 &&
      extraGroups.length === 0 &&
      mismatchedGroups.length === 0,
    missingGroups,
    extraGroups,
    mismatchedGroups,
  };
}

/** From-scratch reduce: scan → map → group → reduceFull, no store involved. */
async function expectedReduced(
  engine: Engine,
  runtime: IndexRuntime,
): Promise<Map<string, ReducedRow>> {
  const { byGroup } = await foldSource(engine, runtime);
  const expected = new Map<string, ReducedRow>();
  for (const [groupKey, entries] of byGroup) {
    expected.set(groupKey, reduceFull(runtime.aggregates, groupKey, entries));
  }
  return expected;
}

/** One scan folded to entries (in scan order) and entries-by-group. */
async function foldSource(
  engine: Engine,
  runtime: IndexRuntime,
): Promise<{ entries: StoredEntry[]; byGroup: Map<string, StoredEntry[]> }> {
  const entries: StoredEntry[] = [];
  const byGroup = new Map<string, StoredEntry[]>();
  await engine.source.scan(runtime.def.collection, (pk, row) => {
    const mapped = normalizeMapOutput(runtime.name, runtime.def.map(row));
    for (const entry of toStoredEntries(runtime.def.reduce.groupBy, pk, mapped)) {
      entries.push(entry);
      const list = byGroup.get(entry.groupKey) ?? [];
      list.push(entry);
      byGroup.set(entry.groupKey, list);
    }
  });
  return { entries, byGroup };
}
