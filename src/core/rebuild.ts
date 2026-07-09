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
import { canonicalJson } from './canonical';
import type { Engine, IndexRuntime } from './engine';
import type { ReducedRow, StoredEntry } from './types';
import { mapVersionOf } from './version';

export async function rebuildIndex(engine: Engine, name: string): Promise<void> {
  const runtime = engine.runtimeOf(name);
  const collection = runtime.def.collection;

  // BEFORE the scan — replayed overlap is idempotent, gaps would not be.
  const resumeCursor = await engine.source.currentCursor(collection);

  await engine.store.transaction(async (tx) => {
    await tx.truncateIndex(name);
    const dirtyGroups = new Set<string>();
    await engine.source.scan(collection, async (pk, row) => {
      await engine.applyChangeInTx(
        runtime,
        { pk, op: 'insert', after: row, seq: resumeCursor },
        tx,
        dirtyGroups,
      );
    });
    for (const groupKey of dirtyGroups) {
      await engine.reReduceInTx(runtime, groupKey, tx);
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
  const byGroup = new Map<string, StoredEntry[]>();
  await engine.source.scan(runtime.def.collection, (pk, row) => {
    const mapped = normalizeMapOutput(runtime.name, runtime.def.map(row));
    for (const entry of toStoredEntries(runtime.def.reduce.groupBy, pk, mapped)) {
      const list = byGroup.get(entry.groupKey) ?? [];
      list.push(entry);
      byGroup.set(entry.groupKey, list);
    }
  });
  const expected = new Map<string, ReducedRow>();
  for (const [groupKey, entries] of byGroup) {
    expected.set(groupKey, reduceFull(runtime.aggregates, groupKey, entries));
  }
  return expected;
}
