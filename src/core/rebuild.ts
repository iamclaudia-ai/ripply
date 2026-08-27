/**
 * Rebuild & verify (DESIGN.md §7).
 *
 * Rebuild: capture the feed position FIRST, then truncate + full scan +
 * resume from that position. Changes committed during the scan may be
 * replayed afterwards — reconcile-from-entries makes the replay converge,
 * so the pre-scan cursor is safe (never skips, only repeats).
 *
 * ## Streaming (issue #3)
 *
 * The scan is PAGED and folded incrementally: entry pages are flushed to
 * the store as they are produced, and only per-group accumulator state is
 * carried. Memory is O(groups + page) instead of O(source rows), which is
 * what makes a large historical backfill fit.
 *
 * This works even for non-linear indexes because a rebuild only ever
 * INSERTS. Non-linearity is a statement about retraction — you cannot pull
 * a value out of a `max` without re-reading the group — and building
 * forward from an empty index retracts nothing. See `createGroupFold`.
 *
 * ## Not one transaction any more
 *
 * The old rebuild did everything in a single store transaction. Folding
 * in memory kept that transaction short (writes only); streaming would put
 * the entire scan inside it, which on a hosted Postgres means a
 * multi-hour transaction — snapshot bloat, and a dropped connection costs
 * the whole run. So the work is committed in chunks:
 *
 *   1. truncate + resume cursor + a REBUILDING meta sentinel
 *   2. entry pages, one transaction each
 *   3. reduced rows, chunked
 *   4. the real map version — the commit that means "this index is good"
 *
 * The index reads empty/partial while this runs, and a crash leaves the
 * sentinel behind, which `ensureFresh` sees as a version mismatch and
 * rebuilds on the next `start()`. Do not drain an index concurrently with
 * its own rebuild.
 *
 * Verify: an independent from-scratch reduce over the source, diffed
 * against the maintained index. Ships as a dev/CI assertion; run it while
 * writes are quiesced or expect false positives from in-flight changes.
 */

import { createGroupFold, normalizeMapOutput, toStoredEntries, type GroupFold } from './aggregates';
import { canonicalJson, pkKeyOf } from './canonical';
import type { Engine, IndexRuntime } from './engine';
import type { ReducedRow, StoredEntry } from './types';
import { mapVersionOf } from './version';

export interface RebuildOptions {
  /**
   * Source rows' worth of entries buffered before a flush. Larger pages
   * mean fewer round trips; smaller pages mean less memory and shorter
   * transactions. Default 2000.
   */
  pageSize?: number;
}

const DEFAULT_PAGE_SIZE = 2000;
/** Reduced rows per transaction in step 3. */
const REDUCED_CHUNK = 1000;

/**
 * Stored while a rebuild is in flight. Can never equal a real map version
 * (which is a hash), so `ensureFresh` treats an interrupted rebuild as a
 * stale index and rebuilds it.
 */
const REBUILDING = '__rebuilding__';

export async function rebuildIndex(
  engine: Engine,
  name: string,
  options: RebuildOptions = {},
): Promise<void> {
  const runtime = engine.runtimeOf(name);
  const collection = runtime.def.collection;
  const pageSize = options.pageSize ?? DEFAULT_PAGE_SIZE;

  // BEFORE the scan — replayed overlap is idempotent, gaps would not be.
  const resumeCursor = await engine.source.currentCursor(collection);

  // 1. Empty the index and mark it as being rebuilt. The cursor moves now
  //    rather than at the end so an interrupted rebuild does not leave a
  //    stale position pointing at a backlog it would replay onto a partial
  //    index — the sentinel forces a fresh rebuild either way.
  await engine.store.transaction(async (tx) => {
    await tx.truncateIndex(name);
    await tx.setCursor(name, resumeCursor);
    await tx.setIndexMeta(name, { mapVersion: REBUILDING });
  });

  // 2. Page the scan: flush entries, carry only accumulators.
  const folds = new Map<string, GroupFold>();
  let page: StoredEntry[] = [];

  const flush = async (): Promise<void> => {
    if (page.length === 0) return;
    const batch = page;
    page = [];
    await engine.store.transaction((tx) => writeEntries(tx, name, batch));
  };

  await engine.source.scan(collection, async (pk, row) => {
    const mapped = normalizeMapOutput(runtime.name, runtime.def.map(row));
    for (const entry of toStoredEntries(runtime.def.reduce.groupBy, pk, mapped)) {
      page.push(entry);
      let fold = folds.get(entry.groupKey);
      if (!fold) {
        fold = createGroupFold(runtime.aggregates, entry.groupKey);
        folds.set(entry.groupKey, fold);
      }
      fold.add(entry);
    }
    // AFTER the row's entries, never inside — a pk's entries must not
    // straddle a page, or the fallback writer's replaceEntries would
    // delete the half already written.
    if (page.length >= pageSize) await flush();
  });
  await flush();

  // 3. Reduced rows, chunked.
  let chunk: ReducedRow[] = [];
  for (const fold of folds.values()) {
    chunk.push(fold.finish());
    if (chunk.length >= REDUCED_CHUNK) {
      const batch = chunk;
      chunk = [];
      await engine.store.transaction((tx) => writeReduced(tx, name, batch));
    }
  }
  if (chunk.length > 0) {
    await engine.store.transaction((tx) => writeReduced(tx, name, chunk));
  }

  // 4. The commit that publishes the index as fresh.
  await engine.store.transaction((tx) =>
    tx.setIndexMeta(name, { mapVersion: mapVersionOf(runtime.def) }),
  );
}

type Tx = Parameters<Parameters<Engine['store']['transaction']>[0]>[0];

async function writeEntries(tx: Tx, name: string, entries: StoredEntry[]): Promise<void> {
  if (tx.insertEntries) {
    await tx.insertEntries(name, entries);
    return;
  }
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

async function writeReduced(tx: Tx, name: string, rows: ReducedRow[]): Promise<void> {
  if (tx.putReducedMany) {
    await tx.putReducedMany(name, rows);
    return;
  }
  for (const row of rows) await tx.putReduced(name, row);
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

export async function verifyIndex(engine: Engine, name: string): Promise<VerifyResult> {
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
    ok: missingGroups.length === 0 && extraGroups.length === 0 && mismatchedGroups.length === 0,
    missingGroups,
    extraGroups,
    mismatchedGroups,
  };
}

/**
 * From-scratch reduce: scan → map → fold, no store involved and no entries
 * held. Memory is O(groups), so verifying right after a large import —
 * exactly when you want to — works at the same scale the rebuild does.
 */
async function expectedReduced(
  engine: Engine,
  runtime: IndexRuntime,
): Promise<Map<string, ReducedRow>> {
  const folds = new Map<string, GroupFold>();
  await engine.source.scan(runtime.def.collection, (pk, row) => {
    const mapped = normalizeMapOutput(runtime.name, runtime.def.map(row));
    for (const entry of toStoredEntries(runtime.def.reduce.groupBy, pk, mapped)) {
      let fold = folds.get(entry.groupKey);
      if (!fold) {
        fold = createGroupFold(runtime.aggregates, entry.groupKey);
        folds.set(entry.groupKey, fold);
      }
      fold.add(entry);
    }
  });

  const expected = new Map<string, ReducedRow>();
  for (const [groupKey, fold] of folds) expected.set(groupKey, fold.finish());
  return expected;
}
