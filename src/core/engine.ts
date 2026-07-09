/**
 * The Ripply engine — backend-free.
 *
 * The reconcile-from-stored-entries loop (DESIGN.md §1):
 *
 *   poll → map → readEntries (the row's CURRENT contribution, from our own
 *   entries table) → per-group linear delta / dirty-mark → replaceEntries →
 *   re-reduce dirty groups → delete empty groups → setCursor
 *
 * …all inside ONE Store transaction. Cursor advance and index writes commit
 * together ⇒ exactly-once when Source and Store are co-located; and because
 * the retraction comes from durable state (not a captured before-image),
 * reprocessing any change is idempotent by construction.
 */

import {
  applyLinearDelta,
  isLinear,
  normalizeMapOutput,
  parseReduceSpec,
  reduceFull,
  toStoredEntries,
  type ParsedAggregate,
} from './aggregates';
import { RipplyError } from './errors';
import { rebuildIndex, verifyIndex, type VerifyResult } from './rebuild';
import type {
  Change,
  IndexDefinition,
  Source,
  Store,
  StoreTx,
} from './types';
import { mapVersionOf } from './version';

export interface CreateEngineOptions {
  source: Source;
  store: Store;
  /** Max changes consumed per transaction. Default 500. */
  batchSize?: number;
}

/** @internal Parsed, validated runtime record for one index. */
export interface IndexRuntime {
  name: string;
  def: IndexDefinition;
  aggregates: ParsedAggregate[];
  /** True when every aggregate accepts signed deltas (O(1) path). */
  linear: boolean;
  /** Hash of collection + map source + reduce spec (invariant 10). */
  version: string;
}

export class Engine {
  readonly source: Source;
  readonly store: Store;
  private readonly batchSize: number;
  private readonly indexes = new Map<string, IndexRuntime>();

  constructor(options: CreateEngineOptions) {
    this.source = options.source;
    this.store = options.store;
    this.batchSize = options.batchSize ?? 500;
    if (this.batchSize < 1) throw new RipplyError('batchSize must be >= 1');
  }

  defineIndex(name: string, def: IndexDefinition): void {
    if (this.indexes.has(name)) {
      throw new RipplyError(`index "${name}" is already defined`);
    }
    if (!def.collection) {
      throw new RipplyError(`index "${name}": collection is required`);
    }
    if (typeof def.map !== 'function') {
      throw new RipplyError(`index "${name}": map must be a function`);
    }
    const aggregates = parseReduceSpec(def.reduce);
    this.indexes.set(name, {
      name,
      def,
      aggregates,
      linear: isLinear(aggregates),
      version: mapVersionOf(def),
    });
  }

  indexNames(): string[] {
    return [...this.indexes.keys()];
  }

  /** @internal */
  runtimeOf(name: string): IndexRuntime {
    const runtime = this.indexes.get(name);
    if (!runtime) throw new RipplyError(`unknown index "${name}"`);
    return runtime;
  }

  /**
   * Install change capture and bring every index up to date with its
   * definition: a missing or mismatched map version triggers a (re)build —
   * a stale index is never served as fresh (invariant 10).
   */
  async start(): Promise<void> {
    const collections = new Set(
      [...this.indexes.values()].map((ix) => ix.def.collection),
    );
    for (const collection of collections) {
      await this.source.install(collection);
    }
    for (const name of this.indexes.keys()) {
      await this.ensureFresh(name);
    }
  }

  /** Rebuild iff the stored map version differs from the definition's. */
  async ensureFresh(name: string): Promise<void> {
    const runtime = this.runtimeOf(name);
    const meta = await this.store.transaction((tx) => tx.getIndexMeta(name));
    if (meta?.mapVersion !== runtime.version) {
      await this.rebuild(name);
    }
  }

  /**
   * Consume one batch of changes for one index. Returns the number of
   * changes applied (0 = caught up).
   */
  async process(name: string): Promise<number> {
    const runtime = this.runtimeOf(name);
    return this.store.transaction(async (tx) => {
      const cursor = await tx.getCursor(name);
      const batch = await this.source.poll(
        runtime.def.collection,
        cursor,
        this.batchSize,
      );
      if (batch.changes.length === 0) return 0;

      const dirtyGroups = new Set<string>();
      for (const change of batch.changes) {
        await this.applyChangeInTx(runtime, change, tx, dirtyGroups);
      }
      // non-linear groups re-reduce once per batch, from entries only
      for (const groupKey of dirtyGroups) {
        await this.reReduceInTx(runtime, groupKey, tx);
      }
      await tx.setCursor(name, batch.nextCursor);
      return batch.changes.length;
    });
  }

  /** Process until every index (or one named index) is fully caught up. */
  async drain(name?: string): Promise<number> {
    const names = name ? [name] : [...this.indexes.keys()];
    let total = 0;
    for (;;) {
      let round = 0;
      for (const indexName of names) {
        round += await this.process(indexName);
      }
      if (round === 0) return total;
      total += round;
    }
  }

  /** Truncate + full scan + resume (DESIGN.md §7). */
  async rebuild(name: string): Promise<void> {
    return rebuildIndex(this, name);
  }

  /** From-scratch reduce vs. the maintained index (dev/CI assertion). */
  async verify(name: string): Promise<VerifyResult> {
    return verifyIndex(this, name);
  }

  /**
   * @internal The reconcile step for one change. Also used by rebuild
   * (which replays scanned rows as inserts through this same code path).
   */
  async applyChangeInTx(
    runtime: IndexRuntime,
    change: Change,
    tx: StoreTx,
    dirtyGroups: Set<string>,
  ): Promise<void> {
    if (change.op !== 'delete' && change.after === null) {
      throw new RipplyError(
        `index "${runtime.name}": ${change.op} change for pk ${JSON.stringify(change.pk)} has no after-image`,
      );
    }

    // Delete is just "no entries" — retraction comes from stored state.
    const mapped =
      change.op === 'delete'
        ? []
        : normalizeMapOutput(runtime.name, runtime.def.map(change.after!));
    const newEntries = toStoredEntries(runtime.def.reduce.groupBy, change.pk, mapped);

    // The row's current contribution, from OUR entries table — never from a
    // captured before-image. Re-applying the same change reads back what we
    // wrote and reconciles to a zero delta: idempotent by construction.
    const oldEntries = await tx.readEntries(runtime.name, change.pk);
    await tx.replaceEntries(runtime.name, change.pk, newEntries);

    const affectedGroups = new Set<string>(
      [...oldEntries, ...newEntries].map((entry) => entry.groupKey),
    );

    if (!runtime.linear) {
      for (const groupKey of affectedGroups) dirtyGroups.add(groupKey);
      return;
    }

    // All-linear index: apply signed deltas, O(1) per group.
    for (const groupKey of affectedGroups) {
      const next = applyLinearDelta(
        runtime.aggregates,
        groupKey,
        await tx.getReduced(runtime.name, groupKey),
        oldEntries.filter((e) => e.groupKey === groupKey).map((e) => e.values),
        newEntries.filter((e) => e.groupKey === groupKey).map((e) => e.values),
      );
      if (next.entryCount <= 0) {
        // decrement-to-zero DELETES the row — no ghost groups (invariant 4)
        await tx.deleteReduced(runtime.name, groupKey);
      } else {
        await tx.putReduced(runtime.name, next);
      }
    }
  }

  /** @internal Recompute one group from its (post-replace) entries. */
  async reReduceInTx(
    runtime: IndexRuntime,
    groupKey: string,
    tx: StoreTx,
  ): Promise<void> {
    const entries = await tx.readGroupEntries(runtime.name, groupKey);
    if (entries.length === 0) {
      await tx.deleteReduced(runtime.name, groupKey);
      return;
    }
    await tx.putReduced(
      runtime.name,
      reduceFull(runtime.aggregates, groupKey, entries),
    );
  }
}

export function createEngine(options: CreateEngineOptions): Engine {
  return new Engine(options);
}
