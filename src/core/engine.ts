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
import { tallyTableOf } from './canonical';
import { RipplyError } from './errors';
import { rebuildIndex, verifyIndex, type VerifyResult } from './rebuild';
import type { Change, IndexDefinition, Source, Store, StoreTx } from './types';
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
  private readonly ensuredStorage = new Set<string>();

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

    // The reduced output doubles as a real table (groupBy fields and
    // aggregate outputs become columns) — reject shapes that can't.
    const RESERVED = new Set(['group_key', 'entry_count', 'vals']);
    const columns = new Set(def.reduce.groupBy);
    for (const { out } of aggregates) {
      if (columns.has(out)) {
        throw new RipplyError(`index "${name}": aggregate "${out}" collides with a groupBy field`);
      }
      columns.add(out);
    }
    for (const column of columns) {
      if (RESERVED.has(column)) {
        throw new RipplyError(`index "${name}": "${column}" is a reserved column name`);
      }
    }
    for (const sqlIndex of def.indexes ?? []) {
      for (const column of sqlIndex) {
        if (!columns.has(column)) {
          throw new RipplyError(
            `index "${name}": SQL index column "${column}" is not a groupBy field or aggregate output`,
          );
        }
      }
    }
    // columnTypes may also target avg component columns (<out>_sum/<out>_count)
    const typedColumns = new Set(columns);
    for (const { out, fn } of aggregates) {
      if (fn === 'avg') {
        typedColumns.add(`${out}_sum`);
        typedColumns.add(`${out}_count`);
      }
    }
    for (const column of Object.keys(def.columnTypes ?? {})) {
      if (!typedColumns.has(column)) {
        throw new RipplyError(`index "${name}": columnTypes key "${column}" is not a tally column`);
      }
    }

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
   * a stale index is never served as fresh (invariant 10). Indexes are
   * handled in cascade (topological) order — upstream tally tables exist
   * and are fresh before a downstream index scans them.
   */
  async start(): Promise<void> {
    const order = this.cascadeOrder();
    for (const name of order) {
      // materialize BEFORE install: a downstream index installs capture on
      // an upstream tally table, which must exist first
      await this.ensureStorage(name);
    }
    const collections = new Set(order.map((name) => this.runtimeOf(name).def.collection));
    for (const collection of collections) {
      await this.source.install(collection);
    }
    for (const name of order) {
      await this.ensureFresh(name);
    }
  }

  /**
   * Indexes in dependency order: an index whose collection is another
   * index's tally table (`ripply_<name>`, see `tallyTableOf`) processes
   * after its upstream. Throws on cycles — a cascade loop would otherwise
   * drain forever.
   */
  cascadeOrder(): string[] {
    const byTallyTable = new Map<string, string>();
    for (const name of this.indexes.keys()) byTallyTable.set(tallyTableOf(name), name);

    const order: string[] = [];
    const state = new Map<string, 'visiting' | 'done'>();
    const visit = (name: string, path: string[]): void => {
      const mark = state.get(name);
      if (mark === 'visiting') {
        throw new RipplyError(`cascading index cycle: ${[...path, name].join(' → ')}`);
      }
      if (mark === 'done') return;
      state.set(name, 'visiting');
      const upstream = byTallyTable.get(this.runtimeOf(name).def.collection);
      if (upstream) visit(upstream, [...path, name]);
      state.set(name, 'done');
      order.push(name);
    };
    for (const name of this.indexes.keys()) visit(name, []);
    return order;
  }

  /** Prepare physical storage (materialized tally table) exactly once. */
  async ensureStorage(name: string): Promise<void> {
    if (this.ensuredStorage.has(name)) return;
    const runtime = this.runtimeOf(name);
    if (this.store.ensureIndex) {
      await this.store.ensureIndex(name, {
        groupBy: [...runtime.def.reduce.groupBy],
        aggregates: runtime.aggregates.map(({ out, fn }) => ({ out, fn })),
        sqlIndexes: (runtime.def.indexes ?? []).map((columns) => [...columns]),
        columnTypes: { ...runtime.def.columnTypes },
      });
    }
    this.ensuredStorage.add(name);
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
    await this.ensureStorage(name);
    return this.store.transaction(async (tx) => {
      const cursor = await tx.getCursor(name);
      const batch = await this.source.poll(runtime.def.collection, cursor, this.batchSize);
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
    // cascade order lets a day→month chain settle in fewer rounds; the
    // until-quiet loop guarantees convergence regardless
    const names = name ? [name] : this.cascadeOrder();
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
    await this.ensureStorage(name);
    return rebuildIndex(this, name);
  }

  /** From-scratch reduce vs. the maintained index (dev/CI assertion). */
  async verify(name: string): Promise<VerifyResult> {
    await this.ensureStorage(name);
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
  async reReduceInTx(runtime: IndexRuntime, groupKey: string, tx: StoreTx): Promise<void> {
    const entries = await tx.readGroupEntries(runtime.name, groupKey);
    if (entries.length === 0) {
      await tx.deleteReduced(runtime.name, groupKey);
      return;
    }
    await tx.putReduced(runtime.name, reduceFull(runtime.aggregates, groupKey, entries));
  }
}

export function createEngine(options: CreateEngineOptions): Engine {
  return new Engine(options);
}
