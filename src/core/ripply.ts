/**
 * createRipply() — the public API (DESIGN.md §6).
 *
 * Wraps the Engine with:
 *  - a background processor: `start()` installs capture, brings every index
 *    up to date with its definition (map versioning), then keeps indexes
 *    fresh via Source wakeups with a poll-interval fallback
 *  - a typed query surface: `.index(name).where({...}).value('count')`,
 *    `.all()`, `.one()`, and `.entries()` for RavenDB-style drill-down
 *  - query-time derivation: `avg` is stored as `{ sum, count }` components
 *    and divided when read
 *
 * All engine work AND queries are serialized through one internal queue —
 * the reference and SQLite stores are effectively single-connection, so
 * overlapping transactions are a bug, not a feature.
 */

import { canonicalJson } from './canonical';
import {
  createEngine,
  type CreateEngineOptions,
  type Engine,
  type IndexRuntime,
} from './engine';
import { RipplyError } from './errors';
import type { VerifyResult } from './rebuild';
import type {
  AggregateSpec,
  Entry,
  IndexDefinition,
  MapFn,
  PkValue,
  ReducedRow,
  Row,
  Store,
  StoreTx,
  Unsubscribe,
} from './types';

export interface CreateRipplyOptions extends CreateEngineOptions {
  /** Fallback poll interval for the background processor, ms. Default 250. */
  pollInterval?: number;
  /** Called when a background drain fails. Default: console.error. */
  onError?: (error: unknown) => void;
  /**
   * After each drain, ask the Source to delete changelog entries that every
   * index on a collection has applied (when the Source supports `prune`).
   * Default true.
   */
  autoPrune?: boolean;
}

/**
 * Like IndexDefinition, but carries the aggregate output names (`TAgg`) and
 * the map's entry type (`TEntry`) so the query surface can be typed:
 * `.value()` only accepts real aggregate names, `.entries()` returns TEntry.
 */
export interface TypedIndexDefinition<
  TRow extends Row,
  TEntry extends Entry,
  TAgg extends string,
> {
  collection: string;
  map: MapFn<TRow, TEntry>;
  reduce: {
    groupBy: Array<Extract<keyof TEntry, string>>;
    aggregate: Record<TAgg, AggregateSpec>;
  };
  /** SQL indexes on the materialized tally table (groupBy/aggregate columns). */
  indexes?: Array<Array<Extract<keyof TEntry, string> | NoInfer<TAgg>>>;
}

/** @internal What the query surface needs from Ripply. */
interface HandleContext {
  runtimeOf(name: string): IndexRuntime;
  withTx<T>(name: string, fn: (tx: StoreTx) => Promise<T>): Promise<T>;
}

export class Ripply {
  readonly engine: Engine;
  private readonly store: Store;
  private readonly pollInterval: number;
  private readonly onError: (error: unknown) => void;
  private readonly autoPrune: boolean;

  private running = false;
  private timer: ReturnType<typeof setInterval> | null = null;
  private unsubs: Unsubscribe[] = [];
  private kickQueued = false;
  /** Serializes engine work and queries (single-connection semantics). */
  private queue: Promise<void> = Promise.resolve();

  private readonly context: HandleContext = {
    runtimeOf: (name) => this.engine.runtimeOf(name),
    withTx: (name, fn) =>
      this.runExclusive(async () => {
        await this.engine.ensureStorage(name);
        return this.store.transaction(fn);
      }),
  };

  constructor(options: CreateRipplyOptions) {
    this.engine = createEngine(options);
    this.store = options.store;
    this.pollInterval = options.pollInterval ?? 250;
    this.autoPrune = options.autoPrune ?? true;
    this.onError =
      options.onError ??
      ((error) => console.error('[ripply] background processing failed:', error));
  }

  defineIndex<
    TRow extends Row = Row,
    TEntry extends Entry = Entry,
    TAgg extends string = string,
  >(
    name: string,
    def: TypedIndexDefinition<TRow, TEntry, TAgg>,
  ): IndexQuery<TEntry, TAgg> {
    if (this.running) {
      throw new RipplyError(`defineIndex("${name}"): define indexes before start()`);
    }
    this.engine.defineIndex(name, def as unknown as IndexDefinition);
    return new IndexQuery<TEntry, TAgg>(this.context, name);
  }

  /** Query handle for a defined index. */
  index<TEntry extends Entry = Entry, TAgg extends string = string>(
    name: string,
  ): IndexQuery<TEntry, TAgg> {
    this.engine.runtimeOf(name); // validates the name
    return new IndexQuery<TEntry, TAgg>(this.context, name);
  }

  /**
   * Install capture, (re)build anything whose map version changed, catch up,
   * then keep processing in the background (wakeups + poll fallback).
   */
  async start(): Promise<void> {
    if (this.running) return;
    await this.runExclusive(() => this.engine.start());
    this.running = true;

    const collections = new Set(
      this.engine.indexNames().map((name) => this.engine.runtimeOf(name).def.collection),
    );
    if (this.engine.source.wakeups) {
      for (const collection of collections) {
        this.unsubs.push(this.engine.source.wakeups(collection, () => this.kick()));
      }
    }
    this.timer = setInterval(() => this.kick(), this.pollInterval);
    // don't hold the process open just to poll
    (this.timer as unknown as { unref?: () => void }).unref?.();
    this.kick();
  }

  /** Stop background processing and wait for in-flight work to finish. */
  async stop(): Promise<void> {
    this.running = false;
    if (this.timer !== null) {
      clearInterval(this.timer);
      this.timer = null;
    }
    for (const unsub of this.unsubs.splice(0)) unsub();
    await this.queue;
  }

  /** Process every index until fully caught up (explicit alternative to wakeups). */
  async drain(): Promise<number> {
    return this.runExclusive(async () => {
      const processed = await this.engine.drain();
      await this.pruneChangelogs();
      return processed;
    });
  }

  async rebuild(name: string): Promise<void> {
    return this.runExclusive(() => this.engine.rebuild(name));
  }

  async verify(name: string): Promise<VerifyResult> {
    return this.runExclusive(() => this.engine.verify(name));
  }

  // -------------------------------------------------------------------------

  private runExclusive<T>(fn: () => Promise<T>): Promise<T> {
    const result = this.queue.then(fn);
    this.queue = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }

  private kick(): void {
    if (!this.running || this.kickQueued) return;
    this.kickQueued = true;
    void this.runExclusive(async () => {
      this.kickQueued = false;
      if (!this.running) return;
      try {
        await this.engine.drain();
        await this.pruneChangelogs();
      } catch (error) {
        this.onError(error);
      }
    });
  }

  /** Delete changes every index on a collection has applied. */
  private async pruneChangelogs(): Promise<void> {
    if (!this.autoPrune || !this.engine.source.prune) return;
    const byCollection = new Map<string, string[]>();
    for (const name of this.engine.indexNames()) {
      const collection = this.engine.runtimeOf(name).def.collection;
      byCollection.set(collection, [...(byCollection.get(collection) ?? []), name]);
    }
    for (const [collection, names] of byCollection) {
      const cursors = await this.store.transaction(async (tx) => {
        const result = [];
        for (const name of names) result.push(await tx.getCursor(name));
        return result;
      });
      await this.engine.source.prune(collection, cursors);
    }
  }
}

/**
 * Query surface for one index. Immutable — `where()` returns a narrowed
 * copy. Matching is by groupBy fields, compared canonically.
 */
export class IndexQuery<TEntry extends Entry = Entry, TAgg extends string = string> {
  constructor(
    private readonly context: HandleContext,
    private readonly name: string,
    private readonly match: Record<string, unknown> = {},
  ) {}

  where(match: Partial<TEntry>): IndexQuery<TEntry, TAgg> {
    return new IndexQuery(this.context, this.name, { ...this.match, ...match });
  }

  /** One object per matching group: groupBy fields + derived aggregates. */
  async all(): Promise<Array<Record<string, unknown>>> {
    const runtime = this.context.runtimeOf(this.name);
    return (await this.matchedRows()).map((row) => deriveRow(runtime, row));
  }

  /** Exactly-one-group read: null when absent, throws when ambiguous. */
  async one(): Promise<Record<string, unknown> | null> {
    const runtime = this.context.runtimeOf(this.name);
    const rows = await this.matchedRows();
    if (rows.length === 0) return null;
    if (rows.length > 1) {
      throw new RipplyError(
        `index "${this.name}": ${rows.length} groups match ${JSON.stringify(this.match)} — narrow the where()`,
      );
    }
    return deriveRow(runtime, rows[0]!);
  }

  /** A single aggregate value from a single group (undefined when absent). */
  async value(field: TAgg): Promise<unknown> {
    const row = await this.one();
    return row ? row[field] : undefined;
  }

  /** Drill-down: the stored entries behind the matching groups, with pks. */
  async entries(): Promise<Array<{ pk: PkValue; entry: TEntry }>> {
    const entries = await this.context.withTx(this.name, (tx) =>
      tx.allEntries(this.name),
    );
    return entries
      .filter((entry) => this.matches((key) => entry.values[key]))
      .map((entry) => ({ pk: entry.pk, entry: entry.values as TEntry }));
  }

  private async matchedRows(): Promise<ReducedRow[]> {
    const rows = await this.context.withTx(this.name, (tx) =>
      tx.allReduced(this.name),
    );
    return rows.filter((row) => this.matches((key) => row.group[key]));
  }

  private matches(get: (key: string) => unknown): boolean {
    return Object.entries(this.match).every(
      ([key, expected]) => canonicalJson(get(key)) === canonicalJson(expected),
    );
  }
}

/** Group fields + aggregate outputs, with avg derived from its components. */
function deriveRow(runtime: IndexRuntime, row: ReducedRow): Record<string, unknown> {
  const result: Record<string, unknown> = { ...row.group };
  for (const agg of runtime.aggregates) {
    const value = row.values[agg.out];
    if (agg.fn === 'avg') {
      const components = value as { sum: number; count: number };
      result[agg.out] = components.count > 0 ? components.sum / components.count : null;
    } else {
      result[agg.out] = value;
    }
  }
  return result;
}

export function createRipply(options: CreateRipplyOptions): Ripply {
  return new Ripply(options);
}
