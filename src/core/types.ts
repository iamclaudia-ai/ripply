/**
 * Core vocabulary for Ripply.
 *
 * Everything in `src/core/` is backend-free: zero dependencies, zero knowledge
 * of any database. Adapters implement `Source` (change capture) and `Store`
 * (entries + reduced + cursors); the engine only speaks these interfaces.
 */

/** Scalar values allowed in primary keys. */
export type Scalar = string | number | boolean | null;

/** A source row: the after-image from capture, or a scanned row. */
export type Row = Record<string, unknown>;

/**
 * Composite primary-key value, ordered to match the collection's declared
 * pk columns. Single-column keys are one-element arrays.
 */
export type PkValue = Scalar[];

/**
 * Change-feed position. Backend-specific: memory and SQLite use an integer
 * seq, Postgres CDC uses an LSN string. `null` means "the beginning".
 */
export type Cursor = number | string | null;

/**
 * One unit of map output: the fields named by `reduce.groupBy` plus the
 * input fields for aggregates. A source row contributes 0..N entries.
 */
export type Entry = Record<string, unknown>;

/**
 * Map function. Multi-emit (RavenDB SelectMany): return an array for
 * multiple entries, a single object for one, or null/undefined for none.
 */
export type MapFn<TRow extends Row = Row, TEntry extends Entry = Entry> = (
  row: TRow,
) => TEntry | TEntry[] | null | undefined;

/** Linear aggregates accept signed deltas in O(1) per change. */
export type LinearAggregateFn = 'sum' | 'count' | 'avg';

/**
 * Non-linear aggregates cannot be maintained from a reduced scalar alone
 * (you can't recover the new min once the old min leaves) — affected groups
 * are marked dirty and re-reduced from that group's stored entries.
 */
export type NonLinearAggregateFn = 'min' | 'max' | 'first' | 'last' | 'distinct';

export type AggregateFn = LinearAggregateFn | NonLinearAggregateFn;

/**
 * How one output field of a reduced row is computed:
 *
 * - `'sum'` (string shorthand) — aggregate over the entry field with the
 *   same name as the output field. `'count'` counts entries.
 * - `{ max: 'revenue' }` — aggregate over an explicitly named entry field.
 *   `{ count: 'field' }` counts entries whose field is non-null.
 */
export type AggregateSpec =
  | AggregateFn
  | { [F in AggregateFn]: Record<F, string> }[AggregateFn];

export interface ReduceSpec {
  /** Entry fields that form the group key (canonical JSON, sorted keys). */
  groupBy: string[];
  /** Output field name → how to compute it. */
  aggregate: Record<string, AggregateSpec>;
}

export interface IndexDefinition<TRow extends Row = Row, TEntry extends Entry = Entry> {
  collection: string;
  map: MapFn<TRow, TEntry>;
  reduce: ReduceSpec;
  /**
   * Optional SQL indexes on the materialized tally table, each an array of
   * tally columns (groupBy fields and/or aggregate outputs). Adapters
   * without materialized tables ignore this.
   */
  indexes?: string[][];
  /**
   * Optional SQL column types for the materialized tally table, keyed by
   * tally column: a groupBy field, an aggregate output, or an avg component
   * (`<out>_sum` / `<out>_count`). Typed adapters (Postgres) default groupBy
   * and `first`/`last` outputs to `text` and numeric aggregates to numeric
   * types — override here when you know better (`{ player_id: 'bigint' }`).
   * Dynamically-typed adapters (SQLite) use these as type affinities.
   */
  columnTypes?: Record<string, string>;
}

/**
 * The physical shape of an index's reduced output, handed to
 * `Store.ensureIndex` so SQL stores can materialize a real, queryable
 * tally table.
 */
export interface IndexSchema {
  groupBy: string[];
  aggregates: Array<{ out: string; fn: AggregateFn }>;
  sqlIndexes: string[][];
  /** Declared column-type overrides (validated by the engine). */
  columnTypes: Record<string, string>;
}

// ---------------------------------------------------------------------------
// Source — change capture (backend-specific)
// ---------------------------------------------------------------------------

/**
 * One captured change. Note there is NO before-image: retraction is
 * reconciled from the store's own entries table, so capture stays tiny.
 */
export interface Change {
  pk: PkValue;
  op: 'insert' | 'update' | 'delete';
  /** After-image of the row; null for deletes. */
  after: Row | null;
  /** Feed position of this change. */
  seq: Cursor;
}

export interface ChangeBatch {
  changes: Change[];
  /** Position to persist after applying `changes`; unchanged cursor if empty. */
  nextCursor: Cursor;
}

export type Unsubscribe = () => void;

export interface Source {
  /**
   * Install change capture for a collection (triggers, outbox, slot…).
   * The collection's pk columns come from the adapter's own configuration.
   * Must be idempotent.
   */
  install(collection: string): Promise<void>;

  /** Read up to `limit` changes strictly after `cursor` (`null` = beginning). */
  poll(collection: string, cursor: Cursor, limit: number): Promise<ChangeBatch>;

  /** Full-table scan, used by rebuild. */
  scan(
    collection: string,
    onRow: (pk: PkValue, row: Row) => void | Promise<void>,
  ): Promise<void>;

  /**
   * The feed position "as of now". Rebuild captures this BEFORE scanning and
   * resumes from it — changes committed during the scan may be replayed, and
   * reconcile-from-entries makes that replay converge (idempotent).
   */
  currentCursor(collection: string): Promise<Cursor>;

  /** Optional low-latency wakeup (pg_notify, sqlite update_hook…). */
  wakeups?(collection: string, onChange: () => void): Unsubscribe;

  /**
   * Optional changelog hygiene: given the cursors of every index reading
   * this collection, delete changes all of them have already applied.
   * The adapter owns cursor ordering (numeric seq, LSN…); a null cursor
   * (an index that never processed) must prune nothing. Returns the number
   * of pruned changes.
   */
  prune?(collection: string, cursors: Cursor[]): Promise<number>;
}

// ---------------------------------------------------------------------------
// Store — entries + reduced + cursors (backend-specific)
// ---------------------------------------------------------------------------

/**
 * One stored intermediate map output: what a source row currently
 * contributes to an index. The RavenDB reduce-bucket leaf. This is what
 * makes retraction, non-linear aggregates, and drill-down possible.
 */
export interface StoredEntry {
  pk: PkValue;
  /** Position within the row's map output (multi-emit ordering). */
  ord: number;
  /** Canonical group key this entry belongs to (denormalized at write). */
  groupKey: string;
  /** The entry as returned by `map`. */
  values: Entry;
}

/** Final aggregate for one group of one index. */
export interface ReducedRow {
  /** Canonical group key (sorted-key JSON of the groupBy fields). */
  groupKey: string;
  /** The groupBy fields, parsed back out of the key for convenient reads. */
  group: Record<string, unknown>;
  /**
   * Aggregate outputs by field name. Internal representation: `avg` is
   * stored as `{ sum, count }` components and derived at query time.
   */
  values: Record<string, unknown>;
  /** Number of entries contributing to this group; 0 ⇒ the row is deleted. */
  entryCount: number;
}

/** Per-index metadata persisted alongside the cursor. */
export interface IndexMeta {
  /** Hash of the map fn source + reduce spec; mismatch at start ⇒ rebuild. */
  mapVersion: string;
}

/**
 * All Store operations happen inside a transaction. Cursor advance and index
 * writes share one transaction ⇒ exactly-once when Source and Store are
 * co-located; reconcile-from-entries keeps replays idempotent otherwise.
 */
export interface StoreTx {
  /** A row's current contribution to an index, ordered by `ord`. */
  readEntries(index: string, pk: PkValue): Promise<StoredEntry[]>;
  /** Replace a row's contribution wholesale (empty array removes it). */
  replaceEntries(index: string, pk: PkValue, entries: StoredEntry[]): Promise<void>;
  /** All entries currently in a group (post-replace state) — for re-reduce. */
  readGroupEntries(index: string, groupKey: string): Promise<StoredEntry[]>;
  /** Every entry of an index (verify/tests/drill-down). */
  allEntries(index: string): Promise<StoredEntry[]>;

  getReduced(index: string, groupKey: string): Promise<ReducedRow | null>;
  putReduced(index: string, row: ReducedRow): Promise<void>;
  deleteReduced(index: string, groupKey: string): Promise<void>;
  /** Every reduced row of an index (query surface/verify/tests). */
  allReduced(index: string): Promise<ReducedRow[]>;

  /** Drop all entries + reduced rows for an index (rebuild). */
  truncateIndex(index: string): Promise<void>;

  getCursor(index: string): Promise<Cursor>;
  setCursor(index: string, cursor: Cursor): Promise<void>;

  getIndexMeta(index: string): Promise<IndexMeta | null>;
  setIndexMeta(index: string, meta: IndexMeta): Promise<void>;

  // --- optional bulk fast paths (used by rebuild) ---------------------------

  /**
   * Insert many entries at once into an index KNOWN TO BE EMPTY for their
   * pks (rebuild calls this right after truncateIndex). SQL stores batch
   * these into multi-row inserts — the difference between seconds and
   * tens of minutes when the database is remote.
   */
  insertEntries?(index: string, entries: StoredEntry[]): Promise<void>;

  /** Upsert many reduced rows at once. Group keys MUST be distinct. */
  putReducedMany?(index: string, rows: ReducedRow[]): Promise<void>;
}

export interface Store {
  /** Run `fn` atomically: all writes commit together or roll back together. */
  transaction<T>(fn: (tx: StoreTx) => Promise<T>): Promise<T>;

  /**
   * Optional: prepare physical storage for an index before any reads or
   * writes. SQL stores use this to materialize the reduced output as a
   * REAL table (`ripply_<name>`) with the groupBy fields and aggregate
   * outputs as columns — queryable by any SQL client with no Ripply code,
   * and a valid `Source` collection for cascading indexes. Must be
   * idempotent; a shape change may drop and recreate (the map-version
   * rebuild repopulates it).
   */
  ensureIndex?(name: string, schema: IndexSchema): Promise<void>;
}
