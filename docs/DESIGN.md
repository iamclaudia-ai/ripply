# Ripply — Design

**Status:** Accepted (extracted from Spark RFC 010, adapted for standalone life)
**Created:** 2026-07-09
**Authors:** Michael & Claudia

## Summary

Ripply keeps pre-computed aggregates fresh in real time, backed by **SQLite or
Postgres**, with **no dependency on any framework**. You point it at existing
tables, declare indexes (map + reduce), and it maintains the aggregates
incrementally as rows change — RavenDB-style, as a small library you drop into
any TS/Bun/Node app.

The engine is storage-agnostic. Two things vary by backend:

- **Change capture (Source):** how we learn a row changed.
  - **SQLite:** triggers → append-only changelog table (required; SQLite has no
    logical replication).
  - **Postgres:** trigger-outbox (default) *or* logical-decoding CDC (opt-in).
- **Aggregate store (Store):** where entries + reduced results live (same DB as
  the source = atomic & exactly-once; different DB = idempotent at-least-once).

## Goals

- Real-time, incremental aggregates (counts/sums/etc. by group) that never
  require a `GROUP BY` scan at query time.
- One engine, pluggable Source + Store. SQLite and Postgres are first-class.
- Correct under crashes and reprocessing (idempotent by construction).
- RavenDB's drill-down: query not just the reduced value but the intermediate
  entries that produced it.
- Small, testable, no framework lock-in. Spark's action log can be a Source
  later (adapter lives on Spark's side, not here).

## Non-Goals (v1)

- Joins across collections / cross-collection indexes (future).
- Distributed/multi-node operation. Single-process engine; the *database* may be
  shared, the engine is one worker.
- A query language. Query API is group-key lookup + the stored aggregate fields.
- Streaming windows / time-decay. (Time buckets via `groupBy` on a derived field
  are fine; sliding windows are out.)

---

## 1. Core Model

An index is a **map** function plus a **reduce** spec:

```ts
map:    (row) => Entry | Entry[] | null     // 0..N entries per source row
reduce: { groupBy: string[], aggregate: {...} }
```

The engine maintains two tables per index:

- **entries** — the intermediate map output: *what each source row currently
  contributes.* Keyed by `(index, source_pk, entry_ord)`. This is the RavenDB
  reduce-bucket leaf, and it's what makes drill-down and non-linear aggregates
  possible.
- **reduced** — the final aggregate per group. Keyed by `(index, group_key)`.

### The central trick: reconcile from stored entries

When a row changes, we do **not** trust a captured "before" image to compute the
retraction. Instead:

```
newEntries = row.deleted ? [] : normalizeToArray(map(row.after))
oldEntries = store.readEntries(index, row.pk)     // our own record of prior contribution

// reconcile oldEntries → newEntries per group:
//   linear aggregates:     reduced[g] += Σ(new in g) − Σ(old in g)
//   non-linear aggregates: mark affected groups dirty, re-reduce from entries
store.replaceEntries(index, row.pk, newEntries)
recomputeDirtyGroups()
```

Why this matters:

1. **Capture only needs `(pk, after-image, isDelete)`.** No before-image
   required. In Postgres that means `REPLICA IDENTITY DEFAULT` (PK only) is
   enough for CDC — no `REPLICA IDENTITY FULL`, a real ops simplification.
2. **Idempotent by construction.** Reprocessing the same change recomputes
   `newEntries`, finds `oldEntries == newEntries`, applies a zero delta. Replays,
   crashes mid-batch, and at-least-once delivery are all safe.
3. **Delete is just `newEntries = []`** — retraction comes entirely from stored
   entries. Uniform code path for insert/update/delete.

This is the Z-set / differential-dataflow formulation (map(before) as
retraction, map(after) as assertion) with the retraction sourced from durable
state instead of the change feed. Same math, smaller capture surface.

---

## 2. The Three Interfaces

```ts
// What produces changes. Backend-specific. (pk columns come from the
// adapter's own configuration, so install() only names the collection.)
interface Source {
  install(collection: string): Promise<void>;         // triggers / slot / publication
  poll(collection: string, cursor: Cursor, limit: number): Promise<ChangeBatch>;
  scan(collection: string, onRow: (pk: PkValue, row: Row) => void): Promise<void>; // full rebuild
  currentCursor(collection: string): Promise<Cursor>; // feed position "now" — rebuild resumes here
  wakeups?(collection: string, onChange: () => void): Unsubscribe;    // NOTIFY / update_hook (optional)
}

interface Change { pk: PkValue; op: 'insert'|'update'|'delete'; after: Row | null; seq: Cursor; }
interface ChangeBatch { changes: Change[]; nextCursor: Cursor; }

// Where entries + reduced + cursors live. Backend-specific — and
// deliberately DUMB: pure keyed storage. All aggregate math (linear deltas,
// dirty-group re-reduce, delete-when-empty) lives in the ENGINE, so adapters
// never reimplement reduce semantics and cannot drift from each other.
// (Phase 0 refinement of this sketch's earlier applyReducedDelta/
// reReduceGroup methods, which would have pushed aggregate logic into every
// adapter.)
interface Store {
  transaction<T>(fn: (tx: StoreTx) => Promise<T>): Promise<T>;
}
interface StoreTx {
  readEntries(index, pk): StoredEntry[];              // the row's current contribution
  replaceEntries(index, pk, entries: StoredEntry[]): void;
  readGroupEntries(index, groupKey): StoredEntry[];   // for non-linear re-reduce
  allEntries(index): StoredEntry[];                   // drill-down / verify
  getReduced(index, groupKey): ReducedRow | null;
  putReduced(index, row: ReducedRow): void;
  deleteReduced(index, groupKey): void;
  allReduced(index): ReducedRow[];                    // query surface
  truncateIndex(index): void;                         // rebuild
  getCursor(index): Cursor;
  setCursor(index, cursor): void;
  getIndexMeta(index): IndexMeta | null;              // map-version hash
  setIndexMeta(index, meta): void;
}
```

The **Engine** is backend-free:

```
process(index):
  store.transaction(tx =>
    batch = source.poll(index.collection, tx.getCursor(index), BATCH)
    for change in batch.changes: applyChange(index, change, tx)   // reconcile-from-entries
    tx.setCursor(index, batch.nextCursor)
  )
```

Cursor advance and index writes share **one Store transaction** → exactly-once
when Source and Store are the same database.

---

## 3. Change Capture per Backend

| Backend | Mechanism | Consistency | Ops burden | Needs before-image? |
|---|---|---|---|---|
| **SQLite** | Triggers → changelog table | Strict (single writer) | None | No (PK + after) |
| **Postgres (default)** | Trigger → outbox table (+ `pg_notify`) | Strict, txn-local | Low (triggers) | No |
| **Postgres (opt-in)** | Logical decoding (wal2json/pgoutput) | Commit-ordered, async | Higher (slot, `wal_level=logical`) | No (REPLICA IDENTITY DEFAULT) |

### SQLite — triggers (required)

SQLite has no CDC, so triggers are the only option. At `install()` we introspect
columns (`PRAGMA table_info`) and generate INSERT/UPDATE/DELETE triggers that
append to a changelog:

```sql
CREATE TABLE _ripply_changelog (
  seq        INTEGER PRIMARY KEY AUTOINCREMENT,   -- monotonic, = commit order (single writer)
  collection TEXT NOT NULL,
  pk         TEXT NOT NULL,     -- JSON
  op         TEXT NOT NULL,     -- insert|update|delete
  after      TEXT               -- json_object(...) of NEW; NULL on delete
);

CREATE TRIGGER _ripply_work_orders_ai AFTER INSERT ON work_orders BEGIN
  INSERT INTO _ripply_changelog(collection, pk, op, after)
  VALUES ('work_orders', json_array(NEW.id), 'insert',
          json_object('id',NEW.id,'status',NEW.status,'revenue',NEW.revenue /* … */));
END;
-- au (UPDATE) and ad (DELETE, after=NULL) analogous
```

Wakeup: SQLite `update_hook` on the engine's connection can fire the processor
immediately; otherwise poll (default 100–250ms).

### Postgres — trigger-outbox (default)

Same idea, one generic trigger function using `to_jsonb(NEW)` (no per-table
column enumeration):

```sql
CREATE FUNCTION _ripply_capture() RETURNS trigger AS $$
DECLARE
  rec jsonb; pk jsonb := '[]'::jsonb; col text;
BEGIN
  rec := CASE WHEN TG_OP='DELETE' THEN to_jsonb(OLD) ELSE to_jsonb(NEW) END;
  FOREACH col IN ARRAY TG_ARGV LOOP        -- pk columns from install()
    pk := pk || jsonb_build_array(rec -> col);
  END LOOP;
  INSERT INTO _ripply_changelog(collection, pk, op, after)
  VALUES (TG_TABLE_NAME, pk, lower(TG_OP),
          CASE WHEN TG_OP='DELETE' THEN NULL ELSE rec END);
  PERFORM pg_notify('_ripply', TG_TABLE_NAME);  -- low-latency wakeup
  RETURN NULL;
END $$ LANGUAGE plpgsql;
```

The change is captured in the **same transaction** as the write → no gap, no
slot. The changelog also stores `txid xid8 DEFAULT pg_current_xact_id()` —
see below. (Wakeups via `LISTEN _ripply` are future work: poolers like
Neon's don't carry LISTEN reliably, so the poll fallback is the default.)

**Ordering gotcha (SOLVED — snapshot-windowed cursors):**
`BIGSERIAL`/identity values are assigned at INSERT time but transactions
commit in any order (a lower seq may commit after a higher one), so a naive
high-water `seq > cursor` poll can observe seq 5 and advance past a
still-in-flight seq 3, skipping it permanently. The shipped solution keeps
the shared read-only changelog AND per-index cursors (so multiple indexes
fan out over one outbox) by making the cursor a JSON string of
`pg_snapshot` values, `{ prev, cur, seq }`:

- a poll freezes a **window**: rows `pg_visible_in_snapshot(txid, cur)` and
  NOT visible in `prev` — an immutable set, because snapshots are immutable;
- the window drains in `seq` order (`seq` tracks progress within it; per-row
  effect order == seq order because row locks serialize writers per row);
- when exhausted, `prev ← cur` and a fresh `cur` opens.

A transaction that grabbed a small seq but committed late is not *visible*
in the frozen window — it lands in a **later window** instead of being
skipped. Late commits are unlosable by construction (invariant 9, proven by
a deterministic held-transaction test plus a concurrent-writers stress
test). `prune()` deletes a change once every index's cursor covers it:
visible in its `prev`, or inside its frozen window at `seq <=` its seq.

(Alternatives considered: consume-and-delete inside the processor tx —
breaks multi-index sharing; a separate per-collection drain worker — more
moving parts. The windowed cursor needs neither.)

### Postgres — logical decoding CDC (opt-in)

For high write volume, "don't touch my schema," or store-in-a-different-DB:
subscribe to a replication slot (wal2json or `pgoutput`). **LSN cursors are
commit-ordered**, which sidesteps the outbox ordering gotcha entirely — a strong
point in CDC's favor. Cost: `wal_level=logical`, slot management (WAL bloats if
the consumer stalls), and eventual consistency. Because we retract from stored
entries, `REPLICA IDENTITY DEFAULT` (PK only) suffices.

**Recommendation:** trigger-outbox is the default for both engines (simple,
strict, exactly-once when co-located). CDC is an opt-in Postgres Source for scale
or decoupling. The Engine and Store don't change between them — only which
`Source` you construct.

---

## 4. Consistency & Idempotency

| Source | Store location | Guarantee | How |
|---|---|---|---|
| SQLite triggers | same file | Exactly-once | Drain + apply + cursor in one tx |
| PG trigger-outbox | same DB | Exactly-once | Apply + cursor in one tx; snapshot-windowed poll |
| PG CDC | same DB | Effectively-once | Apply + LSN cursor in one tx; dedupe by LSN |
| PG CDC | different DB | At-least-once, idempotent | Reconcile-from-entries makes re-apply a no-op |

The reconcile-from-stored-entries design (§1) is what makes the bottom two rows
safe: even when the Store can't share the writer's transaction, re-applying a
change converges to the same state. **Correctness never depends on
exactly-once delivery.**

---

## 5. Aggregates

Classified automatically by invertibility:

- **Linear (delta-maintained):** `sum`, `count`, `avg` (stored as sum+count;
  sum-of-squares if we add variance/stddev). Apply signed deltas to the reduced
  row directly. O(1) per change.
- **Non-linear:** `min`, `max`, `first`, `last`, `distinct`. When the reduced
  row alone can't answer, mark the group dirty and re-reduce **from that
  group's entries only**. `distinct` can store the value set, or an HLL sketch
  for approximate-at-scale.

The entries table is what makes non-linear aggregates maintainable at all — you
cannot recover a new `min` from a reduced scalar once the old min leaves.

**But "one group" is not cheap when the group is huge** (issue #7): a
leaderboard's per-player group grows forever, and NutWords re-read a 115 MB
group per game turn. So the decision is made per change and per aggregate
(`applyDelta`), never per index:

| Aggregate | O(1) from the reduced row | Re-reduce from entries |
|---|---|---|
| `sum` / `count` / `avg` | always | never |
| `min` / `max` | inserts; retracting a value strictly inside the extreme; retracting the extreme when a new value reaches it | the extreme leaves and nothing arriving reaches it (or the row disagrees with the entries) |
| `first` / `last` / `distinct` | never | every change (the row holds no edge position or value set), so an index containing one re-reduces every touched group without reading its row first |

Linear aggregates beside a `max` keep their deltas: one `max` no longer turns
four sums into a scan. The trade is float precision: those sums now accumulate
by signed delta, as all-linear indexes always have, so a float `sum`/`avg` can
differ from a from-scratch reduce in its last bits. An append-only workload over `min`/`max` + linear
aggregates never reads a group at all. Retaining a top-K runner-up list per
extreme would make retraction O(1) too — deferred until a workload needs it.

---

## 6. Public API (sketch)

```ts
import { createRipply } from 'ripply';
import { sqliteSource, sqliteStore } from 'ripply/sqlite';

const ripply = createRipply({
  source: sqliteSource({ db, collections: { work_orders: { pk: ['id'] } } }),
  store:  sqliteStore({ db }),          // same db ⇒ co-located, atomic
});

ripply.defineIndex('countByStatus', {
  collection: 'work_orders',
  map: (wo) => ({ status: wo.status, count: 1 }),
  reduce: { groupBy: ['status'], aggregate: { count: 'sum' } },
});

ripply.defineIndex('revenueByTech', {
  collection: 'work_orders',
  map: (wo) => wo.status === 'completed'
    ? { techId: wo.technician_id, revenue: wo.revenue, count: 1 }
    : null,
  reduce: { groupBy: ['techId'], aggregate: { revenue: 'sum', count: 'sum', top: { max: 'revenue' } } },
});

await ripply.start();   // installs capture, begins processing (poll + wakeups)

// Query — always instant, pre-computed:
await ripply.index('countByStatus').all();
await ripply.index('countByStatus').where({ status: 'pending' }).value('count');   // 49
await ripply.index('revenueByTech').where({ techId: 'tech_1' }).entries();         // drill-down 🤯

// Maintenance:
await ripply.rebuild('countByStatus');   // truncate + full scan + re-map
await ripply.verify('countByStatus');    // incremental vs. from-scratch compare
```

Postgres — same engine, swap the adapter:

```ts
import { pgTriggerSource, pgStore } from 'ripply/postgres';
const ripply = createRipply({
  source: pgTriggerSource({ pool, collections: { work_orders: { pk: ['id'] } } }),
  store:  pgStore({ pool }),
});
// or, opt into CDC:
import { pgLogicalSource } from 'ripply/postgres';
//   source: pgLogicalSource({ connectionString, slot: 'ripply', publication: 'ripply_pub' }),
```

TypeScript generics carry `map`'s return type through to `.value()`/`.entries()`
— full inference, no codegen.

---

## 6.5 Materialized tally tables & cascading indexes

*(Added during Phase 1 — pulled forward from the ergonomics phase.)*

In SQL stores, an index's reduced output **is a real table**, `ripply_<name>`,
with the groupBy fields and aggregate outputs as plain columns:

```sql
SELECT TaskType, Count FROM ripply_TaskCompletionsByTechDate
WHERE HotelId = 'hotels/32546' ORDER BY TaskCompletedDate DESC;
```

- Any SQL client reads it with zero Ripply code — the app doesn't need to
  know how the tally is maintained.
- `IndexDefinition.indexes: string[][]` declares ordinary SQL indexes on it.
- Projection rules: `avg` → derived column **plus** `<out>_sum`/`<out>_count`
  components (so downstream rollups stay exact); `distinct` → JSON array
  text; objects/arrays → JSON text; scalars as-is.
- `group_key` (canonical JSON) stays the primary key, and an internal `vals`
  column is the reconstruction source of truth; projected columns are a
  denormalization maintained in the same transaction.
- `Store.ensureIndex(name, schema)` (optional interface method) materializes
  the table; a shape change drops + recreates it and the map-version rebuild
  repopulates. Non-SQL stores ignore all of this.

**Cascading indexes (RavenDB 4's OutputReduceToCollection):** because the
tally is a real table, Ripply's own trigger capture works on it — a second
index may declare `collection: 'ripply_<upstream>'` and roll it up further
(day → month → year), incremental all the way down. The engine writes the
upstream tally inside its own Store transaction, so the triggers fire in
that same transaction and the downstream changelog entries appear
atomically. `drain()` processes indexes in cascade (topological) order and
loops until quiet, so a whole cascade settles in one drain; cycles are
rejected at `start()`.

Cascade soundness: `sum` of sums, `min` of mins, `max` of maxes ✓; a
downstream "count" must be a **sum** of upstream counts; `avg` cascades via
its `_sum`/`_count` component columns; `first`/`last` don't cascade.

---

## 7. Rebuild & Versioning

- **Map versioning:** hash the map function source; store it with the index
  cursor. On mismatch at `start()`, auto-rebuild (optionally side-by-side: build
  the new index under a temp name, then swap — RavenDB's approach — so queries
  never see a half-built index).
- **Rebuild:** truncate entries+reduced for the index, `source.scan()` the whole
  collection, treat every row as an insert, then resume from the live cursor.
- **verify():** run a from-scratch reduce and diff against the maintained index.
  Ships as a dev/CI assertion and powers property test #1 below.

### Streaming rebuild

Both `rebuild()` and `verify()` **page** the scan and fold incrementally,
carrying only per-group accumulator state. Memory is O(groups + page), not
O(source rows) — which is what makes a large historical backfill fit.

This is sound even for non-linear indexes, because **a rebuild only ever
inserts**. Non-linearity is a statement about *retraction*: you cannot pull a
value out of a `max` without re-reading the group. Building forward from an
empty index retracts nothing, so every aggregate has O(1) forward-pass state:

| fn | forward state |
|---|---|
| `sum`, `count` | running total |
| `avg` | running sum + count |
| `min`, `max` | running extremum |
| `first`, `last` | the winning `(pk, ord)` position and its value |
| `distinct` | the distinct set — bounded by cardinality, not row count |

`distinct` is the one accumulator that is not O(1); its state is the group's
distinct set. `createGroupFold` is order-independent (it compares `(pk, ord)`
positions rather than trusting arrival order), and `reduceFull` is implemented
*on top of it* over the sorted entries — one implementation, so the streaming
rebuild and the dirty-group re-reduce cannot drift apart.

Float summation is the single order-sensitive case: addition is not
associative, so a `sum` folded in scan order can differ in its last bits from
one folded in `(pk, ord)` order. The incremental delta path already accumulates
in change order, so this is a tolerance the engine has always had.

### Rebuild is no longer one transaction

Folding in memory kept the write transaction short. Streaming would put the
entire scan inside it — on hosted Postgres that means a multi-hour transaction,
snapshot bloat, and a dropped connection costing the whole run. So a rebuild
commits in chunks:

1. truncate + resume cursor + a `__rebuilding__` map-version sentinel
2. entry pages, one transaction each (`pageSize`, default 2000 source rows)
3. reduced rows, chunked
4. the real map version — the commit that publishes the index as fresh

Consequences, in exchange for bounded memory and bounded transactions:

- **The index reads empty/partial while a rebuild runs.** Accept the window, or
  don't rebuild during hours you care about.
- **An interrupted rebuild is never served as fresh.** The sentinel can never
  equal a real map version (a hash), so `ensureFresh` sees a mismatch and
  rebuilds on the next `start()`.
- **Never drain an index concurrently with its own rebuild.** The `Ripply`
  facade serializes them through `runExclusive`; a bare `Engine` does not.

### Scanning is keyset-paged

`Source.scan()` pages by primary key rather than reading the table into the
client (`SELECT to_jsonb(t)` over 20M rows was the real memory ceiling, ahead
of anything the fold did). On Postgres the watermark round-trips through text
and is cast back to the pk column's own SQL type — comparing a `bigint` pk as
text would order 1000 before 999 and silently drop most of the table.

Paging across statements instead of reading one snapshot has two consequences:

- A row inserted *below* the watermark after we passed it is missed by the
  scan. Safe: rebuild captured `currentCursor` before scanning, so the insert
  replays from the changelog.
- A row whose **pk changes** mid-scan could be seen twice. Ripply already
  requires immutable primary keys — capture carries only the new pk, so the old
  pk's entries could never be retracted either — and `_ripply_entries`'
  `PRIMARY KEY (idx, pk, ord)` turns a double-scan into a loud unique violation
  rather than silent duplication.

See `docs/BULK-LOAD.md` for the backfill runbook this exists to serve.

---

## 8. Correctness Invariants (the test plan)

Written against the in-memory reference Store first (Phase 0), then re-run
against each real adapter:

1. **Incremental == rebuild** (property-based): for random insert/update/delete
   sequences, the maintained index equals a from-scratch reduce. *(Catches most
   bugs — this is the king test.)*
2. **Idempotent replay:** processing the same change batch twice equals once.
3. **Crash safety:** kill between apply and cursor-commit → no drift (tx rolls
   back together).
4. **Group transitions:** an update moving a row between groups decrements old,
   increments new; a group hitting zero is deleted.
5. **Non-linear:** removing a group's current `min`/`max` re-reduces correctly
   — and inserting into one never reads the group (issue #7).
6. **Multi-emit:** map returning k then k−1 entries retracts exactly one.
7. **Canonical keys:** group objects with differing key order collapse to one
   group.
8. **Delete retraction:** deleting a source row removes its contributions.
9. **Out-of-order commit (PG):** concurrent transactions with interleaved commit
   order still converge (exercises the snapshot-windowed cursor; proven by a
   deterministic held-transaction test and a concurrent-writers stress test).
10. **Map versioning:** a changed map triggers rebuild; a stale index is never
    served as fresh.

---

## 9. Decided Questions

1. **Name:** `ripply` (ripple + tally; both taken in the Postgres ecosystem).
2. **Composite/absent PK:** every collection must declare a unique key;
   collections without one are rejected in v1.
3. **Source shape:** physical tables only in v1; views/joined queries later.
4. **Spark integration:** Ripply stays framework-agnostic. Spark adds a thin
   `sparkSource` adapter on *its* side when ready.
5. **avg/variance:** store the components (sum+count; sum-of-squares later),
   derive at query time.

## Open Questions

1. **Changelog pruning cadence** for multi-index collections (prune below min
   cursor — how often, and who runs it?).
2. **`distinct` at scale** — exact set vs. HLL threshold; where does the set
   live (entries already have the values — maybe distinct is always a group
   re-reduce and needs no stored set at all)?
3. **Concurrent engines** — do we need an advisory lock so two app instances
   don't both run the processor, or is single-worker a documented constraint?

---

*Design by Michael & Claudia 💙*
