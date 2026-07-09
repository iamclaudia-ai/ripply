# Ripply vs. RavenDB Map-Reduce Indexes

An honest comparison against the source of inspiration:
[RavenDB 7.2 map-reduce indexes](https://docs.ravendb.net/7.2/indexes/map-reduce-indexes).
RavenDB is a full document database with two decades of engineering behind its
indexing pipeline; Ripply is a small library that brings the same *model* —
aggregation happens at write time, queries are key lookups "regardless of the
data size" — to the SQLite/Postgres tables you already have.

## Feature by feature

| Capability | RavenDB | Ripply |
|---|---|---|
| Map function | LINQ or JavaScript over documents | TypeScript function over rows |
| Multi-emit (SelectMany fanout) | ✅ | ✅ `map` returns `Entry \| Entry[] \| null` |
| Reduce definition | Hand-written aggregation function; must be associative/commutative, output shape must match map output shape (re-reduce feeds on itself) | Declarative spec (`sum`, `count`, `avg`, `min`, `max`, `first`, `last`, `distinct`) — associativity is the engine's problem, not yours |
| Incremental update mechanics | Re-reduces affected buckets in the index B-tree | Split by invertibility: linear aggregates apply signed deltas **O(1)**; non-linear re-reduce only the dirty group's entries |
| Deletes / retraction | ✅ | ✅ reconcile-from-stored-entries; delete is `newEntries = []` |
| Updates that change group | ✅ | ✅ same reconcile path — old group decremented, new incremented |
| Chained aggregation | `OutputReduceToCollection` → artificial documents → another index consumes them (daily → monthly → yearly) | Cascading indexes — every index's output **is already a real table** (`ripply_<name>`), so chaining is just `collection: 'ripply_daily'`. Same recursive rollups, no artificial-document machinery |
| Cycle protection | Errors on output-to-consumed-collection and circular index outputs | Topological start order + cycle detection (`cascadeOrder()` throws) |
| Reduce-key identity | Hash-suffixed artificial document IDs + optional pattern-ID reference documents for predictable lookup | Canonical JSON group key (sorted keys) — the key *is* deterministic, so lookup is a `WHERE group_key = ?` (or the typed groupBy columns). No reference-document layer needed |
| Queryable results | Query/DocumentQuery API; artificial documents usable by subscriptions/ETL | Plain SQL against the tally table — joins, `ORDER BY`, drizzle, psql, anything. Plus the typed `.all()/.where()/.value()` surface |
| Drill-down | Map-Reduce Visualizer in the Studio (reduce tree) | `_ripply_entries` is queryable per group (`.entries()`, or raw SQL — NutWords' "best word on record" is an argmax over entries) |
| Definition change | Side-by-side indexing (old index serves until new one catches up) | Map-version hash → automatic **blocking** rebuild. Side-by-side swap is on the Phase 4 list |
| Correctness story | Two decades of production | Property-tested invariant suite: incremental == full rebuild over random insert/update/delete sequences, on all three backends; out-of-order-commit convergence proven deterministically |
| Exactly-once | Internal to the database | By construction when Source and Store share a transaction; idempotent at-least-once otherwise |
| Multi-map (one index over several collections) | ✅ `AddMap<T>()` per collection, shared reduce | ❌ one collection per index (union view or two indexes + SQL join today) |
| Dynamic (auto) aggregation from queries | ✅ auto-indexes | ❌ explicit definitions only — deliberate non-goal |
| Sharding | ✅ | ❌ single database — out of scope |
| Server / Studio / monitoring | Full product | None — it's a library. Your existing PG/SQLite tooling is the studio |
| Runs against *your* existing relational tables | ❌ (it's the database) | ✅ that's the whole point |

## The two ideas we kept, and the one we inverted

**Kept #1 — pre-computed aggregation.** RavenDB's core promise: the `GROUP BY`
happens at indexing time, so query cost is independent of data size. Ripply's
tally tables are exactly this.

**Kept #2 — output-as-collection.** RavenDB 4's `OutputReduceToCollection` was
the feature that made rollup chains possible. Ripply makes it the *default*:
there is no "index-internal" result representation to escape from — the reduced
output is born as a real table, which is why cascades cost one line.

**Inverted — the reduce contract.** RavenDB asks you to write reduce as a real
function and trusts you to keep it associative/commutative with a stable shape
(getting this wrong is a classic RavenDB footgun). Ripply flips it: you declare
*what* to aggregate, and the engine owns the algebra — including knowing which
aggregates are invertible (delta-able) and which need a group re-reduce. Less
power (you can't write arbitrary reduce logic), but the failure mode is gone,
and O(1) linear updates fall out for free.

## Honest gaps

Multi-map, side-by-side rebuilds, any kind of visualizer, fanout/perf
safeguards, auto-indexes, sharding. If you need those *and* a document
database, use the big dog — it's excellent. If you have a Postgres/SQLite app
and want RavenDB-style always-fresh aggregates without adopting a database,
that's Ripply.
