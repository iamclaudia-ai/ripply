# Background & Origin

Ripply was extracted from **Spark** (`~/Projects/claudia/spark`), a minimal
Actor Model framework we built together (Bun + SQLite, actors with commands/
queries, Immer mutations, WebSocket subscriptions, full action log). Spark's
RFC 003 proposed map-reduce indexes maintained incrementally from the action
log's `(state_before, state_after)` pairs. A 2026-07 design review (see
`spark/CODE_REVIEW.md`) concluded the indexer was the killer feature and worth
standing alone — more useful as a package than the whole framework, for now.

## The RavenDB inspiration

Michael comes from RavenDB, where real-time (eventually consistent) map-reduce
indexes were a signature feature: aggregates like counts-by-status were always
pre-computed, and the reduce happened automatically **even for updates and
deletes**. The 🤯 moment (Michael's words): updating a document simply updates
the reduce buckets it was part of — inline, split, move, or delete — then
re-reduces the leaves. Keeping aggregates fresh in real time became easy and
fast because the intermediate map output (the bucket leaves) is *stored*, not
recomputed.

That insight is Ripply's `entries` table. It also powers RavenDB-style
drill-down: not just "49 pending," but *which* 49. RavenDB additionally keeps a
B-tree of intermediate reduce buckets for groups with millions of entries;
Ripply v1 uses a single level (entries → reduced), which suffices at its target
scale. Add the tree later if ever needed.

## Findings from the Spark review that shaped this design

1. **Delta maintenance is Z-sets.** RFC 003's map(before)-as-retraction /
   map(after)-as-assertion formulation is a special case of DBSP/differential
   dataflow (the theory under Materialize and Feldera). The theory says exactly
   which aggregates are cheap: *linear* ones (`sum`/`count`/`avg`) accept signed
   deltas; non-linear ones (`min`/`max`/`distinct`) need the underlying multiset
   — hence the entries table.
2. **RFC 003's schema stored only reduced values** — so `min`/`max`/`distinct`
   could not actually be maintained incrementally as specified. Fixed here by
   the entries table + dirty-group re-reduce.
3. **Deltas are not idempotent** (apply +1 twice = silently wrong forever), so
   cursor advance and index writes must share one transaction — or the whole
   pipeline must be idempotent another way. Ripply does both: transactions when
   co-located, and reconcile-from-stored-entries so replays converge regardless.
4. **`map` must be multi-emit** (`Entry | Entry[] | null`) — one-entry-per-
   source-row breaks the moment a row conceptually contains a collection.
5. **Group keys must be canonical JSON** (sorted keys) or phantom groups appear.
6. **Decrement-to-zero must delete the reduced row** or typo'd groups haunt the
   index forever.
7. **Map functions must be versioned** (hash stored with cursor, auto-rebuild on
   mismatch) — a changed map with a stale index is silently wrong.
8. **Deletes must be visible to the indexer.** In Spark, `destroyActor` didn't
   even log — the review caught that deletions could never retract. Ripply's
   capture layer treats delete as a first-class op from day one.
9. **Inline/strict beats background/stale at small scale.** RavenDB's stale-
   index dance exists because it's a multi-node database. Co-located
   trigger-outbox + same-transaction processing gives strict consistency
   almost for free on a single node; async CDC is the opt-in, not the default.

## The key departure from the Spark RFC

Spark's action log hands you `(state_before, state_after)` for free, so RFC 003
diffed before vs. after images. Standalone Ripply instead **retracts from its
own stored entries** (§1 of DESIGN.md). That removes the before-image
requirement from capture entirely — which is what makes plain triggers, outbox
tables, and PK-only CDC (`REPLICA IDENTITY DEFAULT`) all sufficient, and makes
every reprocessing path idempotent. Same math, smaller capture surface, more
places it can run.

## Ecosystem positioning (why this gap is real)

- Postgres materialized views: no incremental maintenance (`REFRESH` = rescan).
- `pg_ivm`: incremental but restrictive (query forms, ops).
- Timescale continuous aggregates: time-series only.
- Materialize / Feldera: whole databases — powerful, heavy.
- **Ripply:** app-level, tiny, RavenDB-ergonomics, SQLite *and* Postgres. A
  lightweight incremental map-reduce for the 99% case: keep my dashboard
  counters correct in real time without rescanning.
