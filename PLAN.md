# Ripply — Plan

Phases from `docs/DESIGN.md` §9 (Spark RFC 010), with Phase 0 broken down.
Rule: **the engine is proven against the in-memory reference before any
database adapter exists.** Invariant tests are the spec.

## Phase 0 — Engine + in-memory reference ⬅️ CURRENT

Prove the design. Zero database code.

- [x] `src/core/types.ts` — `Source`, `Store`, `StoreTx`, `Change`,
      `ChangeBatch`, `Entry`, `IndexDefinition`, `ReduceSpec`, `Cursor`
- [x] `src/core/canonical.ts` — canonical group-key serialization (sorted keys)
      + PK serialization
- [x] `src/core/aggregates.ts` — aggregate registry classified by invertibility:
      linear (`sum`, `count`, `avg` as sum+count) apply deltas; non-linear
      (`min`, `max`, `first`, `last`, `distinct`) trigger group re-reduce
- [x] `src/core/engine.ts` — the reconcile-from-entries loop:
      poll → map → readEntries → per-group delta / dirty-mark → replaceEntries →
      re-reduce dirty groups → deleteReducedIfEmpty → setCursor, all in one
      Store transaction
- [x] `src/core/rebuild.ts` — truncate + `source.scan()` + resume; `verify()`
      (from-scratch reduce vs. maintained index)
- [x] `src/memory/` — reference `memorySource` (append changes manually in
      tests; supports simulated interleaved commit visibility) and
      `memoryStore` (transactional via snapshot/rollback)
- [x] `src/core/__tests__/invariants.test.ts` — invariants 1–8 from DESIGN.md §8:
  - [x] 1. property test: incremental == full rebuild over random op sequences
        (the king test — 5 seeds × 250 random ops × 4 index shapes, checked
        against an independent oracle; mutation-tested: broken retraction
        fails 11/15 tests)
  - [x] 2. idempotent replay (same batch twice == once)
  - [x] 3. crash safety (throw between apply and cursor commit → no drift)
  - [x] 4. group transitions + zero-group deletion
  - [x] 5. non-linear re-reduce (remove the min)
  - [x] 6. multi-emit retraction (k entries → k−1)
  - [x] 7. canonical key collapse
  - [x] 8. delete retraction
- [x] Map versioning (fn-source hash stored with cursor; mismatch → rebuild)
      — invariant 10
- [x] `createRipply()` public API wrapper (DESIGN.md §6) with typed query
      surface (`.all()` / `.where()` / `.value()` / `.entries()`), background
      processor (wakeups + poll fallback, serialized work queue), avg derived
      at query time

**Phase 0 complete** — 18 tests green, §6 API compiles with aggregate-name
and entry-type inference. Deviation from the original sketch, by design:
Store is dumb keyed storage; all aggregate math lives in the engine
(DESIGN.md §2 updated).

**Exit criteria:** all invariants green; API in DESIGN.md §6 compiles against
the real types with full inference.

## Phase 1 — SQLite adapter

- [x] `sqliteSource`: trigger codegen from `PRAGMA table_info`, `_ripply_changelog`,
      cursor-based poll + `Source.prune` below min cursor (generalized from
      consume-and-delete: destructive polling breaks multi-index sharing and
      idempotent replay; prune gives the same hygiene — Ripply auto-prunes
      after drains). No `update_hook` in bun:sqlite → poll fallback only.
- [x] `sqliteStore`: entries/reduced/cursors tables, one BEGIN IMMEDIATE tx
      per batch
- [x] Re-run all Phase 0 invariants against SQLite (same test suite, adapter
      matrix — `runInvariantSuite` in `src/core/__tests__/suite.ts`)
- [x] rebuild()/verify() end-to-end (durability across close/reopen, trigger
      regen after ALTER TABLE, identifier hardening, auto-prune)
- [x] **RavenDB oracle** (`scripts/ravendb-oracle.ts`): ported
      `HotelRoom/TaskCompletionsByTechDate` from a live RavenDB 3.x db,
      452 real docs → exact group match across 4 hotels. Initial build 19ms,
      incremental 8ms.
- [x] **Materialized tally tables** (pulled forward from Phase 4): reduced
      output is a real `ripply_<name>` table with groupBy/aggregate columns,
      plain-SQL queryable, `indexes: [...]` declares SQL indexes, avg gets
      `_sum`/`_count` component columns (DESIGN.md §6.5)
- [x] **Cascading indexes** (RavenDB 4 OutputReduceToCollection): an index
      may consume another index's tally table; topo-ordered start, cycle
      detection, one drain settles the whole cascade
- [x] Example app (`examples/work-orders/`): live SSE dashboard — random
      insert/update/delete simulator, four indexes incl. the day→month
      cascade, every chart read with plain SQL from the tally tables

**Phase 1 complete — ship it usable. ✅**

## Phase 2 — Postgres trigger-outbox

- [x] `postgresSource`: ONE generic `_ripply_capture()` trigger fn
      (`to_jsonb(NEW/OLD)`, pk columns via TG_ARGV, `pg_notify` for future
      wakeups), `_ripply_changelog` outbox with `txid xid8` — zero deps via
      Bun's native `Bun.sql`
- [x] **Ordering gotcha solved: snapshot-windowed cursors.** Cursor =
      `{prev, cur, seq}` of `pg_snapshot`s; a poll freezes a window
      (visible in `cur`, not in `prev`), drains it in seq order, then
      advances. A late-committing low seq lands in a later window instead
      of being skipped. Keeps the shared read-only changelog + per-index
      cursors + `prune()` (covers-based) — no consume-and-delete needed.
      (Found the hard way: `ORDER BY seq` binding to a `seq::text` output
      alias sorts lexicographically — the stress test caught it.)
- [x] `postgresStore`: `sql.begin` transactions, TYPED materialized
      `ripply_<name>` tally tables (`columnTypes` overrides, numeric
      defaults double precision — Bun returns int8 as string), upsert
      putReduced preserving cascade trigger capture, `::text::jsonb`
      params (Bun double-encodes pre-stringified jsonb)
- [x] `scan()` = server-side `to_jsonb(t)` so rebuild rows are
      byte-identical to trigger after-images (else phantom groups)
- [x] Invariant 9 (out-of-order commit convergence): deterministic
      held-transaction test + 8-writer concurrent stress with live drainer,
      vs the independent oracle
- [x] Adapter matrix: full invariant suite on PG (`src/postgres/__tests__/`,
      local Docker PG; `RIPPLY_TEST_PG` to point elsewhere)
- [x] Cascading indexes + tally tests on PG (day→month, NutWords-shaped
      player_stats with bigint group key)

**Phase 2 complete — 60 tests green across memory + SQLite + Postgres. ✅**
Deferred: `wakeups` via LISTEN/NOTIFY (needs a dedicated unpooled
connection; Neon's pooler drops LISTEN — poll fallback is the default).

## Phase 3 — Postgres CDC (opt-in)

- [ ] `pgLogicalSource` (wal2json or pgoutput), LSN cursors
- [ ] Cross-DB idempotent apply (Store ≠ Source DB)
- [ ] Slot lag / health metrics

## Phase 4 — Ergonomics

- [ ] Full TS inference: map return type → `.value()` / `.all()` / `.entries()`
- [ ] Drill-down query surface polish
- [ ] Side-by-side rebuild (build under temp name, swap)
- [ ] Changelog pruning policy
- [ ] Docs site / README examples

## Phase 5 — Reactivity (optional)

- [ ] Change notifications on reduced rows (PG `LISTEN`, SQLite post-commit
      hook) → subscription API
- [ ] (Later, in Spark's repo) `sparkSource` adapter + `useIndex` hook consuming
      Ripply
