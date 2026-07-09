# Ripply — Plan

Phases from `docs/DESIGN.md` §9 (Spark RFC 010), with Phase 0 broken down.
Rule: **the engine is proven against the in-memory reference before any
database adapter exists.** Invariant tests are the spec.

## Phase 0 — Engine + in-memory reference ⬅️ CURRENT

Prove the design. Zero database code.

- [ ] `src/core/types.ts` — `Source`, `Store`, `StoreTx`, `Change`,
      `ChangeBatch`, `Entry`, `IndexDefinition`, `ReduceSpec`, `Cursor`
- [ ] `src/core/canonical.ts` — canonical group-key serialization (sorted keys)
      + PK serialization
- [ ] `src/core/aggregates.ts` — aggregate registry classified by invertibility:
      linear (`sum`, `count`, `avg` as sum+count) apply deltas; non-linear
      (`min`, `max`, `first`, `last`, `distinct`) trigger group re-reduce
- [ ] `src/core/engine.ts` — the reconcile-from-entries loop:
      poll → map → readEntries → per-group delta / dirty-mark → replaceEntries →
      re-reduce dirty groups → deleteReducedIfEmpty → setCursor, all in one
      Store transaction
- [ ] `src/core/rebuild.ts` — truncate + `source.scan()` + resume; `verify()`
      (from-scratch reduce vs. maintained index)
- [ ] `src/memory/` — reference `memorySource` (append changes manually in
      tests; supports simulated interleaved commit visibility) and
      `memoryStore` (transactional via snapshot/rollback)
- [ ] `src/core/__tests__/invariants.test.ts` — invariants 1–8 from DESIGN.md §8:
  - [ ] 1. property test: incremental == full rebuild over random op sequences
        (the king test — write it FIRST, watch it drive the engine)
  - [ ] 2. idempotent replay (same batch twice == once)
  - [ ] 3. crash safety (throw between apply and cursor commit → no drift)
  - [ ] 4. group transitions + zero-group deletion
  - [ ] 5. non-linear re-reduce (remove the min)
  - [ ] 6. multi-emit retraction (k entries → k−1)
  - [ ] 7. canonical key collapse
  - [ ] 8. delete retraction
- [ ] Map versioning (fn-source hash stored with cursor; mismatch → rebuild)
      — invariant 10

**Exit criteria:** all invariants green; API in DESIGN.md §6 compiles against
the real types with full inference.

## Phase 1 — SQLite adapter

- [ ] `sqliteSource`: trigger codegen from `PRAGMA table_info`, `_ripply_changelog`,
      consume-and-delete drain, `update_hook` wakeup + poll fallback
- [ ] `sqliteStore`: entries/reduced/cursors tables, one-tx-per-batch
- [ ] Re-run all Phase 0 invariants against SQLite (same test suite, adapter
      matrix)
- [ ] rebuild()/verify() end-to-end
- [ ] Example app (`examples/work-orders/`)

**Ship it usable after this phase.**

## Phase 2 — Postgres trigger-outbox

- [ ] Generic `_ripply_capture()` trigger fn (`to_jsonb(NEW)`), `pg_notify` wakeup
- [ ] Consume-and-delete drain; multi-index fan-out with prune-below-min-cursor
- [ ] Invariant 9 (out-of-order commit convergence) — needs a real PG in CI
      (testcontainers or a local instance)
- [ ] Adapter matrix: full invariant suite on PG

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
