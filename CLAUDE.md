# Ripply 🌊

Real-time incremental map-reduce indexes for SQLite and Postgres. RavenDB-style
pre-computed aggregates as a small standalone TypeScript library — no framework,
no server, no lock-in.

**Name:** "ripply" = ripple + tally (both taken in the Postgres ecosystem).
Changes *ripple* through the index and keep the *tally* fresh.

## What this is

You point Ripply at existing tables, declare indexes (a `map` function + a
`reduce` spec), and it maintains aggregates incrementally as rows change —
inserts, updates, *and deletes*. Counts-by-status, revenue-by-tech, etc. are
always pre-computed; query time is a key lookup, never a `GROUP BY` scan.

Extracted from the Spark project's RFC 003 (`~/Projects/claudia/spark`) — see
`docs/BACKGROUND.md` for the origin story and `docs/DESIGN.md` for the full
design. Ripply is deliberately Spark-agnostic; Spark will eventually *consume*
this package via a thin adapter on its side.

## The load-bearing design decisions (do not casually revisit)

1. **Reconcile from stored entries, not from before-images.** On a change we
   read the row's *current contribution* from our own entries table and
   reconcile toward the new map output. This makes capture tiny (pk + after +
   isDelete only), makes reprocessing idempotent by construction, and makes
   delete just `newEntries = []`. It's the Z-set/DBSP formulation with the
   retraction sourced from durable state.
2. **Two tables per index:** `entries` (intermediate map output per source row —
   the RavenDB reduce-bucket leaf, enables drill-down) and `reduced` (final
   aggregate per group).
3. **Aggregates split by invertibility.** Linear (`sum`, `count`, `avg` as
   sum+count) apply signed deltas O(1). Non-linear (`min`, `max`, `first`,
   `last`, `distinct`) mark the group dirty and re-reduce from that group's
   entries only.
4. **Three interfaces:** `Source` (change capture), `Store` (entries + reduced +
   cursors), `Engine` (backend-free core). Cursor advance and index writes share
   one Store transaction → exactly-once when Source and Store are co-located;
   idempotent at-least-once otherwise.
5. **Capture per backend:** SQLite = generated triggers → changelog table (only
   option). Postgres default = trigger-outbox with `to_jsonb(NEW)` +
   `pg_notify`. Postgres opt-in = logical-decoding CDC (LSN cursors are
   commit-ordered, which sidesteps the outbox serial-vs-commit-order gotcha).
6. **`map` returns `Entry | Entry[] | null`** (multi-emit, RavenDB SelectMany).
7. **Group keys are canonical JSON (sorted keys)** — non-negotiable, prevents
   phantom groups.
8. **Phase 0 first:** the engine is proven against an in-memory reference
   Source/Store with property tests *before* any database adapter exists. The
   king test: **incremental result == full rebuild** over random
   insert/update/delete sequences.

## Current status

Phases 0–2 **complete**: engine + in-memory reference, SQLite adapter,
Postgres adapter (trigger-outbox with **snapshot-windowed cursors** — see
DESIGN.md §3 for why a naive seq cursor silently skips out-of-order commits
on Postgres). 60 tests green across the three-backend adapter matrix;
invariant 9 proven deterministically. Postgres tests need a reachable PG
(`postgres://postgres:postgres@localhost:5432` by default, or set
`RIPPLY_TEST_PG`). Next: Phase 3 (CDC opt-in) / Phase 4 (ergonomics) per
`PLAN.md`.

## Conventions

- **Bun** for everything: `bun test`, `bun run typecheck`. No Node-specific APIs
  in core (adapters may use them).
- Package name: `ripply`. Exports: `ripply` (engine + types), `ripply/sqlite`,
  `ripply/postgres`, `ripply/memory` (reference adapters, also used by tests).
- Strict TypeScript. Core (`src/core/`) must have zero dependencies and zero
  knowledge of any database.
- Conventional commits. Commit locally and often. Never push without asking.
- Tests live next to what they test (`src/core/__tests__/`). Property/invariant
  tests are the spec — when design and test disagree, stop and discuss.

## Doc map

| File | What |
|---|---|
| `docs/DESIGN.md` | Full design (adapted from Spark RFC 010) — the source of truth |
| `docs/BACKGROUND.md` | Origin: Spark RFC 003, RavenDB inspiration, review findings that shaped the design |
| `PLAN.md` | Phase checklist + Phase 0 task breakdown |
