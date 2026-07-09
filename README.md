# Ripply 🌊

**Real-time incremental map-reduce indexes for SQLite and Postgres.**

Pre-computed, always-fresh aggregates — counts by status, revenue by month,
workload by assignee — maintained incrementally as your rows change. Inserts,
updates, **and deletes**. Query time is a key lookup, never a `GROUP BY` scan.

Inspired by RavenDB's map-reduce indexes; built as a small standalone
TypeScript library for Bun/Node. No framework, no server, no lock-in.

```ts
import { createRipply } from "ripply";
import { sqliteSource, sqliteStore } from "ripply/sqlite";

const ripply = createRipply({
  source: sqliteSource({ db, collections: { work_orders: { pk: ["id"] } } }),
  store: sqliteStore({ db }),
});

ripply.defineIndex("countByStatus", {
  collection: "work_orders",
  map: (wo) => ({ status: wo.status, count: 1 }),
  reduce: { groupBy: ["status"], aggregate: { count: "sum" } },
});

await ripply.start(); // installs change capture, processes incrementally

await ripply.index("countByStatus").all();
// [{ status: "pending", count: 49 }, { status: "completed", count: 20 }, ...]

// Update a row → the affected groups update in real time. Delete one → the
// tally goes down. No rescans.
```

## How it works

1. **Capture** — SQLite: generated triggers append to a changelog table.
   Postgres: trigger-outbox (default) or logical-decoding CDC (opt-in).
2. **Map** — each changed row is mapped to zero-or-more index entries.
3. **Reconcile** — the row's *previous* contribution is read from Ripply's own
   entries table and reconciled toward the new one. Linear aggregates
   (`sum`/`count`/`avg`) apply O(1) deltas; non-linear (`min`/`max`/`distinct`)
   re-reduce just the affected group from its entries.
4. **Drill down** — the intermediate entries are queryable: not just
   "49 pending," but *which* 49.

Reprocessing is idempotent by construction, so crashes and replays never
corrupt an index. When source and store share a database, updates are
exactly-once and transactional.

## Status

🚧 Early development — Phase 0 (core engine + property tests). See `PLAN.md`.

## License

MIT © [Claudia](https://github.com/iamclaudia-ai)

---

*Built with 💙 by Michael & Claudia*
