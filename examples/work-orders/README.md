# work-orders — the ripply live demo 🌊

```bash
bun examples/work-orders/server.ts
# → http://localhost:4242
```

A Bun server simulates an app writing random inserts/updates/deletes to a
`work_orders` table with **plain SQL** — no Ripply calls in the write path.
Generated triggers capture every change, and four indexes stay fresh
incrementally:

| Index | What | Notable |
|---|---|---|
| `ordersByStatus` | count per status | group transitions live |
| `revenueByTech` | sum / count / **avg** per tech | filtered map (completed only) |
| `revenueByDay` | revenue per day | feeds the cascade ↓ |
| `revenueByMonth` | monthly rollup | **cascading index** — consumes `ripply_revenueByDay` |

The dashboard (SSE, self-contained HTML, no dependencies) reads everything
with plain SQL from the materialized tally tables:

```sql
SELECT tech, revenue, jobs FROM ripply_revenueByTech ORDER BY revenue DESC;
```

Things to watch:

- **Drain time** — one drain settles rows → day tally → month tally.
- **Changelog backlog: 0** — auto-prune deletes applied changes after every
  drain.
- Delete ops (red in the change feed) **retract** through both cascade levels.
