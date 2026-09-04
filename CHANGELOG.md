# Changelog

## 0.1.0

### Streaming rebuild (#3)

`rebuild()` and `verify()` no longer hold the source in memory. The scan is
keyset-paged and folded into per-group accumulators, so memory is
**O(groups + page)** instead of O(source rows) — large historical backfills
now fit.

This is sound even for non-linear indexes (`min`/`max`/`first`/`last`/
`distinct`), because a rebuild only ever _inserts_: non-linearity is a
statement about retraction, and building forward from an empty index retracts
nothing. `reduceFull` is now implemented on top of the same fold, so the
streaming rebuild and the dirty-group re-reduce cannot drift apart.

**Behavior change — `rebuild()` is no longer a single transaction.** It commits
in chunks (truncate + sentinel → entry pages → reduced rows → map version),
because streaming would otherwise put the entire scan inside one transaction —
a multi-hour transaction against a hosted Postgres. Consequences:

- **An index reads empty/partial while it rebuilds.** Previously a rebuild was
  invisible to readers until it committed.
- **An interrupted rebuild is never served as fresh.** It leaves a
  `__rebuilding__` map-version sentinel, which `ensureFresh` sees as a mismatch
  and rebuilds on the next `start()`.
- **Do not drain an index concurrently with its own rebuild.** The `Ripply`
  facade serializes them via `runExclusive`; a bare `Engine` does not, and
  neither does anything across processes.

### Added

- `rebuild(name, options)` accepts `RebuildOptions { pageSize }` — source rows
  buffered per flush, default 2000. `pageSize` counts source rows, so size it
  by map fan-out for multi-emit indexes.
- `createGroupFold(aggregates, groupKey)` — the order-independent streaming
  fold, exported for adapters and tests.

### Fixed

- `Source.scan()` in both the Postgres and SQLite adapters materialized the
  entire table client-side before the fold ever ran. Both now page by primary
  key. On Postgres the keyset watermark round-trips through text and is cast
  back to the pk column's own SQL type — comparing a `bigint` pk as text orders
  1000 before 999 and silently drops most of the table.
- `first`/`last` in a streaming fold now carry the winning `(pk, ord)` position
  rather than trusting arrival order. v1 semantics are "first/last in pk
  order", and pk order is lexicographic over canonical JSON (`wo_10` sorts
  before `wo_2`), so an arrival-order fold looks correct on small fixtures and
  diverges at scale.

### Notes

- Primary keys must be immutable. This was already true (capture carries only
  the new pk, so an old pk's entries could never be retracted); keyset paging
  makes it load-bearing in one more place, and `_ripply_entries`'
  `PRIMARY KEY (idx, pk, ord)` turns a violation into a loud error rather than
  silent duplication.
- `distinct` is the one aggregate whose accumulator is not fixed-size — its
  state is the group's distinct set, bounded by cardinality rather than row
  count.
- New: `docs/BULK-LOAD.md`, the backfill runbook.

## 0.0.3

Initial published release: engine + in-memory reference, SQLite adapter,
Postgres adapter (trigger-outbox with snapshot-windowed cursors), materialized
tally tables, cascading indexes.
