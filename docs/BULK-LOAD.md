# Bulk loading a captured table

Loading a lot of rows into a table Ripply captures — a migration, a historical
import, a seed — needs a different shape than normal writes. This is the
runbook.

**The short version:** suppress capture for your import connection only, load
the data, then rebuild the affected indexes. Do *not* let the import go through
the incremental path.

---

## Why not just let it drain?

Every inserted row writes a changelog row, and those rows then have to be
streamed through the normal incremental path. Three things go wrong at once:

1. **The changelog can't be pruned.** `prune()` deletes a change only once
   *every* index on that collection has passed it, so the changelog stays at
   full size for the entire catch-up.
2. **Catch-up is slow.** Batches are 500 changes by default; a 400K-row import
   is measured in hours.
3. **Some aggregates still re-read whole groups.** Since 0.2.0 `min`/`max`
   beside linear aggregates are O(1) on insert, so an import into a
   leaderboard no longer re-reads each player's group per batch. But an index
   with `first`/`last`/`distinct` still re-reduces every touched group from
   its **entire** entry set, once per 500-change batch — and few groups with
   enormous entry sets turns that quadratic.

A rebuild does the same work in one forward pass. For a bulk load it isn't an
optimization, it's the only viable mode.

---

## The runbook

### 0. Before you start

- **Drain to a clean changelog.** `await ripply.drain()` until it returns 0.
  You want `_ripply_changelog` empty going in, so nothing pre-existing blocks
  pruning later.
- **Know your window.** A rebuild commits in chunks, so **the index reads
  empty/partial while it runs**. Stats pages will show zeros. If that's not
  acceptable, you want a shadow build — see "Not covered here" below.
- **Stop the Ripply processor** for the collections you're loading, or at least
  don't call `drain()` concurrently with a rebuild of the same index.

### 1. Suppress capture — for your connection only

Do **not** `ALTER TABLE ... DISABLE TRIGGER`. That disables capture for
*everyone*, including live traffic, and any player write during the import is
lost with no signal. Suppress it per-session instead:

```sql
SET session_replication_role = replica;   -- this connection skips triggers
--  ... the entire import ...
RESET session_replication_role;
```

Your app's connections keep firing capture normally throughout. Only the rows
*you* insert go uncaptured — which is exactly right, because the rebuild is
about to pick them up from the table itself.

Two things to check before you plan around this:

- **Verify your role can set it.** It's normally available to the table owner;
  confirm on your actual database (`SET session_replication_role = replica;`
  in a scratch session) before the import window, not during it.
- **It must be the same session as the load.** If your importer uses a
  connection pool, either pin it to one connection or set it at the top of
  every transaction. A pooled `SET` that lands on a different connection than
  the `INSERT` silently does nothing — and you'd only find out from the
  changelog size.

If you can't use it, the fallback is to let the changelog fill and accept the
write amplification; the rebuild in step 3 makes those rows moot either way,
and `prune()` clears them once every index's cursor has passed. You just paid
for writes nobody consumed.

### 2. Load the data

Normal bulk-load hygiene applies and is unrelated to Ripply: `COPY` over
row-by-row inserts, drop and recreate non-essential indexes on the source
table, batch your transactions.

### 3. Rebuild the blast radius — and only the blast radius

**The rule:** rebuild every index whose collection you loaded into, plus
everything transitively downstream of those. Nothing else.

Do **not** loop `engine.cascadeOrder()` unfiltered. It returns *every* index,
and `rebuild` truncates before it scans — so an unfiltered loop takes indexes
unrelated to your import down for the duration and pays for a full scan of
their collections, which is the exact cost this runbook exists to avoid. An
append-only table nobody prunes is the worst thing to scan by accident.

Do **not** hand-write the list either. It rots the same way a hand-written
order would: someone adds a cascade downstream of an index you rebuild, and
the numbers are quietly stale until somebody notices.

Compute it. `cascadeOrder()` is upstream-first, so one pass propagates
downstream reachability:

```ts
import { tallyTableOf } from 'ripply';

/** Indexes to rebuild after loading `collections`, in cascade order. */
function blastRadius(order: string[], collectionOf: Record<string, string>, loaded: string[]) {
  const dirty = new Set(loaded);
  const affected: string[] = [];
  for (const name of order) {          // upstream before downstream
    if (!dirty.has(collectionOf[name]!)) continue;
    affected.push(name);
    // a downstream index reads THIS index's tally table as its collection
    dirty.add(tallyTableOf(name));
  }
  return affected;
}

for (const name of blastRadius(engine.cascadeOrder(), COLLECTION_OF, ['game_play', 'game_player'])) {
  await ripply.rebuild(name, { pageSize: 10_000 });
}
```

`COLLECTION_OF` is a `{ indexName: collectionName }` map you keep next to your
index definitions. (The engine knows this already, but `runtimeOf` is internal
— if this map turns out to be a recurring annoyance, that's an argument for a
`rebuildFor(collections)` helper in the library rather than in every caller.)

Rebuild captures the feed position **before** scanning, so anything real
players did during the load and during the scan replays from the changelog
afterwards — reconcile-from-entries makes that replay converge. Live play is
never lost, only applied late.

**Tuning `pageSize`.** Smaller pages mean less memory and shorter
transactions; larger pages mean fewer per-transaction round trips, which
dominate against a remote database. The default is 2000. `pageSize` counts
**source rows**, so a multi-emit index (one entry per word formed) writes many
more entries per page than a 1:1 index — size it by the emit fan-out, not by
the row count.

```ts
await ripply.rebuild('word_stats', { pageSize: 10_000 });
```

### 4. Drain, verify, prune

```ts
await ripply.drain();                        // replay the overlap window
console.log(await ripply.verify('player_stats'));   // { ok: true, ... }
```

`verify()` runs an independent from-scratch reduce and diffs it against the
maintained index. It streams too, so it works at import scale — which is
exactly when you want it. Run it while writes are quiet, or expect false
positives from in-flight changes.

Then confirm `_ripply_changelog` is back to a sane size.

---

## What if it dies halfway?

Re-run the rebuild. It's idempotent — it truncates first, and an interrupted
run leaves a `__rebuilding__` map-version sentinel that can never match a real
map version, so `ensureFresh` treats the index as stale and rebuilds it on the
next `start()` even if nobody remembers to.

What you must **not** do is drain an index whose rebuild was interrupted and
assume the numbers are real. They aren't, and the sentinel is there to make
that state detectable rather than plausible.

---

## Sizing

Memory during a rebuild is O(groups + page), not O(source rows):

- **groups** — one accumulator per group key. A leaderboard grouped by
  `player_id` with a few thousand players is nothing. Grouping by `word` with
  ~188K groups is still fine.
- **page** — `pageSize` source rows' worth of entries, in flight.
- **`distinct` is the exception.** Its accumulator is the group's distinct set,
  held for the whole scan. Bounded by cardinality rather than row count, but
  check that bound before putting a `distinct` on a high-cardinality group.
  Every other aggregate — including `avg`, which folds as a `{ sum, count }`
  pair — is a fixed-size accumulator per group.

---

## Not covered here

**Rebuilding a live index with no visible downtime.** That needs a shadow build
(build under a second index name, then swap) with an epoch on `_ripply_entries`
so the swap is a one-row update rather than re-pointing millions of rows, plus
a `RENAME` for the tally table. It also has to rebuild downstream cascades
transitively — a rename bypasses the tally table's own capture trigger, so
downstream indexes would otherwise keep aggregating a table that got swapped
out from under them. Deliberately not built yet; the chunked rebuild above
covers a planned maintenance window.
