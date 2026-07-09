/**
 * RavenDB oracle — verify Ripply against the system that inspired it.
 *
 * Fetches real HotelRooms documents from a live RavenDB 3.x database, loads
 * them into SQLite through Ripply's trigger capture (half BEFORE start() to
 * exercise the initial build, half after to exercise live incremental
 * processing), ports the LINQ map-reduce index
 * `HotelRoom/TaskCompletionsByTechDate` to a Ripply index, and diffs our
 * reduced groups against RavenDB's own index output.
 *
 * Same documents, same index logic, two engines — the tallies must agree.
 *
 *   bun scripts/ravendb-oracle.ts
 *   RAVEN_URL=http://etna:9000 RAVEN_DB=Fuse3 HOTELS=hotels/32546 bun scripts/ravendb-oracle.ts
 */

import { Database } from 'bun:sqlite';
import { createRipply } from '../src/index';
import { sqliteSource, sqliteStore } from '../src/sqlite/index';

const RAVEN_URL = process.env.RAVEN_URL ?? 'http://etna:9000';
const RAVEN_DB = process.env.RAVEN_DB ?? 'Fuse3';
const HOTELS = (
  process.env.HOTELS ?? 'hotels/32546,hotels/30816,hotels/27959,hotels/32645'
).split(',');

/** RavenDB's DateTime.MinValue serialization — the index's `where` filter. */
const MIN_DATE = '0001-01-01T00:00:00.0000000';

// ---------------------------------------------------------------------------
// RavenDB client (read-only, plain HTTP)
// ---------------------------------------------------------------------------

interface RavenQueryPage {
  TotalResults: number;
  IsStale: boolean;
  Results: Array<Record<string, unknown>>;
}

async function ravenQuery(indexName: string, luceneQuery: string) {
  const results: Array<Record<string, unknown>> = [];
  let stale = false;
  const pageSize = 512;
  for (let start = 0; ; start += pageSize) {
    const url =
      `${RAVEN_URL}/databases/${RAVEN_DB}/indexes/${indexName}` +
      `?query=${encodeURIComponent(luceneQuery)}&start=${start}&pageSize=${pageSize}`;
    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`RavenDB ${response.status} for ${url}`);
    }
    const page = (await response.json()) as RavenQueryPage;
    stale ||= page.IsStale;
    results.push(...page.Results);
    if (results.length >= page.TotalResults || page.Results.length === 0) {
      return { results, stale };
    }
  }
}

// ---------------------------------------------------------------------------
// The ported index — LINQ → TypeScript, semantics preserved
// ---------------------------------------------------------------------------

// RavenDB (C#):
//   from room in docs.HotelRooms
//   from key in room.Json.tasks                                  → multi-emit
//   where room.Json[key].taskCompletedDate != DateTime.MinValue  → null PASSES
//   select new { HotelId, TaskType = taskName, TaskTechName,
//                TaskCompletedDate, Count = 1 }
// reduce: group by all four fields, Count = g.Sum(x => x.Count)

interface TaskDoc {
  taskName?: string | null;
  taskTechName?: string | null;
  taskCompletedDate?: string | null;
}

function mapRoom(room: Record<string, unknown>) {
  const doc = JSON.parse(room.doc as string) as Record<string, unknown>;
  const json = (doc.Json ?? {}) as Record<string, unknown>;
  const tasks = Array.isArray(json.tasks) ? (json.tasks as string[]) : [];
  return tasks
    .filter((key) => {
      const task = json[key] as TaskDoc | undefined;
      // Raven's dynamic null propagation: a missing task/date is null, and
      // null != DateTime.MinValue — only real MinValue dates are filtered.
      return (task?.taskCompletedDate ?? null) !== MIN_DATE;
    })
    .map((key) => {
      const task = (json[key] ?? {}) as TaskDoc;
      return {
        HotelId: (doc.HotelId as string) ?? null,
        TaskType: task.taskName ?? null,
        TaskTechName: task.taskTechName ?? null,
        TaskCompletedDate: task.taskCompletedDate ?? null,
        Count: 1,
      };
    });
}

// ---------------------------------------------------------------------------
// Comparison
// ---------------------------------------------------------------------------

type GroupKey = string;
type GroupCounts = Map<GroupKey, number>;

function keyOf(row: Record<string, unknown>): GroupKey {
  return JSON.stringify([
    row.HotelId ?? null,
    row.TaskType ?? null,
    row.TaskTechName ?? null,
    row.TaskCompletedDate ?? null,
  ]);
}

function toCounts(rows: Array<Record<string, unknown>>): GroupCounts {
  const counts: GroupCounts = new Map();
  for (const row of rows) {
    counts.set(keyOf(row), Number(row.Count));
  }
  return counts;
}

function diffCounts(expected: GroupCounts, actual: GroupCounts) {
  const missing: string[] = [];
  const extra: string[] = [];
  const mismatched: string[] = [];
  for (const [key, count] of expected) {
    const ours = actual.get(key);
    if (ours === undefined) missing.push(key);
    else if (ours !== count) mismatched.push(`${key}: raven=${count} ripply=${ours}`);
  }
  for (const key of actual.keys()) {
    if (!expected.has(key)) extra.push(key);
  }
  return { missing, extra, mismatched, ok: !missing.length && !extra.length && !mismatched.length };
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

console.log(`🌊 Ripply vs RavenDB oracle — ${RAVEN_URL}/databases/${RAVEN_DB}`);
console.log(`   hotels: ${HOTELS.join(', ')}\n`);

// 1. fetch the documents
const docsByHotel = new Map<string, Array<Record<string, unknown>>>();
for (const hotel of HOTELS) {
  const { results, stale } = await ravenQuery('HotelRoom/Search', `HotelId: ${hotel}`);
  const docs = results.filter((doc) => doc.HotelId === hotel); // exact matches only
  if (stale) console.warn(`   ⚠️ Raven Search results for ${hotel} were stale`);
  docsByHotel.set(hotel, docs);
}
const allDocs = [...docsByHotel.values()].flat();
console.log(`   fetched ${allDocs.length} room documents\n`);

// 2. a real SQLite database with Ripply on top
const db = new Database(':memory:');
db.exec(`CREATE TABLE hotel_rooms (id TEXT PRIMARY KEY, hotel_id TEXT, doc TEXT);`);

const ripply = createRipply({
  source: sqliteSource({ db, collections: { hotel_rooms: { pk: ['id'] } } }),
  store: sqliteStore({ db }),
  pollInterval: 60_000, // we drain explicitly
});

const taskCompletions = ripply.defineIndex('TaskCompletionsByTechDate', {
  collection: 'hotel_rooms',
  map: mapRoom,
  reduce: {
    groupBy: ['HotelId', 'TaskType', 'TaskTechName', 'TaskCompletedDate'],
    aggregate: { Count: { sum: 'Count' } },
  },
});

const insert = db.query(
  `INSERT INTO hotel_rooms (id, hotel_id, doc) VALUES (?1, ?2, ?3)`,
);
const insertDoc = (doc: Record<string, unknown>) => {
  const meta = doc['@metadata'] as Record<string, unknown>;
  const { ['@metadata']: _dropped, ...body } = doc;
  insert.run(meta['@id'] as string, doc.HotelId as string, JSON.stringify(body));
};

// half the docs BEFORE start() — exercises the initial scan build…
const half = Math.ceil(allDocs.length / 2);
for (const doc of allDocs.slice(0, half)) insertDoc(doc);
const t0 = performance.now();
await ripply.start();
const tBuild = performance.now() - t0;

// …half after — exercises trigger capture + incremental processing
const t1 = performance.now();
for (const doc of allDocs.slice(half)) insertDoc(doc);
await ripply.drain();
const tIncremental = performance.now() - t1;

console.log(
  `   initial build (${half} rooms): ${tBuild.toFixed(0)}ms · ` +
    `incremental (${allDocs.length - half} rooms): ${tIncremental.toFixed(0)}ms\n`,
);

// 3. compare, hotel by hotel
let failures = 0;
for (const hotel of HOTELS) {
  const { results: ravenRows, stale } = await ravenQuery(
    'HotelRoom/TaskCompletionsByTechDate',
    `HotelId: ${hotel}`,
  );
  if (stale) console.warn(`   ⚠️ Raven reduce results for ${hotel} were stale`);

  const ours = await taskCompletions.where({ HotelId: hotel }).all();
  const diff = diffCounts(toCounts(ravenRows), toCounts(ours));

  const rooms = docsByHotel.get(hotel)!.length;
  const icon = diff.ok ? '✅' : '❌';
  console.log(
    `${icon} ${hotel}  rooms=${rooms}  raven groups=${ravenRows.length}  ripply groups=${ours.length}`,
  );
  if (!diff.ok) {
    failures++;
    for (const key of diff.missing.slice(0, 5)) console.log(`     missing: ${key}`);
    for (const key of diff.extra.slice(0, 5)) console.log(`     extra:   ${key}`);
    for (const line of diff.mismatched.slice(0, 5)) console.log(`     count:   ${line}`);
    const more =
      diff.missing.length + diff.extra.length + diff.mismatched.length - 15;
    if (more > 0) console.log(`     … and ${more} more`);
  }
}

await ripply.stop();
db.close();

console.log(
  failures === 0
    ? `\n🎉 Ripply's incremental tallies match RavenDB's map-reduce exactly.`
    : `\n💥 ${failures} hotel(s) diverged — investigate above.`,
);
process.exit(failures === 0 ? 0 : 1);
