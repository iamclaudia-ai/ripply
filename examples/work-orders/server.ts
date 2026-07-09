/**
 * Ripply live demo — work orders 🌊
 *
 * A Bun server that:
 *   1. runs Ripply over an in-memory SQLite database,
 *   2. simulates an app writing random inserts/updates/deletes,
 *   3. streams the tallies to a live dashboard over SSE.
 *
 * The point to notice: every snapshot below is read with PLAIN SQL from the
 * materialized tally tables (ripply_*). The "app" needs zero knowledge of
 * how the aggregates are maintained — and revenueByMonth is a CASCADING
 * index that consumes the day tally's table.
 *
 *   bun examples/work-orders/server.ts   →  http://localhost:4242
 */

import { Database } from 'bun:sqlite';
import { createRipply } from '../../src/index';
import { sqliteSource, sqliteStore } from '../../src/sqlite/index';

const PORT = Number(process.env.PORT ?? 4242);

// ---------------------------------------------------------------------------
// Database + Ripply
// ---------------------------------------------------------------------------

const db = new Database(':memory:');
db.exec(`
  CREATE TABLE work_orders (
    id            TEXT PRIMARY KEY,
    status        TEXT NOT NULL,
    technician    TEXT NOT NULL,
    revenue       INTEGER NOT NULL,
    completed_day TEXT
  );
`);

const ripply = createRipply({
  source: sqliteSource({
    db,
    collections: {
      work_orders: { pk: ['id'] },
      ripply_revenueByDay: { pk: ['group_key'] }, // ← a tally is a collection too
    },
  }),
  store: sqliteStore({ db }),
  pollInterval: 60_000, // the simulator drains explicitly after each write
});

ripply.defineIndex('ordersByStatus', {
  collection: 'work_orders',
  map: (wo) => ({ status: wo.status, n: 1 }),
  reduce: { groupBy: ['status'], aggregate: { orders: { sum: 'n' } } },
});

ripply.defineIndex('revenueByTech', {
  collection: 'work_orders',
  map: (wo) =>
    wo.status === 'completed'
      ? { tech: wo.technician, revenue: wo.revenue as number, n: 1 }
      : null,
  reduce: {
    groupBy: ['tech'],
    aggregate: { revenue: 'sum', jobs: { sum: 'n' }, avgJob: { avg: 'revenue' } },
  },
  indexes: [['revenue']],
});

ripply.defineIndex('revenueByDay', {
  collection: 'work_orders',
  map: (wo) =>
    wo.status === 'completed' && wo.completed_day
      ? { day: wo.completed_day as string, revenue: wo.revenue as number, n: 1 }
      : null,
  reduce: { groupBy: ['day'], aggregate: { revenue: 'sum', jobs: { sum: 'n' } } },
  indexes: [['day']],
});

// The cascade: months roll up the DAY TALLY, not the source rows.
ripply.defineIndex('revenueByMonth', {
  collection: 'ripply_revenueByDay',
  map: (day) => ({
    month: (day.day as string).slice(0, 7),
    revenue: day.revenue as number,
    jobs: day.jobs as number,
  }),
  reduce: {
    groupBy: ['month'],
    aggregate: { revenue: 'sum', jobs: 'sum', peakDay: { max: 'revenue' } },
  },
});

// ---------------------------------------------------------------------------
// The simulated application (plain SQL writes; triggers do the capturing)
// ---------------------------------------------------------------------------

const TECHS = ['Kai', 'Marina', 'Nami', 'Cruz', 'Isla'];
const STATUSES = ['pending', 'active', 'completed'] as const;

const pick = <T>(items: readonly T[]): T => items[Math.floor(Math.random() * items.length)]!;

function randomDay(): string {
  const daysAgo = Math.floor(Math.random() * 45);
  return new Date(Date.now() - daysAgo * 86_400_000).toISOString().slice(0, 10);
}

interface WorkOrder {
  id: string;
  status: (typeof STATUSES)[number];
  technician: string;
  revenue: number;
  completed_day: string | null;
}

function randomOrder(id: string): WorkOrder {
  const status = Math.random() < 0.45 ? 'completed' : pick(STATUSES);
  return {
    id,
    status,
    technician: pick(TECHS),
    revenue: 40 + Math.floor(Math.random() * 460),
    completed_day: status === 'completed' ? randomDay() : null,
  };
}

let nextId = 1;
const live: string[] = [];
let opsApplied = 0;
const opsLog: Array<{ kind: 'insert' | 'update' | 'delete'; line: string }> = [];

const insertStmt = db.query(
  `INSERT INTO work_orders (id, status, technician, revenue, completed_day)
   VALUES (?1, ?2, ?3, ?4, ?5)`,
);
const updateStmt = db.query(
  `UPDATE work_orders SET status = ?2, technician = ?3, revenue = ?4, completed_day = ?5
   WHERE id = ?1`,
);

function logOp(kind: 'insert' | 'update' | 'delete', line: string) {
  opsApplied++;
  opsLog.push({ kind, line });
  if (opsLog.length > 12) opsLog.shift();
}

function performRandomOp(): void {
  const roll = Math.random();
  if (live.length < 15 || roll < 0.5) {
    const order = randomOrder(`wo_${String(nextId++).padStart(4, '0')}`);
    insertStmt.run(order.id, order.status, order.technician, order.revenue, order.completed_day);
    live.push(order.id);
    logOp(
      'insert',
      `+ ${order.id}  ${order.technician}  $${order.revenue}  ${order.status}${order.completed_day ? '  ' + order.completed_day : ''}`,
    );
  } else if (roll < 0.85) {
    const id = pick(live);
    const order = randomOrder(id);
    updateStmt.run(order.id, order.status, order.technician, order.revenue, order.completed_day);
    logOp('update', `~ ${id}  → ${order.status}  $${order.revenue}${order.completed_day ? '  ' + order.completed_day : ''}`);
  } else {
    const id = live.splice(Math.floor(Math.random() * live.length), 1)[0]!;
    db.query(`DELETE FROM work_orders WHERE id = ?1`).run(id);
    logOp('delete', `− ${id}  removed`);
  }
}

// ---------------------------------------------------------------------------
// Snapshots — PLAIN SQL against the materialized tally tables
// ---------------------------------------------------------------------------

let lastDrainMs = 0;
let drainCount = 0;
let drainTotalMs = 0;

function snapshot() {
  const one = <T>(sql: string): T => db.query(sql).get() as T;
  return {
    stats: {
      revenue: one<{ v: number }>(
        `SELECT COALESCE(SUM(revenue), 0) AS v FROM ripply_revenueByMonth`,
      ).v,
      orders: one<{ v: number }>(`SELECT COUNT(*) AS v FROM work_orders`).v,
      ops: opsApplied,
      drainMs: lastDrainMs,
      avgDrainMs: drainCount ? drainTotalMs / drainCount : 0,
      backlog: one<{ v: number }>(`SELECT COUNT(*) AS v FROM _ripply_changelog`).v,
    },
    byStatus: db
      .query(`SELECT status, orders FROM ripply_ordersByStatus ORDER BY orders DESC`)
      .all(),
    byTech: db
      .query(
        `SELECT tech, revenue, jobs, avgJob FROM ripply_revenueByTech ORDER BY revenue DESC`,
      )
      .all(),
    byDay: (
      db
        .query(
          `SELECT day, revenue, jobs FROM ripply_revenueByDay ORDER BY day DESC LIMIT 21`,
        )
        .all() as Array<Record<string, unknown>>
    ).reverse(),
    byMonth: db
      .query(
        `SELECT month, revenue, jobs, peakDay FROM ripply_revenueByMonth ORDER BY month`,
      )
      .all(),
    ops: [...opsLog].reverse(),
  };
}

// ---------------------------------------------------------------------------
// SSE plumbing + server
// ---------------------------------------------------------------------------

const encoder = new TextEncoder();
const clients = new Set<ReadableStreamDefaultController<Uint8Array>>();

function broadcast(): void {
  const chunk = encoder.encode(`data: ${JSON.stringify(snapshot())}\n\n`);
  for (const client of clients) {
    try {
      client.enqueue(chunk);
    } catch {
      clients.delete(client);
    }
  }
}

Bun.serve({
  port: PORT,
  fetch(req) {
    const { pathname } = new URL(req.url);
    if (pathname === '/') {
      return new Response(Bun.file(new URL('./index.html', import.meta.url).pathname), {
        headers: { 'Content-Type': 'text/html; charset=utf-8' },
      });
    }
    if (pathname === '/events') {
      let controller: ReadableStreamDefaultController<Uint8Array>;
      const stream = new ReadableStream<Uint8Array>({
        start(c) {
          controller = c;
          clients.add(c);
          c.enqueue(encoder.encode(`data: ${JSON.stringify(snapshot())}\n\n`));
        },
        cancel() {
          clients.delete(controller);
        },
      });
      return new Response(stream, {
        headers: {
          'Content-Type': 'text/event-stream',
          'Cache-Control': 'no-cache',
          Connection: 'keep-alive',
        },
      });
    }
    return new Response('not found', { status: 404 });
  },
});

// ---------------------------------------------------------------------------
// Go
// ---------------------------------------------------------------------------

// seed some history BEFORE start() — the initial build picks it up via scan
for (let i = 0; i < 40; i++) {
  const order = randomOrder(`wo_${String(nextId++).padStart(4, '0')}`);
  insertStmt.run(order.id, order.status, order.technician, order.revenue, order.completed_day);
  live.push(order.id);
}
await ripply.start();

async function tick(): Promise<void> {
  const burst = 1 + Math.floor(Math.random() * 3);
  for (let i = 0; i < burst; i++) performRandomOp();
  const t = performance.now();
  await ripply.drain(); // rows → day tally → month tally, one drain
  lastDrainMs = performance.now() - t;
  drainCount++;
  drainTotalMs += lastDrainMs;
  broadcast();
  setTimeout(tick, 350 + Math.random() * 550);
}
void tick();

console.log(`🌊 ripply live demo → http://localhost:${PORT}`);
console.log(`   watching tables: ripply_ordersByStatus, ripply_revenueByTech,`);
console.log(`                    ripply_revenueByDay → ripply_revenueByMonth (cascade)`);
