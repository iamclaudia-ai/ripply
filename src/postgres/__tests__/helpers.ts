/**
 * Postgres test plumbing. Requires a reachable Postgres (local Docker is
 * fine): `postgres://postgres:postgres@localhost:5432` by default, or set
 * `RIPPLY_TEST_PG` to a base URL (no database path). Each test FILE gets
 * its own database; each backend/test resets the public schema.
 */

import { SQL } from 'bun';

const BASE = process.env.RIPPLY_TEST_PG ?? 'postgres://postgres:postgres@localhost:5432';

/** Create (once) and return the URL of a per-file test database. */
export async function testDatabaseUrl(name: string): Promise<string> {
  const admin = new SQL({ url: `${BASE}/postgres`, max: 1 });
  const exists = (await admin`SELECT 1 FROM pg_database WHERE datname = ${name}`) as unknown[];
  if (exists.length === 0) {
    await admin.unsafe(`CREATE DATABASE "${name}"`);
  }
  await admin.close();
  return `${BASE}/${name}`;
}

/** Nuke everything (tables, triggers, functions) — a clean slate. */
export async function resetSchema(sql: SQL): Promise<void> {
  await sql.unsafe('DROP SCHEMA public CASCADE');
  await sql.unsafe('CREATE SCHEMA public');
}
