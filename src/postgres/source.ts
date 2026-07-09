/**
 * Postgres Source — trigger-outbox change capture (DESIGN.md §3).
 *
 * `install()` attaches one generic plpgsql trigger function,
 * `_ripply_capture()`, to each captured table. It appends
 * `(pk, op, after)` to the `_ripply_changelog` outbox in the SAME
 * transaction as the write — `to_jsonb(NEW)` for the after-image, pk
 * columns via trigger arguments — and `pg_notify`s '_ripply' for any
 * future low-latency listener. This is the right default everywhere, and
 * especially on hosted Postgres (Neon): no replication slot to manage, no
 * WAL retention fighting scale-to-zero, and it works through connection
 * poolers. Logical-decoding CDC is the Phase 3 opt-in.
 *
 * ## The ordering gotcha, solved: snapshot-windowed cursors
 *
 * A BIGSERIAL `seq` is assigned at INSERT time but transactions commit in
 * any order — a naive `WHERE seq > cursor` poll can observe seq 5 and
 * advance past seq 3 while seq 3's transaction is still in flight,
 * PERMANENTLY skipping it (DESIGN.md §8 invariant 9). SQLite doesn't have
 * this problem (single writer); Postgres does.
 *
 * The cursor here is therefore not a seq but a JSON string
 * `{ prev, cur, seq }` of `pg_snapshot` values:
 *
 *   - a poll freezes a WINDOW: rows visible in `cur` and NOT visible in
 *     `prev` — an immutable set, because snapshots are immutable
 *   - the window is drained in seq order (`seq` tracks progress within it;
 *     seq order == effect order for any single row, because row locks
 *     serialize writers per row)
 *   - when the window is exhausted, `prev ← cur` and a fresh `cur` opens
 *
 * A transaction that grabbed a small seq but committed late is simply not
 * visible in the frozen window — it lands in a LATER window instead of
 * being skipped. Late commits are unlosable by construction.
 *
 * The changelog is read-only to consumers (per-index cursors; multiple
 * indexes share it) and cleaned by `prune()` once every index on a
 * collection has advanced past a change.
 *
 * v1 notes: collection == table name in the current schema; one Ripply
 * processor per database (same assumption as SQLite). Map functions see
 * rows AS to_jsonb RENDERS THEM — from the trigger's after-image and from
 * scan() alike (scan selects `to_jsonb(t)` server-side so both paths are
 * byte-identical): bigint/numeric are JSON numbers, timestamps are ISO
 * strings, json/jsonb are parsed values. No `wakeups` yet: Neon's pooler
 * doesn't carry LISTEN reliably, so the Ripply poll fallback (default
 * 250ms) provides freshness.
 */

import type { SQL } from 'bun';
import { RipplyError } from '../core/errors';
import type { Change, ChangeBatch, Cursor, PkValue, Row, Source } from '../core/types';

export interface PostgresSourceOptions {
  sql: SQL;
  collections: Record<string, { pk: string[] }>;
}

const IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;

function ident(name: string): string {
  if (!IDENTIFIER.test(name)) {
    throw new RipplyError(`postgres: invalid identifier "${name}"`);
  }
  return `"${name}"`;
}

function literal(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

/** A pg_snapshot in which nothing is visible — "the beginning". */
const BEGINNING = '1:1:';

/** The windowed cursor, serialized as canonical-ish JSON in `Cursor`. */
interface WindowCursor {
  /** Everything visible here has been applied. */
  prev: string;
  /** The frozen window being drained (null = no open window). */
  cur: string | null;
  /** Progress within the frozen window (last applied seq). */
  seq: number;
}

interface ChangeRecord {
  seq: string;
  pk: string;
  op: Change['op'];
  after: string | null;
}

const WINDOW_FILTER = `
  pg_visible_in_snapshot(txid, $CUR::pg_snapshot)
  AND NOT pg_visible_in_snapshot(txid, $PREV::pg_snapshot)`;

export class PostgresSource implements Source {
  private readonly sql: SQL;
  private readonly collections = new Map<string, { pk: string[] }>();
  private changelogReady: Promise<void> | null = null;

  constructor(options: PostgresSourceOptions) {
    this.sql = options.sql;
    for (const [name, config] of Object.entries(options.collections)) {
      if (!Array.isArray(config.pk) || config.pk.length === 0) {
        throw new RipplyError(`collection "${name}": pk must name at least one column`);
      }
      ident(name);
      config.pk.forEach(ident);
      this.collections.set(name, { pk: [...config.pk] });
    }
  }

  async install(collection: string): Promise<void> {
    const config = this.mustCollection(collection);
    await this.ensureChangelog();

    const columns = (
      (await this.sql.unsafe(
        `SELECT column_name FROM information_schema.columns
         WHERE table_schema = current_schema() AND table_name = $1`,
        [collection],
      )) as Array<{ column_name: string }>
    ).map((record) => record.column_name);
    if (columns.length === 0) {
      throw new RipplyError(`postgres source: table "${collection}" does not exist`);
    }
    for (const pkColumn of config.pk) {
      if (!columns.includes(pkColumn)) {
        throw new RipplyError(
          `postgres source: table "${collection}" has no pk column "${pkColumn}"`,
        );
      }
    }

    // drop + recreate: idempotent, and refreshes the pk argument list
    const table = ident(collection);
    await this.sql.unsafe(`DROP TRIGGER IF EXISTS _ripply_capture ON ${table}`);
    await this.sql.unsafe(
      `CREATE TRIGGER _ripply_capture
       AFTER INSERT OR UPDATE OR DELETE ON ${table}
       FOR EACH ROW EXECUTE FUNCTION _ripply_capture(${config.pk.map(literal).join(', ')})`,
    );
  }

  async poll(collection: string, cursor: Cursor, limit: number): Promise<ChangeBatch> {
    this.mustCollection(collection);
    await this.ensureChangelog();

    let state = parseCursor(cursor);
    if (state.cur === null) {
      state = { prev: state.prev, cur: await this.snapshot(), seq: 0 };
    }
    let records = await this.selectWindow(collection, windowOf(state), limit);
    if (records.length === 0) {
      // window exhausted — advance and open the next one WITHIN this poll,
      // or new commits (invisible in the frozen window) would never surface
      state = { prev: state.cur!, cur: await this.snapshot(), seq: 0 };
      records = await this.selectWindow(collection, windowOf(state), limit);
    }

    const next: WindowCursor =
      records.length === limit
        ? { prev: state.prev, cur: state.cur, seq: Number(records[records.length - 1]!.seq) }
        : { prev: state.cur!, cur: null, seq: 0 };

    return {
      changes: records.map((record) => ({
        pk: JSON.parse(record.pk) as PkValue,
        op: record.op,
        after: record.after === null ? null : (JSON.parse(record.after) as Row),
        seq: Number(record.seq),
      })),
      nextCursor: serializeCursor(next),
    };
  }

  async scan(
    collection: string,
    onRow: (pk: PkValue, row: Row) => void | Promise<void>,
  ): Promise<void> {
    const config = this.mustCollection(collection);
    // Render each row through to_jsonb ON THE SERVER — the exact same
    // rendering the capture trigger uses for after-images. A rebuild's
    // scan and incremental capture then see byte-identical row shapes
    // (bigint → number, timestamptz → offset ISO string, jsonb → value),
    // which invariant 1 depends on. Client-side driver decoding would
    // disagree (Bun returns int8 as a string, timestamps as Dates).
    const records = (await this.sql.unsafe(
      `SELECT to_jsonb(t)::text AS row FROM ${ident(collection)} AS t`,
    )) as Array<{ row: string }>;
    for (const record of records) {
      const row = JSON.parse(record.row) as Row;
      await onRow(config.pk.map((column) => row[column]) as PkValue, row);
    }
  }

  async currentCursor(collection: string): Promise<Cursor> {
    this.mustCollection(collection);
    // everything visible NOW counts as applied (a rebuild's scan sees it);
    // anything not yet visible replays through later windows
    return serializeCursor({ prev: await this.snapshot(), cur: null, seq: 0 });
  }

  async prune(collection: string, cursors: Cursor[]): Promise<number> {
    this.mustCollection(collection);
    await this.ensureChangelog();
    if (cursors.length === 0 || cursors.some((cursor) => cursor === null)) return 0;

    // a change is prunable when EVERY index has applied it: visible in the
    // index's `prev` snapshot, or inside its frozen window at seq <= its seq
    const clauses: string[] = [];
    const params: Array<string | number | null> = [collection];
    for (const cursor of cursors) {
      const state = parseCursor(cursor);
      const p = params.length + 1;
      clauses.push(
        `(pg_visible_in_snapshot(txid, $${p}::pg_snapshot)
          OR ($${p + 1}::pg_snapshot IS NOT NULL
              AND pg_visible_in_snapshot(txid, $${p + 1}::pg_snapshot)
              AND seq <= $${p + 2}))`,
      );
      params.push(state.prev, state.cur, state.seq);
    }
    const result = (await this.sql.unsafe(
      `DELETE FROM _ripply_changelog WHERE collection = $1 AND ${clauses.join(' AND ')}`,
      params,
    )) as { count?: number };
    return result.count ?? 0;
  }

  // ---------------------------------------------------------------------------

  private async snapshot(): Promise<string> {
    const [record] = (await this.sql`SELECT pg_current_snapshot()::text AS snap`) as [
      { snap: string },
    ];
    return record.snap;
  }

  private async selectWindow(
    collection: string,
    state: { prev: string; cur: string; seq: number },
    limit: number,
  ): Promise<ChangeRecord[]> {
    // NOTE: ORDER BY must be the QUALIFIED column c.seq — the select list
    // aliases `seq::text AS seq`, and an unqualified `ORDER BY seq` binds
    // to that TEXT output column, silently sorting lexicographically
    // ("10" < "2") and breaking the numeric within-window progression.
    return (await this.sql.unsafe(
      `SELECT c.seq::text AS seq, c.pk::text AS pk, c.op, c.after::text AS after
       FROM _ripply_changelog c
       WHERE c.collection = $1 AND c.seq > $2
         AND ${WINDOW_FILTER.replace('$CUR', '$3').replace('$PREV', '$4')}
       ORDER BY c.seq
       LIMIT $5`,
      [collection, state.seq, state.cur, state.prev, limit],
    )) as ChangeRecord[];
  }

  private ensureChangelog(): Promise<void> {
    this.changelogReady ??= (async () => {
      await this.sql.unsafe(`
        CREATE TABLE IF NOT EXISTS _ripply_changelog (
          seq        BIGSERIAL PRIMARY KEY,
          txid       xid8 NOT NULL DEFAULT pg_current_xact_id(),
          collection TEXT NOT NULL,
          pk         JSONB NOT NULL,
          op         TEXT NOT NULL,
          after      JSONB
        )`);
      await this.sql.unsafe(`
        CREATE INDEX IF NOT EXISTS _ripply_changelog_by_collection
          ON _ripply_changelog (collection, seq)`);
      // ONE generic capture function for every table; pk columns arrive as
      // trigger arguments, the after-image is just to_jsonb(row)
      await this.sql.unsafe(`
        CREATE OR REPLACE FUNCTION _ripply_capture() RETURNS trigger AS $fn$
        DECLARE
          rec jsonb;
          pk  jsonb := '[]'::jsonb;
          col text;
        BEGIN
          rec := CASE WHEN TG_OP = 'DELETE' THEN to_jsonb(OLD) ELSE to_jsonb(NEW) END;
          FOREACH col IN ARRAY TG_ARGV LOOP
            pk := pk || jsonb_build_array(rec -> col);
          END LOOP;
          INSERT INTO _ripply_changelog (collection, pk, op, after)
          VALUES (
            TG_TABLE_NAME,
            pk,
            lower(TG_OP),
            CASE WHEN TG_OP = 'DELETE' THEN NULL ELSE rec END
          );
          PERFORM pg_notify('_ripply', TG_TABLE_NAME);
          RETURN NULL;
        END
        $fn$ LANGUAGE plpgsql`);
    })();
    return this.changelogReady;
  }

  private mustCollection(collection: string): { pk: string[] } {
    const config = this.collections.get(collection);
    if (!config) throw new RipplyError(`unknown collection "${collection}"`);
    return config;
  }
}

/** Narrow a cursor whose window is known to be open. */
function windowOf(state: WindowCursor): { prev: string; cur: string; seq: number } {
  if (state.cur === null) throw new RipplyError('postgres source: window not open');
  return { prev: state.prev, cur: state.cur, seq: state.seq };
}

function parseCursor(cursor: Cursor): WindowCursor {
  if (cursor === null) return { prev: BEGINNING, cur: null, seq: 0 };
  if (typeof cursor !== 'string') {
    throw new RipplyError(
      `postgres source cursors are JSON strings, got ${JSON.stringify(cursor)}`,
    );
  }
  try {
    const state = JSON.parse(cursor) as WindowCursor;
    if (typeof state.prev !== 'string') throw new Error('missing prev');
    return { prev: state.prev, cur: state.cur ?? null, seq: state.seq ?? 0 };
  } catch (error) {
    throw new RipplyError(`postgres source: malformed cursor ${cursor}: ${String(error)}`);
  }
}

function serializeCursor(state: WindowCursor): string {
  return JSON.stringify({ prev: state.prev, cur: state.cur, seq: state.seq });
}

export function postgresSource(options: PostgresSourceOptions): PostgresSource {
  return new PostgresSource(options);
}
