/**
 * In-memory reference Source.
 *
 * Holds real tables (Map of pk → row) plus an append-only changelog, exactly
 * mirroring the SQLite trigger/changelog shape. Tests drive it through
 * `insert`/`update`/`delete` helpers; the engine consumes it purely through
 * the `Source` interface. Clarity over speed — this is the reference.
 */

import { RipplyError } from '../core/errors';
import { pkKeyOf } from '../core/canonical';
import type {
  Change,
  ChangeBatch,
  Cursor,
  PkValue,
  Row,
  Scalar,
  Source,
  Unsubscribe,
} from '../core/types';

export interface MemorySourceOptions {
  collections: Record<string, { pk: string[] }>;
}

interface MemoryCollection {
  pk: string[];
  rows: Map<string, Row>; // pkKey → current row
}

interface ChangelogRecord {
  seq: number;
  collection: string;
  change: Change;
}

export class MemorySource implements Source {
  private readonly collections = new Map<string, MemoryCollection>();
  private readonly changelog: ChangelogRecord[] = [];
  private nextSeq = 1;
  private readonly listeners = new Map<string, Set<() => void>>();

  constructor(options: MemorySourceOptions) {
    for (const [name, config] of Object.entries(options.collections)) {
      if (!Array.isArray(config.pk) || config.pk.length === 0) {
        throw new RipplyError(`collection "${name}": pk must name at least one column`);
      }
      this.collections.set(name, { pk: [...config.pk], rows: new Map() });
    }
  }

  // -------------------------------------------------------------------------
  // Source interface
  // -------------------------------------------------------------------------

  async install(collection: string): Promise<void> {
    this.mustCollection(collection); // capture is inherent here; just validate
  }

  async poll(collection: string, cursor: Cursor, limit: number): Promise<ChangeBatch> {
    this.mustCollection(collection);
    const from = cursor === null ? 0 : asSeq(cursor);
    const records = this.changelog
      .filter((r) => r.collection === collection && r.seq > from)
      .slice(0, limit);
    return {
      changes: records.map((r) => structuredClone(r.change)),
      nextCursor: records.length ? records[records.length - 1]!.seq : cursor,
    };
  }

  async scan(
    collection: string,
    onRow: (pk: PkValue, row: Row) => void | Promise<void>,
  ): Promise<void> {
    const col = this.mustCollection(collection);
    for (const [pkKey, row] of col.rows) {
      await onRow(JSON.parse(pkKey) as PkValue, structuredClone(row));
    }
  }

  async currentCursor(collection: string): Promise<Cursor> {
    this.mustCollection(collection);
    return this.nextSeq > 1 ? this.nextSeq - 1 : null;
  }

  wakeups(collection: string, onChange: () => void): Unsubscribe {
    const set = this.listeners.get(collection) ?? new Set();
    set.add(onChange);
    this.listeners.set(collection, set);
    return () => set.delete(onChange);
  }

  // -------------------------------------------------------------------------
  // Test drivers (play the role of application writes + triggers)
  // -------------------------------------------------------------------------

  insert(collection: string, row: Row): void {
    const col = this.mustCollection(collection);
    const pk = this.pkOf(collection, col, row);
    const key = pkKeyOf(pk);
    if (col.rows.has(key)) {
      throw new RipplyError(`insert: duplicate pk ${key} in "${collection}"`);
    }
    col.rows.set(key, structuredClone(row));
    this.append(collection, { pk, op: 'insert', after: structuredClone(row) });
  }

  update(collection: string, row: Row): void {
    const col = this.mustCollection(collection);
    const pk = this.pkOf(collection, col, row);
    const key = pkKeyOf(pk);
    if (!col.rows.has(key)) {
      throw new RipplyError(`update: no row with pk ${key} in "${collection}"`);
    }
    col.rows.set(key, structuredClone(row));
    this.append(collection, { pk, op: 'update', after: structuredClone(row) });
  }

  delete(collection: string, pk: PkValue): void {
    const col = this.mustCollection(collection);
    const key = pkKeyOf(pk);
    if (!col.rows.delete(key)) {
      throw new RipplyError(`delete: no row with pk ${key} in "${collection}"`);
    }
    this.append(collection, { pk, op: 'delete', after: null });
  }

  /** Current table contents (for test oracles). */
  snapshot(collection: string): Array<{ pk: PkValue; row: Row }> {
    const col = this.mustCollection(collection);
    return [...col.rows.entries()].map(([pkKey, row]) => ({
      pk: JSON.parse(pkKey) as PkValue,
      row: structuredClone(row),
    }));
  }

  rowCount(collection: string): number {
    return this.mustCollection(collection).rows.size;
  }

  // -------------------------------------------------------------------------

  private append(collection: string, change: Omit<Change, 'seq'>): void {
    const seq = this.nextSeq++;
    this.changelog.push({ seq, collection, change: { ...change, seq } });
    for (const listener of this.listeners.get(collection) ?? []) {
      listener();
    }
  }

  private mustCollection(collection: string): MemoryCollection {
    const col = this.collections.get(collection);
    if (!col) throw new RipplyError(`unknown collection "${collection}"`);
    return col;
  }

  private pkOf(collection: string, col: MemoryCollection, row: Row): PkValue {
    return col.pk.map((column) => {
      const value = row[column];
      if (
        value === undefined ||
        (value !== null &&
          typeof value !== 'string' &&
          typeof value !== 'number' &&
          typeof value !== 'boolean')
      ) {
        throw new RipplyError(
          `collection "${collection}": pk column "${column}" must be a scalar, got ${JSON.stringify(value)}`,
        );
      }
      return value as Scalar;
    });
  }
}

function asSeq(cursor: Cursor): number {
  if (typeof cursor !== 'number') {
    throw new RipplyError(`memory source cursors are numbers, got ${JSON.stringify(cursor)}`);
  }
  return cursor;
}

export function memorySource(options: MemorySourceOptions): MemorySource {
  return new MemorySource(options);
}
