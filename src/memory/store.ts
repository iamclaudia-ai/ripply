/**
 * In-memory reference Store.
 *
 * Transactions are real: the whole state is snapshotted (structuredClone) at
 * transaction start and restored if the callback throws — so crash-safety
 * tests exercise genuine rollback, not a simulation. Reads return clones to
 * prevent aliasing. Clarity over speed — this is the reference.
 */

import { pkKeyOf } from '../core/canonical';
import { RipplyError } from '../core/errors';
import type {
  Cursor,
  IndexMeta,
  PkValue,
  ReducedRow,
  Store,
  StoredEntry,
  StoreTx,
} from '../core/types';

interface IndexState {
  entries: Map<string, StoredEntry[]>; // pkKey → this row's entries, by ord
  reduced: Map<string, ReducedRow>; // groupKey → reduced row
  cursor: Cursor;
  meta: IndexMeta | null;
}

export class MemoryStore implements Store {
  private indexes = new Map<string, IndexState>();
  private inTransaction = false;

  async transaction<T>(fn: (tx: StoreTx) => Promise<T>): Promise<T> {
    if (this.inTransaction) {
      throw new RipplyError('memory store does not support nested transactions');
    }
    const snapshot = structuredClone(this.indexes);
    this.inTransaction = true;
    try {
      return await fn(new MemoryStoreTx(this));
    } catch (error) {
      this.indexes = snapshot; // rollback
      throw error;
    } finally {
      this.inTransaction = false;
    }
  }

  /** @internal */
  stateOf(index: string): IndexState {
    let state = this.indexes.get(index);
    if (!state) {
      state = { entries: new Map(), reduced: new Map(), cursor: null, meta: null };
      this.indexes.set(index, state);
    }
    return state;
  }
}

class MemoryStoreTx implements StoreTx {
  constructor(private readonly store: MemoryStore) {}

  async readEntries(index: string, pk: PkValue): Promise<StoredEntry[]> {
    const entries = this.store.stateOf(index).entries.get(pkKeyOf(pk)) ?? [];
    return structuredClone(entries);
  }

  async replaceEntries(index: string, pk: PkValue, entries: StoredEntry[]): Promise<void> {
    const state = this.store.stateOf(index);
    const key = pkKeyOf(pk);
    if (entries.length === 0) {
      state.entries.delete(key);
    } else {
      state.entries.set(key, structuredClone(entries));
    }
  }

  async readGroupEntries(index: string, groupKey: string): Promise<StoredEntry[]> {
    const all: StoredEntry[] = [];
    for (const entries of this.store.stateOf(index).entries.values()) {
      for (const entry of entries) {
        if (entry.groupKey === groupKey) all.push(structuredClone(entry));
      }
    }
    return all;
  }

  async allEntries(index: string): Promise<StoredEntry[]> {
    const all: StoredEntry[] = [];
    for (const entries of this.store.stateOf(index).entries.values()) {
      all.push(...structuredClone(entries));
    }
    return all.sort((a, b) => {
      const ka = pkKeyOf(a.pk);
      const kb = pkKeyOf(b.pk);
      if (ka !== kb) return ka < kb ? -1 : 1;
      return a.ord - b.ord;
    });
  }

  async getReduced(index: string, groupKey: string): Promise<ReducedRow | null> {
    const row = this.store.stateOf(index).reduced.get(groupKey);
    return row ? structuredClone(row) : null;
  }

  async putReduced(index: string, row: ReducedRow): Promise<void> {
    this.store.stateOf(index).reduced.set(row.groupKey, structuredClone(row));
  }

  async deleteReduced(index: string, groupKey: string): Promise<void> {
    this.store.stateOf(index).reduced.delete(groupKey);
  }

  async allReduced(index: string): Promise<ReducedRow[]> {
    return [...this.store.stateOf(index).reduced.values()]
      .map((row) => structuredClone(row))
      .sort((a, b) => (a.groupKey < b.groupKey ? -1 : a.groupKey > b.groupKey ? 1 : 0));
  }

  async truncateIndex(index: string): Promise<void> {
    const state = this.store.stateOf(index);
    state.entries.clear();
    state.reduced.clear();
  }

  async getCursor(index: string): Promise<Cursor> {
    return this.store.stateOf(index).cursor;
  }

  async setCursor(index: string, cursor: Cursor): Promise<void> {
    this.store.stateOf(index).cursor = cursor;
  }

  async getIndexMeta(index: string): Promise<IndexMeta | null> {
    const meta = this.store.stateOf(index).meta;
    return meta ? { ...meta } : null;
  }

  async setIndexMeta(index: string, meta: IndexMeta): Promise<void> {
    this.store.stateOf(index).meta = { ...meta };
  }
}

export function memoryStore(): MemoryStore {
  return new MemoryStore();
}
