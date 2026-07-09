/**
 * Adapter matrix, memory column: the shared invariant suite running
 * against the in-memory reference adapters. (The SQLite column lives in
 * src/sqlite/__tests__/invariants.test.ts.)
 */

import { memorySource, memoryStore } from '../../memory/index';
import { runInvariantSuite, type SuiteBackend } from './suite';

function memoryBackend(): SuiteBackend {
  const source = memorySource({ collections: { work_orders: { pk: ['id'] } } });
  const store = memoryStore();
  return {
    source,
    store,
    insert: (row) => source.insert('work_orders', row),
    update: (row) => source.update('work_orders', row),
    remove: (id) => source.delete('work_orders', [id]),
    snapshot: () => source.snapshot('work_orders'),
  };
}

runInvariantSuite('memory', memoryBackend);
