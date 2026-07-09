/**
 * ripply — real-time incremental map-reduce indexes for SQLite and Postgres.
 *
 * This entry point is the backend-free core. Adapters:
 *   `ripply/memory`   — in-memory reference (also used by the invariant tests)
 *   `ripply/sqlite`   — Phase 1
 *   `ripply/postgres` — Phase 2/3
 */

export * from './core/types';
export { RipplyError } from './core/errors';
export { canonicalJson, groupKeyOf, groupOfKey, pkKeyOf } from './core/canonical';
export {
  parseReduceSpec,
  isLinear,
  type ParsedAggregate,
} from './core/aggregates';
export { fnv1aHex, mapVersionOf } from './core/version';
export {
  Engine,
  createEngine,
  type CreateEngineOptions,
  type IndexRuntime,
} from './core/engine';
export type { VerifyResult } from './core/rebuild';
export {
  Ripply,
  IndexQuery,
  createRipply,
  type CreateRipplyOptions,
  type TypedIndexDefinition,
} from './core/ripply';
