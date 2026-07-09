/**
 * ripply/postgres — the Postgres adapter (trigger-outbox capture).
 *
 * ```ts
 * import { SQL } from 'bun';
 * import { createRipply } from 'ripply';
 * import { postgresSource, postgresStore } from 'ripply/postgres';
 *
 * const sql = new SQL(process.env.DATABASE_URL);
 * const ripply = createRipply({
 *   source: postgresSource({ sql, collections: { games: { pk: ['id'] } } }),
 *   store: postgresStore({ sql }),
 * });
 * ```
 */

export {
  PostgresSource,
  postgresSource,
  type PostgresSourceOptions,
} from './source';
export {
  PostgresStore,
  postgresStore,
  type PostgresStoreOptions,
} from './store';
