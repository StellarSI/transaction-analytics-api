/**
 * Postgres-backed implementation of the {@link BackfillStore} port (issue #17).
 *
 * Wraps the shared `postgres.Sql` pool so the Hubble backfill can batch
 * insert into `ledger_operations` and perform the idempotency lookup without
 * the backfill module itself depending on the concrete database driver.
 * Kept separate from `backfill.ts` so unit tests of the transformation and
 * orchestration logic never load the database driver.
 */

import type { LedgerOperationInsert } from '../../db/schema/ledger';
import type { BackfillStore } from './backfill';

/**
 * Minimal `postgres.Sql` surface the store needs: a tagged-template query
 * helper and the multi-row VALUES builder `sql(rows, ...columns)`. Satisfied
 * by the shared pool from `src/db/pool.ts`.
 *
 * The tagged-template call resolves to `unknown` because postgres.js returns
 * row arrays for SELECT but a `Result` object for INSERT/UPDATE; callers
 * narrow with a cast at each call site.
 */
export interface BackfillSql {
  (strings: TemplateStringsArray, ...values: unknown[]): Promise<unknown>;
  (rows: readonly unknown[], ...columns: string[]): unknown;
}

/** Column list shared by the insert and the conflict-key lookup. */
const INSERT_COLUMNS = [
  'ledger_sequence',
  'ledger_closed_at',
  'operation_type',
  'source_account',
  'destination_account',
  'asset_code',
  'asset_issuer',
  'asset_type',
  'amount',
  'fee_charged',
  'transaction_hash',
  'operation_index',
  'successful',
] as const;

/**
 * Creates a {@link BackfillStore} bound to a `postgres.Sql` instance.
 *
 * Idempotency: `insertOperations` filters rows whose
 * `(transaction_hash, operation_index)` key already exists in the table,
 * then inserts the remainder. The lookup is a single `= ANY($1)` query so a
 * batch costs one extra round-trip instead of one per row.
 */
export function createPostgresBackfillStore(sql: BackfillSql): BackfillStore {
  return {
    async insertOperations(rows: LedgerOperationInsert[]): Promise<number> {
      if (rows.length === 0) return 0;

      // Build the list of keys to check: `${txHash}:${opIndex}`.
      const keys = rows
        .filter((r) => r.transaction_hash)
        .map((r) => `${r.transaction_hash}:${r.operation_index ?? 0}`);

      let existing = new Set<string>();
      if (keys.length > 0) {
        const found = (await sql`
          SELECT DISTINCT transaction_hash || ':' || COALESCE(operation_index, 0) AS k
          FROM ledger_operations
          WHERE transaction_hash || ':' || COALESCE(operation_index, 0) = ANY(${keys})
        `) as Array<{ k: string }>;
        existing = new Set(found.map((r) => r.k));
      }

      const fresh = rows.filter((r) => {
        if (!r.transaction_hash) return true;
        return !existing.has(`${r.transaction_hash}:${r.operation_index ?? 0}`);
      });
      if (fresh.length === 0) return 0;

      const result = (await sql`
        INSERT INTO ledger_operations ${sql(fresh, ...INSERT_COLUMNS)}
        ON CONFLICT DO NOTHING
      `) as { count?: number };
      return result.count ?? fresh.length;
    },

    async findExistingKeys(
      fromIso: string,
      toIso: string,
    ): Promise<Set<string>> {
      const found = (await sql`
        SELECT DISTINCT transaction_hash || ':' || COALESCE(operation_index, 0) AS k
        FROM ledger_operations
        WHERE ledger_closed_at >= ${fromIso}
          AND ledger_closed_at < ${toIso}
          AND transaction_hash IS NOT NULL
      `) as Array<{ k: string }>;
      return new Set(found.map((r) => r.k));
    },
  };
}
