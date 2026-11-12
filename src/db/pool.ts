import postgres from 'postgres';
import { config } from '../config';

/**
 * PostgreSQL connection pool.
 *
 * Created from `DATABASE_URL` with a maximum of 20 concurrent connections.
 * Exported for use by the migrator, the health check, and the ingestion /
 * aggregation code that follows.
 */
export const pool = postgres(config.DATABASE_URL, {
  max: 20,
  connect_timeout: 10,
  idle_timeout: 30,
});

/**
 * Health check for the database connection.
 *
 * Runs a trivial query against the database and reports whether it is
 * reachable, together with the round-trip latency. Used by GET /v1/health.
 */
export async function dbHealthCheck(): Promise<{
  ok: boolean;
  latencyMs: number;
  detail: string;
}> {
  const startedAt = Date.now();
  try {
    await pool`SELECT 1`;
    return {
      ok: true,
      latencyMs: Date.now() - startedAt,
      detail: 'postgres reachable',
    };
  } catch (error) {
    return {
      ok: false,
      latencyMs: Date.now() - startedAt,
      detail: error instanceof Error ? error.message : String(error),
    };
  }
}

/**
 * Sequence of the most recent ledger ingested into `ledger_operations`, or
 * `null` when nothing has been ingested yet. Used by GET /v1/health to
 * report the ingestion lag against the current Horizon head.
 */
export async function getLatestLedgerFromDb(): Promise<number | null> {
  const rows = await pool`
    SELECT COALESCE(MAX(ledger_sequence), 0)::bigint AS max_seq
    FROM ledger_operations
  `;
  const maxSeq = Number(rows[0]?.max_seq ?? 0);
  return Number.isFinite(maxSeq) && maxSeq > 0 ? maxSeq : null;
}

/**
 * Closes the pool gracefully so database connections are released when the
 * process is stopped.
 */
export async function closePool(): Promise<void> {
  await pool.end();
}
