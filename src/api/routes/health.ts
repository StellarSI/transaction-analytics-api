/**
 * Health check route (issue #29).
 *
 * `GET /v1/health` reports ingestion status: database, Redis, Horizon and RPC
 * reachability plus the ingestion lag between the current Horizon head and
 * the newest ledger ingested into the database. It always answers 200 — a
 * dependency outage is reflected in the `status` / `checks` payload rather
 * than the HTTP status, so load balancers can distinguish "process alive"
 * from "fully healthy". No authentication is required.
 *
 * Response shape:
 *
 * ```json
 * {
 *   "status": "healthy" | "degraded",
 *   "timestamp": "...",
 *   "checks": {
 *     "database": { "ok": true, "latencyMs": 2, "detail": "postgres reachable" },
 *     "redis":    { "ok": true, "latencyMs": 1, "detail": "redis reachable" },
 *     "horizon":  { "ok": true, "latencyMs": 18, "detail": "horizon reachable", "latest_ledger": 7654321 },
 *     "rpc":      { "ok": true, "latencyMs": 12, "detail": "stellar rpc reachable", "latest_ledger": 7654321 }
 *   },
 *   "ingestion_lag_seconds": 10
 * }
 * ```
 *
 * `ingestion_lag_seconds` is the number of ledgers by which the database
 * trails the head, multiplied by the ~5 second Stellar ledger close time. It
 * is `null` when either the Horizon head or the newest ingested ledger is
 * unknown (for example, before the first ingestion), which also reports the
 * service as `degraded`. Horizon latency is wall-clock and network-bound;
 * budget for it — the 100ms response target presumes a healthy, nearby
 * Horizon endpoint.
 *
 * The checkers are injectable so unit tests can exercise the route without a
 * live PostgreSQL, Redis, Horizon or Stellar RPC instance.
 */

import type { FastifyInstance } from 'fastify';
import { dbHealthCheck, getLatestLedgerFromDb } from '../../db/pool';
import { redisHealthCheck } from '../../redis/health';
import { horizonHealthCheck, type HorizonHealth } from '../../ingestor/horizon/health';
import { rpcHealthCheck, type RpcHealth } from '../../ingestor/rpc/health';

export interface HealthCheckResult {
  ok: boolean;
  latencyMs: number;
  detail: string;
}

export interface HealthRouteOptions {
  checkDatabase?: () => Promise<HealthCheckResult>;
  checkRedis?: () => Promise<HealthCheckResult>;
  checkHorizon?: () => Promise<HorizonHealth>;
  checkRpc?: () => Promise<RpcHealth>;
  /** Newest ledger stored in `ledger_operations`, or `null` when none exists. */
  getLatestLedger?: () => Promise<number | null>;
}

/** Approximate average Stellar ledger close time, in seconds. */
export const SECONDS_PER_LEDGER = 5;

type AnyCheck = { ok: boolean; latencyMs: number; detail: string };

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/** Checks must never take the route down with them — fold failures into `{ ok: false }`. */
async function safe<T extends AnyCheck>(check: () => Promise<T>): Promise<T> {
  try {
    return await check();
  } catch (error) {
    return { ok: false, latencyMs: 0, detail: errorMessage(error) } as T;
  }
}

export function registerHealthRoutes(
  app: FastifyInstance,
  options: HealthRouteOptions,
  done: (err?: Error) => void,
): void {
  const checkDatabase = options.checkDatabase ?? dbHealthCheck;
  const checkRedis = options.checkRedis ?? redisHealthCheck;
  const checkHorizon = options.checkHorizon ?? horizonHealthCheck;
  const checkRpc = options.checkRpc ?? rpcHealthCheck;
  const getLatestLedger = options.getLatestLedger ?? getLatestLedgerFromDb;

  app.get('/health', async () => {
    const [database, redis, horizon, rpc, latestLedger] = await Promise.all([
      safe(checkDatabase),
      safe(checkRedis),
      safe(checkHorizon),
      safe(checkRpc),
      getLatestLedger().catch(() => null),
    ]);

    // Horizon is the ingestion source of truth; fall back to the RPC head.
    const headLedger = horizon.latest_ledger ?? rpc.latest_ledger ?? null;
    const lagLedgers =
      headLedger !== null && latestLedger !== null
        ? Math.max(0, headLedger - latestLedger)
        : null;
    const ingestionLagSeconds = lagLedgers === null ? null : lagLedgers * SECONDS_PER_LEDGER;

    const healthy = database.ok && redis.ok && horizon.ok && rpc.ok && ingestionLagSeconds !== null;

    return {
      status: healthy ? 'healthy' : 'degraded',
      timestamp: new Date().toISOString(),
      checks: { database, redis, horizon, rpc },
      ingestion_lag_seconds: ingestionLagSeconds,
    };
  });

  done();
}
