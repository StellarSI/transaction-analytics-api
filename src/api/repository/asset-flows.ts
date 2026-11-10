/**
 * Read model for the asset flow endpoint (issue #24).
 *
 * The HTTP layer depends on the small {@link AssetFlowRepository} port rather
 * than on SQL directly, which keeps route handlers unit-testable with an
 * in-memory fake. The production implementation is a thin adapter over the
 * shared `postgres.Sql` pool.
 *
 * Interval routing:
 * - `1m` reads the `asset_flow_aggregates` base hypertable (the minute buckets
 *   written by the aggregator).
 * - `1h` / `1d` read the matching TimescaleDB continuous aggregates
 *   (`asset_flow_aggregates_1h` / `asset_flow_aggregates_1d`) so historical
 *   queries hit materialized rollups instead of scanning minute rows.
 *
 * Numeric columns are selected as text (`::text`) and returned as strings:
 * Stellar amounts are 7-decimal values and must never round-trip through
 * JavaScript floats.
 */

import type postgres from 'postgres';

export type AssetFlowInterval = '1m' | '1h' | '1d';

export const ASSET_FLOW_INTERVALS: readonly AssetFlowInterval[] = ['1m', '1h', '1d'];

/** One time bucket returned by the asset flow endpoint. */
export interface AssetFlowBucket {
  /** Bucket start in UTC, ISO 8601 (`YYYY-MM-DDTHH:MM:SSZ`). */
  start: string;
  /** Total amount moved in the bucket, as a decimal string. */
  volume: string;
  /** Number of payment legs in the bucket. */
  payments: number;
  /** Issuer-attributed net flow, as a decimal string. */
  net_flow: string;
}

export interface AssetFlowQuery {
  assetCode: string;
  assetIssuer: string;
  interval: AssetFlowInterval;
  /** Inclusive lower bound on `bucket_start`. */
  from?: Date;
  /** Exclusive upper bound on `bucket_start`. */
  to?: Date;
  limit: number;
}

export interface AssetFlowRepository {
  getAssetFlowBuckets(query: AssetFlowQuery): Promise<AssetFlowBucket[]>;
}

/**
 * Minimal tagged-template surface used by the Postgres adapter. The shared
 * `postgres.Sql` pool satisfies it structurally; `src/index.ts` narrows it at
 * the boundary, mirroring the aggregator stores.
 */
export interface AssetFlowSql {
  (strings: TemplateStringsArray, ...values: unknown[]): Promise<unknown>;
}

const BASE_TABLE = 'asset_flow_aggregates';

const CONTINUOUS_AGGREGATE_TABLES: Partial<Record<AssetFlowInterval, string>> = {
  '1h': 'asset_flow_aggregates_1h',
  '1d': 'asset_flow_aggregates_1d',
};

interface AssetFlowBucketRow {
  bucket_start: string;
  volume: string;
  payments: string;
  net_flow: string;
}

function toBucket(row: AssetFlowBucketRow): AssetFlowBucket {
  return {
    start: row.bucket_start,
    volume: row.volume,
    payments: Number(row.payments),
    net_flow: row.net_flow,
  };
}

/**
 * Creates a repository backed by PostgreSQL/TimescaleDB. `sql` is the shared
 * connection pool from `src/db/pool.ts`.
 */
export function createPostgresAssetFlowRepository(sql: AssetFlowSql): AssetFlowRepository {
  // The narrow `AssetFlowSql` type only describes the call signature; the
  // runtime value is the full `postgres.Sql` object, which also provides the
  // identifier/fragment helpers used to build the query safely.
  const db = sql as unknown as postgres.Sql;

  return {
    async getAssetFlowBuckets(query: AssetFlowQuery): Promise<AssetFlowBucket[]> {
      const table = CONTINUOUS_AGGREGATE_TABLES[query.interval] ?? BASE_TABLE;
      const intervalFilter = query.interval === '1m' ? db`AND interval = '1m'` : db``;
      const fromFilter = query.from ? db`AND bucket_start >= ${query.from}` : db``;
      const toFilter = query.to ? db`AND bucket_start < ${query.to}` : db``;

      const rows = (await db`
        SELECT to_char(bucket_start AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS bucket_start,
               volume::text AS volume,
               payments::text AS payments,
               net_flow::text AS net_flow
        FROM ${db(table)}
        WHERE asset_code = ${query.assetCode}
          AND asset_issuer = ${query.assetIssuer}
          ${intervalFilter}
          ${fromFilter}
          ${toFilter}
        ORDER BY bucket_start ASC
        LIMIT ${query.limit}
      `) as AssetFlowBucketRow[];

      return rows.map(toBucket);
    },
  };
}
