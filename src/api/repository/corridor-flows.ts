/**
 * Read model for the corridor flow endpoint (issue #26).
 *
 * Depends on the {@link CorridorFlowRepository} port (not SQL directly) so the
 * route can be exercised with an in-memory fake. The production adapter reads
 * from the TimescaleDB hypertable / continuous aggregates:
 * - `1m` -> `corridor_aggregates`
 * - `1h` / `1d` -> `corridor_aggregates_1h` / `_1d`
 *
 * Numeric columns are selected as text and returned as strings to preserve
 * 7-decimal Stellar precision (no JavaScript float round-trips).
 */

import type postgres from 'postgres';

export type CorridorFlowInterval = '1m' | '1h' | '1d';

export const CORRIDOR_FLOW_INTERVALS: readonly CorridorFlowInterval[] = ['1m', '1h', '1d'];

export interface CorridorBucket {
  /** Bucket start in UTC, ISO 8601 (`YYYY-MM-DDTHH:MM:SSZ`). */
  start: string;
  source_asset: string;
  dest_asset: string;
  source_anchor: string | null;
  dest_anchor: string | null;
  /** Total destination-leg volume, as a decimal string. */
  volume: string;
  /** Number of payment legs in the bucket. */
  payments: number;
}

export interface CorridorFlowQuery {
  interval: CorridorFlowInterval;
  from?: Date;
  to?: Date;
  sourceAnchor?: string;
  destAnchor?: string;
  sourceAsset?: string;
  destAsset?: string;
  limit: number;
  offset: number;
}

/** Port consumed by the HTTP route. */
export interface CorridorFlowRepository {
  getCorridorBuckets(query: CorridorFlowQuery): Promise<CorridorBucket[]>;
}

/** Minimal `postgres.Sql` surface the adapter relies on. */
export type CorridorFlowSql = Pick<postgres.Sql, 'unsafe'>;

const INTERVAL_SOURCES: Record<CorridorFlowInterval, string> = {
  '1m': 'corridor_aggregates',
  '1h': 'corridor_aggregates_1h',
  '1d': 'corridor_aggregates_1d',
};

interface CorridorRow {
  bucket_start: string | Date;
  source_asset: string;
  dest_asset: string;
  source_anchor: string | null;
  dest_anchor: string | null;
  volume: string | number;
  payments: number | string;
}

function toIsoUtc(value: string | Date): string {
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) {
    throw new Error(`invalid bucket_start value: ${String(value)}`);
  }
  return date.toISOString().replace(/\.\d{3}Z$/, 'Z');
}

export function toCorridorBucket(row: CorridorRow): CorridorBucket {
  return {
    start: toIsoUtc(row.bucket_start),
    source_asset: row.source_asset,
    dest_asset: row.dest_asset,
    source_anchor: row.source_anchor ?? null,
    dest_anchor: row.dest_anchor ?? null,
    volume: String(row.volume),
    payments: typeof row.payments === 'number' ? row.payments : Number(row.payments),
  };
}

/** Production repository backed by the shared `postgres` pool. */
export function createPostgresCorridorFlowRepository(sql: CorridorFlowSql): CorridorFlowRepository {
  return {
    async getCorridorBuckets(query) {
      const source = INTERVAL_SOURCES[query.interval];
      const conditions: string[] = [];
      const values: unknown[] = [];
      const push = (value: unknown): string => {
        values.push(value);
        return `$${values.length}`;
      };

      if (query.from) {
        conditions.push(`bucket_start >= ${push(query.from)}::timestamptz`);
      }
      if (query.to) {
        conditions.push(`bucket_start < ${push(query.to)}::timestamptz`);
      }
      if (query.sourceAnchor !== undefined) {
        conditions.push(`source_anchor = ${push(query.sourceAnchor)}`);
      }
      if (query.destAnchor !== undefined) {
        conditions.push(`dest_anchor = ${push(query.destAnchor)}`);
      }
      if (query.sourceAsset !== undefined) {
        conditions.push(`source_asset = ${push(query.sourceAsset)}`);
      }
      if (query.destAsset !== undefined) {
        conditions.push(`dest_asset = ${push(query.destAsset)}`);
      }
      const where = conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '';
      const limitParam = push(query.limit);
      const offsetParam = push(query.offset);

      const rows = await sql.unsafe<CorridorRow[]>(
        `SELECT to_char(bucket_start AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS bucket_start,
                source_asset, dest_asset, source_anchor, dest_anchor,
                volume::text AS volume,
                payments
         FROM ${source}
         ${where}
         ORDER BY bucket_start ASC, source_asset ASC, dest_asset ASC
         LIMIT ${limitParam}
         OFFSET ${offsetParam}`,
        values as never[],
      );
      return rows.map(toCorridorBucket);
    },
  };
}
