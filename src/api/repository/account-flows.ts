/**
 * Read model for the account flow endpoint (issue #25).
 *
 * Depends on the {@link AccountFlowRepository} port (not SQL directly) so the
 * route can be exercised with an in-memory fake. The production adapter reads
 * from the TimescaleDB hypertable / continuous aggregates:
 * - `1m` -> `account_flow_aggregates` (minute buckets with counterparties).
 * - `1h` / `1d` -> `account_flow_aggregates_1h` / `_1d` rollups. These views
 *   do not carry `counterparties`, so `counterparties` is returned as an empty
 *   array for rolled-up intervals (minute buckets retain top counterparties).
 *
 * Numeric columns are selected as text and returned as strings to preserve
 * 7-decimal Stellar precision (no JavaScript float round-trips).
 */

import type postgres from 'postgres';

export type AccountFlowInterval = '1m' | '1h' | '1d';

export const ACCOUNT_FLOW_INTERVALS: readonly AccountFlowInterval[] = ['1m', '1h', '1d'];

export interface AccountCounterparty {
  account_id: string;
  volume: string;
  count: number;
  direction: 'in' | 'out';
}

export interface AccountFlowBucket {
  /** Bucket start in UTC, ISO 8601 (`YYYY-MM-DDTHH:MM:SSZ`). */
  start: string;
  /** Total amount received in the bucket, as a decimal string. */
  inflow: string;
  /** Total amount sent in the bucket, as a decimal string. */
  outflow: string;
  /** Top counterparties by volume for the bucket. */
  counterparties: AccountCounterparty[];
  /** Net flow (inflow - outflow), as a decimal string. */
  net_flow: string;
}

export interface AccountFlowQuery {
  accountId: string;
  interval: AccountFlowInterval;
  /** Inclusive lower bound on `bucket_start`. */
  from?: Date;
  /** Exclusive upper bound on `bucket_start`. */
  to?: Date;
  limit: number;
}

/** Port consumed by the HTTP route. */
export interface AccountFlowRepository {
  getAccountFlowBuckets(query: AccountFlowQuery): Promise<AccountFlowBucket[]>;
}

/** Minimal `postgres.Sql` surface the adapter relies on. */
export type AccountFlowSql = Pick<postgres.Sql, 'unsafe'>;

const INTERVAL_SOURCES: Record<AccountFlowInterval, string> = {
  '1m': 'account_flow_aggregates',
  '1h': 'account_flow_aggregates_1h',
  '1d': 'account_flow_aggregates_1d',
};

interface AccountFlowRow {
  bucket_start: string | Date;
  inflow: string | number;
  outflow: string | number;
  counterparties: AccountCounterparty[] | string | null;
  net_flow: string | number;
}

function toIsoUtc(value: string | Date): string {
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) {
    throw new Error(`invalid bucket_start value: ${String(value)}`);
  }
  return date.toISOString().replace(/\.\d{3}Z$/, 'Z');
}

function parseCounterparties(value: AccountCounterparty[] | string | null): AccountCounterparty[] {
  if (value == null) return [];
  if (Array.isArray(value)) return value;
  try {
    const parsed = JSON.parse(value) as unknown;
    return Array.isArray(parsed) ? (parsed as AccountCounterparty[]) : [];
  } catch {
    return [];
  }
}

/** Maps a database row to the API bucket shape. */
export function toAccountFlowBucket(row: AccountFlowRow): AccountFlowBucket {
  return {
    start: toIsoUtc(row.bucket_start),
    inflow: String(row.inflow),
    outflow: String(row.outflow),
    counterparties: parseCounterparties(row.counterparties),
    net_flow: String(row.net_flow),
  };
}

/** Production repository backed by the shared `postgres` pool. */
export function createPostgresAccountFlowRepository(sql: AccountFlowSql): AccountFlowRepository {
  return {
    async getAccountFlowBuckets(query) {
      const source = INTERVAL_SOURCES[query.interval];
      const conditions = ['account_id = $1', '($2::timestamptz IS NULL OR bucket_start >= $2::timestamptz)', '($3::timestamptz IS NULL OR bucket_start < $3::timestamptz)'];
      const values: unknown[] = [query.accountId, query.from ?? null, query.to ?? null, query.limit];
      const counterpartiesSelect =
        query.interval === '1m' ? 'counterparties' : "'[]'::jsonb AS counterparties";
      const rows = await sql.unsafe<AccountFlowRow[]>(
        `SELECT to_char(bucket_start AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS bucket_start,
                inflow::text AS inflow,
                outflow::text AS outflow,
                ${counterpartiesSelect},
                net_flow::text AS net_flow
         FROM ${source}
         WHERE ${conditions.join(' AND ')}
         ORDER BY bucket_start ASC
         LIMIT $4`,
        values as never[],
      );
      return rows.map(toAccountFlowBucket);
    },
  };
}
