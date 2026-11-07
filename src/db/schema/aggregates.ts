/**
 * Schema for the aggregate metric hypertables.
 *
 * Mirrors `migrations/0003_aggregates.sql` so that aggregation jobs and API
 * queries share a single source of truth for column names and types. Numeric
 * columns (volume, inflow, outflow, net_flow) are represented as strings to
 * preserve decimal precision across the TypeScript boundary.
 */

export interface AssetFlowAggregate {
  bucket_start: string; // ISO 8601 timestamptz
  interval: string;
  asset_code: string;
  asset_issuer: string | null;
  volume: string; // numeric stored as string
  payments: number;
  net_flow: string;
}

export interface AccountFlowAggregate {
  bucket_start: string;
  interval: string;
  account_id: string;
  inflow: string;
  outflow: string;
  counterparties: unknown[]; // jsonb
  net_flow: string;
}

export interface CorridorAggregate {
  bucket_start: string;
  interval: string;
  source_asset: string;
  dest_asset: string;
  source_anchor: string | null;
  dest_anchor: string | null;
  volume: string;
  payments: number;
}

export interface TrustlineAggregate {
  bucket_start: string;
  interval: string;
  account_id: string;
  trustline_count: number;
  new_trustlines: number;
  removed_trustlines: number;
  trust_limit_changes: number;
  total_trust_limit: string;
  payments_count: number;
  active_hours: number;
  dormancy_days: number;
}

export interface AggregateFilters {
  interval: string;
  from_bucket_start?: string;
  to_bucket_start?: string;
  asset_code?: string;
  asset_issuer?: string;
  account_id?: string;
  source_asset?: string;
  dest_asset?: string;
  source_anchor?: string;
  dest_anchor?: string;
  limit?: number;
  offset?: number;
}

export const ASSET_FLOW_AGGREGATES_TABLE = 'asset_flow_aggregates';
export const ACCOUNT_FLOW_AGGREGATES_TABLE = 'account_flow_aggregates';
export const CORRIDOR_AGGREGATES_TABLE = 'corridor_aggregates';
export const TRUSTLINE_AGGREGATES_TABLE = 'trustline_aggregates';

export const ASSET_FLOW_AGGREGATES_COLUMNS = [
  'bucket_start',
  'interval',
  'asset_code',
  'asset_issuer',
  'volume',
  'payments',
  'net_flow',
] as const;

export const ACCOUNT_FLOW_AGGREGATES_COLUMNS = [
  'bucket_start',
  'interval',
  'account_id',
  'inflow',
  'outflow',
  'counterparties',
  'net_flow',
] as const;

export const CORRIDOR_AGGREGATES_COLUMNS = [
  'bucket_start',
  'interval',
  'source_asset',
  'dest_asset',
  'source_anchor',
  'dest_anchor',
  'volume',
  'payments',
] as const;

export const TRUSTLINE_AGGREGATES_COLUMNS = [
  'bucket_start',
  'interval',
  'account_id',
  'trustline_count',
  'new_trustlines',
  'removed_trustlines',
  'trust_limit_changes',
  'total_trust_limit',
  'payments_count',
  'active_hours',
  'dormancy_days',
] as const;

export type AssetFlowAggregateColumn =
  (typeof ASSET_FLOW_AGGREGATES_COLUMNS)[number];
export type AccountFlowAggregateColumn =
  (typeof ACCOUNT_FLOW_AGGREGATES_COLUMNS)[number];
export type CorridorAggregateColumn =
  (typeof CORRIDOR_AGGREGATES_COLUMNS)[number];
export type TrustlineAggregateColumn =
  (typeof TRUSTLINE_AGGREGATES_COLUMNS)[number];
