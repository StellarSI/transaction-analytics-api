/**
 * Schema for the `ledger_operations` hypertable.
 *
 * Mirrors `migrations/0002_ledger_operations.sql` so that ingestion code and
 * API queries share a single source of truth for column names and types.
 * Numeric columns (amount, fee_charged) are represented as strings to
 * preserve decimal precision across the TypeScript boundary.
 */

export interface LedgerOperation {
  id: number;
  ledger_sequence: number;
  ledger_closed_at: string; // ISO 8601 timestamptz
  operation_type: string;
  source_account: string | null;
  destination_account: string | null;
  asset_code: string | null;
  asset_issuer: string | null;
  asset_type: string | null;
  amount: string | null; // numeric stored as string to preserve precision
  fee_charged: string | null;
  transaction_hash: string | null;
  operation_index: number | null;
  successful: boolean;
}

export interface LedgerOperationInsert {
  ledger_sequence: number;
  ledger_closed_at: string | Date;
  operation_type: string;
  source_account?: string | null;
  destination_account?: string | null;
  asset_code?: string | null;
  asset_issuer?: string | null;
  asset_type?: string | null;
  amount?: string | number | null;
  fee_charged?: string | number | null;
  transaction_hash?: string | null;
  operation_index?: number | null;
  successful?: boolean;
}

export interface LedgerOperationQuery {
  ledger_sequence?: number;
  source_account?: string;
  asset_code?: string;
  asset_issuer?: string;
  from_ledger_closed_at?: string;
  to_ledger_closed_at?: string;
  successful?: boolean;
  limit?: number;
  offset?: number;
}

export const LEDGER_OPERATIONS_TABLE = 'ledger_operations';

export const LEDGER_OPERATIONS_COLUMNS = [
  'id',
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

export type LedgerOperationColumn = (typeof LEDGER_OPERATIONS_COLUMNS)[number];
