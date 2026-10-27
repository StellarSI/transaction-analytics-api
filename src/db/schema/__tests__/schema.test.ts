import { describe, it, expect } from 'vitest';
import {
  LEDGER_OPERATIONS_TABLE,
  LEDGER_OPERATIONS_COLUMNS,
  type LedgerOperation,
  type LedgerOperationInsert,
  type LedgerOperationQuery,
} from '../ledger';
import {
  ASSET_FLOW_AGGREGATES_TABLE,
  ACCOUNT_FLOW_AGGREGATES_TABLE,
  CORRIDOR_AGGREGATES_TABLE,
  ASSET_FLOW_AGGREGATES_COLUMNS,
  ACCOUNT_FLOW_AGGREGATES_COLUMNS,
  CORRIDOR_AGGREGATES_COLUMNS,
  type AssetFlowAggregate,
  type AccountFlowAggregate,
  type CorridorAggregate,
  type AggregateFilters,
} from '../aggregates';

describe('ledger schema', () => {
  it('exports the ledger_operations table name', () => {
    expect(LEDGER_OPERATIONS_TABLE).toBe('ledger_operations');
  });

  it('exports all ledger_operations columns', () => {
    expect(LEDGER_OPERATIONS_COLUMNS).toContain('id');
    expect(LEDGER_OPERATIONS_COLUMNS).toContain('ledger_sequence');
    expect(LEDGER_OPERATIONS_COLUMNS).toContain('ledger_closed_at');
    expect(LEDGER_OPERATIONS_COLUMNS).toContain('operation_type');
    expect(LEDGER_OPERATIONS_COLUMNS).toContain('source_account');
    expect(LEDGER_OPERATIONS_COLUMNS).toContain('destination_account');
    expect(LEDGER_OPERATIONS_COLUMNS).toContain('asset_code');
    expect(LEDGER_OPERATIONS_COLUMNS).toContain('asset_issuer');
    expect(LEDGER_OPERATIONS_COLUMNS).toContain('asset_type');
    expect(LEDGER_OPERATIONS_COLUMNS).toContain('amount');
    expect(LEDGER_OPERATIONS_COLUMNS).toContain('fee_charged');
    expect(LEDGER_OPERATIONS_COLUMNS).toContain('transaction_hash');
    expect(LEDGER_OPERATIONS_COLUMNS).toContain('operation_index');
    expect(LEDGER_OPERATIONS_COLUMNS).toContain('successful');
    expect(LEDGER_OPERATIONS_COLUMNS.length).toBe(14);
  });

  it('defines the LedgerOperation type shape', () => {
    const op: LedgerOperation = {
      id: 1,
      ledger_sequence: 42,
      ledger_closed_at: '2026-10-25T12:00:00Z',
      operation_type: 'payment',
      source_account: 'GABC...',
      destination_account: 'GXYZ...',
      asset_code: 'USDC',
      asset_issuer: 'GA5Z...',
      asset_type: 'credit_alphanum4',
      amount: '100.50',
      fee_charged: '0.00001',
      transaction_hash: 'abc123',
      operation_index: 0,
      successful: true,
    };
    expect(op.id).toBe(1);
    expect(op.successful).toBe(true);
    expect(typeof op.amount).toBe('string');
  });

  it('defines the LedgerOperationInsert type shape', () => {
    const insert: LedgerOperationInsert = {
      ledger_sequence: 42,
      ledger_closed_at: new Date(),
      operation_type: 'payment',
      source_account: 'GABC...',
      amount: '100.50',
    };
    expect(insert.ledger_sequence).toBe(42);
    expect(insert.successful).toBeUndefined();
  });

  it('defines the LedgerOperationQuery type shape', () => {
    const query: LedgerOperationQuery = {
      ledger_sequence: 42,
      source_account: 'GABC...',
      successful: true,
      limit: 10,
      offset: 0,
    };
    expect(query.limit).toBe(10);
  });
});

describe('aggregates schema', () => {
  it('exports all aggregate table names', () => {
    expect(ASSET_FLOW_AGGREGATES_TABLE).toBe('asset_flow_aggregates');
    expect(ACCOUNT_FLOW_AGGREGATES_TABLE).toBe('account_flow_aggregates');
    expect(CORRIDOR_AGGREGATES_TABLE).toBe('corridor_aggregates');
  });

  it('exports asset_flow_aggregates columns', () => {
    expect(ASSET_FLOW_AGGREGATES_COLUMNS).toContain('bucket_start');
    expect(ASSET_FLOW_AGGREGATES_COLUMNS).toContain('interval');
    expect(ASSET_FLOW_AGGREGATES_COLUMNS).toContain('asset_code');
    expect(ASSET_FLOW_AGGREGATES_COLUMNS).toContain('asset_issuer');
    expect(ASSET_FLOW_AGGREGATES_COLUMNS).toContain('volume');
    expect(ASSET_FLOW_AGGREGATES_COLUMNS).toContain('payments');
    expect(ASSET_FLOW_AGGREGATES_COLUMNS).toContain('net_flow');
    expect(ASSET_FLOW_AGGREGATES_COLUMNS.length).toBe(7);
  });

  it('exports account_flow_aggregates columns', () => {
    expect(ACCOUNT_FLOW_AGGREGATES_COLUMNS).toContain('bucket_start');
    expect(ACCOUNT_FLOW_AGGREGATES_COLUMNS).toContain('interval');
    expect(ACCOUNT_FLOW_AGGREGATES_COLUMNS).toContain('account_id');
    expect(ACCOUNT_FLOW_AGGREGATES_COLUMNS).toContain('inflow');
    expect(ACCOUNT_FLOW_AGGREGATES_COLUMNS).toContain('outflow');
    expect(ACCOUNT_FLOW_AGGREGATES_COLUMNS).toContain('counterparties');
    expect(ACCOUNT_FLOW_AGGREGATES_COLUMNS).toContain('net_flow');
    expect(ACCOUNT_FLOW_AGGREGATES_COLUMNS.length).toBe(7);
  });

  it('exports corridor_aggregates columns', () => {
    expect(CORRIDOR_AGGREGATES_COLUMNS).toContain('bucket_start');
    expect(CORRIDOR_AGGREGATES_COLUMNS).toContain('interval');
    expect(CORRIDOR_AGGREGATES_COLUMNS).toContain('source_asset');
    expect(CORRIDOR_AGGREGATES_COLUMNS).toContain('dest_asset');
    expect(CORRIDOR_AGGREGATES_COLUMNS).toContain('source_anchor');
    expect(CORRIDOR_AGGREGATES_COLUMNS).toContain('dest_anchor');
    expect(CORRIDOR_AGGREGATES_COLUMNS).toContain('volume');
    expect(CORRIDOR_AGGREGATES_COLUMNS).toContain('payments');
    expect(CORRIDOR_AGGREGATES_COLUMNS.length).toBe(8);
  });

  it('defines the AssetFlowAggregate type shape', () => {
    const agg: AssetFlowAggregate = {
      bucket_start: '2026-10-25T12:00:00Z',
      interval: '1h',
      asset_code: 'USDC',
      asset_issuer: 'GA5Z...',
      volume: '10000.00',
      payments: 50,
      net_flow: '5000.00',
    };
    expect(agg.asset_code).toBe('USDC');
    expect(typeof agg.volume).toBe('string');
    expect(typeof agg.payments).toBe('number');
  });

  it('defines the AccountFlowAggregate type shape', () => {
    const agg: AccountFlowAggregate = {
      bucket_start: '2026-10-25T12:00:00Z',
      interval: '1h',
      account_id: 'GABC...',
      inflow: '5000.00',
      outflow: '2000.00',
      counterparties: ['GXYZ...', 'GDEF...'],
      net_flow: '3000.00',
    };
    expect(agg.account_id).toBe('GABC...');
    expect(Array.isArray(agg.counterparties)).toBe(true);
  });

  it('defines the CorridorAggregate type shape', () => {
    const agg: CorridorAggregate = {
      bucket_start: '2026-10-25T12:00:00Z',
      interval: '1h',
      source_asset: 'USDC',
      dest_asset: 'XLM',
      source_anchor: 'GA5Z...',
      dest_anchor: 'GBQQ...',
      volume: '15000.00',
      payments: 75,
    };
    expect(agg.source_asset).toBe('USDC');
    expect(agg.dest_asset).toBe('XLM');
  });

  it('defines the AggregateFilters type shape', () => {
    const filters: AggregateFilters = {
      interval: '1h',
      from_bucket_start: '2026-10-25T00:00:00Z',
      to_bucket_start: '2026-10-25T23:59:59Z',
      asset_code: 'USDC',
      limit: 100,
      offset: 0,
    };
    expect(filters.interval).toBe('1h');
    expect(filters.limit).toBe(100);
  });
});
