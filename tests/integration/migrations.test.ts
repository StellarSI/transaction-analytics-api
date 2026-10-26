import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { pool } from '@/db/pool';
import { runMigrations, migrationStatus } from '@/db/migrate';

/**
 * Integration tests for the TimescaleDB migrations.
 *
 * These tests require a running PostgreSQL/TimescaleDB instance reachable
 * via DATABASE_URL. They run the full migration stack and assert that the
 * ledger_operations and aggregate hypertables exist with the expected
 * schema, indexes, and compression settings.
 */

describe('Database migrations', () => {
  beforeAll(async () => {
    await runMigrations('up');
  }, 60000);

  afterAll(async () => {
    await pool.end();
  });

  it('applies all migrations including ledger and aggregate hypertables', async () => {
    const status = await migrationStatus();
    const versions = status.map((entry) => entry.version);
    expect(versions).toContain('0001_enable_timescaledb.sql');
    expect(versions).toContain('0002_ledger_operations.sql');
    expect(versions).toContain('0003_aggregates.sql');
    // Every migration file should be marked applied.
    expect(status.every((entry) => entry.applied)).toBe(true);
  });

  it('creates the ledger_operations hypertable', async () => {
    const result = await pool<{ hypertable_name: string }[]>`
      SELECT hypertable_name
      FROM timescaledb_information.hypertables
      WHERE hypertable_name = 'ledger_operations'
    `;
    expect(result.length).toBe(1);
    expect(result[0].hypertable_name).toBe('ledger_operations');
  });

  it('creates the three aggregate metric hypertables', async () => {
    for (const table of [
      'asset_flow_aggregates',
      'account_flow_aggregates',
      'corridor_aggregates',
    ]) {
      const result = await pool<{ hypertable_name: string }[]>`
        SELECT hypertable_name
        FROM timescaledb_information.hypertables
        WHERE hypertable_name = ${table}
      `;
      expect(result.length).toBe(1);
      expect(result[0].hypertable_name).toBe(table);
    }
  });

  it('has expected columns on ledger_operations', async () => {
    const result = await pool<{ column_name: string }[]>`
      SELECT column_name
      FROM information_schema.columns
      WHERE table_name = 'ledger_operations'
      ORDER BY ordinal_position
    `;
    const columns = result.map((row) => row.column_name);
    for (const col of [
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
    ]) {
      expect(columns).toContain(col);
    }
  });

  it('has expected columns on aggregate hypertables', async () => {
    const assetCols = await pool<{ column_name: string }[]>`
      SELECT column_name
      FROM information_schema.columns
      WHERE table_name = 'asset_flow_aggregates'
      ORDER BY ordinal_position
    `;
    const assetColumnNames = assetCols.map((row) => row.column_name);
    for (const col of [
      'bucket_start',
      'interval',
      'asset_code',
      'asset_issuer',
      'volume',
      'payments',
      'net_flow',
    ]) {
      expect(assetColumnNames).toContain(col);
    }

    const accountCols = await pool<{ column_name: string }[]>`
      SELECT column_name
      FROM information_schema.columns
      WHERE table_name = 'account_flow_aggregates'
      ORDER BY ordinal_position
    `;
    const accountColumnNames = accountCols.map((row) => row.column_name);
    for (const col of [
      'bucket_start',
      'interval',
      'account_id',
      'inflow',
      'outflow',
      'counterparties',
      'net_flow',
    ]) {
      expect(accountColumnNames).toContain(col);
    }

    const corridorCols = await pool<{ column_name: string }[]>`
      SELECT column_name
      FROM information_schema.columns
      WHERE table_name = 'corridor_aggregates'
      ORDER BY ordinal_position
    `;
    const corridorColumnNames = corridorCols.map((row) => row.column_name);
    for (const col of [
      'bucket_start',
      'interval',
      'source_asset',
      'dest_asset',
      'source_anchor',
      'dest_anchor',
      'volume',
      'payments',
    ]) {
      expect(corridorColumnNames).toContain(col);
    }
  });

  it('has compression enabled on all hypertables', async () => {
    for (const table of [
      'ledger_operations',
      'asset_flow_aggregates',
      'account_flow_aggregates',
      'corridor_aggregates',
    ]) {
      const result = await pool<{ compress_segmentby: string | null }[]>`
        SELECT compress_segmentby
        FROM timescaledb_information.hypertables
        WHERE hypertable_name = ${table}
      `;
      expect(result.length).toBe(1);
      expect(result[0].compress_segmentby).not.toBeNull();
    }
  });

  it('has unique indexes preventing duplicate buckets on aggregates', async () => {
    // asset_flow_aggregates unique index on (bucket_start, interval, asset_code, asset_issuer)
    const assetResult = await pool<{ indexname: string }[]>`
      SELECT indexname
      FROM pg_indexes
      WHERE tablename = 'asset_flow_aggregates'
        AND indexdef ILIKE '%bucket_start%interval%asset_code%asset_issuer%'
    `;
    expect(assetResult.length).toBeGreaterThanOrEqual(1);

    // account_flow_aggregates unique index on (bucket_start, interval, account_id)
    const accountResult = await pool<{ indexname: string }[]>`
      SELECT indexname
      FROM pg_indexes
      WHERE tablename = 'account_flow_aggregates'
        AND indexdef ILIKE '%bucket_start%interval%account_id%'
    `;
    expect(accountResult.length).toBeGreaterThanOrEqual(1);

    // corridor_aggregates unique index on (bucket_start, interval, source_asset, dest_asset)
    const corridorResult = await pool<{ indexname: string }[]>`
      SELECT indexname
      FROM pg_indexes
      WHERE tablename = 'corridor_aggregates'
        AND indexdef ILIKE '%bucket_start%interval%source_asset%dest_asset%'
    `;
    expect(corridorResult.length).toBeGreaterThanOrEqual(1);
  });

  it('has chunk interval of 1 day on ledger_operations', async () => {
    const result = await pool<{ chunk_time_interval: string }[]>`
      SELECT chunk_time_interval
      FROM timescaledb_information.hypertables
      WHERE hypertable_name = 'ledger_operations'
    `;
    expect(result.length).toBe(1);
    expect(result[0].chunk_time_interval).toContain('1 day');
  });

  it('has chunk interval of 1 hour on aggregate hypertables', async () => {
    for (const table of [
      'asset_flow_aggregates',
      'account_flow_aggregates',
      'corridor_aggregates',
    ]) {
      const result = await pool<{ chunk_time_interval: string }[]>`
        SELECT chunk_time_interval
        FROM timescaledb_information.hypertables
        WHERE hypertable_name = ${table}
      `;
      expect(result.length).toBe(1);
      expect(result[0].chunk_time_interval).toContain('1 hour');
    }
  });
});
