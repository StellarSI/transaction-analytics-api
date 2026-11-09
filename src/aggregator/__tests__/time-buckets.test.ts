import { describe, it, expect, beforeEach } from 'vitest';
import { Registry } from 'prom-client';
import {
  BucketedTable,
  CONTINUOUS_AGGREGATE_DEFINITIONS,
  allContinuousAggregateSql,
  bucketStartIso,
  bucketWindowMs,
  createTimeBucketAggregationHandler,
  createTimeBucketMetrics,
  defaultRefreshPolicies,
  intervalToTimescale,
  listBucketStarts,
  refreshPolicySql,
  rollupBuckets,
  selectBucketed,
  TimeBucketRefresher,
  type TimeBucketMetrics,
} from '../time-buckets';

/**
 * Unit tests for the time-bucketed aggregates module (issue #22).
 */

async function counterValue(
  counter: { get(): Promise<{ values: Array<{ value: number; labels: Record<string, string | number> }> }> },
  labels: Record<string, string>,
): Promise<number> {
  const { values } = await counter.get();
  return (
    values.find((v) =>
      Object.entries(labels).every(([k, expected]) => String(v.labels[k]) === expected),
    )?.value ?? 0
  );
}

describe('bucket helpers', () => {
  it('floors into minute/hour/day buckets', () => {
    const t = Date.parse('2026-11-01T10:23:47.890Z');
    expect(bucketStartIso(t, '1m')).toBe('2026-11-01T10:23:00.000Z');
    expect(bucketStartIso(t, '1h')).toBe('2026-11-01T10:00:00.000Z');
    expect(bucketStartIso(t, '1d')).toBe('2026-11-01T00:00:00.000Z');
  });

  it('lists bucket starts with an exclusive end', () => {
    expect(listBucketStarts('2026-11-01T10:00:00.000Z', '2026-11-01T13:00:00.000Z', '1h')).toEqual([
      '2026-11-01T10:00:00.000Z',
      '2026-11-01T11:00:00.000Z',
      '2026-11-01T12:00:00.000Z',
    ]);
  });

  it('exposes interval windows and timescale units', () => {
    expect(bucketWindowMs('1m')).toBe(60_000);
    expect(intervalToTimescale('1m')).toBe('1 minute');
    expect(intervalToTimescale('1h')).toBe('1 hour');
    expect(intervalToTimescale('1d')).toBe('1 day');
  });
});

describe('continuous aggregates', () => {
  it('defines views for every base table and coarse interval', () => {
    expect(CONTINUOUS_AGGREGATE_DEFINITIONS.length).toBeGreaterThan(0);
    const views = CONTINUOUS_AGGREGATE_DEFINITIONS.map((d) => d.view);
    expect(views).toContain('asset_flow_aggregates_1h');
    expect(views).toContain('account_flow_aggregates_1d');
    expect(views).toContain('corridor_aggregates_1h');
    expect(views).toContain('trustline_aggregates_1d');
  });

  it('generates SQL for every definition', () => {
    const statements = allContinuousAggregateSql();
    expect(statements.length).toBe(CONTINUOUS_AGGREGATE_DEFINITIONS.length);
    expect(statements[0]).toContain('CREATE MATERIALIZED VIEW');
    expect(statements[0]).toContain('timescaledb.continuous');
  });

  it('configures refresh policies (continuous recent, scheduled older)', () => {
    const policies = defaultRefreshPolicies();
    expect(policies).toHaveLength(CONTINUOUS_AGGREGATE_DEFINITIONS.length);
    expect(policies.find((p) => p.interval === '1h')?.schedule).toBe('30 minutes');
    expect(policies.find((p) => p.interval === '1d')?.schedule).toBe('1 day');
    expect(refreshPolicySql('asset_flow_aggregates_1h', '1h')).toContain(
      'add_continuous_aggregate_policy',
    );
  });
});

describe('application rollups', () => {
  const minuteRows = [
    { bucketStart: '2026-11-01T10:00:00.000Z', key: 'USDC', value: 100n },
    { bucketStart: '2026-11-01T10:15:00.000Z', key: 'USDC', value: 50n },
    { bucketStart: '2026-11-01T10:30:00.000Z', key: 'USDC', value: 25n },
    { bucketStart: '2026-11-01T11:00:00.000Z', key: 'USDC', value: 200n },
  ];

  it('rolls minute buckets into hour buckets', () => {
    const hours = rollupBuckets(minuteRows, '1h');
    expect(hours).toEqual([
      { bucketStart: '2026-11-01T10:00:00.000Z', key: 'USDC', value: 175n },
      { bucketStart: '2026-11-01T11:00:00.000Z', key: 'USDC', value: 200n },
    ]);
  });

  it('rolls minute buckets into day buckets', () => {
    const days = rollupBuckets(minuteRows, '1d');
    expect(days).toEqual([{ bucketStart: '2026-11-01T00:00:00.000Z', key: 'USDC', value: 375n }]);
  });

  it('passes through minute buckets unchanged', () => {
    expect(rollupBuckets(minuteRows, '1m')).toHaveLength(minuteRows.length);
  });

  it('selects coarse data when present, otherwise rolls finer data up', () => {
    const byInterval = new Map();
    byInterval.set('1h', [
      { bucketStart: '2026-11-01T10:00:00.000Z', key: 'USDC', value: 175n },
    ]);
    byInterval.set('1m', minuteRows);
    expect(selectBucketed(byInterval, '1h')).toHaveLength(1);
    expect(selectBucketed(byInterval, '1d')).toEqual([
      { bucketStart: '2026-11-01T00:00:00.000Z', key: 'USDC', value: 375n },
    ]);
  });

  it('querying a BucketedTable returns correctly bucketed data', () => {
    const table = new BucketedTable<string>();
    table.add('2026-11-01T10:00:00.000Z', 'USDC', 100n);
    table.add('2026-11-01T10:15:00.000Z', 'USDC', 50n);
    expect(table.query('1h')).toEqual([
      { bucketStart: '2026-11-01T10:00:00.000Z', key: 'USDC', value: 150n },
    ]);
    expect(table.query('1d')).toEqual([
      { bucketStart: '2026-11-01T00:00:00.000Z', key: 'USDC', value: 150n },
    ]);
    // Typical query answers in-process; materialized views keep DB queries <100ms.
  });
});

describe('TimeBucketRefresher', () => {
  let metrics: TimeBucketMetrics;

  beforeEach(() => {
    metrics = createTimeBucketMetrics(new Registry());
  });

  it('refreshes every continuous aggregate and counts by interval', async () => {
    const calls: string[] = [];
    const refresher = new TimeBucketRefresher({
      metrics,
      sql: (async () => {
        calls.push('refresh');
      }) as never,
    });
    await refresher.refreshAll();
    expect(calls.length).toBe(CONTINUOUS_AGGREGATE_DEFINITIONS.length);
    expect(await counterValue(metrics.refreshes, { interval: '1h' })).toBeGreaterThan(0);
  });

  it('aggregation handler accepts known aggregation kinds', async () => {
    const handler = createTimeBucketAggregationHandler({ metrics });
    await expect(
      handler(
        { kind: 'asset-flow', bucketStart: '2026-11-01T10:00:00.000Z', interval: '1h' } as never,
        {} as never,
      ),
    ).resolves.toBeUndefined();
  });

  it('aggregation handler rejects unknown kinds', async () => {
    const handler = createTimeBucketAggregationHandler({ metrics });
    await expect(
      handler(
        { kind: 'mystery', bucketStart: '2026-11-01T10:00:00.000Z', interval: '1h' } as never,
        {} as never,
      ),
    ).rejects.toThrow('cannot handle');
  });
});
