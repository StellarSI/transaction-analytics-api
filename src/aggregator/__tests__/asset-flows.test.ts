import { describe, it, expect, beforeEach } from 'vitest';
import { Registry } from 'prom-client';
import {
  AssetFlowAggregator,
  bucketStartIso,
  computeAssetFlowDeltas,
  createAssetFlowAggregationHandler,
  createAssetFlowHandlers,
  createAssetFlowMetrics,
  createInMemoryAssetFlowStore,
  createInMemoryAssetFlowSink,
  formatAmount,
  isAssetFlowInterval,
  listBucketStarts,
  parseAmount,
  pathPaymentPayloadToOperations,
  type AssetFlowMetrics,
  type AssetFlowStoredOperation,
} from '../asset-flows';
import { createMemoryCursorStore } from '../../ingestor/types';

/**
 * Unit tests for the asset flow aggregator (issue #18). Everything runs
 * against the in-memory store — no database or Redis required.
 */

const HOUR = '2026-11-01T10:00:00.000Z';

function paymentPayload(overrides: Record<string, unknown> = {}) {
  return {
    kind: 'payment' as const,
    id: 'op-1',
    cursor: '1-1',
    ledger: 100,
    ledgerClosedAt: '2026-11-01T10:00:30.000Z',
    from: 'GA',
    to: 'GB',
    asset: { type: 'native', code: 'XLM' },
    amount: '10.0000000',
    ...overrides,
  };
}

function pathPaymentPayload(overrides: Record<string, unknown> = {}) {
  return {
    kind: 'path_payment' as const,
    variant: 'strict-receive' as const,
    id: 'op-pp',
    cursor: '1-2',
    ledger: 101,
    ledgerClosedAt: '2026-11-01T10:05:00.000Z',
    from: 'GA',
    to: 'GB',
    sendAsset: { type: 'credit_alphanum4', code: 'USDC', issuer: 'GISS' },
    sendAmount: '5.0000000',
    sendMax: '6.0000000',
    destinationAsset: { type: 'native', code: 'XLM' },
    destinationAmount: '50.0000000',
    corridor: { path: [], sameAnchor: false, crossIssuer: false },
    ...overrides,
  };
}

function storedOperation(overrides: Partial<AssetFlowStoredOperation> = {}): AssetFlowStoredOperation {
  return {
    asset_code: 'USDC',
    asset_issuer: 'GISS',
    amount: '10.0000000',
    source_account: 'GA',
    destination_account: 'GB',
    ledger_sequence: 100,
    ledger_closed_at: '2026-11-01T10:00:30.000Z',
    ...overrides,
  };
}

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

describe('decimal arithmetic', () => {
  it('round-trips stroop precision amounts', () => {
    expect(formatAmount(parseAmount('10.0000000'))).toBe('10.0000000');
    expect(formatAmount(parseAmount('0.0000001'))).toBe('0.0000001');
    expect(formatAmount(parseAmount('1234.5678901'))).toBe('1234.5678901');
    expect(formatAmount(parseAmount('-5.5000000'))).toBe('-5.5000000');
  });

  it('rejects malformed or over-precision amounts', () => {
    expect(() => parseAmount('1.12345678')).toThrow('invalid amount');
    expect(() => parseAmount('not-a-number')).toThrow('invalid amount');
    expect(() => parseAmount('10')).not.toThrow(); // integer part alone is fine
  });
});

describe('bucket helpers', () => {
  it('floors timestamps into minute/hour/day buckets', () => {
    const t = Date.parse('2026-11-01T10:23:47.890Z');
    expect(bucketStartIso(t, '1m')).toBe('2026-11-01T10:23:00.000Z');
    expect(bucketStartIso(t, '1h')).toBe('2026-11-01T10:00:00.000Z');
    expect(bucketStartIso(t, '1d')).toBe('2026-11-01T00:00:00.000Z');
  });

  it('lists bucket starts with an exclusive end', () => {
    expect(
      listBucketStarts('2026-11-01T10:00:00.000Z', '2026-11-01T13:00:00.000Z', '1h'),
    ).toEqual([
      '2026-11-01T10:00:00.000Z',
      '2026-11-01T11:00:00.000Z',
      '2026-11-01T12:00:00.000Z',
    ]);
  });

  it('recognizes supported intervals', () => {
    expect(isAssetFlowInterval('1m')).toBe(true);
    expect(isAssetFlowInterval('1h')).toBe(true);
    expect(isAssetFlowInterval('1d')).toBe(true);
    expect(isAssetFlowInterval('1w')).toBe(false);
  });
});

describe('computeAssetFlowDeltas', () => {
  it('aggregates volume and payments per asset and bucket', () => {
    const deltas = computeAssetFlowDeltas(
      [
        { assetCode: 'XLM', assetIssuer: null, amount: '10.0000000', fromAccount: 'GA', toAccount: 'GB', ledgerClosedAt: '2026-11-01T10:00:30.000Z' },
        { assetCode: 'XLM', assetIssuer: null, amount: '2.5000000', fromAccount: 'GB', toAccount: 'GA', ledgerClosedAt: '2026-11-01T10:00:59.000Z' },
        { assetCode: 'USDC', assetIssuer: 'GISS', amount: '100.0000000', fromAccount: 'GA', toAccount: 'GB', ledgerClosedAt: '2026-11-01T10:00:10.000Z' },
      ],
      ['1h'],
    );
    const xlm = deltas.find((d) => d.assetCode === 'XLM');
    const usdc = deltas.find((d) => d.assetCode === 'USDC');
    expect(xlm).toMatchObject({ volume: '12.5000000', payments: 2, assetIssuer: '' });
    expect(usdc).toMatchObject({ volume: '100.0000000', payments: 1, assetIssuer: 'GISS' });
  });

  it('attributes net flow to the issuer (mint/redeem), zero otherwise', () => {
    const deltas = computeAssetFlowDeltas(
      [
        { assetCode: 'USDC', assetIssuer: 'GISS', amount: '50.0000000', fromAccount: 'GISS', toAccount: 'GA', ledgerClosedAt: '2026-11-01T10:00:10.000Z' }, // mint
        { assetCode: 'USDC', assetIssuer: 'GISS', amount: '20.0000000', fromAccount: 'GA', toAccount: 'GISS', ledgerClosedAt: '2026-11-01T10:00:11.000Z' }, // redeem
        { assetCode: 'USDC', assetIssuer: 'GISS', amount: '7.0000000', fromAccount: 'GA', toAccount: 'GB', ledgerClosedAt: '2026-11-01T10:00:12.000Z' }, // external
        { assetCode: 'XLM', assetIssuer: null, amount: '99.0000000', fromAccount: 'GA', toAccount: 'GB', ledgerClosedAt: '2026-11-01T10:00:13.000Z' }, // native
      ],
      ['1h'],
    );
    const usdc = deltas.find((d) => d.assetCode === 'USDC');
    expect(usdc?.netFlow).toBe('30.0000000'); // 50 minted - 20 redeemed
    const xlm = deltas.find((d) => d.assetCode === 'XLM');
    expect(xlm?.netFlow).toBe('0.0000000');
  });

  it('throws on operations without a parseable ledger close time', () => {
    expect(() =>
      computeAssetFlowDeltas([
        { assetCode: 'XLM', amount: '1.0000000', fromAccount: 'GA', toAccount: 'GB' },
      ]),
    ).toThrow('missing ledgerClosedAt');
  });
});

describe('pathPaymentPayloadToOperations', () => {
  it('maps a strict-receive path payment to both legs', () => {
    const legs = pathPaymentPayloadToOperations(pathPaymentPayload() as never);
    expect(legs).toHaveLength(2);
    const send = legs.find((l) => l.assetCode === 'USDC');
    expect(send).toMatchObject({ amount: '5.0000000', assetIssuer: 'GISS' });
    const dest = legs.find((l) => l.assetCode === 'XLM');
    expect(dest).toMatchObject({ amount: '50.0000000' });
  });

  it('falls back to sendMax/destinationMin when exact amounts are missing', () => {
    const legs = pathPaymentPayloadToOperations(
      pathPaymentPayload({ sendAmount: undefined, destinationAmount: undefined, destinationMin: '40.0000000' }) as never,
    );
    const send = legs.find((l) => l.assetCode === 'USDC');
    const dest = legs.find((l) => l.assetCode === 'XLM');
    expect(send?.amount).toBe('6.0000000'); // sendMax approximation
    expect(dest?.amount).toBe('40.0000000'); // destinationMin approximation
  });
});

describe('AssetFlowAggregator apply path', () => {
  let metrics: AssetFlowMetrics;

  beforeEach(() => {
    metrics = createAssetFlowMetrics(new Registry());
  });

  it('processes payments end to end into the store', async () => {
    const store = createInMemoryAssetFlowStore();
    const handlers = createAssetFlowHandlers({ store, metrics, intervals: ['1m', '1h', '1d'] });
    await handlers.payment!(paymentPayload() as never, {} as never);

    const rows = store.rows();
    expect(rows).toHaveLength(3);
    const hourRow = rows.find((r) => r.interval === '1h');
    expect(hourRow).toMatchObject({
      bucket_start: HOUR,
      asset_code: 'XLM',
      volume: '10.0000000',
      payments: 1,
      net_flow: '0.0000000',
    });
    expect(await counterValue(metrics.operationsProcessed, { kind: 'payment' })).toBe(1);
    expect(await counterValue(metrics.bucketsUpdated, { interval: '1h' })).toBe(1);
  });

  it('processes a path payment into both asset buckets', async () => {
    const store = createInMemoryAssetFlowStore();
    const handlers = createAssetFlowHandlers({ store, metrics, intervals: ['1h'] });
    await handlers.path_payment!(pathPaymentPayload() as never, {} as never);

    const rows = store.rows();
    expect(rows).toHaveLength(2);
    const usdc = rows.find((r) => r.asset_code === 'USDC');
    expect(usdc).toMatchObject({ volume: '5.0000000', payments: 1, asset_issuer: 'GISS' });
    const xlm = rows.find((r) => r.asset_code === 'XLM');
    expect(xlm).toMatchObject({ volume: '50.0000000', payments: 1, asset_issuer: '' });
  });

  it('is additive across upserts (no read-modify-write lost updates)', async () => {
    const store = createInMemoryAssetFlowStore();
    const handlers = createAssetFlowHandlers({ store, metrics, intervals: ['1h'] });
    await handlers.payment!(paymentPayload() as never, {} as never);
    await handlers.payment!(paymentPayload({ id: 'op-2', amount: '5.0000000', ledger: 101 }) as never, {} as never);

    const row = store.rows().find((r) => r.interval === '1h');
    expect(row).toMatchObject({ volume: '15.0000000', payments: 2 });
  });

  it('throws on invalid amounts so the queue retries the job', async () => {
    const store = createInMemoryAssetFlowStore();
    const handlers = createAssetFlowHandlers({ store, metrics, intervals: ['1h'] });
    await expect(
      handlers.payment!(paymentPayload({ amount: 'oops' }) as never, {} as never),
    ).rejects.toThrow('invalid amount');
    expect(await counterValue(metrics.operationsProcessed, { kind: 'payment' })).toBe(1);
  });
});

describe('recompute and reorg handling', () => {
  let metrics: AssetFlowMetrics;

  beforeEach(() => {
    metrics = createAssetFlowMetrics(new Registry());
  });

  it('recomputes a closed bucket from stored operations without doubling', async () => {
    const store = createInMemoryAssetFlowStore([
      storedOperation({ amount: '10.0000000' }),
      storedOperation({ amount: '5.0000000', source_account: 'GB', destination_account: 'GA' }),
      storedOperation({ asset_code: 'XLM', asset_issuer: null, amount: '99.0000000' }),
    ]);
    const aggregator = new AssetFlowAggregator({ store, metrics, intervals: ['1h'] });

    await aggregator.recomputeBucket(HOUR, '1h');
    const first = store.rows().find((r) => r.asset_code === 'USDC');
    expect(first).toMatchObject({ volume: '15.0000000', payments: 2 });

    // Recomputing again must not double-count.
    await aggregator.recomputeBucket(HOUR, '1h');
    const second = store.rows().find((r) => r.asset_code === 'USDC');
    expect(second).toMatchObject({ volume: '15.0000000', payments: 2 });
  });

  it('detects a ledger sequence drop and recomputes the trailing window', async () => {
    const store = createInMemoryAssetFlowStore([storedOperation({ amount: '42.0000000' })]);
    const highWaterStore = createMemoryCursorStore({ 'asset-flow:last-ledger-sequence': '100' });
    const handlers = createAssetFlowHandlers({
      store,
      metrics,
      intervals: ['1h'],
      highWaterStore,
      reorgLookbackBuckets: { '1h': 1 },
    });

    await handlers.payment!(
      paymentPayload({
        ledger: 90,
        asset: { type: 'credit_alphanum4', code: 'USDC', issuer: 'GISS' },
        amount: '10.0000000',
        ledgerClosedAt: '2026-11-01T10:00:30.000Z',
      }) as never,
      {} as never,
    );

    expect(await counterValue(metrics.reorgsDetected, { interval: '1h' })).toBe(1);
    // The bucket was recomputed from stored ops (42), then the triggering op (10) was folded in.
    const row = store.rows().find((r) => r.asset_code === 'USDC' && r.interval === '1h');
    expect(row?.volume).toBe('52.0000000');
  });

  it('loads the persisted high-water mark', async () => {
    const highWaterStore = createMemoryCursorStore({ 'asset-flow:last-ledger-sequence': '42' });
    const store = createInMemoryAssetFlowStore();
    const aggregator = new AssetFlowAggregator({ store, metrics, highWaterStore });
    expect(await aggregator.loadHighWaterMark()).toBe(42);
    expect(aggregator.highWaterLedgerSequence).toBe(42);
  });

  it('aggregation handler recomputes the requested bucket', async () => {
    const store = createInMemoryAssetFlowStore([storedOperation({ amount: '7.0000000' })]);
    const handler = createAssetFlowAggregationHandler({ store, metrics, intervals: ['1h'] });
    await handler(
      { kind: 'asset-flow', bucketStart: HOUR, interval: '1h' } as never,
      {} as never,
    );
    const row = store.rows().find((r) => r.interval === '1h');
    expect(row).toMatchObject({ asset_code: 'USDC', volume: '7.0000000' });
  });

  it('aggregation handler rejects non-asset-flow kinds', async () => {
    const store = createInMemoryAssetFlowStore();
    const handler = createAssetFlowAggregationHandler({ store, metrics });
    await expect(
      handler({ kind: 'corridor', bucketStart: HOUR, interval: '1h' } as never, {} as never),
    ).rejects.toThrow('cannot handle');
  });
});

describe('update events', () => {
  let metrics: AssetFlowMetrics;

  beforeEach(() => {
    metrics = createAssetFlowMetrics(new Registry());
  });

  it('emits one event per upserted bucket row with deltas and totals', async () => {
    const sink = createInMemoryAssetFlowSink();
    const store = createInMemoryAssetFlowStore();
    const handlers = createAssetFlowHandlers({
      store,
      metrics,
      intervals: ['1h'],
      events: [sink],
    });
    await handlers.payment!(paymentPayload() as never, {} as never);

    expect(sink.events).toHaveLength(1);
    expect(sink.events[0]).toMatchObject({
      kind: 'asset_flow_updated',
      interval: '1h',
      bucketStart: HOUR,
      assetCode: 'XLM',
      volumeDelta: '10.0000000',
      paymentsDelta: 1,
      netFlowDelta: '0.0000000',
      volume: '10.0000000',
    });
  });

  it('a failing sink does not fail the aggregation', async () => {
    const store = createInMemoryAssetFlowStore();
    const handlers = createAssetFlowHandlers({
      store,
      metrics,
      intervals: ['1h'],
      events: [{ emit: () => { throw new Error('sse down'); } }],
    });
    await expect(
      handlers.payment!(paymentPayload() as never, {} as never),
    ).resolves.toBeUndefined();
    expect(await counterValue(metrics.failuresTotal, { stage: 'events' })).toBe(1);
    expect(await counterValue(metrics.bucketsUpdated, { interval: '1h' })).toBe(1);
  });
});
