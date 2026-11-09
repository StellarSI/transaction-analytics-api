import { describe, it, expect, beforeEach } from 'vitest';
import { Registry } from 'prom-client';
import {
  AnchorRegistry,
  bucketStartIso,
  computeCorridorDeltas,
  createCorridorAggregationHandler,
  createCorridorHandlers,
  createCorridorMetrics,
  createInMemoryCorridorSink,
  createInMemoryCorridorStore,
  describeAsset,
  formatAmount,
  parseAmount,
  pathPaymentPayloadToCorridor,
  paymentPayloadToCorridor,
  resolveAnchor,
  type CorridorMetrics,
} from '../corridors';
import { createMemoryCursorStore } from '../../ingestor/types';

/**
 * Unit tests for the corridor aggregator (issue #21). Everything runs
 * against the in-memory store — no database or Redis required.
 */

const HOUR = '2026-11-01T10:00:00.000Z';

function pathPaymentPayload(overrides: Record<string, unknown> = {}) {
  return {
    kind: 'path_payment' as const,
    variant: 'strict-receive' as const,
    id: 'op-pp-1',
    cursor: '1-pp-1',
    ledger: 100,
    ledgerClosedAt: '2026-11-01T10:15:00.000Z',
    from: 'GA',
    to: 'GB',
    sendAsset: { type: 'credit_alphanum4', code: 'USDC', issuer: 'GUSDC' },
    sendAmount: '5.0000000',
    destinationAsset: { type: 'credit_alphanum4', code: 'EURC', issuer: 'GEURC' },
    destinationAmount: '4.5000000',
    corridor: {
      path: [],
      sameAnchor: false,
      crossIssuer: true,
      sourceAnchor: undefined,
      destinationAnchor: undefined,
    },
    ...overrides,
  };
}

function paymentPayload(overrides: Record<string, unknown> = {}) {
  return {
    kind: 'payment' as const,
    id: 'op-p-1',
    cursor: '1-p-1',
    ledger: 101,
    ledgerClosedAt: '2026-11-01T10:20:00.000Z',
    from: 'GA',
    to: 'GB',
    asset: { type: 'native', code: 'XLM' },
    amount: '10.0000000',
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
  });

  it('rejects malformed amounts', () => {
    expect(() => parseAmount('oops')).toThrow('invalid amount');
  });
});

describe('anchor resolution', () => {
  it('resolves anchors from issuers and falls back to issuer', () => {
    const registry = new AnchorRegistry();
    registry.register('GUSDC', { name: 'Anchor USDC', homeDomain: 'usdc.example' });
    expect(resolveAnchor('GUSDC', registry)).toBe('GUSDC');
    expect(resolveAnchor('GUNKNOWN', registry)).toBe('GUNKNOWN');
    expect(resolveAnchor(undefined, registry)).toBeNull();
  });

  it('prefers explicit corridor metadata', () => {
    const registry = new AnchorRegistry();
    expect(
      resolveAnchor('GUSDC', registry, { issuer: 'GEXPLICIT', name: 'Explicit' }),
    ).toBe('GEXPLICIT');
  });

  it('formats assets as CODE:ISSUER', () => {
    expect(describeAsset({ type: 'credit_alphanum4', code: 'USDC', issuer: 'GUSDC' })).toBe(
      'USDC:GUSDC',
    );
    expect(describeAsset({ type: 'native', code: 'XLM' })).toBe('XLM');
  });
});

describe('bucket helpers', () => {
  it('floors into minute/hour/day buckets', () => {
    const t = Date.parse('2026-11-01T10:23:47.890Z');
    expect(bucketStartIso(t, '1m')).toBe('2026-11-01T10:23:00.000Z');
    expect(bucketStartIso(t, '1h')).toBe('2026-11-01T10:00:00.000Z');
    expect(bucketStartIso(t, '1d')).toBe('2026-11-01T00:00:00.000Z');
  });
});

describe('payload mapping', () => {
  it('maps a path payment to a destination-leg corridor', () => {
    const operations = pathPaymentPayloadToCorridor(pathPaymentPayload() as never);
    expect(operations).toHaveLength(1);
    expect(operations[0]).toMatchObject({
      sourceAsset: 'USDC:GUSDC',
      destAsset: 'EURC:GEURC',
      sourceAnchor: 'GUSDC',
      destAnchor: 'GEURC',
      amount: '4.5000000',
    });
  });

  it('maps a regular payment to a degenerate corridor', () => {
    const operation = paymentPayloadToCorridor(paymentPayload() as never);
    expect(operation).toMatchObject({
      sourceAsset: 'XLM',
      destAsset: 'XLM',
      sourceAnchor: null,
      destAnchor: null,
      amount: '10.0000000',
    });
  });
});

describe('computeCorridorDeltas', () => {
  it('aggregates volume and payments by anchor pair and asset pair', () => {
    const deltas = computeCorridorDeltas(
      [
        {
          sourceAsset: 'USDC:GUSDC',
          destAsset: 'EURC:GEURC',
          sourceAnchor: 'GUSDC',
          destAnchor: 'GEURC',
          amount: '4.5000000',
          ledgerClosedAt: '2026-11-01T10:10:00.000Z',
        },
        {
          sourceAsset: 'USDC:GUSDC',
          destAsset: 'EURC:GEURC',
          sourceAnchor: 'GUSDC',
          destAnchor: 'GEURC',
          amount: '1.0000000',
          ledgerClosedAt: '2026-11-01T10:20:00.000Z',
        },
        {
          sourceAsset: 'XLM',
          destAsset: 'XLM',
          sourceAnchor: null,
          destAnchor: null,
          amount: '10.0000000',
          ledgerClosedAt: '2026-11-01T10:05:00.000Z',
        },
      ],
      ['1h'],
    );

    expect(deltas).toHaveLength(2);
    const corridor = deltas.find((d) => d.sourceAsset === 'USDC:GUSDC');
    expect(corridor).toMatchObject({
      volume: '5.5000000',
      payments: 2,
      sourceAnchor: 'GUSDC',
      destAnchor: 'GEURC',
    });
    const native = deltas.find((d) => d.sourceAsset === 'XLM');
    expect(native).toMatchObject({ volume: '10.0000000', payments: 1 });
  });

  it('throws on missing ledger close time', () => {
    expect(() =>
      computeCorridorDeltas(
        [
          {
            sourceAsset: 'XLM',
            destAsset: 'XLM',
            sourceAnchor: null,
            destAnchor: null,
            amount: '1.0000000',
          },
        ],
        ['1h'],
      ),
    ).toThrow('missing ledgerClosedAt');
  });
});

describe('CorridorAggregator apply path', () => {
  let metrics: CorridorMetrics;

  beforeEach(() => {
    metrics = createCorridorMetrics(new Registry());
  });

  it('processes path payments end to end into the store', async () => {
    const store = createInMemoryCorridorStore();
    const handlers = createCorridorHandlers({ store, metrics, intervals: ['1h'] });
    await handlers.path_payment!(pathPaymentPayload() as never, {} as never);

    const rows = store.rows();
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      bucket_start: HOUR,
      source_asset: 'USDC:GUSDC',
      dest_asset: 'EURC:GEURC',
      source_anchor: 'GUSDC',
      volume: '4.5000000',
      payments: 1,
    });
    expect(await counterValue(metrics.operationsProcessed, { kind: 'path_payment' })).toBe(1);
  });

  it('identifies anchors from issuers', async () => {
    const registry = new AnchorRegistry();
    registry.register('GUSDC', { name: 'Anchor USDC' });
    const store = createInMemoryCorridorStore();
    const handlers = createCorridorHandlers({ store, metrics, intervals: ['1h'], registry });
    await handlers.path_payment!(pathPaymentPayload() as never, {} as never);
    expect(store.rows()[0].source_anchor).toBe('GUSDC');
  });

  it('breaks down by asset pair', async () => {
    const store = createInMemoryCorridorStore();
    const handlers = createCorridorHandlers({ store, metrics, intervals: ['1h'] });
    await handlers.payment!(paymentPayload() as never, {} as never);
    await handlers.payment!(
      paymentPayload({
        id: 'op-p-2',
        asset: { type: 'credit_alphanum4', code: 'USDC', issuer: 'GUSDC' },
        amount: '5.0000000',
        ledger: 102,
      }) as never,
      {} as never,
    );

    const rows = store.rows();
    expect(rows).toHaveLength(2);
    expect(rows.map((r) => r.source_asset).sort()).toEqual(['USDC:GUSDC', 'XLM']);
  });

  it('is additive across upserts', async () => {
    const store = createInMemoryCorridorStore();
    const handlers = createCorridorHandlers({ store, metrics, intervals: ['1h'] });
    await handlers.path_payment!(pathPaymentPayload() as never, {} as never);
    await handlers.path_payment!(
      pathPaymentPayload({ id: 'op-pp-2', destinationAmount: '0.5000000', ledger: 101 }) as never,
      {} as never,
    );

    const row = store.rows().find((r) => r.source_asset === 'USDC:GUSDC');
    expect(row).toMatchObject({ volume: '5.0000000', payments: 2 });
  });
});

describe('recompute and reorg handling', () => {
  let metrics: CorridorMetrics;

  beforeEach(() => {
    metrics = createCorridorMetrics(new Registry());
  });

  it('detects a ledger sequence drop and recomputes the trailing window', async () => {
    const highWaterStore = createMemoryCursorStore({ 'corridor:last-ledger-sequence': '100' });
    const store = createInMemoryCorridorStore();
    const handlers = createCorridorHandlers({
      store,
      metrics,
      intervals: ['1h'],
      highWaterStore,
      reorgLookbackBuckets: { '1h': 1 },
    });

    await handlers.payment!(
      paymentPayload({ ledger: 90, ledgerClosedAt: '2026-11-01T10:00:30.000Z' }) as never,
      {} as never,
    );

    expect(await counterValue(metrics.reorgsDetected, { interval: '1h' })).toBe(1);
  });

  it('aggregation handler rejects non-corridor kinds', async () => {
    const store = createInMemoryCorridorStore();
    const handler = createCorridorAggregationHandler({ store, metrics });
    await expect(
      handler({ kind: 'asset-flow', bucketStart: HOUR, interval: '1h' } as never, {} as never),
    ).rejects.toThrow('cannot handle');
  });

  it('aggregation handler runs without stored operations', async () => {
    const store = createInMemoryCorridorStore();
    const handler = createCorridorAggregationHandler({ store, metrics, intervals: ['1h'] });
    await handler({ kind: 'corridor', bucketStart: HOUR, interval: '1h' } as never, {} as never);
    expect(store.rows()).toHaveLength(0);
  });
});

describe('update events', () => {
  let metrics: CorridorMetrics;

  beforeEach(() => {
    metrics = createCorridorMetrics(new Registry());
  });

  it('emits one event per upserted bucket row', async () => {
    const sink = createInMemoryCorridorSink();
    const store = createInMemoryCorridorStore();
    const handlers = createCorridorHandlers({ store, metrics, intervals: ['1h'], events: [sink] });
    await handlers.path_payment!(pathPaymentPayload() as never, {} as never);

    expect(sink.events).toHaveLength(1);
    expect(sink.events[0]).toMatchObject({
      kind: 'corridor_updated',
      sourceAsset: 'USDC:GUSDC',
      volumeDelta: '4.5000000',
    });
  });

  it('a failing sink does not fail the aggregation', async () => {
    const store = createInMemoryCorridorStore();
    const handlers = createCorridorHandlers({
      store,
      metrics,
      intervals: ['1h'],
      events: [{ emit: () => { throw new Error('sse down'); } }],
    });
    await expect(
      handlers.payment!(paymentPayload() as never, {} as never),
    ).resolves.toBeUndefined();
    expect(await counterValue(metrics.failuresTotal, { stage: 'events' })).toBe(1);
  });
});
