import { describe, it, expect, beforeEach } from 'vitest';
import { Registry } from 'prom-client';
import {
  bucketStartIso,
  computeActiveHoursCount,
  computeDormancyDays,
  computeTrustlineDeltas,
  createInMemoryTrustlineSink,
  createInMemoryTrustlineStore,
  createTrustlineAggregationHandler,
  createTrustlineHandlers,
  createTrustlineMetrics,
  formatAmount,
  parseAmount,
  type TrustlineMetrics,
} from '../trustlines';
import { createMemoryCursorStore } from '../../ingestor/types';

/**
 * Unit tests for the trustline aggregator (issue #20). Everything runs
 * against the in-memory store — no database or Redis required.
 */

const HOUR = '2026-11-01T10:00:00.000Z';

function changeTrustPayload(overrides: Record<string, unknown> = {}) {
  return {
    kind: 'change_trust' as const,
    id: 'op-ct-1',
    cursor: '1-ct-1',
    ledger: 100,
    ledgerClosedAt: '2026-11-01T10:00:30.000Z',
    sourceAccount: 'GA',
    trustor: 'GISSUER',
    asset: { type: 'credit_alphanum4', code: 'USDC', issuer: 'GISSUER' },
    limit: '1000.0000000',
    isDeletion: false,
    ...overrides,
  };
}

function paymentPayload(overrides: Record<string, unknown> = {}) {
  return {
    kind: 'payment' as const,
    id: 'op-pay-1',
    cursor: '1-pay-1',
    ledger: 101,
    ledgerClosedAt: '2026-11-01T10:15:00.000Z',
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
    expect(formatAmount(parseAmount('-5.5000000'))).toBe('-5.5000000');
  });

  it('rejects malformed amounts', () => {
    expect(() => parseAmount('1.12345678')).toThrow('invalid amount');
    expect(() => parseAmount('not-a-number')).toThrow('invalid amount');
  });
});

describe('bucket helpers', () => {
  it('floors timestamps into minute/hour/day buckets', () => {
    const t = Date.parse('2026-11-01T10:23:47.890Z');
    expect(bucketStartIso(t, '1m')).toBe('2026-11-01T10:23:00.000Z');
    expect(bucketStartIso(t, '1h')).toBe('2026-11-01T10:00:00.000Z');
    expect(bucketStartIso(t, '1d')).toBe('2026-11-01T00:00:00.000Z');
  });
});

describe('activity helpers', () => {
  it('computes dormancy days', () => {
    expect(computeDormancyDays(null, HOUR)).toBe(0);
    expect(computeDormancyDays('2026-11-01T10:00:00.000Z', HOUR)).toBe(0);
    expect(computeDormancyDays('2026-10-30T10:00:00.000Z', HOUR)).toBe(2);
  });

  it('computes distinct active hours', () => {
    expect(
      computeActiveHoursCount([
        '2026-11-01T10:10:00.000Z',
        '2026-11-01T10:20:00.000Z',
        '2026-11-01T12:00:00.000Z',
      ]),
    ).toBe(2);
  });
});

describe('computeTrustlineDeltas', () => {
  it('tracks new trustlines, limit changes and total limit', () => {
    const deltas = computeTrustlineDeltas(
      [
        {
          accountId: 'GA',
          assetCode: 'USDC',
          assetIssuer: 'GISSUER',
          limit: '1000.0000000',
          isDeletion: false,
          ledgerClosedAt: '2026-11-01T10:05:00.000Z',
        },
        {
          accountId: 'GA',
          assetCode: 'USDT',
          assetIssuer: 'GISSUER',
          limit: '500.0000000',
          isDeletion: false,
          ledgerClosedAt: '2026-11-01T10:10:00.000Z',
        },
      ],
      [],
      ['1h'],
    );
    expect(deltas).toHaveLength(1);
    expect(deltas[0]).toMatchObject({
      accountId: 'GA',
      trustlineCount: 2,
      newTrustlines: 2,
      removedTrustlines: 0,
      trustLimitChanges: 2,
      totalTrustLimit: '1500.0000000',
    });
  });

  it('tracks removed trustlines and nets trustline_count', () => {
    const deltas = computeTrustlineDeltas(
      [
        {
          accountId: 'GA',
          assetCode: 'USDC',
          assetIssuer: 'GISSUER',
          limit: '0.0000000',
          isDeletion: true,
          ledgerClosedAt: '2026-11-01T10:05:00.000Z',
        },
        {
          accountId: 'GA',
          assetCode: 'USDT',
          assetIssuer: 'GISSUER',
          limit: '500.0000000',
          isDeletion: false,
          ledgerClosedAt: '2026-11-01T10:06:00.000Z',
        },
      ],
      [],
      ['1h'],
    );
    expect(deltas[0]).toMatchObject({
      trustlineCount: 0,
      newTrustlines: 1,
      removedTrustlines: 1,
      trustLimitChanges: 2,
      totalTrustLimit: '500.0000000',
    });
  });

  it('computes activity patterns for account', () => {
    const deltas = computeTrustlineDeltas(
      [],
      [
        { accountId: 'GA', ledgerClosedAt: '2026-11-01T08:10:00.000Z' },
        { accountId: 'GA', ledgerClosedAt: '2026-11-01T08:20:00.000Z' },
        { accountId: 'GA', ledgerClosedAt: '2026-11-01T12:00:00.000Z' },
      ],
      ['1d'],
    );
    expect(deltas).toHaveLength(1);
    expect(deltas[0]).toMatchObject({
      accountId: 'GA',
      paymentsCount: 3,
      activeHours: 2,
      dormancyDays: 0,
    });
  });

  it('throws on operations without a parseable ledger close time', () => {
    expect(() =>
      computeTrustlineDeltas(
        [
          {
            accountId: 'GA',
            assetCode: 'USDC',
            assetIssuer: 'GISSUER',
            limit: '1.0000000',
            isDeletion: false,
          },
        ],
        [],
        ['1h'],
      ),
    ).toThrow('missing ledgerClosedAt');
  });
});

describe('TrustlineAggregator apply path', () => {
  let metrics: TrustlineMetrics;

  beforeEach(() => {
    metrics = createTrustlineMetrics(new Registry());
  });

  it('processes changeTrust end to end into the store', async () => {
    const store = createInMemoryTrustlineStore();
    const handlers = createTrustlineHandlers({ store, metrics, intervals: ['1h'] });
    await handlers.change_trust!(changeTrustPayload() as never, {} as never);

    const rows = store.rows();
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      bucket_start: HOUR,
      account_id: 'GA',
      trustline_count: 1,
      new_trustlines: 1,
      removed_trustlines: 0,
      trust_limit_changes: 1,
      total_trust_limit: '1000.0000000',
    });
    expect(await counterValue(metrics.operationsProcessed, { kind: 'change_trust' })).toBe(1);
    expect(await counterValue(metrics.bucketsUpdated, { interval: '1h' })).toBe(1);
  });

  it('processes payments for activity patterns', async () => {
    const store = createInMemoryTrustlineStore();
    const handlers = createTrustlineHandlers({ store, metrics, intervals: ['1h'] });
    await handlers.payment!(paymentPayload() as never, {} as never);

    const rows = store.rows();
    // Both sender and recipient get an activity row.
    expect(rows).toHaveLength(2);
    expect(rows.find((r) => r.account_id === 'GA')?.payments_count).toBe(1);
    expect(rows.find((r) => r.account_id === 'GB')?.payments_count).toBe(1);
  });

  it('is additive across upserts', async () => {
    const store = createInMemoryTrustlineStore();
    const handlers = createTrustlineHandlers({ store, metrics, intervals: ['1h'] });
    await handlers.change_trust!(changeTrustPayload() as never, {} as never);
    await handlers.change_trust!(
      changeTrustPayload({ id: 'op-ct-2', limit: '500.0000000' }) as never,
      {} as never,
    );

    const row = store.rows().find((r) => r.interval === '1h');
    expect(row).toMatchObject({ new_trustlines: 2, trust_limit_changes: 2 });
    expect(row?.total_trust_limit).toBe('1500.0000000');
  });
});

describe('recompute and reorg handling', () => {
  let metrics: TrustlineMetrics;

  beforeEach(() => {
    metrics = createTrustlineMetrics(new Registry());
  });

  it('detects a ledger sequence drop and recomputes the trailing window', async () => {
    const highWaterStore = createMemoryCursorStore({ 'trustline:last-ledger-sequence': '100' });
    const store = createInMemoryTrustlineStore();
    const handlers = createTrustlineHandlers({
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

  it('aggregation handler recomputes the requested bucket', async () => {
    const store = createInMemoryTrustlineStore();
    const handler = createTrustlineAggregationHandler({ store, metrics, intervals: ['1h'] });
    await handler(
      { kind: 'trustline', bucketStart: HOUR, interval: '1h' } as never,
      {} as never,
    );
    // No stored operations: recompute deletes and writes nothing.
    expect(store.rows()).toHaveLength(0);
  });

  it('aggregation handler rejects non-trustline kinds', async () => {
    const store = createInMemoryTrustlineStore();
    const handler = createTrustlineAggregationHandler({ store, metrics });
    await expect(
      handler({ kind: 'corridor', bucketStart: HOUR, interval: '1h' } as never, {} as never),
    ).rejects.toThrow('cannot handle');
  });
});

describe('update events', () => {
  let metrics: TrustlineMetrics;

  beforeEach(() => {
    metrics = createTrustlineMetrics(new Registry());
  });

  it('emits one event per upserted bucket row', async () => {
    const sink = createInMemoryTrustlineSink();
    const store = createInMemoryTrustlineStore();
    const handlers = createTrustlineHandlers({ store, metrics, intervals: ['1h'], events: [sink] });
    await handlers.change_trust!(changeTrustPayload() as never, {} as never);

    expect(sink.events).toHaveLength(1);
    expect(sink.events[0]).toMatchObject({
      kind: 'trustline_updated',
      interval: '1h',
      bucketStart: HOUR,
      accountId: 'GA',
      trustlineCountDelta: 1,
      newTrustlinesDelta: 1,
    });
  });

  it('a failing sink does not fail the aggregation', async () => {
    const store = createInMemoryTrustlineStore();
    const handlers = createTrustlineHandlers({
      store,
      metrics,
      intervals: ['1h'],
      events: [{ emit: () => { throw new Error('sse down'); } }],
    });
    await expect(
      handlers.change_trust!(changeTrustPayload() as never, {} as never),
    ).resolves.toBeUndefined();
    expect(await counterValue(metrics.failuresTotal, { stage: 'events' })).toBe(1);
  });
});
