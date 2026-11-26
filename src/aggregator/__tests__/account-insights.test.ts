import { describe, it, expect } from 'vitest';
import { Registry } from 'prom-client';
import {
  AccountFlowAggregator,
  computeAccountFlowDeltas,
  createAccountFlowAggregationHandler,
  createAccountFlowHandlers,
  createAccountFlowMetrics,
  createInMemoryAccountFlowSink,
  createInMemoryAccountFlowStore,
  formatAmount,
  isAccountFlowInterval,
  listBucketStarts,
  parseAmount,
  pathPaymentPayloadToOperations,
  paymentPayloadToOperations,
  bucketStartIso,
  type AccountFlowMetrics,
  type AccountFlowOperation,
} from '../account-insights';
import { createMemoryCursorStore } from '../../ingestor/types';

/** Unit tests for the account-flow aggregator (issues #19, #39). */

const HOUR = '2026-11-01T10:00:00.000Z';
const GA = 'GA';
const GB = 'GB';
const GC = 'GC';

function operation(overrides: Partial<AccountFlowOperation> = {}): AccountFlowOperation {
  return {
    amount: '10.0000000',
    fromAccount: GA,
    toAccount: GB,
    ledgerSequence: 100,
    ledgerClosedAt: HOUR,
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

describe('decimal arithmetic (issue #39)', () => {
  it('round-trips stroop-precision amounts as strings', () => {
    expect(formatAmount(parseAmount('10.0000000'))).toBe('10.0000000');
    expect(formatAmount(parseAmount('0.0000001'))).toBe('0.0000001');
    expect(formatAmount(parseAmount('1234.5678901'))).toBe('1234.5678901');
    expect(formatAmount(parseAmount('-5.5000000'))).toBe('-5.5000000');
  });

  it('rejects malformed and over-precise amounts', () => {
    expect(() => parseAmount('1.12345678')).toThrow('invalid amount');
    expect(() => parseAmount('not-a-number')).toThrow('invalid amount');
    expect(() => parseAmount('10')).not.toThrow();
  });
});

describe('bucket helpers (issue #39)', () => {
  it('floors timestamps into minute/hour/day buckets', () => {
    const t = Date.parse('2026-11-01T10:23:47.890Z');
    expect(bucketStartIso(t, '1m')).toBe('2026-11-01T10:23:00.000Z');
    expect(bucketStartIso(t, '1h')).toBe('2026-11-01T10:00:00.000Z');
    expect(bucketStartIso(t, '1d')).toBe('2026-11-01T00:00:00.000Z');
  });

  it('lists bucket starts with an exclusive end and recognizes intervals', () => {
    expect(listBucketStarts(HOUR, '2026-11-01T13:00:00.000Z', '1h')).toEqual([
      HOUR,
      '2026-11-01T11:00:00.000Z',
      '2026-11-01T12:00:00.000Z',
    ]);
    expect(isAccountFlowInterval('1m')).toBe(true);
    expect(isAccountFlowInterval('1d')).toBe(true);
    expect(isAccountFlowInterval('1s')).toBe(false);
  });
});

describe('computeAccountFlowDeltas (issue #39)', () => {
  it('computes inflow, outflow, net flow and counterparties with string precision', () => {
    const deltas = computeAccountFlowDeltas(
      [
        operation(),
        operation({ amount: '2.5000000', toAccount: GC, ledgerSequence: 101 }),
      ],
      ['1h'],
    );

    expect(deltas).toHaveLength(3);
    expect(deltas.map((d) => d.accountId)).toEqual([GA, GB, GC]);
    expect(deltas[0]).toMatchObject({
      accountId: GA,
      inflow: '0.0000000',
      outflow: '12.5000000',
      netFlow: '-12.5000000',
    });
    expect(deltas[0]?.counterparties).toEqual([
      { account_id: GB, volume: '10.0000000', count: 1, direction: 'out' },
      { account_id: GC, volume: '2.5000000', count: 1, direction: 'out' },
    ]);
    expect(deltas[1]).toMatchObject({
      accountId: GB,
      inflow: '10.0000000',
      outflow: '0.0000000',
      netFlow: '10.0000000',
    });
    expect(deltas[1]?.counterparties).toEqual([
      { account_id: GA, volume: '10.0000000', count: 1, direction: 'in' },
    ]);
    expect(deltas[2]).toMatchObject({
      accountId: GC,
      inflow: '2.5000000',
      netFlow: '2.5000000',
    });
  });

  it('handles zero amounts without dropping the operation', () => {
    const deltas = computeAccountFlowDeltas([operation({ amount: '0.0000000' })], ['1h']);
    expect(deltas).toHaveLength(2);
    expect(deltas[0]).toMatchObject({ inflow: '0.0000000', outflow: '0.0000000' });
    expect(deltas[0]?.counterparties).toEqual([
      { account_id: GB, volume: '0.0000000', count: 1, direction: 'out' },
    ]);
  });

  it('adds exact duplicate operations additively', () => {
    const deltas = computeAccountFlowDeltas([operation(), operation()], ['1h']);
    expect(deltas[0]).toMatchObject({ outflow: '20.0000000', netFlow: '-20.0000000' });
    expect(deltas[1]).toMatchObject({ inflow: '20.0000000' });
    expect(deltas[1]?.counterparties).toEqual([
      { account_id: GA, volume: '20.0000000', count: 2, direction: 'in' },
    ]);
  });

  it('fails fast on missing or unparseable ledger close times', () => {
    expect(() =>
      computeAccountFlowDeltas([operation({ ledgerClosedAt: undefined })], ['1h']),
    ).toThrow('missing ledgerClosedAt');
    expect(() =>
      computeAccountFlowDeltas([operation({ ledgerClosedAt: 'not-a-date' })], ['1h']),
    ).toThrow('unparseable ledgerClosedAt');
  });
});

describe('payload mapping (issue #39)', () => {
  it('maps a payment to one leg and a path payment to two legs', () => {
    const payment = paymentPayloadToOperations({
      kind: 'payment',
      id: 'op-1',
      cursor: '1-1',
      ledger: 100,
      ledgerClosedAt: HOUR,
      from: GA,
      to: GB,
      amount: '10.0000000',
    } as never);
    expect(payment).toEqual([operation()]);

    const path = pathPaymentPayloadToOperations({
      kind: 'path_payment',
      variant: 'strict-receive',
      id: 'op-pp',
      cursor: '1-2',
      ledger: 101,
      ledgerClosedAt: HOUR,
      from: GA,
      to: GB,
      sendAmount: '5.0000000',
      sendMax: '6.0000000',
      destinationAmount: '50.0000000',
      destinationMin: '49.0000000',
    } as never);
    expect(path).toHaveLength(2);
    expect(path.map((leg) => leg.amount)).toEqual(['5.0000000', '50.0000000']);
  });
});

describe('AccountFlowAggregator (issues #19, #39)', () => {
  it('applies operations to the in-memory store and counts metrics', async () => {
    const store = createInMemoryAccountFlowStore();
    const metrics: AccountFlowMetrics = createAccountFlowMetrics(new Registry());
    const aggregator = new AccountFlowAggregator({ store, intervals: ['1h'], metrics });

    const persisted = await aggregator.applyOperations([operation()], { kind: 'payment' });

    expect(persisted.map((row) => row.account_id)).toEqual([GA, GB]);
    expect(store.rows().find((row) => row.account_id === GA)).toMatchObject({
      inflow: '0.0000000',
      outflow: '10.0000000',
      net_flow: '-10.0000000',
    });
    expect(await counterValue(metrics.operationsProcessed, { kind: 'payment' })).toBe(1);
    expect(await counterValue(metrics.bucketsUpdated, { interval: '1h' })).toBe(2);
  });

  it('emits one update event per bucket and tolerates a failing sink', async () => {
    const sink = createInMemoryAccountFlowSink();
    const aggregator = new AccountFlowAggregator({
      store: createInMemoryAccountFlowStore(),
      intervals: ['1h'],
      events: [sink, { emit() { throw new Error('sink down'); } }],
    });

    await aggregator.applyOperations([operation()], { kind: 'payment' });
    expect(sink.events).toHaveLength(2);
    expect(sink.events[0]).toMatchObject({ kind: 'account_flow_updated', interval: '1h' });
    expect(typeof sink.events[0]?.inflowDelta).toBe('string');
  });

  it('detects a ledger rollback and recomputes the trailing window', async () => {
    const cursor = createMemoryCursorStore({ 'account-flow:last-ledger-sequence': '100' });
    const aggregator = new AccountFlowAggregator({
      store: createInMemoryAccountFlowStore(),
      intervals: ['1h'],
      highWaterStore: cursor,
      reorgLookbackBuckets: { '1h': 1 },
    });

    await expect(aggregator.observeLedgerSequence(99, HOUR)).resolves.toBe(true);
    await expect(aggregator.loadHighWaterMark()).resolves.toBe(100);
  });

  it('recomputes a bucket idempotently and exposes handler factories', async () => {
    const store = createInMemoryAccountFlowStore([
      {
        account_id: GA,
        amount: '10.0000000',
        from_account: GA,
        to_account: GB,
        ledger_sequence: 100,
        ledger_closed_at: HOUR,
      },
    ]);
    const aggregator = new AccountFlowAggregator({ store, intervals: ['1h'] });

    const first = await aggregator.recomputeBucket(HOUR, '1h');
    const second = await aggregator.recomputeBucket(HOUR, '1h');
    expect(first.map((row) => row.inflow)).toEqual(second.map((row) => row.inflow));

    const handlers = createAccountFlowHandlers({ store, intervals: ['1h'] });
    expect(typeof handlers.payment).toBe('function');
    const specific = createAccountFlowAggregationHandler({ store, intervals: ['1h'] });
    await expect(specific({ kind: 'other' } as never)).rejects.toThrow('cannot handle');
  });
});
