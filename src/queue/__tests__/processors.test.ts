import { describe, it, expect, vi, beforeEach } from 'vitest';
import { Registry } from 'prom-client';

/**
 * Unit tests for the queue workers (issue #15). Workers are mocked so no
 * Redis connection opens; the processors are exercised with fake jobs.
 */

vi.mock('bullmq', () => ({
  Worker: vi.fn(),
}));

const { Worker } = await import('bullmq');
const {
  createAggregationProcessor,
  createLedgerOperationsProcessor,
  startQueueWorkers,
} = await import('../processors');
const { createQueueMetrics } = await import('../metrics');
const { AGGREGATION_JOBS_QUEUE, LEDGER_OPERATIONS_QUEUE } = await import('../index');

function fakeLedgerJob(data: unknown) {
  return { id: 'op-1', data } as never;
}

function fakeAggregationJob(data: unknown) {
  return { id: 'agg-1', data } as never;
}

function fakeRegistry() {
  return {
    ledgerOperations: { getJobCounts: vi.fn().mockResolvedValue({ waiting: 3, delayed: 1 }) },
    aggregationJobs: { getJobCounts: vi.fn().mockResolvedValue({ waiting: 0, delayed: 0 }) },
    enqueueLedgerOperation: vi.fn(),
    enqueueAggregation: vi.fn(),
    close: vi.fn(),
  } as never;
}

const paymentPayload = {
  kind: 'payment' as const,
  id: 'op-1',
  cursor: '1-1',
  from: 'GA',
  to: 'GB',
  asset: { type: 'native', code: 'XLM' },
  amount: '10.0000000',
};

const aggregationPayload = {
  kind: 'asset-flow' as const,
  bucketStart: '2026-11-01T10:00:00Z',
  interval: '1h' as const,
};

describe('createLedgerOperationsProcessor', () => {
  let metrics: ReturnType<typeof createQueueMetrics>;

  beforeEach(() => {
    metrics = createQueueMetrics(new Registry());
  });

  it('dispatches a valid payload to the registered handler', async () => {
    const handler = vi.fn().mockResolvedValue(undefined);
    const process = createLedgerOperationsProcessor({ payment: handler }, metrics);

    await process(fakeLedgerJob(paymentPayload));

    expect(handler).toHaveBeenCalledWith(
      expect.objectContaining({ kind: 'payment' }),
      expect.objectContaining({ id: 'op-1' }),
    );
    expect(
      metrics.processedTotal.get({ queue: LEDGER_OPERATIONS_QUEUE }).values[0].value,
    ).toBe(1);
  });

  it('acknowledges validated payloads when no handler is registered yet', async () => {
    const process = createLedgerOperationsProcessor(undefined, metrics);
    await expect(process(fakeLedgerJob(paymentPayload))).resolves.toBeUndefined();
  });

  it('rethrows queue validation errors after counting them', async () => {
    const process = createLedgerOperationsProcessor(undefined, metrics);
    await expect(
      process(fakeLedgerJob({ kind: 'payment', id: '', cursor: '' })),
    ).rejects.toThrow('invalid ledger operation');
  });

  it('dogfoods retries: handler errors propagate to BullMQ', async () => {
    const handler = vi.fn().mockRejectedValue(new Error('postgres down'));
    const process = createLedgerOperationsProcessor({ payment: handler }, metrics);
    await expect(process(fakeLedgerJob(paymentPayload))).rejects.toThrow('postgres down');
  });
});

describe('createAggregationProcessor', () => {
  it('dispatches to the matching aggregation handler', async () => {
    const handler = vi.fn().mockResolvedValue(undefined);
    const process = createAggregationProcessor(
      { 'asset-flow': handler } as never,
      undefined,
    );
    await process(fakeAggregationJob(aggregationPayload));
    expect(handler).toHaveBeenCalledWith(
      expect.objectContaining({ bucketStart: '2026-11-01T10:00:00Z' }),
      expect.objectContaining({ id: 'agg-1' }),
    );
  });

  it('fails jobs whose kind has no handler (lost bucket)', async () => {
    const process = createAggregationProcessor({}, undefined);
    await expect(process(fakeAggregationJob(aggregationPayload))).rejects.toThrow(
      'no aggregation handler registered',
    );
  });

  it('rejects malformed aggregation payloads', async () => {
    const process = createAggregationProcessor(
      { 'asset-flow': vi.fn() } as never,
      undefined,
    );
    await expect(
      process(fakeAggregationJob({ kind: 'asset-flow', interval: '5m' })),
    ).rejects.toThrow('invalid aggregation job');
  });
});

describe('startQueueWorkers', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('creates both workers with the queue names and concurrency', () => {
    startQueueWorkers({
      registry: fakeRegistry(),
      connection: { host: 'localhost' } as never,
      concurrency: 2,
      handlers: { payment: vi.fn() },
      aggregationHandlers: { 'asset-flow': vi.fn() },
    });

    const ctor = Worker as unknown as ReturnType<typeof vi.fn>;
    expect(ctor).toHaveBeenCalledTimes(2);
    expect(ctor.mock.calls[0][0]).toBe(LEDGER_OPERATIONS_QUEUE);
    expect(ctor.mock.calls[0][2]).toEqual({ connection: { host: 'localhost' }, concurrency: 2 });
    expect(ctor.mock.calls[1][0]).toBe(AGGREGATION_JOBS_QUEUE);
  });

  it('drains both workers on close', async () => {
    const worker = { close: vi.fn().mockResolvedValue(undefined) };
    (Worker as unknown as ReturnType<typeof vi.fn>).mockImplementation(() => worker);

    const workers = startQueueWorkers({
      registry: fakeRegistry(),
      connection: { host: 'localhost' } as never,
    });
    await workers.close();

    expect(worker.close).toHaveBeenCalledTimes(2);
  });
});
