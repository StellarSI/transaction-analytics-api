import { describe, it, expect, vi, beforeEach } from 'vitest';

/**
 * Unit tests for the BullMQ registry (issue #15) — sockets are mocked: the
 * unit suite must not open connections.bullmq only constructs its internal
 * clients lazily, but the module itself is replaced to capture options.
 */

vi.mock('bullmq', () => ({
  Queue: vi.fn(),
}));

vi.mock('ioredis', () => ({
  Redis: vi.fn(),
}));

const { Queue } = await import('bullmq');
const {
  AGGREGATION_JOBS_QUEUE,
  AggregationJobSchema,
  LEDGER_OPERATIONS_QUEUE,
  LedgerOperationJobSchema,
  QueueValidationError,
  createBullMQOperationQueue,
  createQueueRegistry,
  defaultJobOptions,
} = await import('../index');

type FakeQueue = {
  add: ReturnType<typeof vi.fn>;
  close: ReturnType<typeof vi.fn>;
};

const paymentJob = {
  kind: 'payment',
  id: 'op-1',
  cursor: '1-1',
  from: 'GA',
  to: 'GB',
  asset: { type: 'native', code: 'XLM' },
  amount: '10.0000000',
};

function callableQueue(): FakeQueue {
  return {
    add: vi.fn().mockResolvedValue({ id: 'j1' }),
    close: vi.fn().mockResolvedValue(undefined),
  };
}

describe('defaultJobOptions', () => {
  it('retries 3 attempts with exponential backoff', () => {
    expect(defaultJobOptions().attempts).toBe(3);
    expect(defaultJobOptions().backoff).toEqual({ type: 'exponential', delay: 1_000 });
  });

  it('keeps failed jobs for inspection', () => {
    expect(defaultJobOptions().removeOnFail).toBe(false);
  });
});

describe('createQueueRegistry', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('creates the ledger-operations and aggregation-jobs queues', () => {
    const ledger = callableQueue();
    const aggregation = callableQueue();
    (Queue as unknown as ReturnType<typeof vi.fn>)
      .mockImplementationOnce(() => ledger)
      .mockImplementationOnce(() => aggregation);

    const registry = createQueueRegistry({ connection: { host: 'localhost' } });
    expect(Queue).toHaveBeenCalledWith(
      LEDGER_OPERATIONS_QUEUE,
      expect.objectContaining({ connection: { host: 'localhost' } }),
    );
    expect(Queue).toHaveBeenCalledWith(
      AGGREGATION_JOBS_QUEUE,
      expect.objectContaining({ connection: { host: 'localhost' } }),
    );
    expect(registry.ledgerOperations).toBe(ledger);
    expect(registry.aggregationJobs).toBe(aggregation);
  });

  it('enqueues a validated ledger operation with the kind as job name', async () => {
    const ledger = callableQueue();
    const aggregation = callableQueue();
    (Queue as unknown as ReturnType<typeof vi.fn>)
      .mockImplementationOnce(() => ledger)
      .mockImplementationOnce(() => aggregation);
    const registry = createQueueRegistry({ connection: { host: 'localhost' } });

    await registry.enqueueLedgerOperation(paymentJob as never);
    expect(ledger.add).toHaveBeenCalledWith(
      'payment',
      expect.objectContaining({ id: 'op-1', kind: 'payment' }),
      expect.objectContaining({ jobId: 'op-1' }),
    );
    expect(aggregation.add).not.toHaveBeenCalled();
  });

  it('rejects a ledger operation whose payload fails the Zod schema', async () => {
    const ledger = callableQueue();
    const aggregation = callableQueue();
    (Queue as unknown as ReturnType<typeof vi.fn>)
      .mockImplementationOnce(() => ledger)
      .mockImplementationOnce(() => aggregation);
    const registry = createQueueRegistry({ connection: { host: 'localhost' } });

    const bad = { ...paymentJob, amount: undefined } as never;
    await expect(
      registry.enqueueLedgerOperation(bad),
    ).rejects.toBeInstanceOf(QueueValidationError);
    expect(ledger.add).not.toHaveBeenCalled();
  });

  it('enqueues a validated aggregation job without a dedupe id', async () => {
    const ledger = callableQueue();
    const aggregation = callableQueue();
    (Queue as unknown as ReturnType<typeof vi.fn>)
      .mockImplementationOnce(() => ledger)
      .mockImplementationOnce(() => aggregation);
    const registry = createQueueRegistry({ connection: { host: 'localhost' } });

    const job = { kind: 'asset-flow', bucketStart: '2026-11-01T10:00:00Z', interval: '1h' };
    await registry.enqueueAggregation(job as never);
    expect(aggregation.add).toHaveBeenCalledWith(
      'asset-flow',
      expect.objectContaining({ kind: 'asset-flow' }),
    );
    // Third positional arg absent: no jobId => retries never dedupe.
    expect((aggregation.add.mock.calls[0] as unknown[]).length).toBe(2);
  });

  it('closes both queues on shutdown', async () => {
    const ledger = callableQueue();
    const aggregation = callableQueue();
    (Queue as unknown as ReturnType<typeof vi.fn>)
      .mockImplementationOnce(() => ledger)
      .mockImplementationOnce(() => aggregation);
    const registry = createQueueRegistry({ connection: { host: 'localhost' } });

    await registry.close();
    expect(ledger.close).toHaveBeenCalled();
    expect(aggregation.close).toHaveBeenCalled();
  });
});

describe('createBullMQOperationQueue', () => {
  it('satisfies the OperationQueue port for every payload kind', async () => {
    const queue = callableQueue();

    const offer = {
      kind: 'offer',
      id: 'op-3',
      cursor: '3-1',
      variant: 'sell',
      sellingAsset: { type: 'native', code: 'XLM' },
      buyingAsset: { type: 'credit_alphanum4', code: 'USDC', issuer: 'GISSUER' },
      amount: '5.0000000',
      priceRatio: { n: 1, d: 2 },
      offerId: '7',
      isDeletion: false,
    } as never;

    const cb = {
      kind: 'claimable_balance',
      id: 'op-4',
      cursor: '4-1',
      variant: 'create',
      asset: { type: 'native', code: 'XLM' },
      amount: '3.0000000',
      claimants: [{ destination: 'GCLAIMANT' }],
    } as never;

    const lp = {
      kind: 'liquidity_pool',
      id: 'op-5',
      cursor: '5-1',
      variant: 'deposit',
      poolId: 'abc',
      reserves: [],
      reserveBounds: [],
    } as never;

    const opQueue = createBullMQOperationQueue(queue as never);
    await opQueue.pushPayment(paymentJob as never);
    await opQueue.pushOffer(offer);
    await opQueue.pushClaimableBalance(cb);
    await opQueue.pushLiquidityPool(lp);

    expect(queue.add).toHaveBeenCalledTimes(4);
    expect(queue.add.mock.calls.map((c) => c[0])).toEqual([
      'payment',
      'offer',
      'claimable_balance',
      'liquidity_pool',
    ]);
  });

  it('throws on an invalid payload without enqueuing', async () => {
    const queue = callableQueue();
    const opQueue = createBullMQOperationQueue(queue as never);
    await expect(
      opQueue.pushPayment({ ...paymentJob, id: '' } as never),
    ).rejects.toBeInstanceOf(QueueValidationError);
    expect(queue.add).not.toHaveBeenCalled();
  });
});

describe('Zod schemas', () => {
  it('parses every ingestor payload shape through the discriminated union', () => {
    const pathPayment = {
      kind: 'path_payment',
      variant: 'strict-receive',
      id: 'op-2',
      cursor: '2-1',
      from: 'GA',
      to: 'GB',
      sendAsset: { type: 'native', code: 'XLM' },
      destinationAsset: { type: 'credit_alphanum4', code: 'USDC', issuer: 'GI' },
      corridor: { path: [], sameAnchor: false, crossIssuer: true },
    };
    expect(LedgerOperationJobSchema.safeParse(pathPayment).success).toBe(true);

    const job = {
      kind: 'account-flow',
      bucketStart: '2026-11-01T10:00:00Z',
      interval: '1m',
    };
    expect(AggregationJobSchema.safeParse(job).success).toBe(true);
  });

  it('rejects unknown kinds and malformed envelopes', () => {
    expect(
      LedgerOperationJobSchema.safeParse({ ...paymentJob, kind: 'mystery' }).success,
    ).toBe(false);
    expect(
      LedgerOperationJobSchema.safeParse({ ...paymentJob, cursor: '' }).success,
    ).toBe(false);
    expect(
      AggregationJobSchema.safeParse({ kind: 'asset-flow', interval: '3d' }).success,
    ).toBe(false);
  });
});
