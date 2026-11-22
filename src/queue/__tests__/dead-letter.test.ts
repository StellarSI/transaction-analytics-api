import { describe, it, expect, vi, beforeEach } from 'vitest';

/**
 * Unit tests for Dead Letter Queue (DLQ) manager (Issue #98).
 * Mocked BullMQ to avoid real Redis dependencies and guarantee deterministic execution.
 */

interface MockJobRecord {
  id: string;
  name: string;
  data: {
    metadata?: {
      originalQueue: string;
      originalJobId?: string;
      originalJobName?: string;
      failedAt: string;
      attemptsMade: number;
      errorMessage: string;
      errorStack?: string;
      failureReason?: string;
    };
    payload?: unknown;
  };
  timestamp: number;
  remove: () => Promise<void>;
}

const mockJobs: Map<string, MockJobRecord> = new Map();
let jobCounter = 1;

const mockQueueInstance = {
  name: 'dead-letter-queue',
  add: vi.fn().mockImplementation((name: string, data: MockJobRecord['data'], opts?: { jobId?: string }) => {
    const id = opts?.jobId ?? `dlq_job_${jobCounter++}`;
    const job: MockJobRecord = {
      id,
      name,
      data,
      timestamp: Date.now(),
      remove: vi.fn().mockImplementation(() => {
        mockJobs.delete(id);
        return Promise.resolve();
      }),
    };
    mockJobs.set(id, job);
    return Promise.resolve(job);
  }),
  getJob: vi.fn().mockImplementation((id: string) => {
    return Promise.resolve(mockJobs.get(id) ?? null);
  }),
  getJobs: vi.fn().mockImplementation(() => {
    return Promise.resolve(Array.from(mockJobs.values()));
  }),
  getJobCounts: vi.fn().mockImplementation(() => {
    return Promise.resolve({
      waiting: mockJobs.size,
      delayed: 0,
      completed: 0,
      failed: 0,
    });
  }),
  drain: vi.fn().mockImplementation(() => {
    mockJobs.clear();
    return Promise.resolve();
  }),
  clean: vi.fn().mockResolvedValue([]),
  close: vi.fn().mockResolvedValue(undefined),
};

vi.mock('bullmq', () => ({
  Queue: vi.fn().mockImplementation(() => mockQueueInstance),
}));

// Mock logger to avoid environment issues during testing
vi.mock('../../logger', () => ({
  logger: {
    warn: vi.fn(),
    info: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
  },
}));

const {
  createDeadLetterQueueManager,
  DEAD_LETTER_QUEUE,
  DEFAULT_DLQ_ALERT_THRESHOLD,
  DEFAULT_DLQ_RETENTION_MS,
  FailedMessageMetadataSchema,
} = await import('../dead-letter');

describe('Dead Letter Queue Manager (Issue #98)', () => {
  beforeEach(() => {
    mockJobs.clear();
    jobCounter = 1;
    vi.clearAllMocks();
  });

  it('exports valid DLQ constants and default configurations', () => {
    expect(DEAD_LETTER_QUEUE).toBe('dead-letter-queue');
    expect(DEFAULT_DLQ_RETENTION_MS).toBe(30 * 24 * 60 * 60 * 1000); // 30 days
    expect(DEFAULT_DLQ_ALERT_THRESHOLD).toBe(100);
  });

  it('validates FailedMessageMetadataSchema with strict RFC formats', () => {
    const validMeta = {
      originalQueue: 'ledger-operations',
      originalJobId: 'job-123',
      originalJobName: 'payment',
      failedAt: new Date().toISOString(),
      attemptsMade: 3,
      errorMessage: 'Database connection timeout',
      errorStack: 'Error: timeout\n at pool.ts:10',
      failureReason: 'timeout',
    };

    const parsed = FailedMessageMetadataSchema.safeParse(validMeta);
    expect(parsed.success).toBe(true);
  });

  it('routes failed job to dead letter queue with enriched diagnostics', async () => {
    const manager = createDeadLetterQueueManager();
    const payload = { opId: '123', amount: '100' };
    const testError = new Error('Ledger processing deserialization failed');

    const id = await manager.sendToDeadLetter('ledger-operations', payload, testError, {
      jobId: 'ledger-op-99',
      jobName: 'payment',
      attemptsMade: 3,
      failureReason: 'Deserialization error',
    });

    expect(id).toBeDefined();
    expect(mockQueueInstance.add).toHaveBeenCalledTimes(1);

    const stored = await manager.getDeadLetter(id);
    expect(stored).not.toBeNull();
    expect(stored?.metadata.originalQueue).toBe('ledger-operations');
    expect(stored?.metadata.errorMessage).toBe('Ledger processing deserialization failed');
    expect(stored?.metadata.failureReason).toBe('Deserialization error');
    expect(stored?.payload).toEqual(payload);
  });

  it('handles string errors cleanly during sendToDeadLetter', async () => {
    const manager = createDeadLetterQueueManager();
    const id = await manager.sendToDeadLetter('aggregation-jobs', { bucket: '1h' }, 'Stream closed');

    const stored = await manager.getDeadLetter(id);
    expect(stored?.metadata.errorMessage).toBe('Stream closed');
    expect(stored?.metadata.errorStack).toBeUndefined();
  });

  it('accurately lists and filters DLQ items', async () => {
    const manager = createDeadLetterQueueManager();

    await manager.sendToDeadLetter('ledger-operations', { id: 1 }, 'error 1');
    await manager.sendToDeadLetter('aggregation-jobs', { id: 2 }, 'error 2');

    const allItems = await manager.listDeadLetters();
    expect(allItems).toHaveLength(2);

    const filtered = await manager.listDeadLetters({ originalQueue: 'ledger-operations' });
    expect(filtered).toHaveLength(1);
    expect(filtered[0]?.metadata.originalQueue).toBe('ledger-operations');
  });

  it('evaluates alert threshold triggers when queue size exceeds bounds', async () => {
    const manager = createDeadLetterQueueManager({ alertThreshold: 2 });

    let alert = await manager.checkAlertThreshold();
    expect(alert.alert).toBe(false);
    expect(alert.size).toBe(0);

    await manager.sendToDeadLetter('ledger-operations', { id: 1 }, 'err');
    await manager.sendToDeadLetter('ledger-operations', { id: 2 }, 'err');

    alert = await manager.checkAlertThreshold();
    expect(alert.alert).toBe(true);
    expect(alert.size).toBe(2);
    expect(alert.message).toContain('WARNING: Dead Letter Queue size');
  });

  it('replays a dead letter job back to destination queue and purges from DLQ', async () => {
    const manager = createDeadLetterQueueManager();
    const mockTargetQueue = {
      name: 'ledger-operations',
      add: vi.fn().mockResolvedValue({ id: 'replayed-job-777' }),
    };

    const dlqId = await manager.sendToDeadLetter(
      'ledger-operations',
      { data: 'stellar-tx' },
      'Transient connection failure',
      { jobName: 'payment' },
    );

    const replayResult = await manager.replayDeadLetter(dlqId, mockTargetQueue);

    expect(replayResult.replayed).toBe(true);
    expect(replayResult.targetQueue).toBe('ledger-operations');
    expect(replayResult.jobId).toBe('replayed-job-777');
    expect(mockTargetQueue.add).toHaveBeenCalledWith('payment', { data: 'stellar-tx' });

    // Item should be deleted from DLQ after replay
    const remaining = await manager.getDeadLetter(dlqId);
    expect(remaining).toBeNull();
  });

  it('throws error when replaying if no target queue instance is provided', async () => {
    const manager = createDeadLetterQueueManager();
    const id = await manager.sendToDeadLetter('ledger-operations', {}, 'err');

    await expect(manager.replayDeadLetter(id, undefined)).rejects.toThrow(
      /Cannot replay DLQ job/,
    );
  });

  it('deletes and purges items on demand', async () => {
    const manager = createDeadLetterQueueManager();
    const id = await manager.sendToDeadLetter('ledger-operations', {}, 'err');

    expect(await manager.getDeadLetterCount()).toBe(1);

    const deleted = await manager.deleteDeadLetter(id);
    expect(deleted).toBe(true);
    expect(await manager.getDeadLetter(id)).toBeNull();

    await manager.sendToDeadLetter('ledger-operations', {}, 'err 2');
    await manager.purgeDeadLetters();
    expect(mockQueueInstance.drain).toHaveBeenCalled();
  });
});
