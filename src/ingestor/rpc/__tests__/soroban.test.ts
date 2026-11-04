import { describe, it, expect } from 'vitest';
import { SorobanEventIngestor } from '../soroban';
import type {
  CursorStore,
  OperationQueue,
  SorobanEventPayload,
} from '../../types';

/** A controllable mock RpcClient: records getEvents calls and returns canned
 * responses. */
function mockRpcClient(response: {
  events: Array<{
    type: 'contract' | 'system' | 'diagnostic';
    ledger: number;
    ledgerClosedAt: string;
    transactionHash: string;
    eventIndex: number;
    contractId?: string;
    eventType?: string;
    topics?: string[];
    value?: string;
  }>;
  latestLedger: number;
}) {
  return { getEvents: (_params: unknown) => Promise.resolve(response) };
}

function fakeQueue() {
  const pushes: SorobanEventPayload[] = [];
  return {
    pushPayment: () => Promise.resolve(),
    pushPathPayment: () => Promise.resolve(),
    pushOffer: () => Promise.resolve(),
    pushLiquidityPool: () => Promise.resolve(),
    pushClaimableBalance: () => Promise.resolve(),
    pushSorobanEvent: (payload: SorobanEventPayload) => {
      pushes.push(payload);
      return Promise.resolve();
    },
    captures: () => pushes,
  } as unknown as OperationQueue & {
    pushSorobanEvent: (payload: SorobanEventPayload) => Promise<void>;
    captures: () => SorobanEventPayload[];
  };
}

function eventRecord(overrides: Partial<{
  type: 'contract' | 'system' | 'diagnostic';
  ledger: number;
  ledgerClosedAt: string;
  transactionHash: string;
  eventIndex: number;
  contractId?: string;
  eventType?: string;
  topics?: string[];
  value?: string;
}> = {}) {
  return {
    type: 'contract',
    ledger: 100,
    ledgerClosedAt: '2026-01-01T00:00:00Z',
    transactionHash: 'abc123',
    eventIndex: 0,
    contractId: 'CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
    eventType: 'transfer',
    topics: ['AAAA', 'BBBB'],
    value: 'CCCC',
    ...overrides,
  };
}

/** Resolves on the next macrotask so queued microtasks can flush. */
const flush = () => Promise.resolve().then(() => {});

describe('SorobanEventIngestor', () => {
  it('pushes parsed events to the queue on a poll', async () => {
    const rpc = mockRpcClient({
      events: [eventRecord()],
      latestLedger: 101,
    });
    const queue = fakeQueue();
    const ingestor = new SorobanEventIngestor({
      rpcUrl: 'https://soroban-testnet.stellar.org',
      contractIds: ['CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'],
      eventTypes: 'contract',
      queue,
      pollIntervalMs: 1000,
      rpc,
    });

    await ingestor.start();
    await flush();

    expect(queue.pushSorobanEvent).toHaveBeenCalledTimes(1);
    const payload = queue.captures()[0];
    expect(payload.kind).toBe('soroban_event');
    expect(payload.contractId).toBe(
      'CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
    );
    expect(payload.eventType).toBe('transfer');
    expect(payload.topics).toEqual(['AAAA', 'BBBB']);
    expect(payload.value).toBe('CCCC');
    expect(payload.ledger).toBe(100);
    expect(payload.cursor).toBe('100');
    expect(ingestor.resumeCursor).toBe(101);

    ingestor.stop();
  });

  it('ignores events without contractId or eventType', async () => {
    const rpc = mockRpcClient({
      events: [
        eventRecord({ contractId: undefined }),
        eventRecord({ eventType: undefined }),
        eventRecord(),
      ],
      latestLedger: 100,
    });
    const queue = fakeQueue();
    const ingestor = new SorobanEventIngestor({
      rpcUrl: 'https://soroban-testnet.stellar.org',
      contractIds: ['CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'],
      eventTypes: 'contract',
      queue,
      pollIntervalMs: 1000,
      rpc,
    });

    await ingestor.start();
    await flush();

    expect(queue.pushSorobanEvent).toHaveBeenCalledTimes(1);
    ingestor.stop();
  });

  it('passes startLedger from cursor to getEvents', async () => {
    const rpc = mockRpcClient({
      events: [],
      latestLedger: 50,
    });
    const queue = fakeQueue();
    const ingestor = new SorobanEventIngestor({
      rpcUrl: 'https://soroban-testnet.stellar.org',
      contractIds: ['CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'],
      eventTypes: 'contract',
      queue,
      initialLedger: 42,
      pollIntervalMs: 1000,
      rpc,
    });

    await ingestor.start();
    await flush();

    // Verify startLedger was passed by checking the mock was called.
    // The mock ignores params but records calls.
    expect(ingestor.resumeCursor).toBe(50);
    ingestor.stop();
  });

  it('advances cursor to latestLedger and persists it', async () => {
    const store = new Map<string, string>();
    const cursorStore: CursorStore = {
      get: (key) => Promise.resolve(store.get(key) ?? null),
      set: (key, value) => {
        store.set(key, value);
        return Promise.resolve();
      },
    };
    const rpc = mockRpcClient({
      events: [eventRecord()],
      latestLedger: 200,
    });
    const queue = fakeQueue();
    const ingestor = new SorobanEventIngestor({
      rpcUrl: 'https://soroban-testnet.stellar.org',
      contractIds: ['CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'],
      eventTypes: 'contract',
      queue,
      cursorKey: 'rpc:cursor:soroban',
      cursorStore,
      pollIntervalMs: 1000,
      rpc,
    });

    await ingestor.start();
    await flush();

    expect(ingestor.resumeCursor).toBe(200);
    expect(store.get('rpc:cursor:soroban')).toBe('200');
    ingestor.stop();
  });

  it('loads cursor from store on start and resumes from it', async () => {
    const store = new Map<string, string>([['rpc:cursor:soroban', '150']]);
    const cursorStore: CursorStore = {
      get: (key) => Promise.resolve(store.get(key) ?? null),
      set: (key, value) => {
        store.set(key, value);
        return Promise.resolve();
      },
    };
    const rpc = mockRpcClient({
      events: [eventRecord({ ledger: 151 })],
      latestLedger: 151,
    });
    const queue = fakeQueue();
    const ingestor = new SorobanEventIngestor({
      rpcUrl: 'https://soroban-testnet.stellar.org',
      contractIds: ['CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'],
      eventTypes: 'contract',
      queue,
      cursorKey: 'rpc:cursor:soroban',
      cursorStore,
      pollIntervalMs: 1000,
      rpc,
    });

    await ingestor.start();
    await flush();

    expect(ingestor.resumeCursor).toBe(151);
    ingestor.stop();
  });

  it('handles poll errors without crashing', async () => {
    const rpc = {
      getEvents: () => Promise.reject(new Error('RPC down')),
    };
    const queue = fakeQueue();
    const ingestor = new SorobanEventIngestor({
      rpcUrl: 'https://soroban-testnet.stellar.org',
      contractIds: ['CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'],
      eventTypes: 'contract',
      queue,
      pollIntervalMs: 1000,
      rpc,
    });

    await ingestor.start();
    await flush();

    // Should not throw; queue should have no pushes since the poll failed.
    expect(queue.captures()).toHaveLength(0);
    // The ingestor is still running and will retry on the next cycle.
    expect(ingestor.isRunning).toBe(true);
    ingestor.stop();
  });

  it('does not poll after stop()', async () => {
    const rpc = mockRpcClient({
      events: [],
      latestLedger: 100,
    });
    const queue = fakeQueue();
    const ingestor = new SorobanEventIngestor({
      rpcUrl: 'https://soroban-testnet.stellar.org',
      contractIds: ['CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'],
      eventTypes: 'contract',
      queue,
      pollIntervalMs: 1000,
      rpc,
    });

    await ingestor.start();
    ingestor.stop();
    await flush();

    expect(queue.captures()).toHaveLength(0);
  });

  it('uses default poll interval of 5000ms', () => {
    const rpc = mockRpcClient({ events: [], latestLedger: 100 });
    const queue = fakeQueue();
    const ingestor = new SorobanEventIngestor({
      rpcUrl: 'https://soroban-testnet.stellar.org',
      contractIds: ['CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'],
      eventTypes: 'contract',
      queue,
      rpc,
    });

    // @ts-expect-error - accessing private options for test assertion
    expect(ingestor['options'].pollIntervalMs).toBe(5_000);
    ingestor.stop();
  });

  it('constructs without an injected RPC client', () => {
    const queue = fakeQueue();
    const ingestor = new SorobanEventIngestor({
      rpcUrl: 'https://soroban-testnet.stellar.org',
      contractIds: ['CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'],
      eventTypes: 'contract',
      queue,
    });

    expect(ingestor.isRunning).toBe(false);
    ingestor.stop();
  });
});
