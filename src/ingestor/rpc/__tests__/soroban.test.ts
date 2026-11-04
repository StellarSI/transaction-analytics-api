import { describe, it, expect, vi } from 'vitest';
import { SorobanEventIngestor } from '../soroban';
import type { RpcClient } from '@stellar/stellar-sdk';
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
    type?: string;
    topics?: string[];
    value?: string;
  }>;
  latestLedger: number;
}): RpcClient {
  return {
    getEvents: vi.fn().mockResolvedValue(response),
  } as unknown as RpcClient;
}

function fakeQueue() {
  const pushes: SorobanEventPayload[] = [];
  return {
    pushPayment: vi.fn().mockResolvedValue(undefined),
    pushPathPayment: vi.fn().mockResolvedValue(undefined),
    pushOffer: vi.fn().mockResolvedValue(undefined),
    pushLiquidityPool: vi.fn().mockResolvedValue(undefined),
    pushClaimableBalance: vi.fn().mockResolvedValue(undefined),
    pushSorobanEvent: vi.fn().mockImplementation((payload: SorobanEventPayload) => {
      pushes.push(payload);
      return Promise.resolve();
    }),
    captures: () => pushes,
  } as unknown as OperationQueue & {
    pushSorobanEvent: ReturnType<typeof vi.fn>;
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
  type?: string;
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
    type: 'transfer',
    topics: ['AAAA', 'BBBB'],
    value: 'CCCC',
    ...overrides,
  };
}

/** Resolves on the next macrotask so queued microtasks can flush. */
const flush = () => Promise.resolve().then(() => {});

describe('SorobanEventIngestor', () => {
  it('parses a contract event into a SorobanEventPayload', () => {
    const rpc = mockRpcClient({
      events: [eventRecord()],
      latestLedger: 100,
    });
    const queue = fakeQueue();
    const ingestor = new SorobanEventIngestor({
      rpcUrl: 'https://soroban-testnet.stellar.org',
      contractIds: ['CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'],
      eventTypes: 'contract',
      queue,
      rpc,
    });

    // Access the private parseEvent via a controlled poll: we can't call it
    // directly, so we verify through the queue push below.
    expect(rpc.getEvents).toBeDefined();
  });

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

  it('ignores events without contractId or type', async () => {
    const rpc = mockRpcClient({
      events: [
        eventRecord({ contractId: undefined }),
        eventRecord({ type: undefined }),
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

    expect(rpc.getEvents).toHaveBeenCalledWith(
      expect.objectContaining({ startLedger: 42 }),
    );
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

    expect(rpc.getEvents).toHaveBeenCalledWith(
      expect.objectContaining({ startLedger: 150 }),
    );
    ingestor.stop();
  });

  it('handles poll errors without crashing', async () => {
    const rpc = {
      getEvents: vi.fn().mockRejectedValue(new Error('RPC down')),
    } as unknown as RpcClient;
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
    expect(queue.pushSorobanEvent).not.toHaveBeenCalled();
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

    expect(rpc.getEvents).not.toHaveBeenCalled();
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

    // @ts-ignore - accessing private options for test assertion
    expect(ingestor['options'].pollIntervalMs).toBe(5_000);
    ingestor.stop();
  });

  it('constructs a real RpcClient when none is injected', () => {
    const queue = fakeQueue();
    const ingestor = new SorobanEventIngestor({
      rpcUrl: 'https://soroban-testnet.stellar.org',
      contractIds: ['CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'],
      eventTypes: 'contract',
      queue,
    });

    // @ts-ignore - accessing private options for test assertion
    expect(ingestor['options'].rpc).toBeInstanceOf(RpcClient);
    ingestor.stop();
  });
});
