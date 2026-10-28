import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// Structural view of the mocked client: only the members the tests touch.
type FakeRedis = {
  url: string;
  opts: Record<string, unknown>;
  listeners: Map<string, Array<(...args: unknown[]) => void>>;
  quit: ReturnType<typeof vi.fn>;
  disconnect: ReturnType<typeof vi.fn>;
  emit: (event: string, ...args: unknown[]) => void;
};

// `vi.hoisted` runs before the mock factory, giving both the factory and the
// test body a shared place to record constructed instances.
const { instances } = vi.hoisted(() => ({ instances: [] as FakeRedis[] }));

// Mock ioredis so no socket is opened; every constructed client is recorded.
vi.mock('ioredis', () => {
  class FakeRedis {
    url: string;
    opts: Record<string, unknown>;
    listeners = new Map<string, Array<(...args: unknown[]) => void>>();
    quit = vi.fn().mockResolvedValue('OK');
    disconnect = vi.fn();

    constructor(url: string, opts: Record<string, unknown>) {
      this.url = url;
      this.opts = opts;
      instances.push(this);
    }

    on(event: string, cb: (...args: unknown[]) => void) {
      const list = this.listeners.get(event) ?? [];
      list.push(cb);
      this.listeners.set(event, list);
      return this;
    }

    emit(event: string, ...args: unknown[]) {
      for (const cb of this.listeners.get(event) ?? []) cb(...args);
    }
  }
  return { default: FakeRedis };
});

const {
  createRedisClient,
  getRedis,
  closeRedis,
  DEFAULT_RECONNECT_MAX_DELAY_MS,
} = await import('../client');

const asFake = (client: unknown): FakeRedis => client as FakeRedis;

beforeEach(async () => {
  instances.length = 0;
  await closeRedis(); // reset the singleton between tests
});

afterEach(async () => {
  await closeRedis();
});

describe('createRedisClient', () => {
  it('connects using REDIS_URL by default', () => {
    const client = asFake(createRedisClient());
    expect(client.url).toBe(process.env.REDIS_URL);
  });

  it('honours an explicit url override', () => {
    const client = asFake(createRedisClient({ url: 'redis://example:6380' }));
    expect(client.url).toBe('redis://example:6380');
  });

  it('caps the reconnection backoff at the maximum delay', () => {
    const client = asFake(createRedisClient());
    const retryStrategy = client.opts.retryStrategy as (times: number) => number;
    expect(retryStrategy(1)).toBeLessThanOrEqual(DEFAULT_RECONNECT_MAX_DELAY_MS);
    expect(retryStrategy(1000)).toBe(DEFAULT_RECONNECT_MAX_DELAY_MS);
    expect(retryStrategy(1000)).toBeGreaterThanOrEqual(retryStrategy(1));
  });

  it('limits queued commands issued while disconnected', () => {
    const client = asFake(createRedisClient());
    expect(client.opts.maxRetriesPerRequest).toBe(3);
  });
});

describe('attachConnectionLogging', () => {
  it('subscribes to lifecycle events', () => {
    const client = asFake(createRedisClient());
    for (const event of ['connect', 'ready', 'error', 'close', 'reconnecting', 'end']) {
      expect(client.listeners.get(event)?.length).toBeGreaterThan(0);
    }
  });

  it('does not throw when an error event fires', () => {
    const client = asFake(createRedisClient());
    expect(() => client.emit('error', new Error('boom'))).not.toThrow();
  });
});

describe('getRedis / closeRedis', () => {
  it('returns the same singleton on repeated calls', () => {
    const a = getRedis();
    const b = getRedis();
    expect(a).toBe(b);
    expect(instances).toHaveLength(1);
  });

  it('closes the connection gracefully on closeRedis', async () => {
    const client = asFake(getRedis());
    await closeRedis();
    expect(client.quit).toHaveBeenCalledTimes(1);
  });

  it('is safe to call when no client was ever created', async () => {
    await expect(closeRedis()).resolves.toBeUndefined();
  });

  it('reconnects with a fresh client after closeRedis', async () => {
    const first = getRedis();
    await closeRedis();
    const second = getRedis();
    expect(second).not.toBe(first);
    expect(instances).toHaveLength(2);
  });

  it('falls back to a hard disconnect if quit fails', async () => {
    const client = asFake(getRedis());
    client.quit.mockRejectedValue(new Error('already closed'));
    await closeRedis();
    expect(client.disconnect).toHaveBeenCalledTimes(1);
  });
});
