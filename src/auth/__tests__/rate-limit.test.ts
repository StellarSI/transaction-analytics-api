import Fastify, { type FastifyInstance } from 'fastify';
import { describe, it, expect, afterEach } from 'vitest';
import { setErrorHandler } from '../../api/errors';
import {
  RATE_LIMIT_TIERS,
  RATE_LIMIT_WINDOW_SECONDS,
  createRateLimitGuard,
  createRedisRateLimiter,
  type RateLimitRedis,
} from '../rate-limit';
import type { ApiKeyTier } from '../api-keys';

const apps: FastifyInstance[] = [];

afterEach(async () => {
  await Promise.all(apps.splice(0).map((app) => app.close()));
});

interface FakeEntry {
  value: number;
  ttl: number;
}

/** Minimal in-memory stand-in for the Redis counters. */
function createFakeRedis(): RateLimitRedis & { store: Map<string, FakeEntry> } {
  const store = new Map<string, FakeEntry>();

  return {
    store,
    incr(key: string) {
      const entry = store.get(key) ?? { value: 0, ttl: -1 };
      entry.value += 1;
      store.set(key, entry);
      return Promise.resolve(entry.value);
    },
    expire(key: string, seconds: number, mode?: 'NX') {
      const entry = store.get(key);
      if (!entry) return Promise.resolve(0);
      if (mode === 'NX' && entry.ttl !== -1) return Promise.resolve(0);
      entry.ttl = seconds;
      return Promise.resolve(1);
    },
  };
}

describe('rate limit tiers (issue #31)', () => {
  it('budgets 100, 1000 and 10000 requests per minute', () => {
    expect(RATE_LIMIT_TIERS.free).toBe(100);
    expect(RATE_LIMIT_TIERS.pro).toBe(1_000);
    expect(RATE_LIMIT_TIERS.enterprise).toBe(10_000);
  });

  it('uses a one minute window', () => {
    expect(RATE_LIMIT_WINDOW_SECONDS).toBe(60);
  });
});

describe('createRedisRateLimiter', () => {
  it('counts the requests of a key against its tier budget', async () => {
    const redis = createFakeRedis();
    const limiter = createRedisRateLimiter(redis);
    const now = 1_700_000_000_000;

    const first = await limiter.consume('key-1', 'free', now);
    const second = await limiter.consume('key-1', 'free', now);

    expect(first.limit).toBe(100);
    expect(first.remaining).toBe(99);
    expect(first.allowed).toBe(true);
    expect(second.remaining).toBe(98);
  });

  it('counts every key separately', async () => {
    const redis = createFakeRedis();
    const limiter = createRedisRateLimiter(redis);
    const now = 1_700_000_000_000;

    await limiter.consume('key-1', 'free', now);
    const other = await limiter.consume('key-2', 'free', now);

    expect(other.remaining).toBe(99);
  });

  it('starts a new budget in the next window', async () => {
    const redis = createFakeRedis();
    const limiter = createRedisRateLimiter(redis);
    const now = 1_700_000_000_000;

    await limiter.consume('key-1', 'free', now);
    const nextWindow = await limiter.consume('key-1', 'free', now + 60_000);

    expect(nextWindow.remaining).toBe(99);
    expect([...redis.store.keys()]).toHaveLength(2);
  });

  it('reports when the window resets', async () => {
    const redis = createFakeRedis();
    const limiter = createRedisRateLimiter(redis);
    const now = 1_700_000_000_000;

    const decision = await limiter.consume('key-1', 'pro', now);

    expect(decision.resetAt).toBe(Math.ceil((now + 60_000) / 1000));
    expect(decision.retryAfter).toBeGreaterThan(0);
  });

  it('sets an expiry on the counter key', async () => {
    const redis = createFakeRedis();
    const limiter = createRedisRateLimiter(redis);

    await limiter.consume('key-1', 'free', 1_700_000_000_000);

    const entry = [...redis.store.values()][0];
    expect(entry.ttl).toBe(RATE_LIMIT_WINDOW_SECONDS);
  });
});

describe('rate limit guard (issue #31)', () => {
  async function buildApp(
    tier: ApiKeyTier,
    limiter: Parameters<typeof createRateLimitGuard>[0]['limiter'],
  ): Promise<FastifyInstance> {
    const app = Fastify({ logger: false });
    setErrorHandler(app);
    const guard = createRateLimitGuard({ limiter });

    app.get('/limited', async (request, reply) => {
      request.apiKey = {
        id: 'key-1',
        name: 'test',
        permissions: [],
        tier,
      };
      await guard(request, reply);
      return { ok: true };
    });

    apps.push(app);
    return app;
  }

  it('sets the rate limit headers on an allowed request', async () => {
    const redis = createFakeRedis();
    const app = await buildApp('free', createRedisRateLimiter(redis));

    const response = await app.inject({ method: 'GET', url: '/limited' });

    expect(response.statusCode).toBe(200);
    expect(response.headers['x-ratelimit-limit']).toBe('100');
    expect(response.headers['x-ratelimit-remaining']).toBe('99');
    expect(Number(response.headers['x-ratelimit-reset'])).toBeGreaterThan(0);
  });

  it('rejects a request that exhausted its budget with 429', async () => {
    const limiter = {
      consume: () =>
        Promise.resolve({
          allowed: false,
          limit: 100,
          remaining: 0,
          resetAt: 1_700_000_060,
          retryAfter: 42,
        }),
    };
    const app = await buildApp('free', limiter);

    const response = await app.inject({ method: 'GET', url: '/limited' });

    expect(response.statusCode).toBe(429);
    expect(response.headers['retry-after']).toBe('42');
    expect(response.headers['x-ratelimit-remaining']).toBe('0');
    expect(response.json()).toMatchObject({ code: 'rate_limit' });
  });

  it('does not count requests without an API key', async () => {
    const redis = createFakeRedis();
    const limiter = createRedisRateLimiter(redis);
    const app = Fastify({ logger: false });
    setErrorHandler(app);
    const guard = createRateLimitGuard({ limiter });
    app.get('/open', async (request, reply) => {
      await guard(request, reply);
      return { ok: true };
    });
    apps.push(app);

    const response = await app.inject({ method: 'GET', url: '/open' });

    expect(response.statusCode).toBe(200);
    expect(redis.store.size).toBe(0);
  });
});
