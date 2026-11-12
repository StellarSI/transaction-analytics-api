import { describe, it, expect, afterEach } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { createApp } from '../server';
import type {
  CreateSubscriptionInput,
  Subscription,
  SubscriptionStore,
} from '../../api/repository/subscriptions';

const apps: FastifyInstance[] = [];

afterEach(async () => {
  await Promise.all(apps.splice(0).map((app) => app.close()));
});

function makeStore(
  overrides: Partial<{ createSubscription: SubscriptionStore['createSubscription'] }> = {},
) {
  const calls: CreateSubscriptionInput[] = [];
  const store: SubscriptionStore = {
    createSubscription: async (input) => {
      calls.push(input);
      const subscription: Subscription = {
        id: '2f9d6f66-83fe-4de7-9c3b-9c21d0b16b6d',
        apiKeyId: input.apiKeyId,
        targetType: input.targetType,
        targetValue: input.targetValue,
        url: input.url,
        secret: input.secret,
        events: input.events ?? [],
        active: true,
        createdAt: '2026-11-12T00:00:00.000Z',
        updatedAt: '2026-11-12T00:00:00.000Z',
      };
      return overrides.createSubscription ? overrides.createSubscription(input) : subscription;
    },
  };
  return { store, calls };
}

const account = 'GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN';

function postBody(overrides: Record<string, unknown> = {}) {
  return {
    target_type: 'account',
    target_value: account,
    url: 'https://example.com/webhook',
    ...overrides,
  };
}

async function buildApp(overrides: { store?: SubscriptionStore } = {}) {
  const app = await createApp({
    logger: false,
    checkDatabase: () => Promise.resolve({ ok: true, latencyMs: 1, detail: 'postgres reachable' }),
    checkRedis: () => Promise.resolve({ ok: true, latencyMs: 1, detail: 'redis reachable' }),
    checkHorizon: () =>
      Promise.resolve({
        ok: true,
        latencyMs: 1,
        detail: 'horizon reachable',
        latest_ledger: 100,
      }),
    checkRpc: () =>
      Promise.resolve({
        ok: true,
        latencyMs: 1,
        detail: 'stellar rpc reachable',
        latest_ledger: 100,
      }),
    getLatestLedger: () => Promise.resolve(100),
    ...(overrides.store ? { store: overrides.store } : {}),
  });
  apps.push(app);
  return app;
}

describe('POST /v1/subscriptions (issue #28)', () => {
  it('registers a webhook and returns the subscription id', async () => {
    const { store, calls } = makeStore();
    const app = await buildApp({ store });

    const response = await app.inject({
      method: 'POST',
      url: '/v1/subscriptions',
      headers: { authorization: `Bearer ${'a'.repeat(64)}` },
      payload: postBody({ secret: 'a-very-long-and-secret-value', events: ['account_credited'] }),
    });

    expect(response.statusCode).toBe(201);
    expect(response.json()).toEqual({
      id: '2f9d6f66-83fe-4de7-9c3b-9c21d0b16b6d',
      target_type: 'account',
      target_value: account,
      url: 'https://example.com/webhook',
      events: ['account_credited'],
      created_at: '2026-11-12T00:00:00.000Z',
    });

    expect(calls).toHaveLength(1);
    expect(calls[0]).toMatchObject({
      targetType: 'account',
      targetValue: account,
      url: 'https://example.com/webhook',
      secret: 'a-very-long-and-secret-value',
      events: ['account_credited'],
    });
    // Salted digest of the bearer token, never the token itself.
    expect(calls[0].apiKeyId).toMatch(/^[0-9a-f]{64}$/);
  });

  it('never echoes the secret back in the response', async () => {
    const { store } = makeStore();
    const app = await buildApp({ store });

    const response = await app.inject({
      method: 'POST',
      url: '/v1/subscriptions',
      headers: { authorization: `Bearer ${'b'.repeat(64)}` },
      payload: postBody({ secret: 'another-very-secret-value' }),
    });

    expect(response.statusCode).toBe(201);
    expect(JSON.stringify(response.json())).not.toContain('another-very-secret-value');
  });

  it('namespaces subscriptions per API key', async () => {
    const { store, calls } = makeStore();
    const app = await buildApp({ store });

    for (const token of ['first-token-'.padEnd(32, 'x'), 'second-token-'.padEnd(32, 'y')]) {
      const response = await app.inject({
        method: 'POST',
        url: '/v1/subscriptions',
        headers: { authorization: `Bearer ${token}` },
        payload: postBody(),
      });
      expect(response.statusCode).toBe(201);
    }

    expect(calls[0].apiKeyId).not.toBe(calls[1].apiKeyId);
  });

  it('rejects requests without an API key', async () => {
    const { store, calls } = makeStore();
    const app = await buildApp({ store });

    const response = await app.inject({
      method: 'POST',
      url: '/v1/subscriptions',
      payload: postBody(),
    });

    expect(response.statusCode).toBe(401);
    expect(response.headers['content-type']).toContain('application/problem+json');
    expect(calls).toHaveLength(0);
  });

  it('rejects a malformed bearer token', async () => {
    const { store, calls } = makeStore();
    const app = await buildApp({ store });

    const response = await app.inject({
      method: 'POST',
      url: '/v1/subscriptions',
      headers: { authorization: 'Bearer short' },
      payload: postBody(),
    });

    expect(response.statusCode).toBe(401);
    expect(calls).toHaveLength(0);
  });

  it('accepts an asset and a threshold as targets', async () => {
    const { store, calls } = makeStore();
    const app = await buildApp({ store });

    for (const payload of [
      postBody({ target_type: 'asset', target_value: 'USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN' }),
      postBody({ target_type: 'asset', target_value: 'XLM' }),
      postBody({ target_type: 'threshold', target_value: '1000000.5' }),
    ]) {
      const response = await app.inject({
        method: 'POST',
        url: '/v1/subscriptions',
        headers: { authorization: `Bearer ${'c'.repeat(64)}` },
        payload,
      });
      expect(response.statusCode).toBe(201);
    }

    expect(calls.map((call) => call.targetType)).toEqual(['asset', 'asset', 'threshold']);
  });

  it('rejects an account id that is not a Stellar account (G...)', async () => {
    const { store, calls } = makeStore();
    const app = await buildApp({ store });

    const response = await app.inject({
      method: 'POST',
      url: '/v1/subscriptions',
      headers: { authorization: `Bearer ${'d'.repeat(64)}` },
      payload: postBody({ target_value: 'not-an-account' }),
    });

    expect(response.statusCode).toBe(400);
    expect(response.json()).toMatchObject({ status: 400, code: 'validation_error' });
    expect(calls).toHaveLength(0);
  });

  it('rejects non-https webhook URLs', async () => {
    const { store, calls } = makeStore();
    const app = await buildApp({ store });

    for (const url of ['http://example.com/webhook', 'ftp://example.com/webhook', 'not a url']) {
      const response = await app.inject({
        method: 'POST',
        url: '/v1/subscriptions',
        headers: { authorization: `Bearer ${'e'.repeat(64)}` },
        payload: postBody({ url }),
      });
      expect(response.statusCode).toBe(400);
    }

    expect(calls).toHaveLength(0);
  });

  it('rejects a threshold that is not a positive decimal', async () => {
    const { store, calls } = makeStore();
    const app = await buildApp({ store });

    for (const targetValue of ['-1', 'abc', '1.12345678']) {
      const response = await app.inject({
        method: 'POST',
        url: '/v1/subscriptions',
        headers: { authorization: `Bearer ${'f'.repeat(64)}` },
        payload: postBody({ target_type: 'threshold', target_value: targetValue }),
      });
      expect(response.statusCode).toBe(400);
    }

    expect(calls).toHaveLength(0);
  });

  it('rejects an unknown target type', async () => {
    const { store } = makeStore();
    const app = await buildApp({ store });

    const response = await app.inject({
      method: 'POST',
      url: '/v1/subscriptions',
      headers: { authorization: `Bearer ${'g'.repeat(64)}` },
      payload: postBody({ target_type: 'market' }),
    });

    expect(response.statusCode).toBe(400);
    expect(response.json().code).toBe('validation_error');
  });

  it('rejects a secret shorter than 16 characters', async () => {
    const { store } = makeStore();
    const app = await buildApp({ store });

    const response = await app.inject({
      method: 'POST',
      url: '/v1/subscriptions',
      headers: { authorization: `Bearer ${'h'.repeat(64)}` },
      payload: postBody({ secret: 'mine' }),
    });

    expect(response.statusCode).toBe(400);
  });

  it('rejects malformed event names', async () => {
    const { store } = makeStore();
    const app = await buildApp({ store });

    const response = await app.inject({
      method: 'POST',
      url: '/v1/subscriptions',
      headers: { authorization: `Bearer ${'i'.repeat(64)}` },
      payload: postBody({ events: ['Account Credited!'] }),
    });

    expect(response.statusCode).toBe(400);
  });

  it('defaults events to an empty list', async () => {
    const { store, calls } = makeStore();
    const app = await buildApp({ store });

    const response = await app.inject({
      method: 'POST',
      url: '/v1/subscriptions',
      headers: { authorization: `Bearer ${'j'.repeat(64)}` },
      payload: postBody(),
    });

    expect(response.statusCode).toBe(201);
    expect(calls[0].events).toEqual([]);
  });

  it('returns 500 with a problem details body when the store fails', async () => {
    const { store } = makeStore({
      createSubscription: () => Promise.reject(new Error('database is down')),
    });
    const app = await buildApp({ store });

    const response = await app.inject({
      method: 'POST',
      url: '/v1/subscriptions',
      headers: { authorization: `Bearer ${'k'.repeat(64)}` },
      payload: postBody(),
    });

    expect(response.statusCode).toBe(500);
    expect(response.headers['content-type']).toContain('application/problem+json');
  });
});
