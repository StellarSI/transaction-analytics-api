/**
 * Unit tests for POST /v1/subscriptions (issue #40).
 *
 * These are unit tests: API keys and subscriptions are served by in-memory
 * fakes, so no test database or Redis instance is required (the integration
 * suite in tests/integration covers the real stores).
 */

import { afterEach, describe, expect, it } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { createApp } from '../../server';
import {
  issueApiKey,
  type ApiKey,
  type ApiKeyStore,
  type CreateApiKeyInput,
} from '../../../auth/api-keys';
import type { RateLimiter } from '../../../auth/rate-limit';
import type {
  CreateSubscriptionInput,
  Subscription,
  SubscriptionStore,
} from '../../repository/subscriptions';

const ACCOUNT = `G${'A'.repeat(55)}`;

function createApiKeyStore(): ApiKeyStore {
  const rows = new Map<string, ApiKey>();
  return {
    createApiKey(input: CreateApiKeyInput) {
      const apiKey: ApiKey = {
        id: `key-${rows.size + 1}`,
        name: input.name,
        permissions: input.permissions,
        tier: input.tier,
        createdAt: new Date().toISOString(),
        lastUsedAt: null,
        revokedAt: null,
      };
      rows.set(input.hash, apiKey);
      return Promise.resolve(apiKey);
    },
    findApiKeyByHash(hash: string) {
      return Promise.resolve(rows.get(hash) ?? null);
    },
    markApiKeyUsed() {
      return Promise.resolve();
    },
  };
}

function createSubscriptionStore(inputs: CreateSubscriptionInput[] = []): SubscriptionStore {
  return {
    createSubscription(input: CreateSubscriptionInput) {
      inputs.push(input);
      const subscription: Subscription = {
        id: 'sub-1',
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
      return Promise.resolve(subscription);
    },
  };
}

function limiter(allowed: boolean): RateLimiter {
  return {
    consume() {
      return Promise.resolve({
        allowed,
        limit: 100,
        remaining: 0,
        resetAt: 2_000_000_000,
        retryAfter: 42,
      });
    },
  };
}

const body = {
  target_type: 'account',
  target_value: ACCOUNT,
  url: 'https://hooks.example.com/eta',
  secret: 'a-very-secret-value',
  events: ['flows.assets.updated'],
};

describe('POST /v1/subscriptions (issue #40)', () => {
  let app: FastifyInstance | undefined;

  afterEach(async () => {
    await app?.close();
    app = undefined;
  });

  async function buildApp(options: {
    store?: SubscriptionStore;
    apiKeyStore?: ApiKeyStore;
    rateLimiter?: RateLimiter;
  } = {}): Promise<FastifyInstance> {
    const instance = await createApp({ logger: false, ...options });
    app = instance;
    return instance;
  }

  it('returns 201 with the created subscription and never echoes the secret', async () => {
    const apiKeyStore = createApiKeyStore();
    const issued = await issueApiKey(apiKeyStore, {
      name: 'test-key',
      permissions: ['subscriptions:write'],
      tier: 'pro',
    });
    const inputs: CreateSubscriptionInput[] = [];
    const instance = await buildApp({
      apiKeyStore,
      store: createSubscriptionStore(inputs),
      rateLimiter: limiter(true),
    });

    const response = await instance.inject({
      method: 'POST',
      url: '/v1/subscriptions',
      headers: { authorization: `Bearer ${issued.plaintext}` },
      payload: body,
    });

    expect(response.statusCode).toBe(201);
    expect(response.json()).toEqual({
      id: 'sub-1',
      target_type: 'account',
      target_value: ACCOUNT,
      url: body.url,
      events: body.events,
      created_at: '2026-11-12T00:00:00.000Z',
    });
    expect(inputs[0]).toMatchObject({
      apiKeyId: issued.apiKey.id,
      targetType: 'account',
      targetValue: ACCOUNT,
    });
    expect(response.body).not.toContain(body.secret);
  });

  it('returns 401 when the API key is missing or unknown', async () => {
    const inputs: CreateSubscriptionInput[] = [];
    const instance = await buildApp({
      apiKeyStore: createApiKeyStore(),
      store: createSubscriptionStore(inputs),
      rateLimiter: limiter(true),
    });

    const missing = await instance.inject({
      method: 'POST',
      url: '/v1/subscriptions',
      payload: body,
    });
    const unknown = await instance.inject({
      method: 'POST',
      url: '/v1/subscriptions',
      headers: { authorization: 'Bearer sk_live_unknown' },
      payload: body,
    });

    for (const response of [missing, unknown]) {
      expect(response.statusCode).toBe(401);
      expect(response.headers['content-type']).toContain('application/problem+json');
      expect(response.json()).toMatchObject({ status: 401, code: 'unauthorized' });
    }
    expect(inputs).toHaveLength(0);
  });

  it('returns 429 with Retry-After when the rate limit is exhausted', async () => {
    const apiKeyStore = createApiKeyStore();
    const issued = await issueApiKey(apiKeyStore, {
      name: 'test-key',
      permissions: ['subscriptions:write'],
      tier: 'free',
    });
    const inputs: CreateSubscriptionInput[] = [];
    const instance = await buildApp({
      apiKeyStore,
      store: createSubscriptionStore(inputs),
      rateLimiter: limiter(false),
    });

    const response = await instance.inject({
      method: 'POST',
      url: '/v1/subscriptions',
      headers: { authorization: `Bearer ${issued.plaintext}` },
      payload: body,
    });

    expect(response.statusCode).toBe(429);
    expect(response.headers['retry-after']).toBe('42');
    expect(response.json()).toMatchObject({ status: 429, code: 'rate_limit' });
    expect(inputs).toHaveLength(0);
  });

  it('returns an RFC 7807 problem for invalid bodies before writing', async () => {
    const apiKeyStore = createApiKeyStore();
    const issued = await issueApiKey(apiKeyStore, {
      name: 'test-key',
      permissions: ['*'],
      tier: 'pro',
    });
    const inputs: CreateSubscriptionInput[] = [];
    const instance = await buildApp({
      apiKeyStore,
      store: createSubscriptionStore(inputs),
      rateLimiter: limiter(true),
    });

    const badTarget = await instance.inject({
      method: 'POST',
      url: '/v1/subscriptions',
      headers: { authorization: `Bearer ${issued.plaintext}` },
      payload: { ...body, target_value: 'not-an-account' },
    });
    const badUrl = await instance.inject({
      method: 'POST',
      url: '/v1/subscriptions',
      headers: { authorization: `Bearer ${issued.plaintext}` },
      payload: { ...body, url: 'http://hooks.example.com/eta' },
    });
    const shortSecret = await instance.inject({
      method: 'POST',
      url: '/v1/subscriptions',
      headers: { authorization: `Bearer ${issued.plaintext}` },
      payload: { ...body, secret: 'too-short' },
    });

    for (const response of [badTarget, badUrl, shortSecret]) {
      expect(response.statusCode).toBe(400);
      expect(response.headers['content-type']).toContain('application/problem+json');
      expect(response.json()).toMatchObject({ status: 400, code: 'validation_error' });
    }
    expect(inputs).toHaveLength(0);
  });

  it('fails closed when no API key store is configured', async () => {
    const instance = await buildApp({ store: createSubscriptionStore() });

    const response = await instance.inject({
      method: 'POST',
      url: '/v1/subscriptions',
      headers: { authorization: 'Bearer sk_live_anything' },
      payload: body,
    });

    expect(response.statusCode).toBe(500);
    expect(response.json()).toMatchObject({ status: 500, code: 'internal_error' });
  });
});
