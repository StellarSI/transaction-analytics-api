import Fastify, { type FastifyInstance } from 'fastify';
import { describe, it, expect, afterEach } from 'vitest';
import { setErrorHandler } from '../../api/errors';
import { createApiKeyGuard, extractBearerToken } from '../middleware';
import {
  issueApiKey,
  type ApiKey,
  type ApiKeyPermission,
  type ApiKeyStore,
  type CreateApiKeyInput,
} from '../api-keys';

const apps: FastifyInstance[] = [];

afterEach(async () => {
  await Promise.all(apps.splice(0).map((app) => app.close()));
});

/** In-memory stand-in for the `api_keys` table. */
function createStore(): ApiKeyStore {
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

async function buildApp(
  store: ApiKeyStore,
  permissions?: ApiKeyPermission[],
): Promise<FastifyInstance> {
  const app = Fastify({ logger: false });
  setErrorHandler(app);
  const guard = createApiKeyGuard({ store, permissions });

  app.get('/protected', async (request) => {
    const key = await guard(request);
    return { key_id: key.id, tier: key.tier, name: key.name };
  });

  apps.push(app);
  return app;
}

describe('extractBearerToken', () => {
  it('reads the token from the Authorization header', () => {
    const request = {
      headers: { authorization: 'Bearer sk_live_abc' },
    } as unknown as Parameters<typeof extractBearerToken>[0];

    expect(extractBearerToken(request)).toBe('sk_live_abc');
  });

  it('accepts a lowercase scheme', () => {
    const request = {
      headers: { authorization: 'bearer sk_live_abc' },
    } as unknown as Parameters<typeof extractBearerToken>[0];

    expect(extractBearerToken(request)).toBe('sk_live_abc');
  });

  it('returns null without an Authorization header', () => {
    const request = {
      headers: {},
    } as unknown as Parameters<typeof extractBearerToken>[0];

    expect(extractBearerToken(request)).toBeNull();
  });
});

describe('API key middleware (issue #30)', () => {
  it('rejects a request without an Authorization header', async () => {
    const app = await buildApp(createStore());

    const response = await app.inject({ method: 'GET', url: '/protected' });

    expect(response.statusCode).toBe(401);
    expect(response.json()).toMatchObject({ code: 'unauthorized' });
  });

  it('rejects an unknown key', async () => {
    const app = await buildApp(createStore());

    const response = await app.inject({
      method: 'GET',
      url: '/protected',
      headers: { authorization: 'Bearer sk_live_unknown' },
    });

    expect(response.statusCode).toBe(401);
  });

  it('accepts a valid key and attaches it to the request', async () => {
    const store = createStore();
    const issued = await issueApiKey(store, {
      name: 'trading-bot',
      permissions: ['flows:read'],
      tier: 'pro',
    });
    const app = await buildApp(store);

    const response = await app.inject({
      method: 'GET',
      url: '/protected',
      headers: { authorization: `Bearer ${issued.plaintext}` },
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toMatchObject({
      key_id: issued.apiKey.id,
      tier: 'pro',
      name: 'trading-bot',
    });
  });

  it('rejects a key that misses a required permission', async () => {
    const store = createStore();
    const issued = await issueApiKey(store, {
      name: 'read-only',
      permissions: ['flows:read'],
      tier: 'free',
    });
    const app = await buildApp(store, ['subscriptions:write']);

    const response = await app.inject({
      method: 'GET',
      url: '/protected',
      headers: { authorization: `Bearer ${issued.plaintext}` },
    });

    expect(response.statusCode).toBe(403);
    expect(response.json()).toMatchObject({ code: 'forbidden' });
  });

  it('accepts a key that holds the required permission', async () => {
    const store = createStore();
    const issued = await issueApiKey(store, {
      name: 'writer',
      permissions: ['subscriptions:write'],
      tier: 'free',
    });
    const app = await buildApp(store, ['subscriptions:write']);

    const response = await app.inject({
      method: 'GET',
      url: '/protected',
      headers: { authorization: `Bearer ${issued.plaintext}` },
    });

    expect(response.statusCode).toBe(200);
  });
});
