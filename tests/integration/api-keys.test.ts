import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { pool } from '@/db/pool';
import { runMigrations } from '@/db/migrate';
import { createApp } from '@/api/server';
import {
  createPostgresApiKeyStore,
  issueApiKey,
  type ApiKeySql,
  type ApiKeyStore,
} from '@/auth/api-keys';
import { createRedisRateLimiter } from '@/auth/rate-limit';
import {
  createPostgresSubscriptionStore,
  type SubscriptionSql,
} from '@/api/repository/subscriptions';
import { closeRedis, getRedis } from '@/redis/client';

/**
 * Integration coverage for API key authentication (issue #30) and the
 * per-key rate limit (issue #31).
 *
 * Requires the TimescaleDB and Redis instances provided by CI
 * (DATABASE_URL / REDIS_URL): the key is really stored, hashed and looked
 * up, and the rate limit counter really lives in Redis.
 */

const ACCOUNT = 'GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN';

describe('API key authentication and rate limiting (integration)', () => {
  let app: FastifyInstance;
  let apiKeys: ApiKeyStore;

  beforeAll(async () => {
    await runMigrations('up');
    await pool`DELETE FROM api_keys`;
    await pool`DELETE FROM subscriptions`;

    apiKeys = createPostgresApiKeyStore(pool as unknown as ApiKeySql);

    app = await createApp({
      logger: false,
      store: createPostgresSubscriptionStore(pool as unknown as SubscriptionSql),
      apiKeyStore: apiKeys,
      rateLimiter: createRedisRateLimiter(getRedis()),
      // Keep health independent of Horizon, RPC and the ingestion lag.
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
    });
  }, 60000);

  afterAll(async () => {
    await app?.close();
    await pool`DELETE FROM api_keys`;
    await pool`DELETE FROM subscriptions`;
    await pool.end();
    await closeRedis();
  });

  function subscriptionPayload() {
    return {
      target_type: 'account',
      target_value: ACCOUNT,
      url: 'https://example.com/webhook',
      events: ['account_credited'],
    };
  }

  it('stores only the hash of an issued key', async () => {
    const issued = await issueApiKey(apiKeys, {
      name: 'integration-key',
      permissions: ['flows:read'],
      tier: 'free',
    });

    const rows = await pool`SELECT key_hash FROM api_keys WHERE name = 'integration-key'`;
    expect(rows).toHaveLength(1);
    expect(String(rows[0].key_hash)).not.toContain(issued.plaintext);
    expect(String(rows[0].key_hash)).toContain('$argon2');
  });

  it('accepts a request that presents a valid key', async () => {
    const issued = await issueApiKey(apiKeys, {
      name: 'integration-key',
      permissions: ['subscriptions:write'],
      tier: 'pro',
    });

    const response = await app.inject({
      method: 'POST',
      url: '/v1/subscriptions',
      headers: { authorization: `Bearer ${issued.plaintext}` },
      payload: subscriptionPayload(),
    });

    expect(response.statusCode).toBe(201);

    // The subscription is stored against the verified key id.
    const rows = await pool`SELECT api_key_id FROM subscriptions`;
    expect(rows).toHaveLength(1);
    expect(String(rows[0].api_key_id)).toBe(issued.apiKey.id);
  });

  it('rejects a request without a key', async () => {
    const response = await app.inject({
      method: 'POST',
      url: '/v1/subscriptions',
      payload: subscriptionPayload(),
    });

    expect(response.statusCode).toBe(401);
  });

  it('rejects a request with an unknown key', async () => {
    const response = await app.inject({
      method: 'POST',
      url: '/v1/subscriptions',
      headers: { authorization: 'Bearer sk_live_unknown-key' },
      payload: subscriptionPayload(),
    });

    expect(response.statusCode).toBe(401);
  });

  it('rejects a revoked key', async () => {
    const issued = await issueApiKey(apiKeys, {
      name: 'revoked-key',
      permissions: ['subscriptions:write'],
      tier: 'free',
    });
    await pool`UPDATE api_keys SET revoked_at = NOW() WHERE id = ${issued.apiKey.id}`;

    const response = await app.inject({
      method: 'POST',
      url: '/v1/subscriptions',
      headers: { authorization: `Bearer ${issued.plaintext}` },
      payload: subscriptionPayload(),
    });

    expect(response.statusCode).toBe(401);
  });

  it('counts every request of a key against its tier budget in Redis', async () => {
    const limiter = createRedisRateLimiter(getRedis());
    const keyId = `integration-limit-${Date.now()}`;
    // One timestamp for the whole burst so the window cannot roll over while
    // the budget is consumed.
    const now = Date.now();

    const decisions = [];
    for (let request = 0; request < 101; request++) {
      decisions.push(await limiter.consume(keyId, 'free', now));
    }

    expect(decisions[0].allowed).toBe(true);
    expect(decisions[99].allowed).toBe(true);
    expect(decisions[99].remaining).toBe(0);
    expect(decisions[100].allowed).toBe(false);
    expect(decisions[100].retryAfter).toBeGreaterThan(0);
  });

  it('returns the rate limit headers on an authenticated request', async () => {
    const issued = await issueApiKey(apiKeys, {
      name: 'header-key',
      permissions: ['subscriptions:write'],
      tier: 'enterprise',
    });

    const response = await app.inject({
      method: 'POST',
      url: '/v1/subscriptions',
      headers: { authorization: `Bearer ${issued.plaintext}` },
      payload: subscriptionPayload(),
    });

    expect(response.statusCode).toBe(201);
    expect(response.headers['x-ratelimit-limit']).toBe('10000');
    expect(Number(response.headers['x-ratelimit-remaining'])).toBeGreaterThanOrEqual(0);
  });
});
