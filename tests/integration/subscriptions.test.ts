import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { pool } from '@/db/pool';
import { runMigrations } from '@/db/migrate';
import { createApp } from '@/api/server';
import {
  createPostgresSubscriptionStore,
  type SubscriptionSql,
} from '@/api/repository/subscriptions';

/**
 * Integration coverage for `POST /v1/subscriptions` (issue #28).
 *
 * Requires the TimescaleDB instance provided by CI (DATABASE_URL). Verifies
 * that a subscription round-trips through the `subscriptions` table — the
 * stand-in API key guard (issue #30) namespaces the row to the presented
 * bearer token.
 */

const ACCOUNT = 'GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN';

describe('POST /v1/subscriptions (integration)', () => {
  let app: FastifyInstance;

  beforeAll(async () => {
    await runMigrations('up');
    await pool`DELETE FROM subscriptions`;

    app = await createApp({
      logger: false,
      store: createPostgresSubscriptionStore(pool as unknown as SubscriptionSql),
      // Keep health independent of Redis, Horizon, RPC and the ingestion lag.
      checkDatabase: async () => ({ ok: true, latencyMs: 1, detail: 'postgres reachable' }),
      checkRedis: async () => ({ ok: true, latencyMs: 1, detail: 'redis reachable' }),
      checkHorizon: async () => ({
        ok: true,
        latencyMs: 1,
        detail: 'horizon reachable',
        latest_ledger: 100,
      }),
      checkRpc: async () => ({
        ok: true,
        latencyMs: 1,
        detail: 'stellar rpc reachable',
        latest_ledger: 100,
      }),
      getLatestLedger: async () => 100,
    });
  }, 60000);

  afterAll(async () => {
    await app?.close();
    await pool`DELETE FROM subscriptions`;
    await pool.end();
  });

  it('registers a subscription and persists it against the presented API key', async () => {
    const token = 'integration-test-token-that-is-long';
    const response = await app.inject({
      method: 'POST',
      url: '/v1/subscriptions',
      headers: { authorization: `Bearer ${token}` },
      payload: {
        target_type: 'account',
        target_value: ACCOUNT,
        url: 'https://example.com/webhook',
        secret: 'an-integration-signing-secret',
        events: ['account_credited'],
      },
    });

    expect(response.statusCode).toBe(201);
    const body = response.json() as {
      id: string;
      target_type: string;
      target_value: string;
      url: string;
      events: string[];
      created_at: string;
    };
    expect(body.id).toMatch(/^[0-9a-f-]{36}$/);
    expect(body).toMatchObject({
      target_type: 'account',
      target_value: ACCOUNT,
      url: 'https://example.com/webhook',
      events: ['account_credited'],
    });

    const rows = await pool`
      SELECT api_key_id, target_type, target_value, url, secret, events, active
      FROM subscriptions
      WHERE id = ${body.id}
    `;
    expect(rows).toHaveLength(1);
    expect(rows[0].target_type).toBe('account');
    expect(rows[0].target_value).toBe(ACCOUNT);
    expect(rows[0].secret).toBe('an-integration-signing-secret');
    expect(rows[0].events).toEqual(['account_credited']);
    expect(rows[0].active).toBe(true);
    // Salted digest of the token, never the token itself.
    expect(String(rows[0].api_key_id)).toMatch(/^[0-9a-f]{64}$/);
  });

  it('rejects a request without a bearer token', async () => {
    const response = await app.inject({
      method: 'POST',
      url: '/v1/subscriptions',
      payload: {
        target_type: 'account',
        target_value: ACCOUNT,
        url: 'https://example.com/webhook',
      },
    });

    expect(response.statusCode).toBe(401);
  });
});
