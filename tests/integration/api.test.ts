import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { pool } from '@/db/pool';
import { runMigrations } from '@/db/migrate';
import { createApp } from '@/api/server';
import {
  createPostgresAssetFlowRepository,
  type AssetFlowSql,
} from '@/api/repository/asset-flows';
import {
  createPostgresAccountFlowRepository,
  type AccountFlowSql,
} from '@/api/repository/account-flows';
import {
  createPostgresCorridorFlowRepository,
  type CorridorFlowSql,
} from '@/api/repository/corridor-flows';
import {
  createPostgresSubscriptionStore,
  type SubscriptionSql,
} from '@/api/repository/subscriptions';
import {
  createPostgresApiKeyStore,
  issueApiKey,
  type ApiKeySql,
} from '@/auth/api-keys';
import { createRedisRateLimiter, RATE_LIMIT_TIERS } from '@/auth/rate-limit';
import { closeActiveStreamConnections, type TransactionStreamSubscriber } from '@/api/routes/transactions/stream';
import { closeRedis, getRedis } from '@/redis/client';

/**
 * End-to-end REST coverage for all six documented endpoints (issue #42).
 *
 * Requires the TimescaleDB and Redis instances provided by CI
 * (DATABASE_URL / REDIS_URL). Rows are written into the real aggregate
 * hypertables, API keys are really hashed and stored, and the rate-limit
 * counter really lives in Redis. The Horizon/SSE edge is injected so the
 * suite never makes a live network call.
 */

const ISSUER = 'GB5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN';
const ACCOUNT = `G${'A'.repeat(55)}`;
const ASSET = `TSTU:${ISSUER}`;
const BUCKET_START = '2026-10-01T00:00:00Z';

const MINUTE_ROWS = [
  { bucket_start: '2026-10-01T00:00:00Z', volume: 100.5, payments: 10, net_flow: 5 },
  { bucket_start: '2026-10-01T00:01:00Z', volume: 200.25, payments: 20, net_flow: -5 },
  { bucket_start: '2026-10-01T01:00:00Z', volume: 50, payments: 5, net_flow: 0 },
];

function fakeSubscriber(): TransactionStreamSubscriber & { closed: boolean } {
  return {
    closed: false,
    subscribe(onEvent) {
      onEvent({
        id: '2',
        data: { from: ACCOUNT, asset: 'XLM', type: 'payment' },
      });
    },
    close() {
      this.closed = true;
    },
  };
}

describe('REST API endpoints (integration, issue #42)', () => {
  let app: FastifyInstance;
  let subscriber: TransactionStreamSubscriber & { closed: boolean };
  let limiter: ReturnType<typeof createRedisRateLimiter>;

  beforeAll(async () => {
    await runMigrations('up');
    await pool`DELETE FROM subscriptions`;
    await pool`DELETE FROM api_keys`;
    await pool`DELETE FROM asset_flow_aggregates`;
    await pool`DELETE FROM account_flow_aggregates`;
    await pool`DELETE FROM corridor_aggregates`;

    await pool`
      INSERT INTO asset_flow_aggregates
        (bucket_start, interval, asset_code, asset_issuer, volume, payments, net_flow)
      VALUES
        (${MINUTE_ROWS[0].bucket_start}, '1m', 'TSTU', ${ISSUER}, ${MINUTE_ROWS[0].volume}, ${MINUTE_ROWS[0].payments}, ${MINUTE_ROWS[0].net_flow}),
        (${MINUTE_ROWS[1].bucket_start}, '1m', 'TSTU', ${ISSUER}, ${MINUTE_ROWS[1].volume}, ${MINUTE_ROWS[1].payments}, ${MINUTE_ROWS[1].net_flow}),
        (${MINUTE_ROWS[2].bucket_start}, '1m', 'TSTU', ${ISSUER}, ${MINUTE_ROWS[2].volume}, ${MINUTE_ROWS[2].payments}, ${MINUTE_ROWS[2].net_flow})
    `;
    await pool`CALL refresh_continuous_aggregate('asset_flow_aggregates_1h', NULL, NULL)`;
    await pool`CALL refresh_continuous_aggregate('asset_flow_aggregates_1d', NULL, NULL)`;

    await pool`
      INSERT INTO account_flow_aggregates
        (bucket_start, interval, account_id, inflow, outflow, counterparties, net_flow)
      VALUES
        (${BUCKET_START}, '1h', ${ACCOUNT}, 250.5, 120.25,
         ${JSON.stringify([{ account_id: `G${'B'.repeat(55)}`, volume: '250.5000000', count: 3, direction: 'in' }])},
         130.25)
    `;
    await pool`
      INSERT INTO corridor_aggregates
        (bucket_start, interval, source_asset, dest_asset, source_anchor, dest_anchor, volume, payments)
      VALUES
        (${BUCKET_START}, '1h', 'XLM', ${`TSTU:${ISSUER}`}, NULL, ${ISSUER}, 184220.55, 1243)
    `;

    const sql = pool as unknown as AssetFlowSql & AccountFlowSql & CorridorFlowSql & ApiKeySql;
    const apiKeys = createPostgresApiKeyStore(sql);
    limiter = createRedisRateLimiter(getRedis());
    subscriber = fakeSubscriber();

    app = await createApp({
      logger: false,
      repository: createPostgresAssetFlowRepository(sql),
      accountFlowRepository: createPostgresAccountFlowRepository(sql),
      corridorFlowRepository: createPostgresCorridorFlowRepository(sql),
      store: createPostgresSubscriptionStore(pool as unknown as SubscriptionSql),
      apiKeyStore: apiKeys,
      rateLimiter: limiter,
      createTransactionStreamSubscriber: () => subscriber,
      retryMs: 1234,
      heartbeatMs: 60_000,
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
      getLatestLedger: () => Promise.resolve(95),
    });
  }, 60000);

  afterAll(async () => {
    await closeActiveStreamConnections();
    await app?.close();
    await pool`DELETE FROM subscriptions`;
    await pool`DELETE FROM api_keys`;
    await pool`DELETE FROM asset_flow_aggregates`;
    await pool`DELETE FROM account_flow_aggregates`;
    await pool`DELETE FROM corridor_aggregates`;
    await pool`CALL refresh_continuous_aggregate('asset_flow_aggregates_1h', NULL, NULL)`;
    await pool`CALL refresh_continuous_aggregate('asset_flow_aggregates_1d', NULL, NULL)`;
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

  it('GET /v1/flows/assets/:asset returns minute buckets from Postgres', async () => {
    const response = await app.inject({
      method: 'GET',
      url: `/v1/flows/assets/${ASSET}?interval=1m&from=2026-10-01T00:00:00Z&to=2026-10-01T02:00:00Z`,
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({
      asset: ASSET,
      interval: '1m',
      buckets: [
        { start: '2026-10-01T00:00:00Z', volume: '100.5', payments: 10, net_flow: '5' },
        { start: '2026-10-01T00:01:00Z', volume: '200.25', payments: 20, net_flow: '-5' },
        { start: '2026-10-01T01:00:00Z', volume: '50', payments: 5, net_flow: '0' },
      ],
    });
  });

  it('GET /v1/flows/accounts/:account returns stored inflows and counterparties', async () => {
    const response = await app.inject({
      method: 'GET',
      url: `/v1/flows/accounts/${ACCOUNT}?interval=1h`,
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toMatchObject({
      account: ACCOUNT,
      interval: '1h',
      buckets: [
        {
          start: BUCKET_START,
          inflow: '250.5000000',
          outflow: '120.2500000',
          net_flow: '130.2500000',
        },
      ],
    });
    expect(response.json().buckets[0].counterparties.length).toBeGreaterThan(0);
  });

  it('GET /v1/flows/corridors returns the stored corridor bucket', async () => {
    const response = await app.inject({
      method: 'GET',
      url: '/v1/flows/corridors?interval=1h',
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toMatchObject({
      interval: '1h',
      corridors: [
        {
          start: BUCKET_START,
          source_asset: 'XLM',
          dest_asset: `TSTU:${ISSUER}`,
          dest_anchor: ISSUER,
          payments: 1243,
        },
      ],
      pagination: { limit: 100, offset: 0 },
    });
  });

  it('GET /v1/health reports dependency status and ingestion lag', async () => {
    const response = await app.inject({ method: 'GET', url: '/v1/health' });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toMatchObject({
      status: 'healthy',
      checks: {
        database: { ok: true },
        redis: { ok: true },
        horizon: { ok: true, latest_ledger: 100 },
      },
      // 100 - 95 ledgers at ~5s each.
      ingestion_lag_seconds: 25,
    });
  });

  it('GET /v1/transactions/stream serves SSE frames from the injected subscriber', async () => {
    const timer = setTimeout(() => void closeActiveStreamConnections(), 10);
    const response = await app.inject({
      method: 'GET',
      url: '/v1/transactions/stream',
      headers: { 'last-event-id': '1' },
    });
    clearTimeout(timer);
    await closeActiveStreamConnections();

    expect(response.statusCode).toBe(200);
    expect(response.headers['content-type']).toContain('text/event-stream');
    expect(response.body).toContain('retry: 1234');
    expect(response.body).toContain('id: 2');
    expect(response.body).toContain('event: transaction');
    expect(subscriber.closed).toBe(true);
  });

  it('POST /v1/subscriptions stores a subscription for a valid API key', async () => {
    const apiKeys = createPostgresApiKeyStore(
      pool as unknown as ApiKeySql,
    );
    const issued = await issueApiKey(apiKeys, {
      name: 'integration-rest-key',
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
    const rows = await pool`SELECT api_key_id FROM subscriptions`;
    expect(rows.length).toBeGreaterThan(0);
    expect(String(rows[0].api_key_id)).toBe(issued.apiKey.id);
  });

  it('rejects unauthenticated and unknown keys with 401', async () => {
    const missing = await app.inject({
      method: 'POST',
      url: '/v1/subscriptions',
      payload: subscriptionPayload(),
    });
    expect(missing.statusCode).toBe(401);

    const unknown = await app.inject({
      method: 'POST',
      url: '/v1/subscriptions',
      headers: { authorization: 'Bearer sk_live_unknown-key' },
      payload: subscriptionPayload(),
    });
    expect(unknown.statusCode).toBe(401);
  });

  it('returns 429 with rate-limit headers once the free budget is spent', async () => {
    const apiKeys = createPostgresApiKeyStore(pool as unknown as ApiKeySql);
    const issued = await issueApiKey(apiKeys, {
      name: 'integration-rate-limit-key',
      permissions: ['subscriptions:write'],
      tier: 'free',
    });

    const now = Date.now();
    const budget = RATE_LIMIT_TIERS.free;
    for (let request = 0; request < budget; request += 1) {
      const decision = await limiter.consume(issued.apiKey.id, 'free', now);
      expect(decision.allowed).toBe(true);
    }

    const response = await app.inject({
      method: 'POST',
      url: '/v1/subscriptions',
      headers: { authorization: `Bearer ${issued.plaintext}` },
      payload: subscriptionPayload(),
    });

    expect(response.statusCode).toBe(429);
    expect(response.headers['retry-after']).toBeDefined();
    expect(response.headers['x-ratelimit-limit']).toBe(String(budget));
    expect(response.json()).toMatchObject({ status: 429 });
  });

  it('returns RFC 7807 validation problems for malformed requests', async () => {
    const responses = await Promise.all([
      app.inject({ method: 'GET', url: '/v1/flows/assets/USDC' }),
      app.inject({ method: 'GET', url: '/v1/flows/accounts/not-an-account' }),
      app.inject({ method: 'GET', url: '/v1/flows/corridors?interval=1s' }),
      app.inject({ method: 'GET', url: '/v1/transactions/stream?unexpected=1' }),
      app.inject({
        method: 'POST',
        url: '/v1/subscriptions',
        headers: { authorization: 'Bearer sk_live_unknown-key' },
        payload: { target_type: 'account' },
      }),
    ]);

    for (const response of responses.slice(0, 4)) {
      expect(response.statusCode).toBe(400);
      expect(response.headers['content-type']).toContain('application/problem+json');
      expect(response.json()).toMatchObject({ status: 400, code: 'validation_error' });
    }
    expect(responses[4].statusCode).toBe(401);
  });

  it('returns an empty bucket list for an unknown asset', async () => {
    const response = await app.inject({
      method: 'GET',
      url: '/v1/flows/assets/USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN?interval=1m',
    });

    expect(response.statusCode).toBe(200);
    expect(response.json().buckets).toEqual([]);
  });
});
