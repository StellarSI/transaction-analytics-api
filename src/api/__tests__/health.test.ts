import { describe, it, expect, afterEach } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { createApp } from '../server';
import { SECONDS_PER_LEDGER } from '../routes/health';

const apps: FastifyInstance[] = [];

afterEach(async () => {
  await Promise.all(apps.splice(0).map((app) => app.close()));
});

interface FakeOptions {
  database?: { ok: boolean; latencyMs: number; detail: string };
  redis?: { ok: boolean; latencyMs: number; detail: string };
  horizon?: {
    ok: boolean;
    latencyMs: number;
    detail: string;
    latest_ledger?: number;
  };
  rpc?: { ok: boolean; latencyMs: number; detail: string; latest_ledger?: number };
  latestLedger?: number | null | (() => Promise<number | null>);
}

async function buildApp(options: FakeOptions = {}): Promise<FastifyInstance> {
  const app = await createApp({
    logger: false,
    checkDatabase: () =>
      Promise.resolve(
        options.database ?? { ok: true, latencyMs: 2, detail: 'postgres reachable' },
      ),
    checkRedis: () =>
      Promise.resolve(options.redis ?? { ok: true, latencyMs: 1, detail: 'redis reachable' }),
    checkHorizon: () =>
      Promise.resolve(
        options.horizon ?? {
          ok: true,
          latencyMs: 5,
          detail: 'horizon reachable',
          latest_ledger: 100,
        },
      ),
    checkRpc: () =>
      Promise.resolve(
        options.rpc ?? {
          ok: true,
          latencyMs: 5,
          detail: 'stellar rpc reachable',
          latest_ledger: 100,
        },
      ),
    getLatestLedger:
      typeof options.latestLedger === 'function'
        ? options.latestLedger
        : () => Promise.resolve(options.latestLedger ?? 100),
  });
  apps.push(app);
  return app;
}

describe('GET /v1/health (issue #29)', () => {
  it('reports a healthy payload with zero lag when the db matches the head', async () => {
    const app = await buildApp();
    const response = await app.inject({ method: 'GET', url: '/v1/health' });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({
      status: 'healthy',
      timestamp: expect.any(String),
      checks: {
        database: { ok: true, latencyMs: 2, detail: 'postgres reachable' },
        redis: { ok: true, latencyMs: 1, detail: 'redis reachable' },
        horizon: {
          ok: true,
          latencyMs: 5,
          detail: 'horizon reachable',
          latest_ledger: 100,
        },
        rpc: {
          ok: true,
          latencyMs: 5,
          detail: 'stellar rpc reachable',
          latest_ledger: 100,
        },
      },
      ingestion_lag_seconds: 0,
    });
  });

  it('converts the ledger lag into seconds at SECONDS_PER_LEDGER per ledger', async () => {
    const app = await buildApp({ latestLedger: 96 });
    const response = await app.inject({ method: 'GET', url: '/v1/health' });

    expect(response.statusCode).toBe(200);
    expect(response.json().ingestion_lag_seconds).toBe(4 * SECONDS_PER_LEDGER);
    expect(response.json().status).toBe('healthy');
  });

  it('clamps a database ahead of the head to zero lag', async () => {
    const app = await buildApp({ latestLedger: 110 });
    const response = await app.inject({ method: 'GET', url: '/v1/health' });

    expect(response.statusCode).toBe(200);
    expect(response.json().ingestion_lag_seconds).toBe(0);
  });

  it('reports degraded when a dependency is down', async () => {
    const app = await buildApp({
      redis: { ok: false, latencyMs: 9, detail: 'connection refused' },
    });
    const response = await app.inject({ method: 'GET', url: '/v1/health' });

    expect(response.statusCode).toBe(200);
    expect(response.json().status).toBe('degraded');
    expect(response.json().checks.redis.ok).toBe(false);
  });

  it('reports degraded and a null lag when the horizon head is unknown', async () => {
    const app = await buildApp({
      horizon: { ok: false, latencyMs: 30, detail: 'timeout' },
    });
    const response = await app.inject({ method: 'GET', url: '/v1/health' });

    expect(response.statusCode).toBe(200);
    expect(response.json().status).toBe('degraded');
    expect(response.json().ingestion_lag_seconds).toBeNull();
  });

  it('reports degraded when the rpc head is unknown but horizon knows its head', async () => {
    const app = await buildApp({
      rpc: { ok: false, latencyMs: 8, detail: 'status 500' },
    });
    const response = await app.inject({ method: 'GET', url: '/v1/health' });

    expect(response.statusCode).toBe(200);
    expect(response.json().status).toBe('degraded');
    // Lag still computable from the horizon head.
    expect(response.json().ingestion_lag_seconds).toBe(0);
  });

  it('reports degraded when nothing has been ingested yet', async () => {
    const app = await buildApp({ latestLedger: null });
    const response = await app.inject({ method: 'GET', url: '/v1/health' });

    expect(response.statusCode).toBe(200);
    expect(response.json().status).toBe('degraded');
    expect(response.json().ingestion_lag_seconds).toBeNull();
  });

  it('reports degraded when the ledger query throws', async () => {
    const app = await buildApp({
      latestLedger: () => Promise.reject(new Error('relation does not exist')),
    });
    const response = await app.inject({ method: 'GET', url: '/v1/health' });

    expect(response.statusCode).toBe(200);
    expect(response.json().status).toBe('degraded');
    expect(response.json().ingestion_lag_seconds).toBeNull();
  });

  it('folds a thrown checker into { ok: false } instead of erroring', async () => {
    const app = await createApp({
      logger: false,
      checkDatabase: () => Promise.reject(new Error('pool exhausted')),
      checkRedis: () => Promise.resolve({ ok: true, latencyMs: 1, detail: 'redis reachable' }),
      checkHorizon: () =>
        Promise.resolve({
          ok: true,
          latencyMs: 5,
          detail: 'horizon reachable',
          latest_ledger: 100,
        }),
      checkRpc: () =>
        Promise.resolve({
          ok: true,
          latencyMs: 5,
          detail: 'stellar rpc reachable',
          latest_ledger: 100,
        }),
      getLatestLedger: () => Promise.resolve(100),
    });
    apps.push(app);

    const response = await app.inject({ method: 'GET', url: '/v1/health' });

    expect(response.statusCode).toBe(200);
    expect(response.json().status).toBe('degraded');
    expect(response.json().checks.database).toEqual({
      ok: false,
      latencyMs: 0,
      detail: 'pool exhausted',
    });
  });

  it('requires no authentication', async () => {
    const app = await buildApp();
    const response = await app.inject({
      method: 'GET',
      url: '/v1/health',
      headers: { authorization: 'Bearer invalid-key' },
    });

    expect(response.statusCode).toBe(200);
  });
});
