/** Unit tests for GET /v1/health (issue #40). */

import { afterEach, describe, expect, it } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { createApp } from '../../server';

const ok = (detail: string) => ({ ok: true, latencyMs: 2, detail });
const head = () => ({ ...ok('horizon reachable'), latest_ledger: 100 });

describe('GET /v1/health (issue #40)', () => {
  const apps: FastifyInstance[] = [];

  afterEach(async () => {
    await Promise.all(apps.splice(0).map((app) => app.close()));
  });

  async function buildApp(checkDatabase = () => Promise.resolve(ok('postgres reachable'))) {
    const app = await createApp({
      logger: false,
      checkDatabase,
      checkRedis: () => Promise.resolve(ok('redis reachable')),
      checkHorizon: () => Promise.resolve(head()),
      checkRpc: () =>
        Promise.resolve({ ...ok('rpc reachable'), latest_ledger: 100 }),
      getLatestLedger: () => Promise.resolve(98),
    });
    apps.push(app);
    return app;
  }

  it('returns 200 with checks and ingestion lag computed from the head', async () => {
    const app = await buildApp();

    const response = await app.inject({ method: 'GET', url: '/v1/health' });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toMatchObject({
      status: 'healthy',
      timestamp: expect.any(String),
      checks: {
        database: { ok: true, detail: 'postgres reachable' },
        redis: { ok: true },
        horizon: { ok: true, latest_ledger: 100 },
        rpc: { ok: true },
      },
      ingestion_lag_seconds: 10,
    });
  });

  it('reports degraded status and folds checker failures into the payload', async () => {
    const app = await buildApp(() => Promise.reject(new Error('database down')));

    const response = await app.inject({ method: 'GET', url: '/v1/health' });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toMatchObject({
      status: 'degraded',
      checks: { database: { ok: false, latencyMs: 0, detail: 'database down' } },
    });
  });

  it('requires no authentication', async () => {
    const app = await buildApp();

    const response = await app.inject({
      method: 'GET',
      url: '/v1/health',
      headers: { authorization: 'Bearer sk_live_invalid' },
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toMatchObject({ status: 'healthy' });
  });
});
