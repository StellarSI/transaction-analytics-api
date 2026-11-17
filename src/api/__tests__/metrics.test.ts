/**
 * Tests for the `/metrics` scrape endpoint and HTTP instrumentation (issue #35).
 *
 * Uses the same injected-checkers pattern as the health tests so no network,
 * database or Redis is required.
 */

import { describe, it, expect, afterEach } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { createApp } from '../server';

const apps: FastifyInstance[] = [];

afterEach(async () => {
  await Promise.all(apps.splice(0).map((app) => app.close()));
});

function buildApp(): Promise<FastifyInstance> {
  return createApp({
    logger: false,
    checkDatabase: () =>
      Promise.resolve({ ok: true, latencyMs: 1, detail: 'postgres reachable' }),
    checkRedis: () =>
      Promise.resolve({ ok: true, latencyMs: 1, detail: 'redis reachable' }),
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
  }).then((app) => {
    apps.push(app);
    return app;
  });
}

describe('GET /metrics (issue #35)', () => {
  it('serves Prometheus text format outside /v1', async () => {
    const app = await buildApp();

    const response = await app.inject({ method: 'GET', url: '/metrics' });

    expect(response.statusCode).toBe(200);
    expect(response.headers['content-type']).toContain('text/plain');
    expect(response.body).toContain('# HELP');
    expect(response.body).toContain('# TYPE eta_http_requests_total counter');
  });

  it('is not mounted under /v1', async () => {
    const app = await buildApp();

    const response = await app.inject({ method: 'GET', url: '/v1/metrics' });

    expect(response.statusCode).toBe(404);
  });

  it('records the request itself and other routes with route/method/status labels', async () => {
    const app = await buildApp();

    await app.inject({ method: 'GET', url: '/v1/health' });
    const response = await app.inject({ method: 'GET', url: '/metrics' });

    expect(response.body).toContain(
      'eta_http_requests_total{route="/v1/health",method="GET",status="200"}',
    );
    expect(response.body).toContain('eta_http_request_duration_seconds_count');
  });
});
