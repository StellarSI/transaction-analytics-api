import { describe, it, expect, afterEach } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { createApp } from '../server';

const apps: FastifyInstance[] = [];

async function buildApp(): Promise<FastifyInstance> {
  const app = await createApp({
    logger: false,
    checkDatabase: () => Promise.resolve({ ok: true, latencyMs: 2, detail: 'postgres reachable' }),
    checkRedis: () => Promise.resolve({ ok: true, latencyMs: 1, detail: 'redis reachable' }),
  });
  apps.push(app);
  return app;
}

afterEach(async () => {
  await Promise.all(apps.splice(0).map((app) => app.close()));
});

describe('Fastify server (issue #23)', () => {
  it('serves GET /v1/health with a 200 payload', async () => {
    const app = await buildApp();
    const response = await app.inject({ method: 'GET', url: '/v1/health' });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({
      status: 'ok',
      timestamp: expect.any(String),
      services: {
        database: { ok: true, latencyMs: 2, detail: 'postgres reachable' },
        redis: { ok: true, latencyMs: 1, detail: 'redis reachable' },
      },
    });
  });

  it('reports a degraded status when a dependency is down', async () => {
    const app = await createApp({
      logger: false,
      checkDatabase: () =>
        Promise.resolve({ ok: false, latencyMs: 10, detail: 'connection refused' }),
      checkRedis: () => Promise.resolve({ ok: true, latencyMs: 1, detail: 'redis reachable' }),
    });
    apps.push(app);

    const response = await app.inject({ method: 'GET', url: '/v1/health' });

    expect(response.statusCode).toBe(200);
    expect(response.json().status).toBe('degraded');
    expect(response.json().services.database.ok).toBe(false);
  });

  it('returns an x-request-id header on every response', async () => {
    const app = await buildApp();
    const response = await app.inject({ method: 'GET', url: '/v1/health' });

    expect(response.headers['x-request-id']).toBeTruthy();
  });

  it('honours an inbound x-request-id', async () => {
    const app = await buildApp();
    const response = await app.inject({
      method: 'GET',
      url: '/v1/health',
      headers: { 'x-request-id': 'client-request-id' },
    });

    expect(response.headers['x-request-id']).toBe('client-request-id');
  });

  it('adds helmet security headers', async () => {
    const app = await buildApp();
    const response = await app.inject({ method: 'GET', url: '/v1/health' });

    expect(response.headers['x-content-type-options']).toBe('nosniff');
    expect(response.headers['x-frame-options']).toBeTruthy();
  });

  it('reflects the request origin for CORS', async () => {
    const app = await buildApp();
    const response = await app.inject({
      method: 'GET',
      url: '/v1/health',
      headers: { origin: 'https://example.com' },
    });

    expect(response.headers['access-control-allow-origin']).toBe('https://example.com');
  });

  it('returns RFC 7807 problem details for unknown routes', async () => {
    const app = await buildApp();
    const response = await app.inject({ method: 'GET', url: '/v1/does-not-exist' });

    expect(response.statusCode).toBe(404);
    expect(response.headers['content-type']).toContain('application/problem+json');
    expect(response.json()).toMatchObject({
      status: 404,
      code: 'not_found',
      title: 'Not Found',
      instance: '/v1/does-not-exist',
    });
  });
});
