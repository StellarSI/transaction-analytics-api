/**
 * Health check route (issue #23).
 *
 * `GET /v1/health` reports the status of the dependencies the API needs to
 * serve traffic. It always answers 200 — a dependency outage is reflected in
 * the `status` / `services` payload rather than in the HTTP status, so
 * load balancers can distinguish "process alive" from "fully healthy".
 *
 * The checkers are injectable so unit tests can exercise the route without a
 * live PostgreSQL or Redis instance.
 */

import type { FastifyInstance } from 'fastify';
import { dbHealthCheck } from '../../db/pool';
import { redisHealthCheck } from '../../redis/health';

export interface HealthCheckResult {
  ok: boolean;
  latencyMs: number;
  detail: string;
}

export interface HealthRouteOptions {
  checkDatabase?: () => Promise<HealthCheckResult>;
  checkRedis?: () => Promise<HealthCheckResult>;
}

export function registerHealthRoutes(
  app: FastifyInstance,
  options: HealthRouteOptions,
  done: (err?: Error) => void,
): void {
  const checkDatabase = options.checkDatabase ?? dbHealthCheck;
  const checkRedis = options.checkRedis ?? redisHealthCheck;

  app.get('/health', async () => {
    const [database, redis] = await Promise.all([checkDatabase(), checkRedis()]);
    return {
      status: database.ok && redis.ok ? 'ok' : 'degraded',
      timestamp: new Date().toISOString(),
      services: { database, redis },
    };
  });

  done();
}
