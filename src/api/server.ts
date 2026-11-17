/**
 * Fastify application factory (issue #23).
 *
 * Builds the HTTP server used by `src/index.ts` and by the test suite:
 *
 * - request logging configured from `LOG_LEVEL` / `NODE_ENV`,
 * - `@fastify/helmet` for security headers,
 * - `@fastify/cors` for cross-origin browser clients,
 * - `@fastify/compress` for response compression,
 * - an `x-request-id` header on every response (incoming ids are honoured),
 * - routes registered under the `/v1` prefix,
 * - a global RFC 7807 error handler.
 *
 * The factory is async and returns the instance without listening, so tests
 * can drive it with `app.inject()` and the entrypoint owns the listen/close
 * lifecycle (including graceful shutdown on SIGTERM).
 */

import Fastify, { type FastifyInstance, type FastifyServerOptions } from 'fastify';
import compress from '@fastify/compress';
import { register as defaultRegistry } from 'prom-client';
import { config } from '../config';
import { generateRequestId } from '../observability/logger';
import { getObservabilityMetrics, UNMATCHED_ROUTE } from '../observability/metrics';
import { setErrorHandler } from './errors';
import { registerSecurity } from './security';
import { createApiKeyGuard } from '../auth/middleware';
import { createRateLimitGuard } from '../auth/rate-limit';
import type { ApiKeyStore } from '../auth/api-keys';
import type { RateLimiter } from '../auth/rate-limit';
import type { AccountFlowRepository } from './repository/account-flows';
import type { AssetFlowRepository } from './repository/asset-flows';
import type { CorridorFlowRepository } from './repository/corridor-flows';
import { registerAccountFlowRoutes } from './routes/flows/accounts';
import { registerAssetFlowRoutes } from './routes/flows/assets';
import { registerCorridorFlowRoutes } from './routes/flows/corridors';
import { registerHealthRoutes, type HealthRouteOptions } from './routes/health';
import { registerSubscriptionRoutes, type SubscriptionRouteOptions } from './routes/subscriptions';
import {
  registerTransactionStreamRoutes,
  type TransactionStreamOptions,
} from './routes/transactions/stream';

export interface CreateAppOptions
  extends HealthRouteOptions,
    TransactionStreamOptions,
    SubscriptionRouteOptions {
  /** Read model used by `GET /v1/flows/assets/:asset`. */
  repository?: AssetFlowRepository;
  /** Read model used by `GET /v1/flows/accounts/:account`. */
  accountFlowRepository?: AccountFlowRepository;
  /** Read model used by `GET /v1/flows/corridors`. */
  corridorFlowRepository?: CorridorFlowRepository;
  /**
   * Store used to authenticate API keys (issue #30). Without it the protected
   * routes reject every request instead of trusting an unverified token.
   */
  apiKeyStore?: ApiKeyStore;
  /** Per-API-key rate limiter (issue #31). */
  rateLimiter?: RateLimiter;
  /**
   * Fastify logger override. Pass `false` to silence request logs (unit
   * tests); by default the server logs at `LOG_LEVEL` and pretty-prints in
   * development, matching the rest of the application.
   */
  logger?: false | FastifyServerOptions['logger'];
}

function defaultLoggerOptions(): FastifyServerOptions['logger'] {
  return {
    level: config.LOG_LEVEL,
    transport:
      config.NODE_ENV === 'development'
        ? { target: 'pino-pretty', options: { colorize: true } }
        : undefined,
  };
}

export async function createApp(options: CreateAppOptions = {}): Promise<FastifyInstance> {
  const metrics = getObservabilityMetrics();
  const app = Fastify({
    logger: options.logger === undefined ? defaultLoggerOptions() : options.logger,
    // Honour an inbound `x-request-id` and expose it on the response. When the
    // client does not supply one, generate a UUID per request (issue #34).
    requestIdHeader: 'x-request-id',
    requestIdLogLabel: 'requestId',
    genReqId: () => generateRequestId(),
  });

  await registerSecurity(app);
  await app.register(compress, { global: true });

  app.addHook('onRequest', (request, reply, done) => {
    void reply.header('x-request-id', request.id);
    // Start the HTTP latency timer used by the metrics onResponse hook.
    (request as unknown as { metricsStart?: bigint }).metricsStart =
      process.hrtime.bigint();
    done();
  });

  app.addHook('onResponse', (request, reply, done) => {
    const start = (request as unknown as { metricsStart?: bigint }).metricsStart;
    const requestAny = request as unknown as {
      routeOptions?: { url?: string };
      routerPath?: string;
      metricsStart?: bigint;
    };
    const route = requestAny.routeOptions?.url ?? requestAny.routerPath ?? UNMATCHED_ROUTE;
    // Never count the scrape itself, or Prometheus would feed itself.
    if (route === '/metrics' || start === undefined) {
      done();
      return;
    }
    const durationSeconds = Number(process.hrtime.bigint() - start) / 1e9;
    metrics.recordHttpRequest({
      route,
      method: request.method,
      status: reply.statusCode,
      durationSeconds,
    });
    done();
  });

  setErrorHandler(app);

  // Prometheus scrape endpoint (issue #35). Registered outside `/v1` so the
  // scrape path is stable and unauthenticated, and separate from the public
  // API surface the schema/docs describe.
  app.get('/metrics', async (_request, reply) => {
    reply.header('content-type', defaultRegistry.contentType);
    return reply.send(await defaultRegistry.metrics());
  });

  // API key authentication (issue #30) and the per-key rate limit
  // (issue #31). Both are optional so the health endpoint and the read-only
  // flow endpoints keep working without a configured store; a protected
  // route then rejects every request rather than trusting an unverified
  // token.
  const requireApiKey = options.apiKeyStore
    ? createApiKeyGuard({ store: options.apiKeyStore })
    : undefined;
  const rateLimit = options.rateLimiter
    ? createRateLimitGuard({ limiter: options.rateLimiter })
    : undefined;

  await app.register(
    (v1, _opts, done) => {
      registerHealthRoutes(
        v1,
        {
          checkDatabase: options.checkDatabase,
          checkRedis: options.checkRedis,
          checkHorizon: options.checkHorizon,
          checkRpc: options.checkRpc,
          getLatestLedger: options.getLatestLedger,
        },
        () => undefined,
      );

      void v1.register(
        (flows, _flowOpts, flowsDone) => {
          registerAssetFlowRoutes(flows, { repository: options.repository }, () => undefined);
          registerAccountFlowRoutes(
            flows,
            { repository: options.accountFlowRepository },
            () => undefined,
          );
          registerCorridorFlowRoutes(
            flows,
            { repository: options.corridorFlowRepository },
            () => undefined,
          );
          flowsDone();
        },
        { prefix: '/flows' },
      );

      registerSubscriptionRoutes(
        v1,
        {
          store: options.store,
          requireApiKey,
          rateLimit,
        },
        () => undefined,
      );

      registerTransactionStreamRoutes(v1, options);
      done();
    },
    { prefix: '/v1' },
  );

  return app;
}
