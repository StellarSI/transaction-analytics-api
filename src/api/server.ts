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
import cors from '@fastify/cors';
import helmet from '@fastify/helmet';
import { config } from '../config';
import { setErrorHandler } from './errors';
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
  const app = Fastify({
    logger: options.logger === undefined ? defaultLoggerOptions() : options.logger,
    // Honour an inbound `x-request-id` and expose it on the response.
    requestIdHeader: 'x-request-id',
    requestIdLogLabel: 'requestId',
  });

  await app.register(helmet, { global: true });
  await app.register(cors, { origin: true, credentials: true });
  await app.register(compress, { global: true });

  app.addHook('onRequest', (request, reply, done) => {
    void reply.header('x-request-id', request.id);
    done();
  });

  setErrorHandler(app);

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
        { store: options.store, requireApiKey: options.requireApiKey },
        () => undefined,
      );

      registerTransactionStreamRoutes(v1, options);
      done();
    },
    { prefix: '/v1' },
  );

  return app;
}
