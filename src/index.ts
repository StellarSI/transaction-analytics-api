import { config } from './config';
import { pool } from './db/pool';
import { runMigrations } from './db/migrate';
import { getRedis } from './redis/client';
import { createQueueRegistry, type QueueRegistry } from './queue';
import { startQueueWorkers, type QueueWorkers } from './queue/processors';
import { createApp } from './api/server';
import {
  createPostgresApiKeyStore,
  type ApiKeySql,
} from './auth/api-keys';
import { createRedisRateLimiter } from './auth/rate-limit';
import {
  createPostgresAccountFlowRepository,
  type AccountFlowSql as AccountFlowQuerySql,
} from './api/repository/account-flows';
import {
  createPostgresAssetFlowRepository,
  type AssetFlowSql as AssetFlowQuerySql,
} from './api/repository/asset-flows';
import {
  createPostgresCorridorFlowRepository,
  type CorridorFlowSql as CorridorFlowQuerySql,
} from './api/repository/corridor-flows';
import {
  createPostgresSubscriptionStore,
  type SubscriptionSql,
} from './api/repository/subscriptions';
import { getObservabilityMetrics } from './observability/metrics';
import {
  createAssetFlowAggregationHandler,
  createAssetFlowHandlers,
  createPostgresAssetFlowStore,
  type AssetFlowSql,
} from './aggregator/asset-flows';
import {
  createAccountFlowAggregationHandler,
  createAccountFlowHandlers,
  createPostgresAccountFlowStore,
  type AccountFlowSql,
} from './aggregator/account-insights';
import {
  createTrustlineAggregationHandler,
  createTrustlineHandlers,
  createPostgresTrustlineStore,
  type TrustlineSql,
} from './aggregator/trustlines';
import {
  createCorridorAggregationHandler,
  createCorridorHandlers,
  createPostgresCorridorStore,
  type CorridorSql,
} from './aggregator/corridors';
import { TRANSACTIONS_STREAM_CHANNEL } from './api/routes/transactions/stream';
import { registerWorkerDrain, triggerShutdown } from './common/shutdown';

async function start(): Promise<void> {
  // HTTP API (issue #23): Fastify with helmet/CORS/compression, request ids,
  // RFC 7807 errors and the /v1 routes (health + flow/stream endpoints).
  const app = await createApp({
    repository: createPostgresAssetFlowRepository(pool as unknown as AssetFlowQuerySql),
    accountFlowRepository: createPostgresAccountFlowRepository(
      pool as unknown as AccountFlowQuerySql,
    ),
    corridorFlowRepository: createPostgresCorridorFlowRepository(
      pool as unknown as CorridorFlowQuerySql,
    ),
    store: createPostgresSubscriptionStore(pool as unknown as SubscriptionSql),
    // API key authentication (issue #30) and the per-key rate limit
    // (issue #31) share the pool and the Redis client with the rest of the
    // application.
    apiKeyStore: createPostgresApiKeyStore(pool as unknown as ApiKeySql),
    rateLimiter: createRedisRateLimiter(getRedis()),
  });

  let queueRegistry: QueueRegistry | null = null;
  let queueWorkers: QueueWorkers | null = null;


  try {
    const { applied } = await runMigrations();
    app.log.info({ applied }, 'Database migrations applied');

    // Queue layer (issue #15): consumers for the ledger-operation and
    // aggregation-job queues. The asset flow aggregator (issue #18) consumes
    // payments and path payments and recomputes buckets enqueued by the
    // backfill; kinds without handlers are validated and acknowledged so the
    // queue never wedges while the remaining aggregation passes land.
    queueRegistry = createQueueRegistry();
    // The pool satisfies the tagged-template surface structurally; narrow
    // explicitly at the boundary like the other stores do.
    const assetFlowStore = createPostgresAssetFlowStore(pool as unknown as AssetFlowSql);
    const assetHandlers = createAssetFlowHandlers({ store: assetFlowStore });
    const accountFlowStore = createPostgresAccountFlowStore(pool as unknown as AccountFlowSql);
    const accountHandlers = createAccountFlowHandlers({ store: accountFlowStore });
    const trustlineStore = createPostgresTrustlineStore(pool as unknown as TrustlineSql);
    const trustlineHandlers = createTrustlineHandlers({ store: trustlineStore });
    const corridorStore = createPostgresCorridorStore(pool as unknown as CorridorSql);
    const corridorHandlers = createCorridorHandlers({ store: corridorStore });
    const aggregationHandlers = {
      'asset-flow': createAssetFlowAggregationHandler({ store: assetFlowStore }),
      'account-flow': createAccountFlowAggregationHandler({ store: accountFlowStore }),
      trustline: createTrustlineAggregationHandler({ store: trustlineStore }),
      corridor: createCorridorAggregationHandler({ store: corridorStore }),
    };
    const baseHandlers = {
      ...assetHandlers,
      ...accountHandlers,
      ...trustlineHandlers,
      ...corridorHandlers,
    };

    // Publish ingested ledger operations to the transaction stream channel so
    // `GET /v1/transactions/stream` can fan them out as SSE events (issue #27).
    const publishTransaction = async (payload: unknown): Promise<void> => {
      try {
        const redis = getRedis();
        const record = payload as { id?: string; cursor?: string };
        const id = record?.id ?? record?.cursor ?? `${Date.now()}`;
        await redis.publish(
          TRANSACTIONS_STREAM_CHANNEL,
          JSON.stringify({ id, data: payload }),
        );
      } catch (err) {
        app.log.warn(err, 'Failed to publish transaction stream event');
      }
    };

    const handlers = Object.fromEntries(
      Object.entries(baseHandlers).map(([kind, handler]) => [
        kind,
        async (payload: unknown, job: unknown) => {
          await (handler as (p: unknown, j: unknown) => Promise<void> | void)(payload, job);
          await publishTransaction(payload);
        },
      ]),
    ) as typeof baseHandlers;

    // Time every aggregation pass into `eta_aggregation_duration_seconds` so
    // dashboards can chart aggregation latency per kind (issue #35).
    const obsMetrics = getObservabilityMetrics();
    const timedAggregationHandlers = Object.fromEntries(
      Object.entries(aggregationHandlers).map(([kind, handler]) => [
        kind,
        async (payload: unknown, job: unknown) => {
          const stop = obsMetrics.aggregationDurationSeconds.startTimer({ kind });
          try {
            await (handler as (p: unknown, j: unknown) => Promise<void> | void)(payload, job);
          } finally {
            stop();
          }
        },
      ]),
    ) as typeof aggregationHandlers;

    queueWorkers = startQueueWorkers({ registry: queueRegistry, handlers, aggregationHandlers: timedAggregationHandlers });
    app.log.info('Queue workers started (ledger-operations, aggregation-jobs)');
    // Drain the BullMQ workers during graceful shutdown (issue #36).
    registerWorkerDrain(async () => {
      await queueWorkers?.close();
      await queueRegistry?.close();
    });

    await app.listen({ port: config.PORT, host: '0.0.0.0' });
    app.log.info(`Server listening on port ${config.PORT}`);

    // Graceful shutdown (issue #23): SIGTERM/SIGINT signal handling is
    // registered by createApp via registerSignals (issue #36).
  } catch (err) {
    app.log.error(err);
    await triggerShutdown(app);
  }
}

void start();
