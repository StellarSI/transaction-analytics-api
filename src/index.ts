import Fastify from 'fastify';
import { config } from './config';
import { closePool, dbHealthCheck } from './db/pool';
import { runMigrations } from './db/migrate';
import { closeRedis } from './redis/client';
import { redisHealthCheck } from './redis/health';
import { createQueueRegistry, type QueueRegistry } from './queue';
import { startQueueWorkers, type QueueWorkers } from './queue/processors';

const app = Fastify({
  logger: {
    level: config.LOG_LEVEL,
    transport:
      config.NODE_ENV === 'development'
        ? { target: 'pino-pretty', options: { colorize: true } }
        : undefined,
  },
});

app.get('/v1/health', async () => {
  const database = await dbHealthCheck();
  const redis = await redisHealthCheck();
  return {
    status: database.ok && redis.ok ? 'ok' : 'degraded',
    timestamp: new Date().toISOString(),
    services: { database, redis },
  };
});

let queueRegistry: QueueRegistry | null = null;
let queueWorkers: QueueWorkers | null = null;

async function start(): Promise<void> {
  try {
    const { applied } = await runMigrations();
    app.log.info({ applied }, 'Database migrations applied');

    // Queue layer (issue #15): consumers for the ledger-operation and
    // aggregation-job queues. The per-kind aggregation handlers land with
    // the aggregation issues (#18, #19, #21); until then validated payloads
    // are acknowledged so the queue stays responsive.
    queueRegistry = createQueueRegistry();
    queueWorkers = startQueueWorkers({ registry: queueRegistry });
    app.log.info('Queue workers started (ledger-operations, aggregation-jobs)');

    await app.listen({ port: config.PORT, host: '0.0.0.0' });
    app.log.info(`Server listening on port ${config.PORT}`);
  } catch (err) {
    app.log.error(err);
    await shutdown(1);
  }
}

void start();

/** Runs the full drain (HTTP, queue, DB, Redis) and exits with `code`. */
async function shutdown(code: 0 | 1): Promise<void> {
  app.log.info('Shutting down');
  await app.close();
  // Drain the workers first: Worker.close() resolves once in-flight jobs
  // finished, so nothing in the pipeline is dropped mid-process.
  try {
    await queueWorkers?.close();
  } catch (err) {
    app.log.warn(err, 'Queue workers reported a non-fatal error while draining');
  }
  await queueRegistry?.close();
  await closePool();
  await closeRedis();
  process.exit(code);
}

process.on('SIGTERM', () => void shutdown(0));
process.on('SIGINT', () => void shutdown(0));
