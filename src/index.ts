import Fastify from 'fastify';
import { config } from './config';
import { closePool, dbHealthCheck } from './db/pool';
import { runMigrations } from './db/migrate';
import { closeRedis } from './redis/client';
import { redisHealthCheck } from './redis/health';

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

async function start(): Promise<void> {
  try {
    const { applied } = await runMigrations();
    app.log.info({ applied }, 'Database migrations applied');
    await app.listen({ port: config.PORT, host: '0.0.0.0' });
    app.log.info(`Server listening on port ${config.PORT}`);
  } catch (err) {
    app.log.error(err);
    await closePool();
    process.exit(1);
  }
}

void start();

async function shutdown(): Promise<void> {
  app.log.info('Shutting down');
  await app.close();
  await closePool();
  await closeRedis();
  process.exit(0);
}

process.on('SIGTERM', () => void shutdown());
process.on('SIGINT', () => void shutdown());
