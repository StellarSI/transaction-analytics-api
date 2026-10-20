import Fastify from 'fastify';
import { config } from './config';
import { closePool, dbHealthCheck } from './db/pool';
import { runMigrations } from './db/migrate';

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
  return {
    status: database.ok ? 'ok' : 'degraded',
    timestamp: new Date().toISOString(),
    services: { database },
  };
});

async function start(): Promise<void> {
  try {
    const { applied } = await runMigrations();
    app.log.info('Database migrations applied', { applied });
    await app.listen({ port: config.PORT, host: '0.0.0.0' });
    app.log.info(`Server listening on port ${config.PORT}`);
  } catch (err) {
    app.log.error(err);
    await closePool();
    process.exit(1);
  }
}

start();

async function shutdown(): Promise<void> {
  app.log.info('Shutting down');
  await app.close();
  await closePool();
  process.exit(0);
}

process.on('SIGTERM', () => void shutdown());
process.on('SIGINT', () => void shutdown());
