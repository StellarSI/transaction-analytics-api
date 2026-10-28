import Redis from 'ioredis';
import { config } from '../config';
import { logger } from '../logger';

/**
 * Shared Redis connection (issue #10).
 *
 * A single client is created lazily from `REDIS_URL` and reused by everything
 * that needs Redis: the ingestion queue, the rate limiter and webhook
 * delivery. ioredis reconnects automatically; the retry strategy below caps
 * the backoff so an outage does not hammer the server but reconnection never
 * stalls permanently.
 */

export const DEFAULT_RECONNECT_BASE_DELAY_MS = 500;
export const DEFAULT_RECONNECT_MAX_DELAY_MS = 30_000;

export interface RedisClientOptions {
  /** Connection string; defaults to `REDIS_URL` from the environment. */
  url?: string;
  /** Base delay for the linear reconnection backoff (ms). */
  reconnectBaseDelayMs?: number;
  /** Upper bound for the reconnection backoff (ms). */
  reconnectMaxDelayMs?: number;
  /**
   * Commands issued while disconnected are retried at most this many times
   * before failing, so callers see errors instead of unbounded memory growth.
   */
  maxRetriesPerRequest?: number;
}

/**
 * Creates a new (connecting) Redis client. Prefer {@link getRedis} unless you
 * need an isolated connection — for example in tests.
 */
export function createRedisClient(options: RedisClientOptions = {}): Redis {
  const url = options.url ?? config.REDIS_URL;
  const baseDelay = options.reconnectBaseDelayMs ?? DEFAULT_RECONNECT_BASE_DELAY_MS;
  const maxDelay = options.reconnectMaxDelayMs ?? DEFAULT_RECONNECT_MAX_DELAY_MS;

  const client = new Redis(url, {
    retryStrategy: (times: number) => Math.min(times * baseDelay, maxDelay),
    maxRetriesPerRequest: options.maxRetriesPerRequest ?? 3,
  });

  attachConnectionLogging(client);
  return client;
}

/**
 * Logs Redis lifecycle events so operators can see connect/reconnect/outage
 * transitions in the application log stream.
 */
export function attachConnectionLogging(client: Redis): Redis {
  client.on('connect', () => logger.info('redis: connecting'));
  client.on('ready', () => logger.info('redis: ready'));
  client.on('error', (error: Error) => logger.error({ err: error }, 'redis: connection error'));
  client.on('close', () => logger.warn('redis: connection closed'));
  client.on('reconnecting', (delay: number) => logger.warn({ delay }, 'redis: reconnecting'));
  client.on('end', () => logger.info('redis: connection ended'));
  return client;
}

let defaultClient: Redis | null = null;

/**
 * Returns the process-wide singleton client, creating it on first use so that
 * merely importing modules does not open a connection (keeps unit tests and
 * CLI tooling socket-free).
 */
export function getRedis(): Redis {
  defaultClient ??= createRedisClient();
  return defaultClient;
}

/**
 * Gracefully shuts the shared connection down: waits for in-flight commands to
 * flush (`QUIT`) and falls back to a hard disconnect if the socket is already
 * gone. Safe to call when no client was ever created. Calling {@link getRedis}
 * afterwards transparently reconnects.
 */
export async function closeRedis(): Promise<void> {
  if (!defaultClient) return;
  const client = defaultClient;
  defaultClient = null;
  try {
    await client.quit();
  } catch {
    client.disconnect();
  }
  logger.info('redis: closed shared connection');
}
