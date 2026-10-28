import pino from 'pino';
import { config } from './config';

/**
 * Shared application logger.
 *
 * Configured from the same `LOG_LEVEL` / `NODE_ENV` settings the HTTP server
 * uses so that Redis connection events and Horizon ingestion problems show up
 * in a single, consistently formatted log stream.
 */
export const logger = pino({
  level: config.LOG_LEVEL,
  transport:
    config.NODE_ENV === 'development'
      ? { target: 'pino-pretty', options: { colorize: true } }
      : undefined,
});

export type AppLogger = typeof logger;
