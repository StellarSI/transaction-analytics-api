/**
 * Shared application logger (issue #34).
 *
 * One pino logger for the whole process so Horizon ingestion, queue workers,
 * Redis connection events and the HTTP API emit a single, consistently
 * formatted stream:
 *
 * - level from `LOG_LEVEL`; pretty output in development, JSON in production;
 * - request ids: every log line emitted while handling an HTTP request carries
 *   the Fastify request id (`requestId`), generated as a UUID when the
 *   incoming request does not supply one — see `src/api/server.ts`;
 * - child loggers per module via `createModuleLogger(module)` so ingestor /
 *   aggregator / api / queue lines carry a stable `module` binding;
 * - sensitive fields (API keys, salts, authorization headers) are redacted by
 *   the `redact` option — no caller has to remember to strip them;
 * - `src/logger.ts` re-exports this module for backwards compatibility.
 *
 * Tests inject a custom `destination` (a Writable) to capture output lines.
 */

import { randomUUID } from 'node:crypto';
import type { Writable } from 'node:stream';
import pino, { type Logger, type LoggerOptions } from 'pino';
import { config } from '../config';

/** Censor used for redacted values. */
export const REDACTED = '[redacted]';

/**
 * Paths pino redacts before serializing. Header names cover HTTP request
 * logs; the bare field names catch objects that are built manually
 * (configuration fragments, error payloads, webhook bodies, ...).
 */
export const REDACT_PATHS = [
  // HTTP request containers emitted by Fastify.
  'req.headers.authorization',
  'req.headers.cookie',
  'req.headers["x-api-key"]',
  // Bare fields used across the codebase.
  'authorization',
  'cookie',
  'x-api-key',
  'apiKey',
  'api_key',
  'apiKeySalt',
  'API_KEY_SALT',
  'token',
  'accessToken',
  'refreshToken',
  'secret',
  'password',
  'webhookSecret',
  // Nested shapes that depend on upstream input.
  'headers.authorization',
  'headers.cookie',
  'headers["x-api-key"]',
] as const;

export interface CreateLoggerOptions {
  /** Override the level (defaults to `LOG_LEVEL`). */
  level?: string;
  /**
   * Emit pretty output (defaults to `true` in development, `false` otherwise).
   * Ignored when a custom `destination` is injected.
   */
  pretty?: boolean;
  /** Injected stream for tests; defaults to stdout. */
  destination?: Writable;
}

export function createLogger(options: CreateLoggerOptions = {}): Logger {
  const level = options.level ?? config.LOG_LEVEL;
  const usePretty = options.pretty ?? config.NODE_ENV === 'development';

  const loggerOptions: LoggerOptions = {
    level,
    redact: {
      paths: [...REDACT_PATHS],
      censor: REDACTED,
    },
  };
  if (usePretty && !options.destination) {
    loggerOptions.transport = {
      target: 'pino-pretty',
      options: { colorize: true },
    };
  }
  return options.destination
    ? pino(loggerOptions, options.destination as never)
    : pino(loggerOptions);
}

/** Root logger shared by the whole process. */
export const logger = createLogger();

export type AppLogger = typeof logger;

/**
 * Creates a child logger bound to a module name. Modules should call this
 * once at import time instead of relying on the global logger so every line
 * they emit carries a stable `module` binding.
 */
export function createModuleLogger(
  module: string,
  bindings: Record<string, unknown> = {},
): Logger {
  return logger.child({ module, ...bindings });
}

/** Generates the request id used for HTTP requests (UUID v4). */
export function generateRequestId(): string {
  return randomUUID();
}
