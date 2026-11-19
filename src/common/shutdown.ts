/**
 * Graceful shutdown (issue #36).
 *
 * Registers SIGTERM/SIGINT handlers that perform a coordinated shutdown:
 *   1. Stop accepting new HTTP requests (close the Fastify server).
 *   2. Drain BullMQ workers, waiting for active jobs to complete.
 *   3. Close the DB pool.
 *   4. Close the Redis connection.
 *   5. Close any SSE connections (handled by the server close).
 *   6. Force exit after a 30s timeout if anything hangs.
 *
 * The health endpoint at GET /v1/health reports "draining" while shutdown
 * is in progress so operators can distinguish a planned restart from a crash.
 */

import type { FastifyInstance } from 'fastify';
import { closePool } from '../db/pool';
import { closeRedis } from '../redis/client';

/** Whether a graceful shutdown has been requested. */
let shutdownInProgress = false;

/** Force-exit timeout: 30 seconds (issue #36). */
export const SHUTDOWN_TIMEOUT_MS = 30_000;

/**
 * Returns whether shutdown is in progress. Used by the health route to report
 * the "draining" status (issue #36).
 */
export function getDrainingStatus(): boolean {
  return shutdownInProgress;
}

/**
 * Performs the graceful shutdown sequence. Safe to call more than once —
 * subsequent calls are no-ops while a shutdown is already in progress.
 */
export async function triggerShutdown(app: FastifyInstance): Promise<void> {
  if (shutdownInProgress) return;
  shutdownInProgress = true;

  app.log.info('shutdown: SIGTERM/SIGINT received, starting graceful shutdown');

  // Force exit after 30s if any cleanup step hangs (issue #36).
  const forceExitTimer = setTimeout(() => {
    app.log.warn('shutdown: 30s timeout reached, forcing exit');
    process.exit(1);
  }, SHUTDOWN_TIMEOUT_MS);
  // The timer must not keep the event loop alive on its own.
  forceExitTimer.unref();

  try {
    // 1. Stop accepting new requests and let in-flight requests finish.
    await app.close();
    app.log.info('shutdown: HTTP server closed');

    // 2. Drain BullMQ workers (wait for active jobs). Workers are registered
    //    with `registerWorkerDrain`; each drain promise is awaited in turn.
    for (const drain of workerDrains) {
      await drain();
    }
    app.log.info('shutdown: queue workers drained');

    // 3. Close the DB pool (issue #36).
    await closePool();
    app.log.info('shutdown: DB pool closed');

    // 4. Close the shared Redis connection (issue #36).
    await closeRedis();
    app.log.info('shutdown: Redis connection closed');

    // 5. SSE connections are closed as part of `app.close()` because the
    //    reply streams are tied to the underlying HTTP connections.
  } catch (error) {
    app.log.error({ err: error }, 'shutdown: error during graceful shutdown');
  } finally {
    clearTimeout(forceExitTimer);
  }
}

/** Optional hooks that drain BullMQ workers before the DB/Redis close. */
const workerDrains: Array<() => Promise<void>> = [];

/**
 * Registers a worker drain hook invoked during graceful shutdown. Tests and
 * the BullMQ worker bootstrap can use this to wait for active jobs before
 * the process exits (issue #36).
 */
export function registerWorkerDrain(drain: () => Promise<void>): void {
  workerDrains.push(drain);
}

/** Resets shutdown state — exported for tests only. */
export function resetShutdownState(): void {
  shutdownInProgress = false;
  workerDrains.length = 0;
}

/**
 * Registers SIGTERM/SIGINT handlers that start a graceful shutdown. A plain
 * (non-async) listener is used to satisfy the `no-misused-promises` rule;
 * the returned promise is explicitly ignored with `void`.
 */
export function registerSignals(app: FastifyInstance): void {
  const shutdown = (): void => {
    void triggerShutdown(app);
  };

  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
}
