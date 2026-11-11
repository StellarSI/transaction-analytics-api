import type Redis from 'ioredis';
import { logger } from '../../logger';

/** Default key used in Redis to persist the Horizon paging token. */
export const DEFAULT_HORIZON_CURSOR_KEY = 'ingestor:horizon:cursor';

export interface CheckpointOptions {
  redis: Pick<Redis, 'get' | 'set' | 'del'>;
  key?: string;
  saveInterval?: number; // persist every N operations, default 10
}

export interface CheckpointMetrics {
  /** Age in milliseconds since the cursor was last persisted to storage (null if never saved). */
  checkpointAgeMs: number | null;
  /** Number of detected cursor gaps or stale-cursor resets during the session. */
  gapCount: number;
  /** Number of operations processed since the last persisted checkpoint. */
  opsSinceLastSave: number;
}

export interface CheckpointManager {
  /** Retrieves the saved cursor, or undefined if no checkpoint exists. */
  loadCursor: () => Promise<string | undefined>;
  /** Updates the in-memory cursor and conditionally persists to Redis every saveInterval. */
  recordCursor: (cursor: string) => Promise<void>;
  /** Forces persisting the current cursor immediately (e.g. on shutdown/flush). */
  flush: () => Promise<void>;
  /** Clears a stale or pruned cursor (e.g. Horizon 410 Gone) and increments gap metrics so ingestion can fallback to 'now'. */
  handleStaleCursor: (reason?: string) => Promise<string>;
  /** Records an ingestion gap when a discontinuity in paging tokens or ledgers is detected. */
  recordGap: (expectedCursor?: string, actualCursor?: string) => void;
  /** Returns current checkpoint telemetry (age, gap count, pending ops). */
  getMetrics: () => CheckpointMetrics;
  /** Returns the current active cursor in memory. */
  getCurrentCursor: () => string | undefined;
}

export function createCheckpointManager(options: CheckpointOptions): CheckpointManager {
  const { redis, key = DEFAULT_HORIZON_CURSOR_KEY, saveInterval = 10 } = options;
  let currentCursor: string | undefined;
  let opsSinceLastSave = 0;
  let lastSavedAtMs: number | null = null;
  let gapCount = 0;

  return {
    async loadCursor(): Promise<string | undefined> {
      try {
        const val = await redis.get(key);
        if (val && typeof val === 'string' && val.trim().length > 0) {
          currentCursor = val.trim();
          lastSavedAtMs = Date.now();
          logger.info({ cursor: currentCursor, key }, 'checkpoint: loaded Horizon cursor from store');
          return currentCursor;
        }
      } catch (err) {
        logger.warn({ err, key }, 'checkpoint: failed to read cursor, falling back to clean start');
      }
      return undefined;
    },

    async recordCursor(cursor: string): Promise<void> {
      if (!cursor || typeof cursor !== 'string') return;
      currentCursor = cursor;
      opsSinceLastSave++;

      if (opsSinceLastSave >= saveInterval) {
        opsSinceLastSave = 0;
        await this.flush();
      }
    },

    async flush(): Promise<void> {
      if (!currentCursor) return;
      const cursorToSave = currentCursor;
      opsSinceLastSave = 0;
      try {
        await redis.set(key, cursorToSave);
        lastSavedAtMs = Date.now();
        logger.debug({ cursor: cursorToSave, key }, 'checkpoint: successfully persisted Horizon cursor');
      } catch (err) {
        logger.error({ err, key, cursor: cursorToSave }, 'checkpoint: failed to persist Horizon cursor');
      }
    },

    async handleStaleCursor(reason = 'horizon_history_pruned'): Promise<string> {
      const staleCursor = currentCursor;
      currentCursor = undefined;
      opsSinceLastSave = 0;
      gapCount++;
      try {
        await redis.del(key);
      } catch (err) {
        logger.warn({ err, key }, 'checkpoint: failed to delete stale cursor from Redis');
      }
      logger.warn(
        { staleCursor, key, reason, gapCount },
        'checkpoint: stale Horizon cursor cleared; falling back to stream from now'
      );
      return 'now';
    },

    recordGap(expectedCursor?: string, actualCursor?: string): void {
      gapCount++;
      logger.warn(
        { expectedCursor, actualCursor, gapCount, key },
        'checkpoint: ingestion cursor gap detected'
      );
    },

    getMetrics(): CheckpointMetrics {
      return {
        checkpointAgeMs: lastSavedAtMs !== null ? Math.max(0, Date.now() - lastSavedAtMs) : null,
        gapCount,
        opsSinceLastSave,
      };
    },

    getCurrentCursor(): string | undefined {
      return currentCursor;
    },
  };
}
