import { Queue } from 'bullmq';
import { z } from 'zod';
import {
  DEFAULT_JOB_ATTEMPTS,
  createQueueConnection,
  type QueueConnection,
} from './index';
import { logger } from '../logger';

/**
 * Dead Letter Queue (DLQ) support for ingestion and pipeline failures (issue #98).
 *
 * Captures failed ledger operations and aggregation tasks once their max
 * retry attempts have been exhausted. Provides structured failure diagnostics,
 * payload isolation, error stack serialization, retention policies, and
 * administrative replay/inspection capabilities.
 */

export const DEAD_LETTER_QUEUE = 'dead-letter-queue';

/** Default retention for dead-letter items (30 days in ms). */
export const DEFAULT_DLQ_RETENTION_MS = 30 * 24 * 60 * 60 * 1_000;

/** Default threshold of DLQ items to trigger alerting. */
export const DEFAULT_DLQ_ALERT_THRESHOLD = 100;

/* ------------------------------------------------------------------ */
/* Dead Letter Schemas & Types                                        */
/* ------------------------------------------------------------------ */

export const FailedMessageMetadataSchema = z.object({
  originalQueue: z.string().min(1),
  originalJobId: z.string().optional(),
  originalJobName: z.string().optional(),
  failedAt: z.string().datetime(),
  attemptsMade: z.number().int().nonnegative().default(DEFAULT_JOB_ATTEMPTS),
  errorMessage: z.string(),
  errorStack: z.string().optional(),
  failureReason: z.string().optional(),
});

export type FailedMessageMetadata = z.infer<typeof FailedMessageMetadataSchema>;

export interface DeadLetterPayload<T = unknown> {
  metadata: FailedMessageMetadata;
  payload: T;
}

export interface DeadLetterItem<T = unknown> {
  id: string;
  metadata: FailedMessageMetadata;
  payload: T;
  timestamp: number;
}

export interface DLQInspectionFilter {
  originalQueue?: string;
  limit?: number;
  offset?: number;
}

export interface DLQAlertStatus {
  size: number;
  threshold: number;
  alert: boolean;
  message?: string;
}

/* ------------------------------------------------------------------ */
/* DeadLetterQueueManager Interface & Implementation                   */
/* ------------------------------------------------------------------ */

export interface DeadLetterQueueManager {
  readonly queue: Queue<DeadLetterPayload>;
  /** Sends a failed payload directly into the DLQ with error context. */
  sendToDeadLetter<T = unknown>(
    originalQueue: string,
    payload: T,
    error: Error | string,
    options?: {
      jobId?: string;
      jobName?: string;
      attemptsMade?: number;
      failureReason?: string;
    },
  ): Promise<string>;
  /** Returns the current count of dead letter items. */
  getDeadLetterCount(): Promise<number>;
  /** Checks if the DLQ size exceeds the configured alert threshold. */
  checkAlertThreshold(threshold?: number): Promise<DLQAlertStatus>;
  /** Lists dead letter items with optional filtering and pagination. */
  listDeadLetters(filter?: DLQInspectionFilter): Promise<DeadLetterItem[]>;
  /** Retrieves a single dead letter item by id. */
  getDeadLetter(id: string): Promise<DeadLetterItem | null>;
  /** Removes an item from the DLQ by id. */
  deleteDeadLetter(id: string): Promise<boolean>;
  /** Clears all items in the dead letter queue. */
  purgeDeadLetters(): Promise<void>;
  /**
   * Replays a failed item back to its target destination queue.
   * If targetQueue is omitted, defaults to metadata.originalQueue.
   */
  replayDeadLetter(
    id: string,
    targetQueue?: Pick<Queue<unknown>, 'name' | 'add'>,
  ): Promise<{ replayed: boolean; targetQueue: string; jobId?: string }>;
  close(): Promise<void>;
}

export interface CreateDeadLetterQueueOptions {
  connection?: QueueConnection;
  redisUrl?: string;
  alertThreshold?: number;
  retentionMs?: number;
}

export function createDeadLetterQueueManager(
  options: CreateDeadLetterQueueOptions = {},
): DeadLetterQueueManager {
  const connection: QueueConnection =
    options.connection ?? createQueueConnection(options.redisUrl);

  const retentionMs = options.retentionMs ?? DEFAULT_DLQ_RETENTION_MS;
  const alertThreshold = options.alertThreshold ?? DEFAULT_DLQ_ALERT_THRESHOLD;

  // BullMQ queue configured with 30-day retention for dead letter messages
  const queue = new Queue<DeadLetterPayload>(DEAD_LETTER_QUEUE, {
    connection,
    defaultJobOptions: {
      removeOnComplete: {
        age: Math.floor(retentionMs / 1000),
      },
      removeOnFail: {
        age: Math.floor(retentionMs / 1000),
      },
    },
  });

  return {
    queue,

    async sendToDeadLetter<T = unknown>(
      originalQueue: string,
      payload: T,
      error: Error | string,
      jobOptions?: {
        jobId?: string;
        jobName?: string;
        attemptsMade?: number;
        failureReason?: string;
      },
    ): Promise<string> {
      const errorMessage = typeof error === 'string' ? error : error.message;
      const errorStack = typeof error === 'string' ? undefined : error.stack;

      const metadata: FailedMessageMetadata = {
        originalQueue,
        originalJobId: jobOptions?.jobId,
        originalJobName: jobOptions?.jobName,
        failedAt: new Date().toISOString(),
        attemptsMade: jobOptions?.attemptsMade ?? DEFAULT_JOB_ATTEMPTS,
        errorMessage,
        errorStack,
        failureReason: jobOptions?.failureReason ?? errorMessage,
      };

      const dlqPayload: DeadLetterPayload<T> = {
        metadata,
        payload,
      };

      const job = await queue.add(
        `dlq:${originalQueue}`,
        dlqPayload as unknown as DeadLetterPayload,
        {
          jobId: jobOptions?.jobId ? `dlq_${jobOptions.jobId}` : undefined,
        },
      );

      logger.warn(
        {
          dlqJobId: job.id,
          originalQueue,
          originalJobId: jobOptions?.jobId,
          reason: metadata.failureReason,
        },
        'queue: message moved to dead letter queue',
      );

      return job.id ?? 'unknown';
    },

    async getDeadLetterCount(): Promise<number> {
      const counts = await queue.getJobCounts('waiting', 'delayed', 'completed', 'failed');
      return (counts.waiting ?? 0) + (counts.delayed ?? 0) + (counts.completed ?? 0);
    },

    async checkAlertThreshold(threshold = alertThreshold): Promise<DLQAlertStatus> {
      const size = await this.getDeadLetterCount();
      const alert = size >= threshold;
      return {
        size,
        threshold,
        alert,
        message: alert
          ? `WARNING: Dead Letter Queue size (${size}) has exceeded threshold (${threshold})!`
          : undefined,
      };
    },

    async listDeadLetters(filter?: DLQInspectionFilter): Promise<DeadLetterItem[]> {
      const offset = filter?.offset ?? 0;
      const limit = filter?.limit ?? 50;

      const jobs = await queue.getJobs(
        ['waiting', 'completed', 'delayed'],
        offset,
        offset + limit - 1,
        true,
      );

      const items: DeadLetterItem[] = [];
      for (const job of jobs) {
        if (!job?.data?.metadata) continue;
        if (
          filter?.originalQueue &&
          job.data.metadata.originalQueue !== filter.originalQueue
        ) {
          continue;
        }

        items.push({
          id: String(job.id),
          metadata: job.data.metadata,
          payload: job.data.payload,
          timestamp: job.timestamp,
        });
      }

      return items;
    },

    async getDeadLetter(id: string): Promise<DeadLetterItem | null> {
      const job = await queue.getJob(id);
      if (!job || !job.data?.metadata) {
        return null;
      }

      return {
        id: String(job.id),
        metadata: job.data.metadata,
        payload: job.data.payload,
        timestamp: job.timestamp,
      };
    },

    async deleteDeadLetter(id: string): Promise<boolean> {
      const job = await queue.getJob(id);
      if (!job) {
        return false;
      }
      await job.remove();
      logger.info({ dlqId: id }, 'queue: removed item from dead letter queue');
      return true;
    },

    async purgeDeadLetters(): Promise<void> {
      await queue.drain(true);
      await queue.clean(0, 0, 'completed');
      await queue.clean(0, 0, 'wait');
      logger.info('queue: dead letter queue purged');
    },

    async replayDeadLetter(
      id: string,
      targetQueue?: Pick<Queue<unknown>, 'name' | 'add'>,
    ): Promise<{ replayed: boolean; targetQueue: string; jobId?: string }> {
      const item = await this.getDeadLetter(id);
      if (!item) {
        return { replayed: false, targetQueue: 'none' };
      }

      const queueName = targetQueue ? targetQueue.name : item.metadata.originalQueue;

      if (!targetQueue) {
        throw new Error(
          `Cannot replay DLQ job '${id}': no active target Queue provided for queue '${queueName}'`,
        );
      }

      const jobName = item.metadata.originalJobName ?? 'replayed_job';
      const enqueued = await targetQueue.add(jobName, item.payload);

      await this.deleteDeadLetter(id);

      logger.info(
        {
          dlqId: id,
          targetQueue: queueName,
          newJobId: enqueued.id,
        },
        'queue: replayed dead letter item back to queue',
      );

      return {
        replayed: true,
        targetQueue: queueName,
        jobId: enqueued.id ? String(enqueued.id) : undefined,
      };
    },

    async close(): Promise<void> {
      await queue.close();
    },
  };
}
