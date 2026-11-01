import { type ConnectionOptions, type Job, Worker } from 'bullmq';
import {
  AGGREGATION_JOBS_QUEUE,
  DEFAULT_WORKER_CONCURRENCY,
  LEDGER_OPERATIONS_QUEUE,
  AggregationJobSchema,
  LedgerOperationJobSchema,
  QUEUE_METRICS_SWEEP_MS,
  QueueValidationError,
  type AggregationJob,
  type AggregationJobKind,
  type CreateQueueRegistryOptions,
  type LedgerOperationJob,
  type QueueConnection,
  type QueueRegistry,
} from './index';
import { createQueueMetrics, sweepQueueMetrics, type QueueMetrics } from './metrics';
import { logger } from '../logger';

/**
 * BullMQ workers for the ingestion pipeline (issue #15).
 *
 * Two workers run concurrently:
 * - `ledger-operations` — consumes parsed operations pushed by the ingestors.
 *   The per-kind handlers land with the aggregation issues (#18, #19, #21);
 *   kinds without a registered handler are validated and acknowledged so the
 *   queue never wedges while handlers roll out.
 * - `aggregation-jobs` — consumes time-bucketed aggregation passes produced
 *   by the store/aggregator layer (tracked in #18–#21).
 *
 * Reliability: retries (3 attempts, exponential backoff) come from the
 * registry's `defaultJobOptions`; validation failures and processing errors
 * are counted in the metrics, then rethrown so BullMQ performs the retry.
 * Permanent failures remain in the queue for inspection until the dead letter
 * queue from issue #98 takes over.
 *
 * Shutdown: `close()` waits for the in-flight job of every worker to finish
 * (BullMQ's executor drain) and stops the metrics sweep, so SIGTERM handlers
 * can simply await it — no operation is dropped mid-process.
 */
export type LedgerOperationHandler<K extends LedgerOperationJob['kind']> = (
  payload: Extract<LedgerOperationJob, { kind: K }>,
  job: Job<LedgerOperationJob>,
) => Promise<void> | void;

/** Handlers keyed by operation kind; any subset may be provided. */
export type LedgerOperationHandlers = {
  [K in LedgerOperationJob['kind']]?: LedgerOperationHandler<K>;
};

export type AggregationJobHandler<K extends AggregationJobKind = AggregationJobKind> = (
  payload: AggregationJob,
  job: Job<AggregationJob>,
) => Promise<void> | void;

/** Handlers keyed by aggregation kind; any subset may be provided. */
export type AggregationJobHandlers = {
  [K in AggregationJobKind]?: AggregationJobHandler<K>;
};

/**
 * Processes one `ledger-operations` job: validates the payload, increments
 * the right counters and dispatches to the registered handler, if any.
 * Exported for tests; `startQueueWorkers` wires it into a real Worker.
 */
export function createLedgerOperationsProcessor(
  handlers: LedgerOperationHandlers = {},
  metrics?: QueueMetrics,
): (job: Job<LedgerOperationJob>) => Promise<void> {
  return async (job) => {
    const stopTimer = metrics?.processingSeconds.startTimer({
      queue: LEDGER_OPERATIONS_QUEUE,
    });
    try {
      const parsed = LedgerOperationJobSchema.safeParse(job.data);
      if (!parsed.success) {
        metrics?.invalidTotal.inc({ queue: LEDGER_OPERATIONS_QUEUE });
        throw new QueueValidationError(
          `queue: invalid ledger operation (jobId=${job.id ?? 'unknown'})`,
          parsed.error.issues,
        );
      }

      const payload = parsed.data;
      // The handler map is keyed by kind; this cast collapses the union-keyed
      // lookup into a plain call — the extracted handler already receives the
      // exact narrowed payload for that kind.
      const handler = handlers[payload.kind] as unknown as
        | ((p: LedgerOperationJob, j: Job<LedgerOperationJob>) => Promise<void> | void)
        | undefined;

      if (handler) {
        await handler(payload, job);
      } else {
        // Handlers are the aggregation passes from #18/#19/#21; until they
        // land, ack validated payloads instead of failing the queue.
        logger.debug(
          { kind: payload.kind, jobId: job.id },
          'queue: no ledger handler registered, acknowledging',
        );
      }

      metrics?.processedTotal.inc({ queue: LEDGER_OPERATIONS_QUEUE });
    } catch (error) {
      metrics?.failuresTotal.inc({ queue: LEDGER_OPERATIONS_QUEUE });
      logger.error(
        { err: error, jobId: job.id },
        'queue: ledger operation processing failed, BullMQ will retry',
      );
      throw error;
    } finally {
      stopTimer?.();
    }
  };
}

/**
 * Processes one `aggregation-jobs` job. Unlike ledger operations, every
 * aggregation kind must have a handler: an aggregation pass that cannot run
 * is a lost bucket, so it is retried by BullMQ and eventually fails loudly.
 */
export function createAggregationProcessor(
  handlers: AggregationJobHandlers = {},
  metrics?: QueueMetrics,
): (job: Job<AggregationJob>) => Promise<void> {
  return async (job) => {
    const stopTimer = metrics?.processingSeconds.startTimer({
      queue: AGGREGATION_JOBS_QUEUE,
    });
    try {
      const parsed = AggregationJobSchema.safeParse(job.data);
      if (!parsed.success) {
        metrics?.invalidTotal.inc({ queue: AGGREGATION_JOBS_QUEUE });
        throw new QueueValidationError(
          `queue: invalid aggregation job (jobId=${job.id ?? 'unknown'})`,
          parsed.error.issues,
        );
      }

      const kind = parsed.data.kind;
      const handler = handlers[kind];
      if (!handler) {
        metrics?.failuresTotal.inc({ queue: AGGREGATION_JOBS_QUEUE });
        throw new Error(
          `queue: no aggregation handler registered for kind ${kind}`,
        );
      }
      await handler(parsed.data, job);
      metrics?.processedTotal.inc({ queue: AGGREGATION_JOBS_QUEUE });
    } catch (error) {
      metrics?.failuresTotal.inc({ queue: AGGREGATION_JOBS_QUEUE });
      logger.error(
        { err: error, jobId: job.id, kind: job.data?.kind },
        'queue: aggregation job failed, BullMQ will retry',
      );
      throw error;
    } finally {
      stopTimer?.();
    }
  };
}

/** The two workers plus a shutdown hook that drains active jobs. */
export interface QueueWorkers {
  readonly ledgerOperations: Worker<LedgerOperationJob>;
  readonly aggregationJobs: Worker<AggregationJob>;
  /**
   * Stops the metrics sweep and closes both workers. In-flight jobs are
   * allowed to finish before the promise resolves.
   */
  close(): Promise<void>;
}

export interface StartQueueWorkersOptions {
  /** Registry providing the queues (and their retry policy). */
  registry: QueueRegistry;
  /**
   * Worker-side connection; defaults to the same options used for the
   * queues. Workers need their own blocking connection, so they never share
   * the producer's client.
   */
  connection?: QueueConnection | CreateQueueRegistryOptions['connection'];
  /** Parallelism of each worker process loop. */
  concurrency?: number;
  /** Optional per-kind ledger operation handlers. */
  handlers?: LedgerOperationHandlers;
  /** Optional per-kind aggregation handlers. */
  aggregationHandlers?: AggregationJobHandlers;
  /** Injected metrics (a fresh instance is created by default). */
  metrics?: QueueMetrics;
  /** Interval for the depth/failure-rate sweep; 0 disables the sweep. */
  sweepIntervalMs?: number;
}

export function startQueueWorkers(
  options: StartQueueWorkersOptions,
): QueueWorkers {
  const registry = options.registry;
  const metrics = options.metrics ?? createQueueMetrics();
  const concurrency = options.concurrency ?? DEFAULT_WORKER_CONCURRENCY;
  const sweepIntervalMs =
    options.sweepIntervalMs ?? QUEUE_METRICS_SWEEP_MS;

  const workerOptions = {
    connection: options.connection as ConnectionOptions | undefined,
    concurrency,
  };

  const ledgerOperations = new Worker<LedgerOperationJob>(
    LEDGER_OPERATIONS_QUEUE,
    createLedgerOperationsProcessor(options.handlers, metrics),
    workerOptions,
  );
  const aggregationJobs = new Worker<AggregationJob>(
    AGGREGATION_JOBS_QUEUE,
    createAggregationProcessor(options.aggregationHandlers, metrics),
    workerOptions,
  );

  // Periodic sweep for the gauges prom-client cannot compute by itself.
  const sweep =
    sweepIntervalMs > 0
      ? setInterval(() => {
          void sweepQueueMetrics(
            metrics,
            LEDGER_OPERATIONS_QUEUE,
            async () => {
              const counts = await registry.ledgerOperations.getJobCounts();
              return counts.waiting + counts.delayed;
            },
          );
          void sweepQueueMetrics(
            metrics,
            AGGREGATION_JOBS_QUEUE,
            async () => {
              const counts = await registry.aggregationJobs.getJobCounts();
              return counts.waiting + counts.delayed;
            },
          );
        }, sweepIntervalMs)
      : null;
  sweep?.unref?.();

  return {
    ledgerOperations,
    aggregationJobs,
    async close() {
      if (sweep) clearInterval(sweep);
      // Worker.close() resolves once the running job finished: the drain.
      await Promise.all([ledgerOperations.close(), aggregationJobs.close()]);
      logger.info('queue: workers drained and closed');
    },
  };
}
