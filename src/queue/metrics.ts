import { Counter, Gauge, Histogram, register, type Registry } from 'prom-client';

/**
 * Prometheus observability for the queue layer (issue #15).
 *
 * Acceptance names three signals: queue depth, processing time and failure
 * rate. They are produced here as:
 * - `queue_depth` — Gauge, refreshed by the worker sweep in `processors.ts`.
 * - `queue_job_processing_seconds` — Histogram, observed by every processor.
 * - `queue_failure_rate` — Gauge, computed by the sweep from cumulative
 *   processed/failed counters so operators can alert on it directly.
 *
 * Counters live here rather than inside the processors so producers and
 * workers share one vocabulary. A custom `Registry` can be injected for
 * tests; production uses the global default registry.
 */

export interface QueueMetrics {
  /** Current number of waiting jobs per queue. */
  depth: Gauge<string>;
  /** Total jobs processed successfully per queue. */
  processedTotal: Counter<string>;
  /** Jobs rejected for an invalid payload per queue. */
  invalidTotal: Counter<string>;
  /** Jobs that threw in their processor per queue (pre-retry). */
  failuresTotal: Counter<string>;
  /** Duration of successful processor runs per queue. */
  processingSeconds: Histogram<string>;
  /** Ratio failures / (processed + failures) per queue, set by the sweep. */
  failureRate: Gauge<string>;
}

/** Metric names are stable — treat renames as a breaking dashboard change. */
export const METRIC_PREFIX = 'eta_queue_';

export function createQueueMetrics(registry: Registry = register): QueueMetrics {
  const labels = ['queue'] as const;
  return {
    depth: new Gauge({
      name: `${METRIC_PREFIX}depth`,
      help: 'Jobs waiting in a queue (waiting + delayed)',
      registers: [registry],
      labelNames: [...labels],
    }),
    processedTotal: new Counter({
      name: `${METRIC_PREFIX}processed_total`,
      help: 'Ledger/aggregation jobs processed to completion',
      registers: [registry],
      labelNames: [...labels],
    }),
    invalidTotal: new Counter({
      name: `${METRIC_PREFIX}invalid_total`,
      help: 'Jobs rejected because their payload failed schema validation',
      registers: [registry],
      labelNames: [...labels],
    }),
    failuresTotal: new Counter({
      name: `${METRIC_PREFIX}failures_total`,
      help: 'Job processing attempts that ended in an error (pre-retry)',
      registers: [registry],
      labelNames: [...labels],
    }),
    processingSeconds: new Histogram({
      name: `${METRIC_PREFIX}job_processing_seconds`,
      help: 'Time spent in a job processor',
      registers: [registry],
      labelNames: [...labels],
      buckets: [0.001, 0.01, 0.05, 0.1, 0.5, 1, 5, 15, 60],
    }),
    failureRate: new Gauge({
      name: `${METRIC_PREFIX}failure_rate`,
      help: 'failures / (processed + failures), refreshed by the metrics sweep',
      registers: [registry],
      labelNames: [...labels],
    }),
  };
}

/**
 * Standard sweep for a queue: records the depth gauge and the failure rate
 * from counter deltas. Called on a timer from the workers module, and safe to
 * reuse from tests with a fake depth lookup.
 */
export async function sweepQueueMetrics(
  metrics: QueueMetrics,
  queueName: string,
  readDepth: () => Promise<number>,
): Promise<void> {
  const depth = await readDepth().catch(() => 0);
  metrics.depth.set({ queue: queueName }, depth);

  const processed = metrics.processedTotal.get({ queue: queueName })?.values[0]?.value ?? 0;
  const failures = metrics.failuresTotal.get({ queue: queueName })?.values[0]?.value ?? 0;
  const total = processed + failures;
  metrics.failureRate.set(
    { queue: queueName },
    total === 0 ? 0 : failures / total,
  );
}
