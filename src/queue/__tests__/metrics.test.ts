import { describe, it, expect, beforeEach } from 'vitest';
import { Registry } from 'prom-client';
import { createQueueMetrics, sweepQueueMetrics } from '../metrics';
import { AGGREGATION_JOBS_QUEUE, LEDGER_OPERATIONS_QUEUE } from '../index';

let registry: Registry;
let metrics: ReturnType<typeof createQueueMetrics>;

describe('queue metrics', () => {
  beforeEach(() => {
    registry = new Registry();
    metrics = createQueueMetrics(registry);
  });

  it('registers the depth, failure-rate and histogram families', () => {
    const names = registry
      .getMetricsAsArray()
      .map((m) => m.name);
    expect(names).toEqual(
      expect.arrayContaining([
        'eta_queue_depth',
        'eta_queue_processed_total',
        'eta_queue_invalid_total',
        'eta_queue_failures_total',
        'eta_queue_job_processing_seconds',
        'eta_queue_failure_rate',
      ]),
    );
  });

  it('records processing durations for successful runs', async () => {
    const stop = metrics.processingSeconds.startTimer({ queue: LEDGER_OPERATIONS_QUEUE });
    await new Promise((resolve) => setTimeout(resolve, 5));
    stop();

    const values = await registry.getByName('eta_queue_job_processing_seconds').get();
    expect(values.values.some((v) => v.value > 0)).toBe(true);
  });

  it('sweep sets queue depth from the reader', async () => {
    await sweepQueueMetrics(metrics, LEDGER_OPERATIONS_QUEUE, async () => 42);
    const gauge = await registry.getByName('eta_queue_depth').get();
    const sample = gauge.values.find((v) => v.labels.queue === LEDGER_OPERATIONS_QUEUE);
    expect(sample?.value).toBe(42);
  });

  it('sweep reports zero depth when reading fails', async () => {
    await sweepQueueMetrics(metrics, AGGREGATION_JOBS_QUEUE, async () => {
      throw new Error('redis down');
    });
    const gauge = await registry.getByName('eta_queue_depth').get();
    const sample = gauge.values.find((v) => v.labels.queue === AGGREGATION_JOBS_QUEUE);
    expect(sample?.value).toBe(0);
  });

  it('sweep computes the failure rate from counters', async () => {
    metrics.processedTotal.inc({ queue: LEDGER_OPERATIONS_QUEUE }, 8);
    metrics.failuresTotal.inc({ queue: LEDGER_OPERATIONS_QUEUE }, 2);

    await sweepQueueMetrics(metrics, LEDGER_OPERATIONS_QUEUE, async () => 0);
    const rate = await registry.getByName('eta_queue_failure_rate').get();
    const sample = rate.values.find((v) => v.labels.queue === LEDGER_OPERATIONS_QUEUE);
    expect(sample?.value).toBeCloseTo(0.2);
  });

  it('failure rate is zero when nothing has been processed yet', async () => {
    await sweepQueueMetrics(metrics, LEDGER_OPERATIONS_QUEUE, async () => 0);
    const rate = await registry.getByName('eta_queue_failure_rate').get();
    const sample = rate.values.find((v) => v.labels.queue === LEDGER_OPERATIONS_QUEUE);
    expect(sample?.value).toBe(0);
  });
});
