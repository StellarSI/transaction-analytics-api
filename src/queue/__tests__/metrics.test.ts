import { describe, it, expect, beforeEach } from 'vitest';
import { Registry } from 'prom-client';
import { createQueueMetrics, sweepQueueMetrics, type QueueMetrics } from '../metrics';
import { AGGREGATION_JOBS_QUEUE, LEDGER_OPERATIONS_QUEUE } from '../index';

let registry: Registry;
let metrics: QueueMetrics;

/** Reads one sample `(name, labels)` from the registry JSON output. */
async function sampleOf(
  name: string,
  labels: Record<string, string>,
): Promise<number> {
  const all = await registry.getMetricsAsJSON();
  const metric = all.find((m) => m.name === name);
  const match = metric?.values.find((v) =>
    Object.entries(labels).every(([key, value]) => String(v.labels[key]) === value),
  );
  return match?.value ?? Number.NaN;
}

describe('queue metrics', () => {
  beforeEach(() => {
    registry = new Registry();
    metrics = createQueueMetrics(registry);
  });

  it('registers the depth, failure-rate and histogram families', async () => {
    const all = await registry.getMetricsAsJSON();
    const names = all.map((m) => m.name);
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

    const histogramSum = await sampleOf('eta_queue_job_processing_seconds_sum', {
      queue: LEDGER_OPERATIONS_QUEUE,
    });
    expect(histogramSum).toBeGreaterThan(0);
  });

  it('sweep sets queue depth from the reader', async () => {
    await sweepQueueMetrics(metrics, LEDGER_OPERATIONS_QUEUE, async () => 42);
    expect(
      await sampleOf('eta_queue_depth', { queue: LEDGER_OPERATIONS_QUEUE }),
    ).toBe(42);
  });

  it('sweep reports zero depth when reading fails', async () => {
    await sweepQueueMetrics(metrics, AGGREGATION_JOBS_QUEUE, async () => {
      throw new Error('redis down');
    });
    expect(
      await sampleOf('eta_queue_depth', { queue: AGGREGATION_JOBS_QUEUE }),
    ).toBe(0);
  });

  it('sweep computes the failure rate from counters', async () => {
    metrics.processedTotal.inc({ queue: LEDGER_OPERATIONS_QUEUE }, 8);
    metrics.failuresTotal.inc({ queue: LEDGER_OPERATIONS_QUEUE }, 2);

    await sweepQueueMetrics(metrics, LEDGER_OPERATIONS_QUEUE, async () => 0);
    expect(
      await sampleOf('eta_queue_failure_rate', { queue: LEDGER_OPERATIONS_QUEUE }),
    ).toBeCloseTo(0.2);
  });

  it('failure rate is zero when nothing has been processed yet', async () => {
    await sweepQueueMetrics(metrics, LEDGER_OPERATIONS_QUEUE, async () => 0);
    expect(
      await sampleOf('eta_queue_failure_rate', { queue: LEDGER_OPERATIONS_QUEUE }),
    ).toBe(0);
  });

  it('is isolated per registry so parallel tests cannot bleed counters', async () => {
    const other = new Registry();
    const otherMetrics = createQueueMetrics(other);
    otherMetrics.processedTotal.inc({ queue: LEDGER_OPERATIONS_QUEUE }, 1);

    const read = async (m: QueueMetrics): Promise<number> => {
      const { values } = await m.processedTotal.get();
      return values.find((v) => v.labels.queue === LEDGER_OPERATIONS_QUEUE)?.value ?? 0;
    };
    expect(await read(metrics)).toBe(0);
    expect(await read(otherMetrics)).toBe(1);
  });
});
