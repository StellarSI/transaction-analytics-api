/**
 * Prometheus metrics for the service (issue #35).
 *
 * `GET /metrics` (registered outside `/v1` in `src/api/server.ts`) scrapes the
 * default registry. This module registers the service-owned metrics there:
 *
 * - `eta_http_request_duration_seconds` (histogram) — labels `route`,
 *   `method`, `status`;
 * - `eta_http_requests_total` (counter) — labels `route`, `method`, `status`;
 * - `eta_http_errors_total` (counter) — same labels, incremented for 5xx;
 * - `eta_ingestion_lag_seconds` (gauge) — set from `/v1/health` whenever the
 *   lag is computable;
 * - `eta_aggregation_duration_seconds` (histogram) — label `kind`,
 *   observed around every aggregation handler;
 * - `eta_active_sse_connections` (gauge) — incremented/decremented around the
 *   `GET /v1/transactions/stream` connection lifecycle;
 * - `eta_queue_depth` — gauge reused from the queue module when it already
 *   registered it (`src/queue/metrics.ts`), so both layers share one metric
 *   family instead of colliding in the default registry.
 *
 * Unit tests inject a fresh `Registry` to keep assertions isolated; the
 * production server uses the shared default.
 */

import {
  Counter,
  Gauge,
  Histogram,
  register,
  type Registry,
} from 'prom-client';

export interface ObservabilityMetrics {
  /** HTTP request duration histogram, labels route/method/status. */
  readonly httpRequestDuration: Histogram<'route' | 'method' | 'status'>;
  /** Total HTTP requests, labels route/method/status. */
  readonly httpRequestsTotal: Counter<'route' | 'method' | 'status'>;
  /** Total 5xx HTTP responses, labels route/method/status. */
  readonly httpErrorsTotal: Counter<'route' | 'method' | 'status'>;
  /** Seconds the ingestor trails the ledger head (set from /v1/health). */
  readonly ingestionLagSeconds: Gauge<string>;
  /** Aggregation handler duration histogram, label kind. */
  readonly aggregationDurationSeconds: Histogram<'kind'>;
  /** Open SSE connections (transactions stream). */
  readonly activeSseConnections: Gauge<string>;
  /** Queue depth gauge (shared with the queue module). */
  readonly queueDepth: Gauge<string>;
  /** Records one completed HTTP request into the three HTTP metrics. */
  recordHttpRequest(input: {
    route: string;
    method: string;
    status: number;
    durationSeconds: number;
  }): void;
}

/** Route label used for requests that matched no route. */
export const UNMATCHED_ROUTE = 'unmatched';
/** Metric name shared with `src/queue/metrics.ts` — keep stable. */
export const QUEUE_DEPTH_METRIC = 'eta_queue_depth';

function findOrCreate<T>(
  registry: Registry,
  name: string,
  create: () => T,
): T {
  const existing = registry.getSingleMetric(name);
  if (existing) {
    return existing as unknown as T;
  }
  return create();
}

export function createObservabilityMetrics(
  registry: Registry = register,
): ObservabilityMetrics {
  const httpLabelNames = ['route', 'method', 'status'] as const;

  const httpRequestDuration = findOrCreate(
    registry,
    'eta_http_request_duration_seconds',
    () =>
      new Histogram<(typeof httpLabelNames)[number]>({
        name: 'eta_http_request_duration_seconds',
        help: 'HTTP request duration in seconds',
        labelNames: httpLabelNames,
        buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
        registers: [registry],
      }),
  );

  const httpRequestsTotal = findOrCreate(
    registry,
    'eta_http_requests_total',
    () =>
      new Counter<(typeof httpLabelNames)[number]>({
        name: 'eta_http_requests_total',
        help: 'Total HTTP requests handled by the server',
        labelNames: httpLabelNames,
        registers: [registry],
      }),
  );

  const httpErrorsTotal = findOrCreate(
    registry,
    'eta_http_errors_total',
    () =>
      new Counter<(typeof httpLabelNames)[number]>({
        name: 'eta_http_errors_total',
        help: 'Total HTTP requests that ended in a 5xx response',
        labelNames: httpLabelNames,
        registers: [registry],
      }),
  );

  const ingestionLagSeconds = findOrCreate(
    registry,
    'eta_ingestion_lag_seconds',
    () =>
      new Gauge({
        name: 'eta_ingestion_lag_seconds',
        help: 'Seconds the ingestor trails the Horizon/RPC ledger head',
        registers: [registry],
      }),
  );

  const aggregationDurationSeconds = findOrCreate(
    registry,
    'eta_aggregation_duration_seconds',
    () =>
      new Histogram<'kind'>({
        name: 'eta_aggregation_duration_seconds',
        help: 'Duration of an aggregation handler run, by job kind',
        labelNames: ['kind'],
        buckets: [0.01, 0.05, 0.1, 0.5, 1, 5, 15, 60, 300],
        registers: [registry],
      }),
  );

  const activeSseConnections = findOrCreate(
    registry,
    'eta_active_sse_connections',
    () =>
      new Gauge({
        name: 'eta_active_sse_connections',
        help: 'Open server-sent-event connections on /v1/transactions/stream',
        registers: [registry],
      }),
  );

  // Queue depth is already registered by `src/queue/metrics.ts` in production
  // (with label `queue`). Reuse it so both layers export one family; create
  // it ourselves only when the queue module has not registered it yet.
  const queueDepth = findOrCreate(registry, QUEUE_DEPTH_METRIC, () =>
    new Gauge({
      name: QUEUE_DEPTH_METRIC,
      help: 'Jobs waiting in a queue (waiting + delayed)',
      labelNames: ['queue'],
      registers: [registry],
    }),
  );

  return {
    httpRequestDuration,
    httpRequestsTotal,
    httpErrorsTotal,
    ingestionLagSeconds,
    aggregationDurationSeconds,
    activeSseConnections,
    queueDepth,
    recordHttpRequest({ route, method, status, durationSeconds }): void {
      const labels = { route, method, status: String(status) };
      httpRequestDuration.observe(labels, durationSeconds);
      httpRequestsTotal.inc(labels);
      if (status >= 500) {
        httpErrorsTotal.inc(labels);
      }
    },
  };
}

let cached: ObservabilityMetrics | null = null;

/** Shared instance bound to the default registry (lazy singleton). */
export function getObservabilityMetrics(): ObservabilityMetrics {
  if (!cached) {
    cached = createObservabilityMetrics(register);
  }
  return cached;
}
