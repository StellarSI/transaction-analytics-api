import {
  trace,
  context,
  propagation,
  SpanStatusCode,
  TraceFlags,
  type Tracer,
  type Span,
  type Context,
  type SpanContext,
} from '@opentelemetry/api';
import crypto from 'node:crypto';

export interface TracingConfig {
  serviceName?: string;
  collectorUrl?: string;
  samplingRatio?: number; // 0.0 to 1.0 (default 0.1 / 10%)
  enabled?: boolean;
}

export interface ExportedSpanRecord {
  name: string;
  traceId: string;
  spanId: string;
  parentSpanId?: string;
  status: { code: SpanStatusCode; message?: string };
  attributes: Record<string, string | number | boolean>;
  startTime: number;
  endTime: number;
  sampled: boolean;
}

export const DEFAULT_SERVICE_NAME = 'transaction-analytics-api';
export const DEFAULT_SAMPLING_RATIO = 0.1;
export const DEFAULT_COLLECTOR_URL = 'http://localhost:4318/v1/traces';

let tracer: Tracer | null = null;
let activeServiceName = DEFAULT_SERVICE_NAME;
let activeCollectorUrl = DEFAULT_COLLECTOR_URL;
let activeSamplingRatio = DEFAULT_SAMPLING_RATIO;
let isTracingEnabled = true;

const inMemorySpanBuffer: ExportedSpanRecord[] = [];

/**
 * Initializes distributed tracing configuration for the application.
 * Supports configurable sampling (10% default), collector URL and W3C trace-context propagation.
 */
export function initTracing(options?: TracingConfig): Tracer {
  activeServiceName = options?.serviceName ?? process.env.OTEL_SERVICE_NAME ?? DEFAULT_SERVICE_NAME;
  activeCollectorUrl =
    options?.collectorUrl ?? process.env.OTEL_EXPORTER_OTLP_ENDPOINT ?? DEFAULT_COLLECTOR_URL;

  const rawSampling =
    options?.samplingRatio ??
    (process.env.OTEL_SAMPLING_RATIO ? parseFloat(process.env.OTEL_SAMPLING_RATIO) : DEFAULT_SAMPLING_RATIO);

  // Validate and clamp sampling ratio to [0.0, 1.0], falling back to default if NaN
  if (Number.isFinite(rawSampling)) {
    activeSamplingRatio = Math.max(0.0, Math.min(1.0, rawSampling));
  } else {
    activeSamplingRatio = DEFAULT_SAMPLING_RATIO;
  }

  isTracingEnabled = options?.enabled ?? (process.env.OTEL_TRACING_ENABLED !== 'false');

  tracer = trace.getTracer(activeServiceName);
  return tracer;
}

/**
 * Returns the active tracer instance, initializing with defaults if not already set.
 */
export function getTracer(): Tracer {
  if (!tracer) {
    tracer = initTracing();
  }
  return tracer;
}

/**
 * Returns the configured collector endpoint URL.
 */
export function getCollectorUrl(): string {
  return activeCollectorUrl;
}

/**
 * Determines whether an operation should be sampled based on the active ratio.
 */
export function shouldSample(): boolean {
  if (!isTracingEnabled) return false;
  if (activeSamplingRatio >= 1.0) return true;
  if (activeSamplingRatio <= 0.0) return false;
  return Math.random() < activeSamplingRatio;
}

/**
 * Injects W3C trace-context headers (RFC traceparent: 00-<traceId>-<spanId>-<flags>) into carrier.
 */
export function injectTraceContext(carrier: Record<string, string>): void {
  const activeSpan = trace.getSpan(context.active());
  const spanContext = activeSpan?.spanContext();

  if (spanContext && spanContext.traceId && spanContext.spanId) {
    const flags = (spanContext.traceFlags & TraceFlags.SAMPLED) ? '01' : '00';
    carrier.traceparent = `00-${spanContext.traceId}-${spanContext.spanId}-${flags}`;
  } else {
    // Generate valid root traceparent if none exists in scope
    const traceId = crypto.randomBytes(16).toString('hex');
    const spanId = crypto.randomBytes(8).toString('hex');
    const flags = shouldSample() ? '01' : '00';
    carrier.traceparent = `00-${traceId}-${spanId}-${flags}`;
  }
  propagation.inject(context.active(), carrier);
}

/**
 * Extracts W3C trace-context headers from carrier, parsing standard traceparent header.
 */
export function extractTraceContext(carrier: Record<string, string>): Context {
  const traceparent = carrier.traceparent ?? carrier.Traceparent;
  if (traceparent && typeof traceparent === 'string') {
    const parts = traceparent.trim().split('-');
    if (parts.length === 4 && parts[0] === '00' && parts[1].length === 32 && parts[2].length === 16) {
      const extractedSpanContext: SpanContext = {
        traceId: parts[1],
        spanId: parts[2],
        traceFlags: parts[3] === '01' ? TraceFlags.SAMPLED : TraceFlags.NONE,
        isRemote: true,
      };
      return trace.setSpanContext(context.active(), extractedSpanContext);
    }
  }
  return propagation.extract(context.active(), carrier);
}

/**
 * Executes an operation within a traced span, automatically recording errors, status, and buffer metrics.
 */
export async function withSpan<T>(
  name: string,
  fn: (span: Span) => Promise<T>,
  attributes?: Record<string, string | number | boolean>
): Promise<T> {
  const t = getTracer();
  const sampled = shouldSample();
  const startTime = Date.now();
  const span = t.startSpan(name);

  const mergedAttrs: Record<string, string | number | boolean> = {
    'service.name': activeServiceName,
    'sampling.ratio': activeSamplingRatio,
    'span.sampled': sampled,
    ...(attributes ?? {}),
  };

  for (const [key, value] of Object.entries(mergedAttrs)) {
    span.setAttribute(key, value);
  }

  const dynamicSpan: Span = new Proxy(span, {
    get(target, prop, receiver) {
      if (prop === 'setAttribute') {
        return (key: string, value: string | number | boolean): Span => {
          mergedAttrs[key] = value;
          target.setAttribute(key, value);
          return dynamicSpan;
        };
      }
      const val = Reflect.get(target, prop, receiver) as unknown;
      return typeof val === 'function' ? (val as (...args: unknown[]) => unknown).bind(target) : val;
    },
  });

  let statusCode = SpanStatusCode.OK;
  let statusMessage: string | undefined;

  return context.with(trace.setSpan(context.active(), span), async () => {
    try {
      const result = await fn(dynamicSpan);
      span.setStatus({ code: SpanStatusCode.OK });
      return result;
    } catch (error) {
      statusCode = SpanStatusCode.ERROR;
      statusMessage = error instanceof Error ? error.message : String(error);
      span.setStatus({ code: SpanStatusCode.ERROR, message: statusMessage });
      if (error instanceof Error) {
        span.recordException(error);
      }
      throw error;
    } finally {
      span.end();
      const endTime = Date.now();
      const spanContext = span.spanContext();

      if (sampled) {
        inMemorySpanBuffer.push({
          name,
          traceId: spanContext.traceId,
          spanId: spanContext.spanId,
          status: { code: statusCode, message: statusMessage },
          attributes: mergedAttrs,
          startTime,
          endTime,
          sampled,
        });
      }
    }
  });
}

/**
 * Retrieves the recorded spans exported to buffer (useful for verification and OTLP flush).
 */
export function getExportedSpans(): ExportedSpanRecord[] {
  return [...inMemorySpanBuffer];
}

/**
 * Clears the in-memory span buffer.
 */
export function clearExportedSpans(): void {
  inMemorySpanBuffer.length = 0;
}

/**
 * Fastify / HTTP helper creating spans for inbound API requests.
 */
export function traceApiRequest(route: string, method: string): Span {
  const t = getTracer();
  return t.startSpan(`HTTP ${method.toUpperCase()} ${route}`, {
    attributes: {
      'http.route': route,
      'http.method': method.toUpperCase(),
      'component': 'api',
      'service.name': activeServiceName,
    },
  });
}

/**
 * Ingestion helper creating spans for Horizon ledger / payment ingestion.
 */
export function traceIngestion(kind: string, cursor?: string): Span {
  const t = getTracer();
  const span = t.startSpan(`ingestion.${kind}`, {
    attributes: {
      'ingestion.kind': kind,
      'component': 'ingestor',
      'service.name': activeServiceName,
    },
  });
  if (cursor) {
    span.setAttribute('ingestion.cursor', cursor);
  }
  return span;
}

/**
 * Aggregation helper creating spans for batch rollup tasks.
 */
export function traceAggregation(task: string, entityCount?: number): Span {
  const t = getTracer();
  const span = t.startSpan(`aggregation.${task}`, {
    attributes: {
      'aggregation.task': task,
      'component': 'aggregator',
      'service.name': activeServiceName,
    },
  });
  if (entityCount !== undefined) {
    span.setAttribute('aggregation.entity_count', entityCount);
  }
  return span;
}
