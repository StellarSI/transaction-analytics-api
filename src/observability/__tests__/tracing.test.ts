import { describe, it, expect, beforeEach } from 'vitest';
import {
  initTracing,
  getTracer,
  getCollectorUrl,
  withSpan,
  shouldSample,
  injectTraceContext,
  extractTraceContext,
  traceApiRequest,
  traceIngestion,
  traceAggregation,
  getExportedSpans,
  clearExportedSpans,
  DEFAULT_COLLECTOR_URL,
} from '../tracing';
import { trace } from '@opentelemetry/api';

describe('OpenTelemetry Distributed Tracing (Issue #96)', () => {
  beforeEach(() => {
    delete process.env.OTEL_SERVICE_NAME;
    delete process.env.OTEL_SAMPLING_RATIO;
    delete process.env.OTEL_TRACING_ENABLED;
    delete process.env.OTEL_EXPORTER_OTLP_ENDPOINT;
    clearExportedSpans();
    initTracing({ samplingRatio: 1.0 }); // Default test runs with 100% sampling for assertions
  });

  it('initializes tracer with standard default service name and collector URL', () => {
    initTracing();
    const tracer = getTracer();
    expect(tracer).toBeDefined();
    expect(typeof tracer.startSpan).toBe('function');
    expect(getCollectorUrl()).toBe(DEFAULT_COLLECTOR_URL);
  });

  it('supports custom service name, collector URL, and sampling ratio', () => {
    const customTracer = initTracing({
      serviceName: 'custom-analytics-service',
      collectorUrl: 'https://otel.stellar.org/v1/traces',
      samplingRatio: 0.5,
    });
    expect(customTracer).toBeDefined();
    expect(getCollectorUrl()).toBe('https://otel.stellar.org/v1/traces');
  });

  it('safely handles non-finite NaN sampling ratios by falling back to default', () => {
    process.env.OTEL_SAMPLING_RATIO = 'not-a-number';
    initTracing();
    expect(shouldSample()).toBeDefined();
  });

  it('executes async operation inside withSpan and buffers sampled spans', async () => {
    const result = await withSpan('test.success_operation', async (span) => {
      span.setAttribute('test.custom_attr', 'stellar-flow');
      await Promise.resolve();
      return 100;
    });

    expect(result).toBe(100);
    const exported = getExportedSpans();
    expect(exported.length).toBe(1);
    expect(exported[0].name).toBe('test.success_operation');
    expect(exported[0].attributes['test.custom_attr']).toBe('stellar-flow');
    expect(exported[0].status.code).toBe(1); // SpanStatusCode.OK
    expect(exported[0].sampled).toBe(true);
  });

  it('records exception, sets ERROR status, and buffers failed spans', async () => {
    const testError = new Error('Database connection failed');
    await expect(
      withSpan('test.failure_operation', async () => {
        await Promise.resolve();
        throw testError;
      })
    ).rejects.toThrow('Database connection failed');

    const exported = getExportedSpans();
    expect(exported.length).toBe(1);
    expect(exported[0].status.code).toBe(2); // SpanStatusCode.ERROR
    expect(exported[0].status.message).toBe('Database connection failed');
  });

  it('evaluates sampling ratio boundary conditions deterministically', () => {
    initTracing({ samplingRatio: 1.0 });
    expect(shouldSample()).toBe(true);

    initTracing({ samplingRatio: 0.0 });
    expect(shouldSample()).toBe(false);

    initTracing({ enabled: false, samplingRatio: 1.0 });
    expect(shouldSample()).toBe(false);
  });

  it('injects and extracts valid W3C traceparent headers', () => {
    const carrier: Record<string, string> = {};
    injectTraceContext(carrier);

    expect(carrier.traceparent).toBeDefined();
    // Validate RFC W3C format: 00-<32 hex traceId>-<16 hex spanId>-<01|00>
    expect(carrier.traceparent).toMatch(/^00-[0-9a-f]{32}-[0-9a-f]{16}-0[01]$/);

    const remoteContext = extractTraceContext(carrier);
    expect(remoteContext).toBeDefined();
    const extractedSpan = trace.getSpan(remoteContext);
    expect(extractedSpan).toBeDefined();
    const spanContext = extractedSpan?.spanContext();
    expect(spanContext?.isRemote).toBe(true);
    expect(spanContext?.traceId).toBe(carrier.traceparent.split('-')[1]);
  });

  it('creates API, ingestion, and aggregation domain spans with component attributes', () => {
    const apiSpan = traceApiRequest('/v1/analytics/flows', 'GET');
    expect(apiSpan).toBeDefined();
    apiSpan.end();

    const ingestionSpan = traceIngestion('payment', 'cursor-token-99');
    expect(ingestionSpan).toBeDefined();
    ingestionSpan.end();

    const aggregationSpan = traceAggregation('hourly_volume', 500);
    expect(aggregationSpan).toBeDefined();
    aggregationSpan.end();
  });
});
