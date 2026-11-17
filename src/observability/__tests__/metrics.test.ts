/**
 * Tests for the Prometheus metrics registry (issue #35).
 *
 * Uses a fresh `Registry` per test so assertions are isolated from the
 * production default registry.
 */

import { describe, it, expect } from 'vitest';
import { Registry } from 'prom-client';
import {
  createObservabilityMetrics,
  QUEUE_DEPTH_METRIC,
} from '../metrics';
import { createQueueMetrics } from '../../queue/metrics';

describe('createObservabilityMetrics (issue #35)', () => {
  it('registers the HTTP, ingestion, aggregation, SSE and queue metrics', async () => {
    const registry = new Registry();
    createObservabilityMetrics(registry);

    const output = await registry.metrics();
    expect(output).toContain('eta_http_request_duration_seconds');
    expect(output).toContain('eta_http_requests_total');
    expect(output).toContain('eta_http_errors_total');
    expect(output).toContain('eta_ingestion_lag_seconds');
    expect(output).toContain('eta_aggregation_duration_seconds');
    expect(output).toContain('eta_active_sse_connections');
    expect(output).toContain(QUEUE_DEPTH_METRIC);
  });

  it('recordHttpRequest observes duration and counts requests with labels', async () => {
    const registry = new Registry();
    const metrics = createObservabilityMetrics(registry);

    metrics.recordHttpRequest({
      route: '/v1/health',
      method: 'GET',
      status: 200,
      durationSeconds: 0.123,
    });
    metrics.recordHttpRequest({
      route: '/v1/flows/assets',
      method: 'GET',
      status: 502,
      durationSeconds: 0.5,
    });

    const output = await registry.metrics();
    expect(output).toContain(
      'eta_http_requests_total{route="/v1/health",method="GET",status="200"} 1',
    );
    expect(output).toContain(
      'eta_http_requests_total{route="/v1/flows/assets",method="GET",status="502"} 1',
    );
    expect(output).toContain(
      'eta_http_errors_total{route="/v1/flows/assets",method="GET",status="502"} 1',
    );
    expect(output).not.toContain(
      'eta_http_errors_total{route="/v1/health"',
    );
    expect(output).toMatch(
      /eta_http_request_duration_seconds_count\{route="\/v1\/health",method="GET",status="200"\} 1/,
    );
  });

  it('only increments the error counter for 5xx responses', async () => {
    const registry = new Registry();
    const metrics = createObservabilityMetrics(registry);

    metrics.recordHttpRequest({ route: '/x', method: 'GET', status: 404, durationSeconds: 0.01 });
    metrics.recordHttpRequest({ route: '/x', method: 'GET', status: 503, durationSeconds: 0.01 });

    const output = await registry.metrics();
    expect(output).toContain('eta_http_errors_total{route="/x",method="GET",status="503"} 1');
    expect(output).not.toContain('eta_http_errors_total{route="/x",method="GET",status="404"}');
  });

  it('observes aggregation duration with a kind label', async () => {
    const registry = new Registry();
    const metrics = createObservabilityMetrics(registry);

    const stop = metrics.aggregationDurationSeconds.startTimer({ kind: 'asset-flow' });
    await new Promise((resolve) => setTimeout(resolve, 10));
    stop();

    const output = await registry.metrics();
    expect(output).toMatch(
      /eta_aggregation_duration_seconds_count\{kind="asset-flow"\} 1/,
    );
  });

  it('tracks SSE connections as a gauge that goes up and down', async () => {
    const registry = new Registry();
    const metrics = createObservabilityMetrics(registry);

    metrics.activeSseConnections.inc();
    metrics.activeSseConnections.inc();
    metrics.activeSseConnections.dec();

    const output = await registry.metrics();
    expect(output).toContain('eta_active_sse_connections 1');
  });

  it('publishes the ingestion lag gauge', async () => {
    const registry = new Registry();
    const metrics = createObservabilityMetrics(registry);

    metrics.ingestionLagSeconds.set(35);

    const output = await registry.metrics();
    expect(output).toContain('eta_ingestion_lag_seconds 35');
  });

  it('reuses an already-registered queue depth gauge instead of registering twice', async () => {
    const registry = new Registry();
    const queue = createQueueMetrics(registry);
    const obs = createObservabilityMetrics(registry);

    expect(obs.queueDepth).toBe(queue.depth);
    const output = await registry.metrics();
    const occurrences = output.split(`\n${QUEUE_DEPTH_METRIC}{`).length - 1;
    expect(occurrences).toBeGreaterThanOrEqual(1);
  });

  it('creates the queue depth gauge itself when the queue module has not registered it', async () => {
    const registry = new Registry();
    const obs = createObservabilityMetrics(registry);
    expect(obs.queueDepth).toBeDefined();

    const output = await registry.metrics();
    expect(output).toContain(QUEUE_DEPTH_METRIC);
  });
});
