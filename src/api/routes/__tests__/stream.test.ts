/** Unit tests for GET /v1/transactions/stream (issue #40). */

import { afterEach, describe, expect, it } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { createApp } from '../../server';
import {
  closeActiveStreamConnections,
  matchesTransactionFilters,
  normalizeStreamQuery,
  type TransactionStreamEvent,
  type TransactionStreamSubscriber,
} from '../transactions/stream';

const ACCOUNT = `G${'A'.repeat(55)}`;

function fakeSubscriber(
  event?: TransactionStreamEvent,
): TransactionStreamSubscriber & { closed: boolean } {
  return {
    closed: false,
    subscribe(onEvent) {
      if (event) onEvent(event);
    },
    close() {
      this.closed = true;
    },
  };
}

describe('GET /v1/transactions/stream (issue #40)', () => {
  const apps: FastifyInstance[] = [];

  afterEach(async () => {
    await closeActiveStreamConnections();
    await Promise.all(apps.splice(0).map((app) => app.close()));
  });

  it('normalizes and matches stream filters', () => {
    const filters = normalizeStreamQuery({
      accounts: [ACCOUNT, ACCOUNT],
      assets: 'XLM',
      types: ['payment'],
    });
    expect(filters.accounts).toEqual([ACCOUNT]);
    expect(filters.assets).toEqual(['XLM']);
    expect(filters.types).toEqual(['payment']);

    const event: TransactionStreamEvent = {
      id: '10',
      data: { from: ACCOUNT, asset: 'XLM', type: 'payment' },
    };
    expect(matchesTransactionFilters(event, filters)).toBe(true);
    expect(matchesTransactionFilters(event, { ...filters, cursor: '10' })).toBe(false);
    expect(matchesTransactionFilters(event, { ...filters, accounts: [`G${'B'.repeat(55)}`] })).toBe(
      false,
    );
  });

  it('rejects unknown query parameters with an RFC 7807 problem', async () => {
    const app = await createApp({ logger: false });
    apps.push(app);

    const response = await app.inject({
      method: 'GET',
      url: '/v1/transactions/stream?unexpected=1',
    });

    expect(response.statusCode).toBe(400);
    expect(response.headers['content-type']).toContain('application/problem+json');
    expect(response.json()).toMatchObject({ status: 400, code: 'validation_error' });
  });

  it('returns a problem document when a subscriber cannot be created', async () => {
    const app = await createApp({
      logger: false,
      createTransactionStreamSubscriber() {
        throw new Error('redis unavailable');
      },
    });
    apps.push(app);

    const response = await app.inject({ method: 'GET', url: '/v1/transactions/stream' });

    expect(response.statusCode).toBe(503);
    expect(response.json()).toMatchObject({ status: 503, code: 'internal_error' });
  });

  it('streams SSE frames with retry and cursor handling', async () => {
    const subscriber = fakeSubscriber({
      id: '2',
      data: { from: ACCOUNT, asset: 'XLM', type: 'payment' },
    });
    const app = await createApp({
      logger: false,
      retryMs: 1234,
      heartbeatMs: 60_000,
      createTransactionStreamSubscriber: () => subscriber,
    });
    apps.push(app);

    // The hijacked SSE reply is ended by the tracked cleanup, which we trigger
    // while inject() is waiting for the response to finish.
    const timer = setTimeout(() => void closeActiveStreamConnections(), 10);
    const response = await app.inject({
      method: 'GET',
      url: '/v1/transactions/stream',
      headers: { 'last-event-id': '1' },
    });
    clearTimeout(timer);
    await closeActiveStreamConnections();

    expect(response.statusCode).toBe(200);
    expect(response.headers['content-type']).toContain('text/event-stream');
    expect(response.headers['cache-control']).toBe('no-cache, no-transform');
    expect(response.body).toContain('retry: 1234');
    expect(response.body).toContain('id: 2');
    expect(response.body).toContain('event: transaction');
    expect(subscriber.closed).toBe(true);
  });
});
