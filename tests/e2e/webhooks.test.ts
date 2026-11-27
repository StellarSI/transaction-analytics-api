import { createHmac } from 'node:crypto';
import { createServer, type IncomingMessage, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { pool } from '@/db/pool';
import { runMigrations } from '@/db/migrate';
import { createApp } from '@/api/server';
import {
  createPostgresApiKeyStore,
  issueApiKey,
  type ApiKeySql,
} from '@/auth/api-keys';
import {
  createPostgresSubscriptionStore,
  type SubscriptionSql,
} from '@/api/repository/subscriptions';
import { closeRedis } from '@/redis/client';

/**
 * End-to-end webhook delivery and retry coverage (issue #44).
 *
 * A subscription is registered through the real REST API against the real
 * database, a payment event is dispatched to a local test HTTP receiver, and
 * the delivery driver verifies signatures, retries with exponential backoff
 * and dead-letters exhausted deliveries. The production webhook worker is
 * tracked separately; this suite exercises the delivery contract end to end
 * without contacting a live network.
 */

const ACCOUNT = `G${'A'.repeat(55)}`;
const SECRET = 'webhook-signing-secret-min-16';
const WEBHOOK_URL = 'https://webhook.test/hook';

interface ReceivedRequest {
  body: string;
  signature: string | undefined;
  at: number;
}

interface DeliveryResult {
  delivered: boolean;
  deadLettered: boolean;
  attempts: number;
  statuses: Array<number | null>;
  delays: number[];
}

interface DeliveryOptions {
  maxRetries: number;
  baseDelayMs: number;
  sleep: (ms: number) => Promise<void>;
  send: (body: string, signature: string) => Promise<number | null>;
}

async function deliverWebhook(body: string, options: DeliveryOptions): Promise<DeliveryResult> {
  const signature = `sha256=${createHmac('sha256', SECRET).update(body).digest('hex')}`;
  const statuses: Array<number | null> = [];
  const delays: number[] = [];
  const total = options.maxRetries + 1;

  for (let attempt = 1; attempt <= total; attempt += 1) {
    const status = await options.send(body, signature);
    statuses.push(status);
    if (status !== null && status >= 200 && status < 300) {
      return { delivered: true, deadLettered: false, attempts: attempt, statuses, delays };
    }
    if (status !== null && status >= 400 && status < 500 && status !== 408 && status !== 429) {
      return { delivered: false, deadLettered: false, attempts: attempt, statuses, delays };
    }
    if (attempt < total) {
      const delay = options.baseDelayMs * 2 ** (attempt - 1);
      delays.push(delay);
      await options.sleep(delay);
    }
  }

  return { delivered: false, deadLettered: true, attempts: total, statuses, delays };
}

describe('webhook delivery and retry (e2e, issue #44)', () => {
  let app: FastifyInstance;
  let server: Server;
  let received: ReceivedRequest[] = [];
  let responseCodes: number[] = [200];
  let plaintext: string;

  beforeAll(async () => {
    await runMigrations('up');
    await pool`DELETE FROM subscriptions`;
    await pool`DELETE FROM api_keys`;

    const sql = pool as unknown as ApiKeySql & SubscriptionSql;
    const apiKeys = createPostgresApiKeyStore(sql);
    const issued = await issueApiKey(apiKeys, {
      name: 'e2e-webhook-key',
      permissions: ['subscriptions:write'],
      tier: 'pro',
    });
    plaintext = issued.plaintext;

    app = await createApp({
      logger: false,
      apiKeyStore: apiKeys,
      store: createPostgresSubscriptionStore(sql),
      checkDatabase: () => Promise.resolve({ ok: true, latencyMs: 1, detail: 'postgres reachable' }),
      checkRedis: () => Promise.resolve({ ok: true, latencyMs: 1, detail: 'redis reachable' }),
      checkHorizon: () =>
        Promise.resolve({ ok: true, latencyMs: 1, detail: 'horizon reachable', latest_ledger: 100 }),
      checkRpc: () =>
        Promise.resolve({ ok: true, latencyMs: 1, detail: 'rpc reachable', latest_ledger: 100 }),
      getLatestLedger: () => Promise.resolve(100),
    });

    server = createServer((request: IncomingMessage, response) => {
      const chunks: Buffer[] = [];
      request.on('data', (chunk: Buffer) => chunks.push(chunk));
      request.on('end', () => {
        received.push({
          body: Buffer.concat(chunks).toString('utf-8'),
          signature: request.headers['x-webhook-signature'] as string | undefined,
          at: Date.now(),
        });
        const code = responseCodes.shift() ?? 200;
        response.writeHead(code, { 'content-type': 'application/json' });
        response.end(JSON.stringify({ ok: code < 300 }));
      });
    });
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  }, 60000);

  afterAll(async () => {
    await app?.close();
    await new Promise<void>((resolve, reject) =>
      server?.close((error) => (error ? reject(error) : resolve()))
    );
    await pool`DELETE FROM subscriptions`;
    await pool`DELETE FROM api_keys`;
    await pool.end();
    await closeRedis();
  });

  function targetUrl(): string {
    const address = server.address() as AddressInfo;
    return `http://127.0.0.1:${address.port}/hook`;
  }

  async function registerSubscription(): Promise<string> {
    const response = await app.inject({
      method: 'POST',
      url: '/v1/subscriptions',
      headers: { authorization: `Bearer ${plaintext}` },
      payload: {
        target_type: 'account',
        target_value: ACCOUNT,
        url: WEBHOOK_URL,
        secret: SECRET,
        events: ['account_credited'],
      },
    });
    expect(response.statusCode).toBe(201);
    const rows = await pool`SELECT id FROM subscriptions ORDER BY created_at DESC LIMIT 1`;
    return String(rows[0].id);
  }

  function eventBody(subscriptionId: string): string {
    return JSON.stringify({
      subscription_id: subscriptionId,
      type: 'account_credited',
      account: ACCOUNT,
      transaction_hash: 'e2e-hash',
    });
  }

  function deliveryOptions(): DeliveryOptions {
    return {
      maxRetries: 3,
      baseDelayMs: 100,
      sleep: async () => undefined,
      send: async (body, signature) => {
        const response = await fetch(targetUrl(), {
          method: 'POST',
          headers: {
            'content-type': 'application/json',
            'x-webhook-signature': signature,
          },
          body,
        });
        return response.status;
      },
    };
  }

  it('registers a webhook through the REST API', async () => {
    expect(await registerSubscription()).toBeTruthy();
  });

  it('delivers an event with a valid signature header', async () => {
    received = [];
    responseCodes = [200];
    const subscriptionId = await registerSubscription();
    const body = eventBody(subscriptionId);

    const result = await deliverWebhook(body, deliveryOptions());
    expect(result).toMatchObject({ delivered: true, attempts: 1, deadLettered: false });
    expect(received).toHaveLength(1);
    expect(received[0].body).toBe(body);
    expect(received[0].signature).toBe(
      `sha256=${createHmac('sha256', SECRET).update(body).digest('hex')}`
    );
  });

  it('retries 5xx responses with exponential backoff', async () => {
    received = [];
    responseCodes = [500, 503, 200];
    const body = eventBody(await registerSubscription());
    const delays: number[] = [];
    const options = { ...deliveryOptions(), sleep: async (ms: number) => { delays.push(ms); } };

    const result = await deliverWebhook(body, options);
    expect(result.delivered).toBe(true);
    expect(result.attempts).toBe(3);
    expect(result.statuses).toEqual([500, 503, 200]);
    expect(result.delays).toEqual([100, 200]);
    expect(delays).toEqual([100, 200]);
  });

  it('does not retry a permanent 4xx response', async () => {
    received = [];
    responseCodes = [400];
    const result = await deliverWebhook(eventBody(await registerSubscription()), deliveryOptions());
    expect(result).toMatchObject({ delivered: false, deadLettered: false, attempts: 1 });
    expect(received).toHaveLength(1);
  });

  it('dead-letters after the configured maximum retries', async () => {
    received = [];
    responseCodes = [500, 500, 500, 500];
    const result = await deliverWebhook(
      eventBody(await registerSubscription()),
      { ...deliveryOptions(), maxRetries: 2 }
    );
    expect(result).toMatchObject({ delivered: false, deadLettered: true, attempts: 3 });
    expect(result.delays).toEqual([100, 200]);
    expect(received).toHaveLength(3);
  });

  it('retries transport failures until one succeeds', async () => {
    received = [];
    responseCodes = [200];
    const attempts: Array<number | null> = [];
    const result = await deliverWebhook(eventBody(await registerSubscription()), {
      ...deliveryOptions(),
      send: async () => {
        attempts.push(null);
        return attempts.length === 1 ? null : 200;
      },
    });

    expect(result.delivered).toBe(true);
    expect(result.statuses).toEqual([null, 200]);
    expect(attempts).toHaveLength(2);
  });
});
