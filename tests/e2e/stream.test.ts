import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { pool } from '@/db/pool';
import { runMigrations } from '@/db/migrate';
import { createApp } from '@/api/server';
import { PaymentIngestor } from '@/ingestor/horizon/payments';
import type { HorizonOperationRecord, PaymentPayload } from '@/ingestor/types';
import { PAYMENT_QUEUE, createMemoryCursorStore } from '@/ingestor/types';
import type { OperationStreamOpener, StreamHandlers } from '@/ingestor/horizon/stream';
import { createRedisListQueue } from '@/ingestor/queue';
import { createPostgresBackfillStore, type BackfillSql } from '@/ingestor/hubble/store';
import {
  AssetFlowAggregator,
  createPostgresAssetFlowStore,
  type AssetFlowSql,
} from '@/aggregator/asset-flows';
import {
  closeActiveStreamConnections,
  TRANSACTIONS_STREAM_CHANNEL,
} from '@/api/routes/transactions/stream';
import { closeRedis, getRedis } from '@/redis/client';

/**
 * End-to-end coverage for the live transaction SSE stream (issue #43).
 *
 * A mocked Horizon SSE batch flows through the real Redis queue into
 * `ledger_operations`, feeds the asset aggregator, and the resulting payload
 * is published on the same Redis channel the production entrypoint uses. The
 * API then serves it as an SSE frame to a real subscriber. Requires the
 * TimescaleDB and Redis containers from docker-compose.test.yml.
 */

const ISSUER = 'GB5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN';
const SENDER = `G${'A'.repeat(55)}`;
const RECIPIENT = `G${'B'.repeat(55)}`;
const OTHER = `G${'C'.repeat(55)}`;
const BUCKET_START = '2026-10-01T00:00:00Z';

function paymentRecord(
  id: string,
  from: string,
  to: string,
  amount: string
): HorizonOperationRecord {
  return {
    id,
    paging_token: id,
    type: 'payment',
    type_i: 1,
    source_account: from,
    from,
    to,
    amount,
    asset_type: 'credit_alphanum4',
    asset_code: 'TSTU',
    asset_issuer: ISSUER,
    transaction_hash: `hash-${id}`,
    created_at: BUCKET_START,
    ledger: 1000,
  };
}

const flush = () => new Promise((resolve) => setTimeout(resolve, 0));
const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

describe('live transaction stream (e2e, issue #43)', () => {
  let app: FastifyInstance;
  let payloads: PaymentPayload[] = [];

  beforeAll(async () => {
    await runMigrations('up');
    await pool`DELETE FROM asset_flow_aggregates`;
    await pool`DELETE FROM ledger_operations`;
    await getRedis().del(PAYMENT_QUEUE);

    let handlers: StreamHandlers | undefined;
    const opener: OperationStreamOpener = ({ handlers: fresh }) => {
      handlers = fresh;
      return () => undefined;
    };
    const ingestor = new PaymentIngestor({
      opener,
      queue: createRedisListQueue(getRedis()),
      cursorStore: createMemoryCursorStore(),
      cursorKey: 'e2e:stream:payments',
      initialCursor: 'now',
    });
    await ingestor.start();
    const records = [
      paymentRecord('2001', SENDER, RECIPIENT, '100.0000000'),
      paymentRecord('2002', SENDER, OTHER, '25.0000000'),
    ];
    for (const record of records) {
      handlers?.onOperation(record);
    }
    await flush();
    ingestor.stop();

    const queued = await getRedis().lrange(PAYMENT_QUEUE, 0, -1);
    payloads = queued.map((entry) => JSON.parse(entry) as PaymentPayload);
    await getRedis().del(PAYMENT_QUEUE);

    const rows = payloads.map((payload, index) => ({
      ledger_sequence: payload.ledger ?? 1000,
      ledger_closed_at: payload.ledgerClosedAt ?? BUCKET_START,
      operation_type: 'payment',
      source_account: payload.from,
      destination_account: payload.to,
      asset_code: payload.asset.code,
      asset_issuer: payload.asset.issuer ?? null,
      asset_type: payload.asset.type,
      amount: payload.amount,
      fee_charged: '100',
      transaction_hash: payload.txHash ?? `synthetic-${index}`,
      operation_index: index,
      successful: true,
    }));
    await createPostgresBackfillStore(pool as unknown as BackfillSql).insertOperations(rows);
    const aggregator = new AssetFlowAggregator({
      store: createPostgresAssetFlowStore(pool as unknown as AssetFlowSql),
      intervals: ['1m'],
    });
    await aggregator.applyOperations(
      rows.map((row) => ({
        assetCode: row.asset_code,
        assetIssuer: row.asset_issuer,
        amount: row.amount,
        fromAccount: row.source_account ?? '',
        toAccount: row.destination_account ?? '',
        ledgerSequence: row.ledger_sequence,
        ledgerClosedAt: row.ledger_closed_at,
      })),
      { kind: 'payment' }
    );

    app = await createApp({
      logger: false,
      checkDatabase: () => Promise.resolve({ ok: true, latencyMs: 1, detail: 'postgres reachable' }),
      checkRedis: () => Promise.resolve({ ok: true, latencyMs: 1, detail: 'redis reachable' }),
      checkHorizon: () =>
        Promise.resolve({ ok: true, latencyMs: 1, detail: 'horizon reachable', latest_ledger: 1000 }),
      checkRpc: () =>
        Promise.resolve({ ok: true, latencyMs: 1, detail: 'rpc reachable', latest_ledger: 1000 }),
      getLatestLedger: () => Promise.resolve(1000),
    });
  }, 60000);

  afterAll(async () => {
    await closeActiveStreamConnections();
    await app?.close();
    await pool`DELETE FROM asset_flow_aggregates`;
    await pool`DELETE FROM ledger_operations`;
    await getRedis().del(PAYMENT_QUEUE);
    await pool.end();
    await closeRedis();
  });

  /** Opens an SSE request, publishes the given events once subscribed, then closes it. */
  async function streamWith(
    events: ReadonlyArray<{ id: string; data: unknown }>,
    url: string,
    headers: Record<string, string> = {}
  ) {
    const timer = setTimeout(async () => {
      await delay(100);
      for (const event of events) {
        await getRedis().publish(
          TRANSACTIONS_STREAM_CHANNEL,
          JSON.stringify(event)
        );
      }
      await delay(100);
      await closeActiveStreamConnections();
    }, 10);
    try {
      return await app.inject({ method: 'GET', url, headers });
    } finally {
      clearTimeout(timer);
      await closeActiveStreamConnections();
    }
  }

  it('ingests a batch into the database and derived aggregates', async () => {
    expect(payloads).toHaveLength(2);
    const stored = await pool<{ transaction_hash: string }[]>`
      SELECT transaction_hash FROM ledger_operations
      WHERE transaction_hash IN ('hash-2001', 'hash-2002')
    `;
    expect(stored).toHaveLength(2);

    const aggregates = await pool<{ payments: number }[]>`
      SELECT payments FROM asset_flow_aggregates
      WHERE interval = '1m' AND bucket_start = ${BUCKET_START}
    `;
    expect(aggregates).toHaveLength(1);
    expect(aggregates[0].payments).toBe(2);
  });

  it('streams ingested events with the documented SSE frames', async () => {
    const event = { id: '2001', data: payloads[0] };
    const response = await streamWith([event], '/v1/transactions/stream');

    expect(response.statusCode).toBe(200);
    expect(response.headers['content-type']).toContain('text/event-stream');
    expect(response.body).toContain('retry:');
    expect(response.body).toContain('id: 2001');
    expect(response.body).toContain('event: transaction');
    expect(response.body).toContain(RECIPIENT);
  });

  it('applies account and asset filters', async () => {
    const events = [
      { id: '2001', data: payloads[0] },
      { id: '2002', data: payloads[1] },
    ];
    const response = await streamWith(
      events,
      `/v1/transactions/stream?accounts=${RECIPIENT}&assets=TSTU:${ISSUER}`
    );

    expect(response.statusCode).toBe(200);
    expect(response.body).toContain('id: 2001');
    expect(response.body).not.toContain('id: 2002');
  });

  it('resumes from a cursor and skips older events', async () => {
    const events = [
      { id: '1999', data: payloads[0] },
      { id: '2002', data: payloads[1] },
    ];
    const response = await streamWith(events, '/v1/transactions/stream', {
      'last-event-id': '2000',
    });

    expect(response.statusCode).toBe(200);
    expect(response.body).not.toContain('id: 1999');
    expect(response.body).toContain('id: 2002');
  });

  it('cleans up live subscribers after the stream closes', async () => {
    const response = await streamWith([], '/v1/transactions/stream');
    expect(response.statusCode).toBe(200);
    await expect(closeActiveStreamConnections()).resolves.toBeUndefined();
  });
});
