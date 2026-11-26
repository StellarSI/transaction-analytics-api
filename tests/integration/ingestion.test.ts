import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { pool } from '@/db/pool';
import { runMigrations } from '@/db/migrate';
import { PaymentIngestor } from '@/ingestor/horizon/payments';
import type {
  HorizonOperationRecord,
  PaymentPayload,
} from '@/ingestor/types';
import { PAYMENT_QUEUE, createMemoryCursorStore } from '@/ingestor/types';
import type {
  OperationStreamOpener,
  StreamHandlers,
} from '@/ingestor/horizon/stream';
import { createRedisListQueue } from '@/ingestor/queue';
import { createPostgresBackfillStore, type BackfillSql } from '@/ingestor/hubble/store';
import {
  AssetFlowAggregator,
  createPostgresAssetFlowStore,
  type AssetFlowSql,
} from '@/aggregator/asset-flows';
import {
  createPostgresAccountFlowStore,
  type AccountFlowSql,
} from '@/aggregator/account-insights';
import { closeRedis, getRedis } from '@/redis/client';

/**
 * End-to-end ingestion coverage (issue #41): fake Horizon SSE -> Redis queue
 * -> ledger_operations -> asset/account aggregates -> reorg recompute.
 *
 * Requires the TimescaleDB and Redis instances provided by CI
 * (DATABASE_URL / REDIS_URL). Horizon itself is injected, so no live Stellar
 * network is contacted.
 */

const ISSUER = 'GB5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN';
const BUCKET_START = '2026-10-01T00:00:00Z';
const SENDER = `G${'A'.repeat(55)}`;
const RECIPIENT = `G${'B'.repeat(55)}`;

function paymentRecord(
  id: string,
  hash: string,
  amount: string,
  overrides: Partial<HorizonOperationRecord> = {},
): HorizonOperationRecord {
  return {
    id,
    paging_token: id,
    type: 'payment',
    type_i: 1,
    source_account: SENDER,
    from: SENDER,
    to: RECIPIENT,
    amount,
    asset_type: 'credit_alphanum4',
    asset_code: 'TSTU',
    asset_issuer: ISSUER,
    transaction_hash: hash,
    created_at: BUCKET_START,
    ledger: 1000,
    ...overrides,
  };
}

/** SSE opener that replays a fixed batch once the ingestor attaches. */
function recordingOpener(records: readonly HorizonOperationRecord[]): {
  opener: OperationStreamOpener;
  handlers: () => StreamHandlers;
  closed: () => boolean;
} {
  let current: StreamHandlers | undefined;
  let isClosed = false;
  return {
    opener: ({ handlers }) => {
      current = handlers;
      isClosed = false;
      return () => {
        isClosed = true;
      };
    },
    handlers: () => {
      if (!current) throw new Error('stream was not opened');
      return current;
    },
    closed: () => isClosed,
  };
}

const flush = () => new Promise((resolve) => setTimeout(resolve, 0));

describe('Horizon -> queue -> DB -> aggregates (integration, issue #41)', () => {
  let assetAggregator: AssetFlowAggregator;

  beforeAll(async () => {
    await runMigrations('up');
    await pool`DELETE FROM asset_flow_aggregates`;
    await pool`DELETE FROM account_flow_aggregates`;
    await pool`DELETE FROM ledger_operations`;
    await getRedis().del(PAYMENT_QUEUE);
  }, 60000);

  afterAll(async () => {
    await pool`DELETE FROM asset_flow_aggregates`;
    await pool`DELETE FROM account_flow_aggregates`;
    await pool`DELETE FROM ledger_operations`;
    await getRedis().del(PAYMENT_QUEUE);
    await pool.end();
    await closeRedis();
  });

  it('persists a mocked Horizon batch and derives both aggregate tables', async () => {
    const stream = recordingOpener([
      paymentRecord('1001', 'hash-1', '100.0000000'),
      paymentRecord('1002', 'hash-2', '50.0000000'),
    ]);
    const queue = createRedisListQueue(getRedis());
    const ingestor = new PaymentIngestor({
      opener: stream.opener,
      queue,
      cursorStore: createMemoryCursorStore(),
      cursorKey: 'integration:payments',
      initialCursor: 'now',
    });

    await ingestor.start();
    for (const record of [
      paymentRecord('1001', 'hash-1', '100.0000000'),
      paymentRecord('1002', 'hash-2', '50.0000000'),
    ]) {
      stream.handlers().onOperation(record);
    }
    await flush();
    ingestor.stop();

    expect(stream.closed()).toBe(true);
    expect(ingestor.resumeCursor).toBe('1002');

    // Queue: the ingestor produced two JSON payloads on the Redis list.
    const queued = await getRedis().lrange(PAYMENT_QUEUE, 0, -1);
    expect(queued).toHaveLength(2);
    const payloads = queued.map((entry) => JSON.parse(entry) as PaymentPayload);
    expect(payloads.map((p) => p.kind)).toEqual(['payment', 'payment']);
    expect(payloads[0]).toMatchObject({
      from: SENDER,
      to: RECIPIENT,
      amount: '100.0000000',
      asset: { code: 'TSTU', issuer: ISSUER },
    });
    await getRedis().del(PAYMENT_QUEUE);

    // DB: batch-insert through the same store the Hubble backfill uses.
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
    const backfill = createPostgresBackfillStore(pool as unknown as BackfillSql);
    expect(await backfill.insertOperations(rows)).toBe(2);

    const stored = await pool<{ transaction_hash: string; amount: string }[]>`
      SELECT transaction_hash, amount
      FROM ledger_operations
      WHERE transaction_hash IN ('hash-1', 'hash-2')
      ORDER BY transaction_hash
    `;
    expect(stored).toHaveLength(2);
    expect(stored.map((row) => row.amount)).toEqual(['100.0000000', '50.0000000']);

    // Aggregates: fold the same operations into the real hypertables.
    const assetStore = createPostgresAssetFlowStore(pool as unknown as AssetFlowSql);
    assetAggregator = new AssetFlowAggregator({
      store: assetStore,
      intervals: ['1m'],
    });
    await assetAggregator.applyOperations(
      rows.map((row) => ({
        assetCode: row.asset_code,
        assetIssuer: row.asset_issuer,
        amount: row.amount,
        fromAccount: row.source_account ?? '',
        toAccount: row.destination_account ?? '',
        ledgerSequence: row.ledger_sequence,
        ledgerClosedAt: row.ledger_closed_at,
      })),
      { kind: 'payment' },
    );

    const accountStore = createPostgresAccountFlowStore(
      pool as unknown as AccountFlowSql,
    );
    const { createAccountFlowHandlers } = await import('@/aggregator/account-insights');
    const accountHandlers = createAccountFlowHandlers({
      store: accountStore,
      intervals: ['1m'],
    });
    for (const payload of payloads) {
      await accountHandlers.payment?.(
        payload as Parameters<NonNullable<typeof accountHandlers.payment>>[0],
      );
    }

    const assetRows = await pool<{ volume: string; payments: number }[]>`
      SELECT volume, payments FROM asset_flow_aggregates
      WHERE interval = '1m' AND bucket_start = ${BUCKET_START}
    `;
    expect(assetRows).toHaveLength(1);
    expect(Number(assetRows[0].volume)).toBe(150);
    expect(assetRows[0].payments).toBe(2);

    const accountRows = await pool<{ account_id: string; inflow: string; outflow: string }[]>`
      SELECT account_id, inflow, outflow FROM account_flow_aggregates
      WHERE interval = '1m' AND bucket_start = ${BUCKET_START}
      ORDER BY account_id
    `;
    expect(accountRows.map((row) => row.account_id)).toEqual([SENDER, RECIPIENT]);
    const sender = accountRows.find((row) => row.account_id === SENDER);
    const recipient = accountRows.find((row) => row.account_id === RECIPIENT);
    expect(Number(sender?.outflow)).toBe(150);
    expect(Number(recipient?.inflow)).toBe(150);
  });

  it('recomputes the affected bucket after a reorg removes one operation', async () => {
    // Simulate a reorg: ledger 1000 was replaced and no longer contains hash-2.
    await pool`DELETE FROM ledger_operations WHERE transaction_hash = 'hash-2'`;

    await assetAggregator.recomputeBucket(BUCKET_START, '1m');

    const rows = await pool<{ volume: string; payments: number }[]>`
      SELECT volume, payments FROM asset_flow_aggregates
      WHERE interval = '1m' AND bucket_start = ${BUCKET_START}
    `;
    expect(rows).toHaveLength(1);
    expect(Number(rows[0].volume)).toBe(100);
    expect(rows[0].payments).toBe(1);

    const remaining = await pool<{ transaction_hash: string }[]>`
      SELECT transaction_hash FROM ledger_operations
      WHERE transaction_hash IN ('hash-1', 'hash-2')
    `;
    expect(remaining).toHaveLength(1);
    expect(remaining[0].transaction_hash).toBe('hash-1');
  });
});
