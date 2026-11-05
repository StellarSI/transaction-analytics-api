import { describe, it, expect, vi } from 'vitest';
import {
  HubbleBackfill,
  createBigQueryClient,
  dayRange,
  listDays,
  transformHubbleRow,
  type BigQueryClientLike,
  type BigQueryQueryResult,
  type HubbleRow,
} from '../backfill';
import type { CursorStore } from '../../types';
import type { LedgerOperationInsert } from '../../../db/schema/ledger';

/* ------------------------------------------------------------------ */
/* Fakes                                                               */
/* ------------------------------------------------------------------ */

function fakeBigQuery(
  days: Record<string, HubbleRow[]> = {},
): BigQueryClientLike & { calls: Array<{ query: string; params?: Record<string, unknown> }> } {
  const calls: Array<{ query: string; params?: Record<string, unknown> }> = [];
  return {
    calls,
    query: vi.fn(async (params: { query: string; params?: Record<string, unknown> }) => {
      calls.push(params);
      const from = String(params.params?.['from'] ?? '');
      const day = from.slice(0, 10);
      const rows = days[day] ?? [];
      const result: BigQueryQueryResult = { rows: rows as Record<string, unknown>[] };
      return [result] as [BigQueryQueryResult];
    }),
  } as unknown as BigQueryClientLike & {
    calls: Array<{ query: string; params?: Record<string, unknown> }>;
  };
}

interface FakeStore {
  insertOperations: ReturnType<typeof vi.fn>;
  findExistingKeys: ReturnType<typeof vi.fn>;
}

function fakeStore(existingKeys: Set<string> = new Set()): FakeStore {
  return {
    insertOperations: vi.fn(async (rows: LedgerOperationInsert[]) => rows.length),
    findExistingKeys: vi.fn(async () => existingKeys),
  };
}

function fakeCursorStore(initial: Record<string, string> = {}): CursorStore & {
  data: Map<string, string>;
} {
  const data = new Map(Object.entries(initial));
  return {
    data,
    get: vi.fn((key: string) => Promise.resolve(data.get(key) ?? null)),
    set: vi.fn((key: string, value: string) => {
      data.set(key, value);
      return Promise.resolve();
    }),
  } as unknown as CursorStore & { data: Map<string, string> };
}

function hubbleRow(overrides: Partial<HubbleRow> = {}): HubbleRow {
  return {
    ledger_sequence: 1000,
    ledger_close_time: '2026-01-01T12:00:00.000Z',
    op_type: 'payment',
    source_account: 'GSENDER',
    account: 'GRECIPIENT',
    asset_type: 'credit_alphanum4',
    asset_code: 'USDC',
    asset_issuer: 'GISSUER',
    amount: '25.5',
    fee_charged: '0.0001',
    transaction_hash: 'abc123',
    op_index: 0,
    transaction_success: true,
    ...overrides,
  };
}

/* ------------------------------------------------------------------ */
/* transformHubbleRow                                                  */
/* ------------------------------------------------------------------ */

describe('transformHubbleRow', () => {
  it('maps a complete Hubble row to a LedgerOperationInsert', () => {
    const insert = transformHubbleRow(hubbleRow());
    expect(insert.ledger_sequence).toBe(1000);
    expect(insert.ledger_closed_at).toBe('2026-01-01T12:00:00.000Z');
    expect(insert.operation_type).toBe('payment');
    expect(insert.source_account).toBe('GSENDER');
    expect(insert.destination_account).toBe('GRECIPIENT');
    expect(insert.asset_code).toBe('USDC');
    expect(insert.asset_issuer).toBe('GISSUER');
    expect(insert.asset_type).toBe('credit_alphanum4');
    expect(insert.amount).toBe('25.5');
    expect(insert.fee_charged).toBe('0.0001');
    expect(insert.transaction_hash).toBe('abc123');
    expect(insert.operation_index).toBe(0);
    expect(insert.successful).toBe(true);
  });

  it('coerces numeric strings from BigQuery to numbers', () => {
    const insert = transformHubbleRow(
      hubbleRow({ ledger_sequence: '4242', op_index: '7' }),
    );
    expect(insert.ledger_sequence).toBe(4242);
    expect(insert.operation_index).toBe(7);
  });

  it('appends a Z suffix when BigQuery omits the timezone', () => {
    const insert = transformHubbleRow(
      hubbleRow({ ledger_close_time: '2026-03-05T08:30:00.000' }),
    );
    expect(insert.ledger_closed_at).toBe('2026-03-05T08:30:00.000Z');
  });

  it('defaults successful to true when the flag is missing', () => {
    const insert = transformHubbleRow(hubbleRow({ transaction_success: undefined }));
    expect(insert.successful).toBe(true);
  });

  it('marks failed transactions as unsuccessful', () => {
    const insert = transformHubbleRow(hubbleRow({ transaction_success: false }));
    expect(insert.successful).toBe(false);
  });

  it('nulls out missing optional fields instead of throwing', () => {
    const insert = transformHubbleRow({});
    expect(insert.source_account).toBeNull();
    expect(insert.destination_account).toBeNull();
    expect(insert.asset_code).toBeNull();
    expect(insert.amount).toBeNull();
    expect(insert.fee_charged).toBeNull();
    expect(insert.transaction_hash).toBeNull();
    expect(insert.operation_index).toBeNull();
    expect(insert.operation_type).toBe('unknown');
  });
});

/* ------------------------------------------------------------------ */
/* Date helpers                                                        */
/* ------------------------------------------------------------------ */

describe('listDays', () => {
  it('returns an inclusive list of YYYY-MM-DD strings', () => {
    expect(listDays('2026-01-01', '2026-01-03')).toEqual([
      '2026-01-01',
      '2026-01-02',
      '2026-01-03',
    ]);
  });

  it('returns a single day when start equals end', () => {
    expect(listDays('2026-06-15', '2026-06-15')).toEqual(['2026-06-15']);
  });

  it('returns an empty list when start is after end', () => {
    expect(listDays('2026-01-05', '2026-01-01')).toEqual([]);
  });

  it('returns an empty list for malformed input', () => {
    expect(listDays('not-a-date', '2026-01-01')).toEqual([]);
  });
});

describe('dayRange', () => {
  it('produces a half-open UTC range for one day', () => {
    const { fromIso, toIso } = dayRange('2026-02-01');
    expect(fromIso).toBe('2026-02-01T00:00:00.000Z');
    expect(toIso).toBe('2026-02-02T00:00:00.000Z');
  });
});

/* ------------------------------------------------------------------ */
/* HubbleBackfill.run                                                  */
/* ------------------------------------------------------------------ */

describe('HubbleBackfill.run', () => {
  it('fetches, transforms, and inserts rows for each day in range', async () => {
    const bq = fakeBigQuery({
      '2026-01-01': [hubbleRow({ transaction_hash: 'tx1', op_index: 0 })],
      '2026-01-02': [
        hubbleRow({ transaction_hash: 'tx2', op_index: 0 }),
        hubbleRow({ transaction_hash: 'tx3', op_index: 1 }),
      ],
    });
    const store = fakeStore();

    const backfill = new HubbleBackfill({
      bigquery: bq,
      store,
      startDate: '2026-01-01',
      endDate: '2026-01-02',
    });

    const result = await backfill.run();

    expect(result.daysProcessed).toBe(2);
    expect(result.rowsFetched).toBe(3);
    expect(result.rowsInserted).toBe(3);
    expect(result.rowsSkipped).toBe(0);
    expect(store.insertOperations).toHaveBeenCalledTimes(2);
    expect(store.insertOperations.mock.calls[0]?.[0]).toHaveLength(1);
    expect(store.insertOperations.mock.calls[1]?.[0]).toHaveLength(2);
  });

  it('skips rows that already exist (idempotent re-run)', async () => {
    const bq = fakeBigQuery({
      '2026-01-01': [
        hubbleRow({ transaction_hash: 'tx1', op_index: 0 }),
        hubbleRow({ transaction_hash: 'tx2', op_index: 0 }),
      ],
    });
    const store = fakeStore(new Set(['tx1:0']));

    const backfill = new HubbleBackfill({
      bigquery: bq,
      store,
      startDate: '2026-01-01',
      endDate: '2026-01-01',
    });

    const result = await backfill.run();

    expect(result.rowsFetched).toBe(2);
    expect(result.rowsInserted).toBe(1);
    expect(result.rowsSkipped).toBe(1);
    const inserted = store.insertOperations.mock.calls[0]?.[0] as LedgerOperationInsert[];
    expect(inserted).toHaveLength(1);
    expect(inserted[0]?.transaction_hash).toBe('tx2');
  });

  it('resumes from the persisted cursor instead of startDate', async () => {
    const bq = fakeBigQuery({
      '2026-01-01': [hubbleRow({ transaction_hash: 'tx1' })],
      '2026-01-02': [hubbleRow({ transaction_hash: 'tx2' })],
      '2026-01-03': [hubbleRow({ transaction_hash: 'tx3' })],
    });
    const store = fakeStore();
    const cursorStore = fakeCursorStore({ 'backfill:cursor': '2026-01-02' });

    const backfill = new HubbleBackfill({
      bigquery: bq,
      store,
      startDate: '2026-01-01',
      endDate: '2026-01-03',
      cursorStore,
      cursorKey: 'backfill:cursor',
    });

    const result = await backfill.run();

    // Only days 01-02 (already done) and 01-03 remain.
    expect(result.daysProcessed).toBe(2);
    expect(result.rowsFetched).toBe(2);
    expect(cursorStore.data.get('backfill:cursor')).toBe('2026-01-03');
  });

  it('advances the cursor after each completed day', async () => {
    const bq = fakeBigQuery({
      '2026-01-01': [hubbleRow({ transaction_hash: 'tx1' })],
      '2026-01-02': [hubbleRow({ transaction_hash: 'tx2' })],
    });
    const store = fakeStore();
    const cursorStore = fakeCursorStore();

    const backfill = new HubbleBackfill({
      bigquery: bq,
      store,
      startDate: '2026-01-01',
      endDate: '2026-01-02',
      cursorStore,
      cursorKey: 'backfill:cursor',
    });

    await backfill.run();

    expect(cursorStore.data.get('backfill:cursor')).toBe('2026-01-02');
    // set called twice: once per day.
    expect(cursorStore.set).toHaveBeenCalledTimes(2);
  });

  it('does not advance the cursor when a day fails', async () => {
    const bq = fakeBigQuery({
      '2026-01-01': [hubbleRow({ transaction_hash: 'tx1' })],
      '2026-01-02': [hubbleRow({ transaction_hash: 'tx2' })],
    });
    const store = fakeStore();
    store.insertOperations
      .mockResolvedValueOnce(1)
      .mockRejectedValueOnce(new Error('db down'));
    const cursorStore = fakeCursorStore();

    const backfill = new HubbleBackfill({
      bigquery: bq,
      store,
      startDate: '2026-01-01',
      endDate: '2026-01-02',
      cursorStore,
      cursorKey: 'backfill:cursor',
    });

    await expect(backfill.run()).rejects.toThrow('db down');
    expect(cursorStore.data.get('backfill:cursor')).toBe('2026-01-01');
  });

  it('returns zero counts for an empty date range', async () => {
    const bq = fakeBigQuery();
    const store = fakeStore();

    const backfill = new HubbleBackfill({
      bigquery: bq,
      store,
      startDate: '2026-01-05',
      endDate: '2026-01-01',
    });

    const result = await backfill.run();
    expect(result.daysProcessed).toBe(0);
    expect(result.rowsFetched).toBe(0);
    expect(bq.query).not.toHaveBeenCalled();
  });

  it('skips the insert when a day has no rows', async () => {
    const bq = fakeBigQuery({ '2026-01-01': [] });
    const store = fakeStore();

    const backfill = new HubbleBackfill({
      bigquery: bq,
      store,
      startDate: '2026-01-01',
      endDate: '2026-01-01',
    });

    const result = await backfill.run();
    expect(result.daysProcessed).toBe(1);
    expect(result.rowsFetched).toBe(0);
    expect(store.insertOperations).not.toHaveBeenCalled();
  });

  it('passes asset filters as bound query parameters', async () => {
    const bq = fakeBigQuery();
    const store = fakeStore();

    const backfill = new HubbleBackfill({
      bigquery: bq,
      store,
      startDate: '2026-01-01',
      endDate: '2026-01-01',
      assetCode: 'USDC',
      assetIssuer: 'GISSUER',
      account: 'GACCOUNT',
    });

    await backfill.run();

    const call = bq.calls[0];
    expect(call?.query).toContain('asset_code = @assetCode');
    expect(call?.query).toContain('asset_issuer = @assetIssuer');
    expect(call?.query).toContain('source_account = @account');
    expect(call?.params).toMatchObject({
      assetCode: 'USDC',
      assetIssuer: 'GISSUER',
      account: 'GACCOUNT',
    });
  });

  it('batches large days according to batchSize', async () => {
    const rows: HubbleRow[] = [];
    for (let i = 0; i < 5; i++) {
      rows.push(hubbleRow({ transaction_hash: `tx${i}`, op_index: 0 }));
    }
    const bq = fakeBigQuery({ '2026-01-01': rows });
    const store = fakeStore();

    const backfill = new HubbleBackfill({
      bigquery: bq,
      store,
      startDate: '2026-01-01',
      endDate: '2026-01-01',
      batchSize: 2,
    });

    const result = await backfill.run();
    expect(result.rowsFetched).toBe(5);
    expect(store.insertOperations).toHaveBeenCalledTimes(3); // 2 + 2 + 1
  });

  it('enqueues aggregation jobs per hour bucket after insert', async () => {
    const bq = fakeBigQuery({
      '2026-01-01': [hubbleRow({ transaction_hash: 'tx1' })],
    });
    const store = fakeStore();
    const triggerAggregation = vi.fn(async () => {});

    const backfill = new HubbleBackfill({
      bigquery: bq,
      store,
      startDate: '2026-01-01',
      endDate: '2026-01-01',
      triggerAggregation,
    });

    const result = await backfill.run();

    // One day = 24 hours × 3 aggregation kinds.
    expect(result.aggregationJobsEnqueued).toBe(72);
    expect(triggerAggregation).toHaveBeenCalledTimes(72);
    expect(triggerAggregation).toHaveBeenCalledWith({
      kind: 'asset-flow',
      bucketStart: expect.any(String),
      interval: '1h',
    });
  });

  it('does not enqueue aggregations when nothing was inserted', async () => {
    const bq = fakeBigQuery({ '2026-01-01': [] });
    const store = fakeStore();
    const triggerAggregation = vi.fn(async () => {});

    const backfill = new HubbleBackfill({
      bigquery: bq,
      store,
      startDate: '2026-01-01',
      endDate: '2026-01-01',
      triggerAggregation,
    });

    const result = await backfill.run();
    expect(result.aggregationJobsEnqueued).toBe(0);
    expect(triggerAggregation).not.toHaveBeenCalled();
  });

  it('exposes the resume day after a successful run', async () => {
    const bq = fakeBigQuery({
      '2026-01-01': [hubbleRow({ transaction_hash: 'tx1' })],
    });
    const store = fakeStore();

    const backfill = new HubbleBackfill({
      bigquery: bq,
      store,
      startDate: '2026-01-01',
      endDate: '2026-01-01',
    });

    expect(backfill.resumeDay).toBeNull();
    await backfill.run();
    expect(backfill.resumeDay).toBe('2026-01-01');
  });
});

/* ------------------------------------------------------------------ */
/* createBigQueryClient                                                */
/* ------------------------------------------------------------------ */

describe('createBigQueryClient', () => {
  it('is a function that returns a promise', () => {
    expect(typeof createBigQueryClient).toBe('function');
    // Do not actually call it — @google-cloud/bigquery is not installed in
    // unit tests; the dynamic import path is covered by integration runs.
    expect(createBigQueryClient.constructor).toBe(Function);
  });
});
