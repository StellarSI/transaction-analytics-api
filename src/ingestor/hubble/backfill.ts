/**
 * Hubble/BigQuery historical backfill job (issue #17).
 *
 * Pulls historical Stellar operations from SDF's public BigQuery dataset
 * (`crypto_stellar.crypto_stellar_transactions` / operations), transforms
 * them into the `ledger_operations` schema, and batch-inserts them into the
 * TimescaleDB hypertable. After each day's insert the job enqueues
 * aggregation passes (issues #18, #19, #21) for the affected buckets.
 *
 * Design notes:
 * - The BigQuery client is a structural interface (`BigQueryClientLike`) so
 *   unit tests inject a mock; production wires the real
 *   `@google-cloud/bigquery` client through {@link createBigQueryClient}.
 * - Idempotency: before each batch insert the job queries for transaction
 *   hashes already present in the date range and skips them, so re-running
 *   the same range is safe.
 * - Resumability: the last fully-processed day is persisted to the injected
 *   `CursorStore`; on restart the job resumes from the day after the cursor.
 * - Progress: every batch and every completed day is logged with counts.
 */

import type { LedgerOperationInsert } from '../../db/schema/ledger';
import type { CursorStore } from '../types';
import { logger as defaultLogger } from '../../logger';

/* ------------------------------------------------------------------ */
/* BigQuery port                                                       */
/* ------------------------------------------------------------------ */

/** Shape of a BigQuery query result (rows as plain objects). */
export interface BigQueryQueryResult {
  rows: Record<string, unknown>[];
}

/**
 * Structural interface for the BigQuery client's `query` method. Mirrors
 * `@google-cloud/bigquery`'s `BigQuery.query()` enough for the backfill;
 * tests inject a mock without touching the network.
 */
export interface BigQueryClientLike {
  query(params: {
    query: string;
    params?: Record<string, unknown>;
  }): Promise<[BigQueryQueryResult]>;
}

/** Options accepted by {@link createBigQueryClient}. */
export interface BigQueryClientOptions {
  /** GCP project id holding the Hubble dataset. */
  projectId?: string;
  /** Path to a service-account JSON key file. */
  keyFilename?: string;
}

/**
 * Lazily constructs a real `@google-cloud/bigquery` client.
 *
 * The module specifier is a variable so TypeScript does not attempt to
 * resolve it at compile time — `@google-cloud/bigquery` is an optional
 * runtime dependency that operators install alongside the service account
 * credentials. The structural {@link BigQueryClientLike} interface keeps the
 * rest of the codebase (and all tests) independent of the concrete SDK.
 */
export async function createBigQueryClient(
  options: BigQueryClientOptions = {},
): Promise<BigQueryClientLike> {
  const moduleName = '@google-cloud/bigquery';
  const mod = (await import(moduleName)) as {
    BigQuery: new (opts: BigQueryClientOptions) => BigQueryClientLike;
  };
  return new mod.BigQuery(options);
}

/* ------------------------------------------------------------------ */
/* Hubble row → LedgerOperationInsert                                  */
/* ------------------------------------------------------------------ */

/**
 * One row as returned by the Hubble BigQuery operations table.
 * Fields are optional because BigQuery omits null columns; the transform
 * falls back to `null` for anything missing.
 */
export interface HubbleRow {
  ledger_sequence?: number | string;
  ledger_close_time?: string;
  op_type?: string;
  source_account?: string;
  account?: string;
  asset_type?: string;
  asset_code?: string;
  asset_issuer?: string;
  amount?: string | number;
  fee_charged?: string | number;
  transaction_hash?: string;
  op_index?: number | string;
  transaction_success?: boolean;
}

function toNumberOrNull(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  const n = typeof value === 'number' ? value : Number(value);
  return Number.isFinite(n) ? n : null;
}

function toIntOrNull(value: unknown): number | null {
  const n = toNumberOrNull(value);
  return n === null ? null : Math.trunc(n);
}

function toStringOrNull(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  return String(value);
}

/**
 * Maps a Hubble BigQuery row to a `LedgerOperationInsert` that matches the
 * `ledger_operations` hypertable schema. Pure and total: any missing or
 * malformed field becomes `null` rather than throwing, so one bad row never
 * aborts a whole batch.
 *
 * `ledger_closed_at` is taken from `ledger_close_time`; when BigQuery returns
 * it without a timezone suffix the value is treated as UTC (BigQuery
 * TIMESTAMPs are UTC by definition).
 */
export function transformHubbleRow(row: HubbleRow): LedgerOperationInsert {
  const closedAt = toStringOrNull(row.ledger_close_time);
  return {
    ledger_sequence: toIntOrNull(row.ledger_sequence) ?? 0,
    ledger_closed_at: closedAt
      ? closedAt.endsWith('Z') || closedAt.includes('+')
        ? closedAt
        : `${closedAt}Z`
      : new Date(0).toISOString(),
    operation_type: toStringOrNull(row.op_type) ?? 'unknown',
    source_account: toStringOrNull(row.source_account),
    destination_account: toStringOrNull(row.account),
    asset_code: toStringOrNull(row.asset_code),
    asset_issuer: toStringOrNull(row.asset_issuer),
    asset_type: toStringOrNull(row.asset_type),
    amount: row.amount !== null && row.amount !== undefined ? String(row.amount) : null,
    fee_charged:
      row.fee_charged !== null && row.fee_charged !== undefined
        ? String(row.fee_charged)
        : null,
    transaction_hash: toStringOrNull(row.transaction_hash),
    operation_index: toIntOrNull(row.op_index),
    successful: row.transaction_success !== false,
  };
}

/* ------------------------------------------------------------------ */
/* Database port                                                       */
/* ------------------------------------------------------------------ */

/** Minimal database port the backfill needs; satisfied by `postgres.Sql`. */
export interface BackfillStore {
  /**
   * Inserts rows into `ledger_operations`, skipping any whose
   * `(transaction_hash, operation_index)` pair already exists. Returns the
   * number of rows actually inserted.
   */
  insertOperations(rows: LedgerOperationInsert[]): Promise<number>;
  /**
   * Returns the set of `${transaction_hash}:${operation_index}` keys already
   * present in `ledger_operations` for rows whose `ledger_closed_at` falls in
   * `[fromIso, toIso)`.
   */
  findExistingKeys(fromIso: string, toIso: string): Promise<Set<string>>;
}

/* ------------------------------------------------------------------ */
/* Aggregation trigger port                                            */
/* ------------------------------------------------------------------ */

/** One aggregation pass enqueued after a day is backfilled. */
export interface AggregationTrigger {
  kind: 'asset-flow' | 'account-flow' | 'corridor';
  bucketStart: string;
  interval: '1m' | '1h';
}

/** Enqueues aggregation jobs for a completed backfill range. */
export type AggregationTriggerFn = (job: AggregationTrigger) => Promise<void>;

/* ------------------------------------------------------------------ */
/* Options                                                             */
/* ------------------------------------------------------------------ */

export interface HubbleBackfillOptions {
  /** BigQuery client (structural port). */
  bigquery: BigQueryClientLike;
  /** Database port for batch inserts and idempotency checks. */
  store: BackfillStore;
  /** First day (inclusive, `YYYY-MM-DD`) to backfill. */
  startDate: string;
  /** Last day (inclusive, `YYYY-MM-DD`) to backfill. */
  endDate: string;
  /** Optional durable cursor; without it the job always starts at `startDate`. */
  cursorStore?: CursorStore;
  /** Key under which the resume cursor is persisted. */
  cursorKey?: string;
  /** Optional filter: only backfill operations of this asset code. */
  assetCode?: string;
  /** Optional filter: only backfill operations of this asset issuer. */
  assetIssuer?: string;
  /** Optional filter: only backfill operations involving this account. */
  account?: string;
  /** Rows per batch insert. Default: 500. */
  batchSize?: number;
  /** Enqueued after each completed day. */
  triggerAggregation?: AggregationTriggerFn;
  /** Injected for tests so progress delays are instantaneous. */
  wait?: (ms: number) => Promise<void>;
}

type ResolvedOptions = Omit<HubbleBackfillOptions, 'batchSize' | 'wait'> & {
  batchSize: number;
  wait: (ms: number) => Promise<void>;
};

const defaultWait = (ms: number) =>
  new Promise<void>((resolve) => setTimeout(resolve, ms));

/* ------------------------------------------------------------------ */
/* Result type                                                         */
/* ------------------------------------------------------------------ */

/** Summary returned by {@link HubbleBackfill.run}. */
export interface BackfillResult {
  /** Number of days processed in this run. */
  daysProcessed: number;
  /** Number of rows fetched from BigQuery. */
  rowsFetched: number;
  /** Number of rows newly inserted (excludes already-present rows). */
  rowsInserted: number;
  /** Number of rows skipped because they already existed. */
  rowsSkipped: number;
  /** Number of aggregation jobs enqueued. */
  aggregationJobsEnqueued: number;
}

/* ------------------------------------------------------------------ */
/* HubbleBackfill                                                      */
/* ------------------------------------------------------------------ */

/**
 * Iterates day-by-day over `[startDate, endDate]`, queries Hubble for each
 * day, transforms and batch-inserts the rows, then advances a persisted
 * cursor. Any error in a day aborts the run but leaves the cursor at the
 * last successfully completed day, so a re-run resumes exactly where it
 * stopped.
 */
export class HubbleBackfill {
  private readonly options: ResolvedOptions;
  private cursor: string | null = null;

  constructor(options: HubbleBackfillOptions) {
    this.options = {
      ...options,
      batchSize: options.batchSize ?? 500,
      wait: options.wait ?? defaultWait,
    };
  }

  /** Human-readable job name used in logs. */
  get name(): string {
    return 'hubble-backfill';
  }

  /** Last fully-processed day (`YYYY-MM-DD`), or null before any run. */
  get resumeDay(): string | null {
    return this.cursor;
  }

  /**
   * Runs the backfill for the configured date range. Returns aggregate
   * counts; throws if BigQuery or the database fails, in which case the
   * cursor still points at the last completed day for a safe re-run.
   */
  async run(): Promise<BackfillResult> {
    const start = await this.loadCursor();
    this.cursor = start;

    const days = listDays(start ?? this.options.startDate, this.options.endDate);
    const result: BackfillResult = {
      daysProcessed: 0,
      rowsFetched: 0,
      rowsInserted: 0,
      rowsSkipped: 0,
      aggregationJobsEnqueued: 0,
    };

    if (days.length === 0) {
      defaultLogger.info(
        { job: this.name, start, end: this.options.endDate },
        `${this.name}: nothing to do (start after end)`,
      );
      return result;
    }

    defaultLogger.info(
      {
        job: this.name,
        from: days[0],
        to: days[days.length - 1],
        days: days.length,
      },
      `${this.name}: starting`,
    );

    for (const day of days) {
      const dayResult = await this.processDay(day);
      result.daysProcessed += 1;
      result.rowsFetched += dayResult.fetched;
      result.rowsInserted += dayResult.inserted;
      result.rowsSkipped += dayResult.skipped;
      result.aggregationJobsEnqueued += dayResult.aggregationJobs;

      // Persist the cursor only after the day fully succeeded so a crash
      // mid-day causes that day to be re-processed (idempotently) on restart.
      this.cursor = day;
      await this.persistCursor();

      defaultLogger.info(
        {
          job: this.name,
          day,
          fetched: dayResult.fetched,
          inserted: dayResult.inserted,
          skipped: dayResult.skipped,
          aggregation: dayResult.aggregationJobs,
          progress: `${result.daysProcessed}/${days.length}`,
        },
        `${this.name}: day complete`,
      );
    }

    defaultLogger.info(
      { job: this.name, ...result },
      `${this.name}: finished`,
    );
    return result;
  }

  /** Queries, transforms, and inserts one day of Hubble data. */
  private async processDay(
    day: string,
  ): Promise<{
    fetched: number;
    inserted: number;
    skipped: number;
    aggregationJobs: number;
  }> {
    const { fromIso, toIso } = dayRange(day);
    const rows = await this.fetchDay(day, fromIso, toIso);
    if (rows.length === 0) {
      return { fetched: 0, inserted: 0, skipped: 0, aggregationJobs: 0 };
    }

    const transformed = rows.map(transformHubbleRow);
    const inserted = await this.insertBatched(transformed, fromIso, toIso);
    const skipped = transformed.length - inserted;

    let aggregationJobs = 0;
    if (this.options.triggerAggregation && inserted > 0) {
      aggregationJobs = await this.enqueueAggregations(fromIso, toIso);
    }

    return { fetched: rows.length, inserted, skipped, aggregationJobs };
  }

  /**
   * Runs the Hubble SQL query for one day. The WHERE clause is built with
   * parameter placeholders so account/asset filters are safely bound rather
   * than interpolated.
   */
  private async fetchDay(
    day: string,
    fromIso: string,
    toIso: string,
  ): Promise<HubbleRow[]> {
    const conditions: string[] = [
      'ledger_close_time >= @from',
      'ledger_close_time < @to',
    ];
    const params: Record<string, unknown> = { from: fromIso, to: toIso };

    if (this.options.assetCode) {
      conditions.push('asset_code = @assetCode');
      params.assetCode = this.options.assetCode;
    }
    if (this.options.assetIssuer) {
      conditions.push('asset_issuer = @assetIssuer');
      params.assetIssuer = this.options.assetIssuer;
    }
    if (this.options.account) {
      conditions.push('(source_account = @account OR account = @account)');
      params.account = this.options.account;
    }

    const sql = `
      SELECT
        ledger_sequence,
        ledger_close_time,
        op_type,
        source_account,
        account,
        asset_type,
        asset_code,
        asset_issuer,
        amount,
        fee_charged,
        transaction_hash,
        op_index,
        transaction_success
      FROM \`crypto_stellar.crypto_stellar_transactions\`
      WHERE ${conditions.join(' AND ')}
      ORDER BY ledger_sequence, op_index
    `;

    const [result] = await this.options.bigquery.query({ query: sql, params });
    return result?.rows ?? [];
  }

  /**
   * Inserts transformed rows in batches, filtering out rows whose
   * `(transaction_hash, operation_index)` key already exists so the job is
   * idempotent across re-runs.
   */
  private async insertBatched(
    rows: LedgerOperationInsert[],
    fromIso: string,
    toIso: string,
  ): Promise<number> {
    const existing = await this.options.store.findExistingKeys(fromIso, toIso);
    let inserted = 0;

    for (let i = 0; i < rows.length; i += this.options.batchSize) {
      const batch = rows.slice(i, i + this.options.batchSize).filter((r) => {
        if (!r.transaction_hash) return true; // no key → insert (dedup impossible)
        const key = `${r.transaction_hash}:${r.operation_index ?? 0}`;
        return !existing.has(key);
      });
      if (batch.length === 0) continue;

      const count = await this.options.store.insertOperations(batch);
      inserted += count;

      for (const r of batch) {
        if (r.transaction_hash) {
          existing.add(`${r.transaction_hash}:${r.operation_index ?? 0}`);
        }
      }

      defaultLogger.debug(
        {
          job: this.name,
          batch: Math.floor(i / this.options.batchSize) + 1,
          batchSize: batch.length,
          total: rows.length,
          inserted,
        },
        `${this.name}: batch inserted`,
      );
    }

    return inserted;
  }

  /**
   * Enqueues one aggregation pass per hour bucket covered by the day. The
   * intervals mirror the aggregate hypertables (`1h` primary, `1m` for the
   * finest granularity the queue supports).
   */
  private async enqueueAggregations(
    fromIso: string,
    toIso: string,
  ): Promise<number> {
    const trigger = this.options.triggerAggregation;
    if (!trigger) return 0;

    let count = 0;
    const start = new Date(fromIso);
    const end = new Date(toIso);
    for (let t = start.getTime(); t < end.getTime(); t += 3_600_000) {
      const bucketStart = new Date(t).toISOString();
      for (const kind of ['asset-flow', 'account-flow', 'corridor'] as const) {
        await trigger({ kind, bucketStart, interval: '1h' });
        count += 1;
      }
    }
    return count;
  }

  /** Loads the resume cursor (last completed day), or null on cold start. */
  private async loadCursor(): Promise<string | null> {
    if (!this.options.cursorStore || !this.options.cursorKey) return null;
    try {
      const raw = await this.options.cursorStore.get(this.options.cursorKey);
      if (!raw) return null;
      return /^\d{4}-\d{2}-\d{2}$/.test(raw) ? raw : null;
    } catch (error) {
      defaultLogger.warn(
        { err: error, job: this.name },
        `${this.name}: could not load cursor, starting fresh`,
      );
      return null;
    }
  }

  /** Persists the resume cursor (last completed day). */
  private async persistCursor(): Promise<void> {
    if (!this.options.cursorStore || !this.options.cursorKey) return;
    if (!this.cursor) return;
    try {
      await this.options.cursorStore.set(this.options.cursorKey, this.cursor);
    } catch (error) {
      defaultLogger.warn(
        { err: error, job: this.name },
        `${this.name}: could not persist cursor`,
      );
    }
  }
}

/* ------------------------------------------------------------------ */
/* Date helpers                                                        */
/* ------------------------------------------------------------------ */

/** Inclusive `YYYY-MM-DD` day list from `start` to `end` (inclusive). */
export function listDays(start: string, end: string): string[] {
  const days: string[] = [];
  const startDate = parseDay(start);
  const endDate = parseDay(end);
  if (!startDate || !endDate || startDate > endDate) return days;
  for (
    let t = startDate.getTime();
    t <= endDate.getTime();
    t += 86_400_000
  ) {
    days.push(new Date(t).toISOString().slice(0, 10));
  }
  return days;
}

/** Half-open UTC `[from, to)` ISO range covering one `YYYY-MM-DD` day. */
export function dayRange(day: string): { fromIso: string; toIso: string } {
  const d = parseDay(day) ?? new Date(0);
  return {
    fromIso: d.toISOString(),
    toIso: new Date(d.getTime() + 86_400_000).toISOString(),
  };
}

function parseDay(day: string): Date | null {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(day);
  if (!m) return null;
  const d = new Date(Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3])));
  return Number.isNaN(d.getTime()) ? null : d;
}
