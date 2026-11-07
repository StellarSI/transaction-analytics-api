/**
 * Trustline growth and activity patterns aggregator (issue #20).
 *
 * Consumes parsed `change_trust` operations and payment/path-payment
 * activity from the `ledger-operations` queue (via `src/queue/processors.ts`)
 * and maintains the `trustline_aggregates` hypertable: time-bucketed
 * per-account trustline counts, trust-limit totals and activity patterns
 * (payments per bucket, distinct active hours, dormancy).
 *
 * Real-time path:
 * - Each `change_trust` job is folded into the minute, hour and day buckets
 *   of the ledger close time. A non-zero `limit` counts as a new/changed
 *   trustline; a zero limit counts as a removal. The sum of non-zero limits
 *   is tracked as `total_trust_limit` using bigint stroops.
 * - Each `payment` / `path_payment` leg increments `payments_count` for
 *   both the sender and the recipient, and records the UTC active hour so
 *   `active_hours` (distinct hours of day) can be derived per bucket.
 * - A bucket with any activity reports `dormancy_days = 0`; dormant
 *   accounts simply have no delta for that bucket, so `/v1` consumers can
 *   infer dormancy from the gap between successive buckets. The pure helper
 *   {@link computeDormancyDays} exposes the same arithmetic for callers
 *   that have a last-activity timestamp.
 *
 * Reorg path:
 * Stellar forks can roll back recent ledgers. Whenever an incoming
 * operation carries a ledger sequence lower than the high-water mark this
 * process has seen, the buckets covering the configurable trailing
 * lookback window are deleted and re-aggregated from the
 * `ledger_operations` hypertable, then the incoming operation is processed
 * normally. `recomputeBucket` is also exposed for `trustline` aggregation
 * jobs produced by the Hubble backfill.
 *
 * Precision: amounts travel as decimal strings and are summed with bigint
 * stroops (7 decimal places) — no float rounding anywhere.
 */

// ----------------------------------------------------------------------
// Imports
// ----------------------------------------------------------------------

import { Counter, register, type Registry } from 'prom-client';
import { logger } from '../logger';
import type {
  LedgerOperationHandler,
  AggregationJobHandler,
} from '../queue/processors';
import type {
  ChangeTrustPayload,
  PaymentPayload,
  PathPaymentPayload,
} from '../ingestor/types';
import type { TrustlineAggregate } from '../db/schema/aggregates';
import type { CursorStore } from '../ingestor/types';

// ----------------------------------------------------------------------
// Decimal arithmetic (7-decimal stroops, bigint)
// ----------------------------------------------------------------------

const STROOP_SCALE = 10_000_000n;
const AMOUNT_PATTERN = /^([+-]?)(\d+)(?:\.(\d{1,7}))?$/;

/** Parses a Stellar amount/limit string into stroops. Throws on bad input. */
export function parseAmount(amount: string): bigint {
  const match = AMOUNT_PATTERN.exec(amount.trim());
  if (!match) {
    throw new Error(`invalid amount: ${JSON.stringify(amount)}`);
  }
  const [, sign, intPart, fracPart = ''] = match;
  const fraction = (fracPart + '0000000').slice(0, 7);
  const value = BigInt(intPart) * STROOP_SCALE + BigInt(fraction);
  return sign === '-' ? -value : value;
}

/** Formats stroops back into a 7-decimal string (canonical Stellar form). */
export function formatAmount(value: bigint): string {
  const negative = value < 0n;
  const absolute = negative ? -value : value;
  const intPart = absolute / STROOP_SCALE;
  const fracPart = (absolute % STROOP_SCALE).toString().padStart(7, '0');
  return `${negative ? '-' : ''}${intPart}.${fracPart}`;
}

// ----------------------------------------------------------------------
// Time buckets
// ----------------------------------------------------------------------

export type TrustlineInterval = '1m' | '1h' | '1d';

const INTERVAL_MS: Record<TrustlineInterval, number> = {
  '1m': 60_000,
  '1h': 3_600_000,
  '1d': 86_400_000,
};

export const TRUSTLINE_INTERVALS: readonly TrustlineInterval[] = ['1m', '1h', '1d'];

/** Floor `tsMs` to the start of its interval bucket; returns ISO 8601. */
export function bucketStartIso(tsMs: number, interval: TrustlineInterval): string {
  const size = INTERVAL_MS[interval];
  return new Date(Math.floor(tsMs / size) * size).toISOString();
}

/** ISO 8601 end (exclusive) of the bucket starting at `bucketIso`. */
export function bucketEndIso(bucketIso: string, interval: TrustlineInterval): string {
  const start = Date.parse(bucketIso);
  if (Number.isNaN(start)) {
    throw new Error(`invalid bucketStart: ${JSON.stringify(bucketIso)}`);
  }
  return new Date(start + INTERVAL_MS[interval]).toISOString();
}

/** Inclusive list of bucket starts from `fromIso` to `toIso` (exclusive). */
export function listBucketStarts(
  fromIso: string,
  toIso: string,
  interval: TrustlineInterval,
): string[] {
  const size = INTERVAL_MS[interval];
  const from = Date.parse(fromIso);
  const to = Date.parse(toIso);
  if (Number.isNaN(from) || Number.isNaN(to)) {
    throw new Error(
      `invalid bucket bounds: ${JSON.stringify(fromIso)}..${JSON.stringify(toIso)}`,
    );
  }
  const starts: string[] = [];
  for (let t = Math.floor(from / size) * size; t < to; t += size) {
    starts.push(new Date(t).toISOString());
  }
  return starts;
}

/** True for the intervals this module can aggregate. */
export function isTrustlineInterval(value: string): value is TrustlineInterval {
  return value === '1m' || value === '1h' || value === '1d';
}

// ----------------------------------------------------------------------
// Activity helpers (pure, unit-test friendly)
// ----------------------------------------------------------------------

/**
 * Days of dormancy between the last activity and the bucket start.
 * Returns 0 when there was activity at (or after) the bucket start, or the
 * floored day gap otherwise. A never-active account reports 0.
 */
export function computeDormancyDays(
  lastActivityIso: string | null | undefined,
  bucketStartIsoValue: string,
): number {
  if (!lastActivityIso) return 0;
  const last = Date.parse(lastActivityIso);
  const bucket = Date.parse(bucketStartIsoValue);
  if (Number.isNaN(last) || Number.isNaN(bucket)) return 0;
  if (last >= bucket) return 0;
  return Math.floor((bucket - last) / 86_400_000);
}

/**
 * Distinct UTC hours (0-23) covered by the given ISO timestamps.
 * Used for the `active_hours` metric on day buckets; shorter buckets
 * naturally yield 0 or 1.
 */
export function computeActiveHoursCount(timestamps: readonly string[]): number {
  const hours = new Set<number>();
  for (const ts of timestamps) {
    const ms = Date.parse(ts);
    if (Number.isNaN(ms)) continue;
    hours.add(new Date(ms).getUTCHours());
  }
  return hours.size;
}

// ----------------------------------------------------------------------
// Normalized operations
// ----------------------------------------------------------------------

/** A changeTrust operation normalized for aggregation. */
export interface TrustlineOperation {
  accountId: string;
  assetCode: string;
  assetIssuer: string | null;
  /** Trust limit as decimal string; `0.0000000` removes the trustline. */
  limit: string;
  isDeletion: boolean;
  ledgerSequence?: number;
  ledgerClosedAt?: string;
}

/** A payment-style operation used only for activity patterns. */
export interface ActivityOperation {
  accountId: string;
  ledgerSequence?: number;
  ledgerClosedAt?: string;
}

/** Unified delta per (bucket, account). */
export interface TrustlineDelta {
  interval: TrustlineInterval;
  bucketStart: string;
  accountId: string;
  trustlineCount: number;
  newTrustlines: number;
  removedTrustlines: number;
  trustLimitChanges: number;
  totalTrustLimit: string;
  paymentsCount: number;
  activeHours: number;
  dormancyDays: number;
}

/** Row shape as read back from Postgres. */
export interface TrustlineStoredOperation {
  account_id: string | null;
  operation_type: string | null;
  asset_code: string | null;
  asset_issuer: string | null;
  amount: string | null;
  source_account: string | null;
  destination_account: string | null;
  ledger_sequence?: number | null;
  ledger_closed_at: string;
}

// ----------------------------------------------------------------------
// Core computation (pure)
// ----------------------------------------------------------------------

interface Accumulator {
  bucketStart: string;
  accountId: string;
  newTrustlines: number;
  removedTrustlines: number;
  trustLimitChanges: number;
  totalTrustLimit: bigint;
  paymentsCount: number;
  activeHoursSet: Set<number>;
  lastActivityMs: number | null;
}

function parseTsMs(ledgerClosedAt: string | undefined, opLabel: string): number {
  if (!ledgerClosedAt) {
    throw new Error(`${opLabel} is missing ledgerClosedAt; cannot assign a bucket`);
  }
  const tsMs = Date.parse(ledgerClosedAt);
  if (Number.isNaN(tsMs)) {
    throw new Error(`${opLabel} has unparseable ledgerClosedAt: ${ledgerClosedAt}`);
  }
  return tsMs;
}

function getOrCreate(
  accumulators: Map<string, Accumulator>,
  bucketStart: string,
  accountId: string,
): Accumulator {
  const key = `${bucketStart} ${accountId}`;
  let entry = accumulators.get(key);
  if (!entry) {
    entry = {
      bucketStart,
      accountId,
      newTrustlines: 0,
      removedTrustlines: 0,
      trustLimitChanges: 0,
      totalTrustLimit: 0n,
      paymentsCount: 0,
      activeHoursSet: new Set(),
      lastActivityMs: null,
    };
    accumulators.set(key, entry);
  }
  return entry;
}

/**
 * Folds trustline and activity operations into per-(bucket, account) deltas.
 * Empty input yields an empty list.
 */
export function computeTrustlineDeltas(
  trustlineOps: readonly TrustlineOperation[],
  activityOps: readonly ActivityOperation[],
  intervals: readonly TrustlineInterval[] = TRUSTLINE_INTERVALS,
): TrustlineDelta[] {
  const deltas: TrustlineDelta[] = [];

  for (const interval of intervals) {
    const accumulators = new Map<string, Accumulator>();

    for (const op of trustlineOps) {
      const tsMs = parseTsMs(op.ledgerClosedAt, 'changeTrust operation');
      const bucketStart = bucketStartIso(tsMs, interval);
      const entry = getOrCreate(accumulators, bucketStart, op.accountId);
      entry.trustLimitChanges += 1;
      if (op.isDeletion) {
        entry.removedTrustlines += 1;
      } else {
        entry.newTrustlines += 1;
        entry.totalTrustLimit += parseAmount(op.limit);
      }
      entry.activeHoursSet.add(new Date(tsMs).getUTCHours());
      if (entry.lastActivityMs === null || tsMs > entry.lastActivityMs) {
        entry.lastActivityMs = tsMs;
      }
    }

    for (const op of activityOps) {
      const tsMs = parseTsMs(op.ledgerClosedAt, 'activity operation');
      const bucketStart = bucketStartIso(tsMs, interval);
      const entry = getOrCreate(accumulators, bucketStart, op.accountId);
      entry.paymentsCount += 1;
      entry.activeHoursSet.add(new Date(tsMs).getUTCHours());
      if (entry.lastActivityMs === null || tsMs > entry.lastActivityMs) {
        entry.lastActivityMs = tsMs;
      }
    }

    const intervalDeltas: TrustlineDelta[] = [];
    for (const entry of accumulators.values()) {
      const trustlineCount = entry.newTrustlines - entry.removedTrustlines;
      intervalDeltas.push({
        interval,
        bucketStart: entry.bucketStart,
        accountId: entry.accountId,
        trustlineCount,
        newTrustlines: entry.newTrustlines,
        removedTrustlines: entry.removedTrustlines,
        trustLimitChanges: entry.trustLimitChanges,
        totalTrustLimit: formatAmount(entry.totalTrustLimit),
        paymentsCount: entry.paymentsCount,
        activeHours: entry.activeHoursSet.size,
        // Any bucket that has activity is by definition not dormant.
        dormancyDays: 0,
      });
    }
    intervalDeltas.sort(
      (a, b) =>
        a.bucketStart.localeCompare(b.bucketStart) ||
        a.accountId.localeCompare(b.accountId) ||
        a.interval.localeCompare(b.interval),
    );
    deltas.push(...intervalDeltas);
  }

  return deltas.sort(
    (a, b) =>
      a.bucketStart.localeCompare(b.bucketStart) ||
      a.accountId.localeCompare(b.accountId) ||
      a.interval.localeCompare(b.interval),
  );
}

/** Maps a queue changeTrust payload to a normalized operation. */
export function changeTrustPayloadToOperation(
  payload: ChangeTrustPayload,
): TrustlineOperation {
  const accountId = payload.sourceAccount ?? payload.trustor;
  return {
    accountId,
    assetCode: payload.asset.code,
    assetIssuer: payload.asset.issuer ?? null,
    limit: payload.limit,
    isDeletion: payload.isDeletion,
    ledgerSequence: payload.ledger,
    ledgerClosedAt: payload.ledgerClosedAt,
  };
}

/** Maps a queue payment payload to activity (from + to accounts). */
export function paymentPayloadToActivity(
  payload: PaymentPayload,
): ActivityOperation[] {
  const common = {
    ledgerSequence: payload.ledger,
    ledgerClosedAt: payload.ledgerClosedAt,
  };
  return [
    { ...common, accountId: payload.from },
    { ...common, accountId: payload.to },
  ];
}

/** Maps a queue path-payment payload to activity (from + to accounts). */
export function pathPaymentPayloadToActivity(
  payload: PathPaymentPayload,
): ActivityOperation[] {
  const common = {
    ledgerSequence: payload.ledger,
    ledgerClosedAt: payload.ledgerClosedAt,
  };
  return [
    { ...common, accountId: payload.from },
    { ...common, accountId: payload.to },
  ];
}

/** Maps stored `ledger_operations` rows into trustline + activity ops. */
export function storedOperationsToTrustline(
  rows: readonly TrustlineStoredOperation[],
): { trustlineOps: TrustlineOperation[]; activityOps: ActivityOperation[] } {
  const trustlineOps: TrustlineOperation[] = [];
  const activityOps: ActivityOperation[] = [];
  for (const row of rows) {
    if (!row.source_account) continue;
    const isChangeTrust = row.operation_type === 'change_trust';
    const isPayment =
      row.operation_type === 'payment' || row.operation_type === 'path_payment';
    if (!isChangeTrust && !isPayment) continue;

    if (isChangeTrust) {
      if (!row.amount) continue;
      const limit = row.amount;
      const isDeletion = (() => {
        try {
          return parseAmount(limit) === 0n;
        } catch {
          return false;
        }
      })();
      trustlineOps.push({
        accountId: row.source_account,
        assetCode: row.asset_code ?? '',
        assetIssuer: row.asset_issuer,
        limit,
        isDeletion,
        ledgerSequence: row.ledger_sequence ?? undefined,
        ledgerClosedAt: row.ledger_closed_at,
      });
    }
    if (isPayment) {
      activityOps.push({
        accountId: row.source_account,
        ledgerSequence: row.ledger_sequence ?? undefined,
        ledgerClosedAt: row.ledger_closed_at,
      });
      if (row.destination_account && row.destination_account !== row.source_account) {
        activityOps.push({
          accountId: row.destination_account,
          ledgerSequence: row.ledger_sequence ?? undefined,
          ledgerClosedAt: row.ledger_closed_at,
        });
      }
    }
  }
  return { trustlineOps, activityOps };
}

// ----------------------------------------------------------------------
// Store port
// ----------------------------------------------------------------------

export interface TrustlineStore {
  readOperations(fromIso: string, toIso: string): Promise<TrustlineStoredOperation[]>;
  upsertAggregates(deltas: readonly TrustlineDelta[]): Promise<TrustlineAggregate[]>;
  deleteAggregates(interval: TrustlineInterval, bucketStarts: readonly string[]): Promise<number>;
}

export interface TrustlineSql {
  (strings: TemplateStringsArray, ...values: unknown[]): Promise<unknown>;
}

export function createPostgresTrustlineStore(sql: TrustlineSql): TrustlineStore {
  return {
    async readOperations(fromIso, toIso) {
      const rows = (await sql`
        SELECT NULL AS account_id, operation_type, asset_code, asset_issuer, amount,
               source_account, destination_account, ledger_sequence, ledger_closed_at
        FROM ledger_operations
        WHERE ledger_closed_at >= ${fromIso}
          AND ledger_closed_at < ${toIso}
          AND successful = true
          AND operation_type IN ('change_trust', 'payment', 'path_payment')
      `) as TrustlineStoredOperation[];
      return rows;
    },

    async upsertAggregates(deltas) {
      const persisted: TrustlineAggregate[] = [];
      for (const delta of deltas) {
        const rows = (await sql`
          INSERT INTO trustline_aggregates
            (bucket_start, interval, account_id, trustline_count, new_trustlines,
             removed_trustlines, trust_limit_changes, total_trust_limit,
             payments_count, active_hours, dormancy_days)
          VALUES
            (${delta.bucketStart}, ${delta.interval}, ${delta.accountId},
             ${delta.trustlineCount}, ${delta.newTrustlines},
             ${delta.removedTrustlines}, ${delta.trustLimitChanges},
             ${delta.totalTrustLimit}, ${delta.paymentsCount},
             ${delta.activeHours}, ${delta.dormancyDays})
          ON CONFLICT (bucket_start, interval, account_id)
          DO UPDATE SET
            trustline_count = trustline_aggregates.trustline_count + EXCLUDED.trustline_count,
            new_trustlines = trustline_aggregates.new_trustlines + EXCLUDED.new_trustlines,
            removed_trustlines = trustline_aggregates.removed_trustlines + EXCLUDED.removed_trustlines,
            trust_limit_changes = trustline_aggregates.trust_limit_changes + EXCLUDED.trust_limit_changes,
            total_trust_limit = trustline_aggregates.total_trust_limit + EXCLUDED.total_trust_limit,
            payments_count = trustline_aggregates.payments_count + EXCLUDED.payments_count,
            active_hours = trustline_aggregates.active_hours + EXCLUDED.active_hours,
            dormancy_days = trustline_aggregates.dormancy_days + EXCLUDED.dormancy_days
          RETURNING to_char(bucket_start AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.000"Z"') AS bucket_start,
                    interval, account_id, trustline_count, new_trustlines,
                    removed_trustlines, trust_limit_changes,
                    total_trust_limit::text AS total_trust_limit,
                    payments_count, active_hours, dormancy_days
        `) as TrustlineAggregate[];
        persisted.push(...rows);
      }
      return persisted;
    },

    async deleteAggregates(interval, bucketStarts) {
      if (bucketStarts.length === 0) return 0;
      const result = (await sql`
        DELETE FROM trustline_aggregates
        WHERE interval = ${interval}
          AND bucket_start = ANY(${bucketStarts as string[]})
      `) as { count?: number };
      return result.count ?? 0;
    },
  };
}

export function createInMemoryTrustlineStore(
  seed: readonly TrustlineStoredOperation[] = [],
): TrustlineStore & { rows(): TrustlineAggregate[] } {
  const operations = [...seed];
  const aggregates = new Map<string, TrustlineAggregate>();

  const keyOf = (row: {
    bucket_start: string;
    interval: string;
    account_id: string;
  }): string => `${row.bucket_start} ${row.interval} ${row.account_id}`;

  return {
    readOperations(fromIso, toIso) {
      const from = Date.parse(fromIso);
      const to = Date.parse(toIso);
      return Promise.resolve(
        operations.filter((op) => {
          const t = Date.parse(op.ledger_closed_at);
          return !Number.isNaN(t) && t >= from && t < to;
        }),
      );
    },
    upsertAggregates(deltas) {
      const persisted: TrustlineAggregate[] = [];
      for (const delta of deltas) {
        const candidate: TrustlineAggregate = {
          bucket_start: delta.bucketStart,
          interval: delta.interval,
          account_id: delta.accountId,
          trustline_count: delta.trustlineCount,
          new_trustlines: delta.newTrustlines,
          removed_trustlines: delta.removedTrustlines,
          trust_limit_changes: delta.trustLimitChanges,
          total_trust_limit: delta.totalTrustLimit,
          payments_count: delta.paymentsCount,
          active_hours: delta.activeHours,
          dormancy_days: delta.dormancyDays,
        };
        const key = keyOf(candidate);
        const existing = aggregates.get(key);
        if (existing) {
          const merged: TrustlineAggregate = {
            ...existing,
            trustline_count: existing.trustline_count + candidate.trustline_count,
            new_trustlines: existing.new_trustlines + candidate.new_trustlines,
            removed_trustlines: existing.removed_trustlines + candidate.removed_trustlines,
            trust_limit_changes: existing.trust_limit_changes + candidate.trust_limit_changes,
            total_trust_limit: formatAmount(
              parseAmount(existing.total_trust_limit) + parseAmount(candidate.total_trust_limit),
            ),
            payments_count: existing.payments_count + candidate.payments_count,
            active_hours: existing.active_hours + candidate.active_hours,
            dormancy_days: existing.dormancy_days + candidate.dormancy_days,
          };
          aggregates.set(key, merged);
          persisted.push(merged);
        } else {
          aggregates.set(key, candidate);
          persisted.push(candidate);
        }
      }
      return Promise.resolve(persisted);
    },
    deleteAggregates(interval, bucketStarts) {
      const targets = new Set(bucketStarts);
      let removed = 0;
      for (const [key, row] of [...aggregates]) {
        if (row.interval === interval && targets.has(row.bucket_start)) {
          aggregates.delete(key);
          removed += 1;
        }
      }
      return Promise.resolve(removed);
    },
    rows() {
      return [...aggregates.values()].map((row) => ({ ...row }));
    },
  };
}

// ----------------------------------------------------------------------
// Update events
// ----------------------------------------------------------------------

export interface TrustlineUpdatedEvent {
  kind: 'trustline_updated';
  interval: string;
  bucketStart: string;
  accountId: string;
  trustlineCountDelta: number;
  newTrustlinesDelta: number;
  removedTrustlinesDelta: number;
  trustLimitChangesDelta: number;
  totalTrustLimitDelta: string;
  paymentsCountDelta: number;
  activeHoursDelta: number;
  dormancyDaysDelta: number;
  trustlineCount?: number;
  totalTrustLimit?: string;
  paymentsCount?: number;
  activeHours?: number;
  updatedAt: string;
}

export interface TrustlineEventSink {
  emit(event: TrustlineUpdatedEvent): void | Promise<void>;
}

export function createInMemoryTrustlineSink(): TrustlineEventSink & {
  events: TrustlineUpdatedEvent[];
} {
  const events: TrustlineUpdatedEvent[] = [];
  return {
    events,
    emit(event) {
      events.push(event);
    },
  };
}

export function createRedisTrustlineSink(
  client: { publish(channel: string, message: string): Promise<unknown> },
  channel = 'eta:flows:trustlines',
): TrustlineEventSink {
  return {
    async emit(event) {
      await client.publish(channel, JSON.stringify(event));
    },
  };
}

// ----------------------------------------------------------------------
// Prometheus metrics
// ----------------------------------------------------------------------

export interface TrustlineMetrics {
  operationsProcessed: Counter<'kind'>;
  bucketsUpdated: Counter<'interval'>;
  reorgsDetected: Counter<'interval'>;
  failuresTotal: Counter<'stage'>;
}

export const TRUSTLINE_METRIC_PREFIX = 'eta_trustline_';

let defaultTrustlineMetrics: TrustlineMetrics | undefined;
function getDefaultTrustlineMetrics(): TrustlineMetrics {
  if (!defaultTrustlineMetrics) {
    defaultTrustlineMetrics = createTrustlineMetrics();
  }
  return defaultTrustlineMetrics;
}

export function createTrustlineMetrics(
  registry: Registry = register,
): TrustlineMetrics {
  return {
    operationsProcessed: new Counter({
      name: `${TRUSTLINE_METRIC_PREFIX}operations_processed_total`,
      help: 'Operations folded into trustline aggregates, by ledger job kind',
      registers: [registry],
      labelNames: ['kind'],
    }),
    bucketsUpdated: new Counter({
      name: `${TRUSTLINE_METRIC_PREFIX}buckets_updated_total`,
      help: 'Trustline bucket rows upserted, by interval',
      registers: [registry],
      labelNames: ['interval'],
    }),
    reorgsDetected: new Counter({
      name: `${TRUSTLINE_METRIC_PREFIX}reorgs_detected_total`,
      help: 'Ledger rollback detections that triggered a trailing recompute, by interval',
      registers: [registry],
      labelNames: ['interval'],
    }),
    failuresTotal: new Counter({
      name: `${TRUSTLINE_METRIC_PREFIX}failures_total`,
      help: 'Trustline aggregation failures, by pipeline stage',
      registers: [registry],
      labelNames: ['stage'],
    }),
  };
}

// ----------------------------------------------------------------------
// Aggregator
// ----------------------------------------------------------------------

export interface TrustlineAggregatorOptions {
  store: TrustlineStore;
  intervals?: readonly TrustlineInterval[];
  metrics?: TrustlineMetrics;
  events?: readonly TrustlineEventSink[];
  highWaterStore?: CursorStore;
  highWaterKey?: string;
  reorgLookbackBuckets?: Partial<Record<TrustlineInterval, number>>;
  log?: typeof logger;
}

const DEFAULT_REORG_LOOKBACK: Record<TrustlineInterval, number> = {
  '1m': 60,
  '1h': 24,
  '1d': 7,
};

const DEFAULT_HIGH_WATER_KEY = 'trustline:last-ledger-sequence';

export class TrustlineAggregator {
  private readonly store: TrustlineStore;
  private readonly intervals: readonly TrustlineInterval[];
  private readonly metrics: TrustlineMetrics;
  private readonly events: readonly TrustlineEventSink[];
  private readonly highWaterStore?: CursorStore;
  private readonly highWaterKey: string;
  private readonly reorgLookback: Record<TrustlineInterval, number>;
  private readonly log: typeof logger;
  private lastLedgerSequence: number | null = null;

  constructor(options: TrustlineAggregatorOptions) {
    if (!options.store) {
      throw new Error('TrustlineAggregator requires a store');
    }
    this.store = options.store;
    this.intervals = options.intervals ?? TRUSTLINE_INTERVALS;
    for (const interval of this.intervals) {
      if (!isTrustlineInterval(interval)) {
        throw new Error(`unsupported interval: ${String(interval)}`);
      }
    }
    this.metrics = options.metrics ?? getDefaultTrustlineMetrics();
    this.events = options.events ?? [];
    this.highWaterStore = options.highWaterStore;
    this.highWaterKey = options.highWaterKey ?? DEFAULT_HIGH_WATER_KEY;
    this.reorgLookback = { ...DEFAULT_REORG_LOOKBACK, ...options.reorgLookbackBuckets };
    this.log = options.log ?? logger;
  }

  get highWaterLedgerSequence(): number | null {
    return this.lastLedgerSequence;
  }

  async loadHighWaterMark(): Promise<number | null> {
    if (this.lastLedgerSequence !== null) return this.lastLedgerSequence;
    if (!this.highWaterStore) return null;
    const raw = await this.highWaterStore.get(this.highWaterKey).catch(() => null);
    const parsed = raw === null ? NaN : Number(raw);
    if (Number.isFinite(parsed)) {
      this.lastLedgerSequence = parsed;
      return parsed;
    }
    return null;
  }

  private async persistHighWaterMark(sequence: number): Promise<void> {
    if (!this.highWaterStore) return;
    await this.highWaterStore.set(this.highWaterKey, String(sequence)).catch((error: unknown) => {
      this.log.warn({ err: error }, 'aggregator: failed to persist ledger high-water mark');
    });
  }

  async applyTrustlineOperations(
    operations: readonly TrustlineOperation[],
    context: { kind: string } = { kind: 'unknown' },
  ): Promise<TrustlineAggregate[]> {
    if (operations.length === 0) return [];
    this.metrics.operationsProcessed.inc({ kind: context.kind });
    const deltas = computeTrustlineDeltas(operations, [], this.intervals);
    if (deltas.length === 0) return [];
    const persisted = await this.upsertAndEmit(deltas);
    for (const delta of deltas) {
      this.metrics.bucketsUpdated.inc({ interval: delta.interval });
    }
    return persisted;
  }

  async applyActivityOperations(
    operations: readonly ActivityOperation[],
    context: { kind: string } = { kind: 'unknown' },
  ): Promise<TrustlineAggregate[]> {
    if (operations.length === 0) return [];
    this.metrics.operationsProcessed.inc({ kind: context.kind });
    const deltas = computeTrustlineDeltas([], operations, this.intervals);
    if (deltas.length === 0) return [];
    const persisted = await this.upsertAndEmit(deltas);
    for (const delta of deltas) {
      this.metrics.bucketsUpdated.inc({ interval: delta.interval });
    }
    return persisted;
  }

  /** Convenience: single call for both trustline and activity ops. */
  async applyOperations(
    trustlineOps: readonly TrustlineOperation[],
    activityOps: readonly ActivityOperation[] = [],
    context: { kind: string } = { kind: 'unknown' },
  ): Promise<TrustlineAggregate[]> {
    if (trustlineOps.length === 0 && activityOps.length === 0) return [];
    this.metrics.operationsProcessed.inc({ kind: context.kind });
    const deltas = computeTrustlineDeltas(trustlineOps, activityOps, this.intervals);
    if (deltas.length === 0) return [];
    const persisted = await this.upsertAndEmit(deltas);
    for (const delta of deltas) {
      this.metrics.bucketsUpdated.inc({ interval: delta.interval });
    }
    return persisted;
  }

  async recomputeBucket(
    bucketStart: string,
    interval: TrustlineInterval,
  ): Promise<TrustlineAggregate[]> {
    const toIso = bucketEndIso(bucketStart, interval);
    const rows = await this.store.readOperations(bucketStart, toIso);
    const { trustlineOps, activityOps } = storedOperationsToTrustline(rows);
    const deltas = computeTrustlineDeltas(trustlineOps, activityOps, [interval]);
    const deleted = await this.store.deleteAggregates(interval, [bucketStart]);
    this.log.info(
      { bucketStart, interval, deleted },
      'aggregator: recomputed trustline bucket from ledger_operations',
    );
    if (deltas.length === 0) return [];
    const persisted = await this.upsertAndEmit(deltas);
    for (const delta of deltas) {
      this.metrics.bucketsUpdated.inc({ interval: delta.interval });
    }
    return persisted;
  }

  async recomputeTrailingWindow(referenceMs: number = Date.now()): Promise<void> {
    for (const interval of this.intervals) {
      const lookback = this.reorgLookback[interval] ?? 0;
      if (lookback <= 0) continue;
      const size = INTERVAL_MS[interval];
      const end = Math.floor(referenceMs / size) * size + size;
      const from = end - lookback * size;
      const fromIso = new Date(from).toISOString();
      const toIso = new Date(end).toISOString();
      try {
        const rows = await this.store.readOperations(fromIso, toIso);
        const { trustlineOps, activityOps } = storedOperationsToTrustline(rows);
        const deltas = computeTrustlineDeltas(trustlineOps, activityOps, [interval]);
        const bucketStarts = listBucketStarts(fromIso, toIso, interval);
        await this.store.deleteAggregates(interval, bucketStarts);
        if (deltas.length > 0) {
          await this.upsertAndEmit(deltas);
        }
        this.metrics.reorgsDetected.inc({ interval });
        this.log.info(
          { interval, from: fromIso, to: toIso, operations: trustlineOps.length + activityOps.length },
          'aggregator: recomputed trailing window after ledger rollback',
        );
      } catch (error) {
        this.metrics.failuresTotal.inc({ stage: 'recompute' });
        this.log.error({ err: error, interval }, 'aggregator: trailing recompute failed');
        throw error;
      }
    }
  }

  async observeLedgerSequence(
    ledgerSequence: number | undefined,
    referenceTime?: string | number,
  ): Promise<boolean> {
    if (ledgerSequence === undefined) return false;
    const last = await this.loadHighWaterMark();
    const referenceMs =
      typeof referenceTime === 'number'
        ? referenceTime
        : typeof referenceTime === 'string'
          ? Date.parse(referenceTime)
          : Date.now();
    const anchor = Number.isFinite(referenceMs) ? referenceMs : Date.now();
    if (last !== null && ledgerSequence < last) {
      this.log.warn(
        { incoming: ledgerSequence, lastProcessed: last },
        'aggregator: ledger sequence dropped; Stellar fork/rollback detected, recomputing',
      );
      await this.recomputeTrailingWindow(anchor);
      this.lastLedgerSequence = Math.max(last, ledgerSequence);
      await this.persistHighWaterMark(this.lastLedgerSequence);
      return true;
    }
    this.lastLedgerSequence = Math.max(last ?? 0, ledgerSequence);
    await this.persistHighWaterMark(this.lastLedgerSequence);
    return false;
  }

  private async upsertAndEmit(deltas: readonly TrustlineDelta[]): Promise<TrustlineAggregate[]> {
    let persisted: TrustlineAggregate[];
    try {
      persisted = await this.store.upsertAggregates(deltas);
    } catch (error) {
      this.metrics.failuresTotal.inc({ stage: 'upsert' });
      throw error;
    }
    const updatedAt = new Date().toISOString();
    for (const delta of deltas) {
      const totals = persisted.find(
        (p) => p.bucket_start === delta.bucketStart && p.account_id === delta.accountId,
      );
      const event: TrustlineUpdatedEvent = {
        kind: 'trustline_updated',
        interval: delta.interval,
        bucketStart: delta.bucketStart,
        accountId: delta.accountId,
        trustlineCountDelta: delta.trustlineCount,
        newTrustlinesDelta: delta.newTrustlines,
        removedTrustlinesDelta: delta.removedTrustlines,
        trustLimitChangesDelta: delta.trustLimitChanges,
        totalTrustLimitDelta: delta.totalTrustLimit,
        paymentsCountDelta: delta.paymentsCount,
        activeHoursDelta: delta.activeHours,
        dormancyDaysDelta: delta.dormancyDays,
        trustlineCount: totals?.trustline_count,
        totalTrustLimit: totals?.total_trust_limit,
        paymentsCount: totals?.payments_count,
        activeHours: totals?.active_hours,
        updatedAt,
      };
      for (const sink of this.events) {
        try {
          await sink.emit(event);
        } catch (error) {
          this.metrics.failuresTotal.inc({ stage: 'events' });
          this.log.error({ err: error }, 'aggregator: event sink failed');
        }
      }
    }
    return persisted;
  }
}

// ----------------------------------------------------------------------
// Queue handler factories
// ----------------------------------------------------------------------

export type TrustlineHandlers = {
  change_trust?: LedgerOperationHandler<'change_trust'>;
  payment?: LedgerOperationHandler<'payment'>;
  path_payment?: LedgerOperationHandler<'path_payment'>;
};

/**
 * Builds the `ledger-operations` handlers for the aggregator (issue #20):
 * changeTrust for trustline growth, plus payment/path-payment for activity
 * patterns. Pass into `startQueueWorkers({ handlers })`.
 */
export function createTrustlineHandlers(
  options: TrustlineAggregatorOptions,
): TrustlineHandlers {
  const aggregator = new TrustlineAggregator(options);
  return {
    change_trust: async (payload) => {
      await aggregator.observeLedgerSequence(payload.ledger, payload.ledgerClosedAt);
      await aggregator.applyTrustlineOperations([changeTrustPayloadToOperation(payload)], {
        kind: 'change_trust',
      });
    },
    payment: async (payload) => {
      await aggregator.observeLedgerSequence(payload.ledger, payload.ledgerClosedAt);
      await aggregator.applyActivityOperations(paymentPayloadToActivity(payload), {
        kind: 'payment',
      });
    },
    path_payment: async (payload) => {
      await aggregator.observeLedgerSequence(payload.ledger, payload.ledgerClosedAt);
      await aggregator.applyActivityOperations(pathPaymentPayloadToActivity(payload), {
        kind: 'path_payment',
      });
    },
  };
}

/**
 * Builds the `trustline` aggregation-job handler: recomputes one closed
 * bucket from `ledger_operations`. Registered on the `aggregation-jobs`
 * worker via `startQueueWorkers({ aggregationHandlers })`.
 */
export function createTrustlineAggregationHandler(
  options: TrustlineAggregatorOptions,
): AggregationJobHandler {
  const aggregator = new TrustlineAggregator(options);
  return async (job) => {
    if (job.kind !== 'trustline') {
      throw new Error(`trustline aggregator cannot handle aggregation kind ${job.kind}`);
    }
    const interval = job.interval;
    if (!isTrustlineInterval(interval)) {
      throw new Error(`unsupported trustline interval: ${String(interval)}`);
    }
    await aggregator.recomputeBucket(job.bucketStart, interval);
  };
}
