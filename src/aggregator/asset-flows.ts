/**
 * Asset flow aggregator (issue #18).
 *
 * Consumes parsed operations from the `ledger-operations` queue (via the
 * handler port in `src/queue/processors.ts`) and maintains the
 * `asset_flow_aggregates` hypertable: time-bucketed per-asset volume,
 * payment counts and net flow, grouped by asset code + asset issuer +
 * minute/hour/day bucket.
 *
 * Real-time path: each payment / path payment job is folded into the
 * minute, hour and day buckets of the ledger close time and upserted with
 * an additive `INSERT ... ON CONFLICT ... DO UPDATE SET ... + EXCLUDED ...`
 * so concurrent workers never double-count or lose a flow.
 *
 * Reorg path: Stellar forks can roll back recent ledgers. Whenever an
 * incoming operation carries a ledger sequence lower than the high-water
 * mark this process has seen, the buckets covering the configurable
 * trailing lookback window are deleted and re-aggregated from the
 * `ledger_operations` hypertable (the derived-data source of truth), then
 * the incoming operation is processed normally. `recomputeBuckets` is also
 * exposed for the `asset-flow` aggregation jobs produced by the Hubble
 * backfill (issue #17): they recompute one closed bucket from the stored
 * operations.
 *
 * Semantics worth knowing before consuming `/v1/flows/assets/{asset}`:
 *
 * - `volume`   — sum of the leg amounts that moved the asset, per bucket.
 * - `payments` — number of operation legs that moved the asset (a path
 *                payment counts once in each of the two asset buckets it
 *                touches).
 * - `net_flow` — issuer-attributed net issuance: amount paid OUT of the
 *                asset's issuer account (minting) minus amount received BY
 *                the issuer (redemption). Moves between two non-issuer
 *                accounts always net to zero. The native asset (no issuer)
 *                always reports `0`.
 *
 * `asset_issuer` is written as `''` for the native asset (and for rows
 * re-aggregated from `ledger_operations`, where the native asset carries a
 * NULL issuer). The hypertable's primary key treats NULLs as distinct, so
 * storing an empty string keeps the `ON CONFLICT` upsert idempotent.
 *
 * Precision: amounts travel as decimal strings and are summed with bigint
 * stroops (7 decimal places) — no float rounding anywhere.
 */

import { Counter, register, type Registry } from 'prom-client';
import { logger } from '../logger';
import type {
  LedgerOperationHandler,
  AggregationJobHandler,
} from '../queue/processors';
import type { PaymentPayload, PathPaymentPayload } from '../ingestor/types';
import type { AssetFlowAggregate } from '../db/schema/aggregates';
import type { CursorStore } from '../ingestor/types';

/* ------------------------------------------------------------------ */
/* Decimal arithmetic (7-decimal stroops, bigint)                      */
/* ------------------------------------------------------------------ */

const STROOP_SCALE = 10_000_000n;
const AMOUNT_PATTERN = /^([+-]?)(\d+)(?:\.(\d{1,7}))?$/;

/** Parses a Stellar amount string into stroops. Throws on malformed input. */
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

/* ------------------------------------------------------------------ */
/* Time buckets                                                        */
/* ------------------------------------------------------------------ */

export type AssetFlowInterval = '1m' | '1h' | '1d';

const INTERVAL_MS: Record<AssetFlowInterval, number> = {
  '1m': 60_000,
  '1h': 3_600_000,
  '1d': 86_400_000,
};

export const ASSET_FLOW_INTERVALS: readonly AssetFlowInterval[] = ['1m', '1h', '1d'];

/** Floor `tsMs` to the start of its interval bucket; returns ISO 8601. */
export function bucketStartIso(tsMs: number, interval: AssetFlowInterval): string {
  const size = INTERVAL_MS[interval];
  return new Date(Math.floor(tsMs / size) * size).toISOString();
}

/** ISO 8601 end (exclusive) of the bucket starting at `bucketIso`. */
export function bucketEndIso(bucketIso: string, interval: AssetFlowInterval): string {
  const start = Date.parse(bucketIso);
  if (Number.isNaN(start)) {
    throw new Error(`invalid bucketStart: ${JSON.stringify(bucketIso)}`);
  }
  return new Date(start + INTERVAL_MS[interval]).toISOString();
}

/** Inclusive list of bucket starts from `fromIso` (inclusive) to `toIso` (exclusive). */
export function listBucketStarts(
  fromIso: string,
  toIso: string,
  interval: AssetFlowInterval,
): string[] {
  const size = INTERVAL_MS[interval];
  const from = Date.parse(fromIso);
  const to = Date.parse(toIso);
  if (Number.isNaN(from) || Number.isNaN(to)) {
    throw new Error(`invalid bucket bounds: ${JSON.stringify(fromIso)}..${JSON.stringify(toIso)}`);
  }
  const starts: string[] = [];
  for (let t = Math.floor(from / size) * size; t < to; t += size) {
    starts.push(new Date(t).toISOString());
  }
  return starts;
}

/** True for the intervals this module can aggregate. */
export function isAssetFlowInterval(value: string): value is AssetFlowInterval {
  return value === '1m' || value === '1h' || value === '1d';
}

/* ------------------------------------------------------------------ */
/* Normalized operation legs                                           */
/* ------------------------------------------------------------------ */

/** One asset transfer leg to fold into the aggregates. */
export interface AssetFlowOperation {
  assetCode: string;
  /** Issuer account; null/undefined for the native asset. */
  assetIssuer?: string | null;
  /** Non-negative decimal string; throws at compute time when invalid. */
  amount: string;
  fromAccount: string;
  toAccount: string;
  ledgerSequence?: number;
  /** Ledger close time (ISO 8601); drives the bucket assignment. */
  ledgerClosedAt?: string;
}

export interface AssetFlowDelta {
  interval: AssetFlowInterval;
  bucketStart: string;
  assetCode: string;
  /** Issuer as stored in the table: `''` for the native asset. */
  assetIssuer: string;
  volume: string;
  payments: number;
  netFlow: string;
}

/** Ledger-operations rows as read back from Postgres (all strings). */
export interface AssetFlowStoredOperation {
  asset_code: string;
  asset_issuer: string | null;
  amount: string;
  source_account: string | null;
  destination_account: string | null;
  ledger_sequence: number | null;
  ledger_closed_at: string;
}

/* ------------------------------------------------------------------ */
/* Core computation (pure, unit-test friendly)                         */
/* ------------------------------------------------------------------ */

interface Accumulator {
  bucketStart: string;
  assetCode: string;
  assetIssuer: string;
  volume: bigint;
  payments: number;
  netFlow: bigint;
}

function accumulateInto(
  accumulators: Map<string, Accumulator>,
  op: AssetFlowOperation,
  tsMs: number,
  interval: AssetFlowInterval,
): void {
  const bucketStart = bucketStartIso(tsMs, interval);
  // The table primary key treats NULLs as distinct; store '' for native.
  const issuer = op.assetIssuer ?? '';
  const key = `${bucketStart} ${issuer} ${op.assetCode}`;
  let entry = accumulators.get(key);
  if (!entry) {
    entry = { bucketStart, assetCode: op.assetCode, assetIssuer: issuer, volume: 0n, payments: 0, netFlow: 0n };
    accumulators.set(key, entry);
  }
  const amount = parseAmount(op.amount);
  entry.volume += amount;
  entry.payments += 1;
  // Issuer-attributed net flow: minted (out of issuer) minus redeemed
  // (into issuer). Self-transfers and moves between non-issuer accounts
  // cancel out; native asset (no issuer) stays at zero.
  if (issuer !== '') {
    const fromIsIssuer = op.fromAccount === issuer;
    const toIsIssuer = op.toAccount === issuer;
    if (fromIsIssuer && !toIsIssuer) entry.netFlow += amount;
    else if (toIsIssuer && !fromIsIssuer) entry.netFlow -= amount;
  }
}

/**
 * Folds operations into per-(bucket, asset) deltas. Returns one delta per
 * interval, bucket and asset; an empty input yields an empty list.
 */
export function computeAssetFlowDeltas(
  operations: readonly AssetFlowOperation[],
  intervals: readonly AssetFlowInterval[] = ASSET_FLOW_INTERVALS,
): AssetFlowDelta[] {
  const deltas: AssetFlowDelta[] = [];
  for (const interval of intervals) {
    const accumulators = new Map<string, Accumulator>();
    for (const op of operations) {
      if (!op.ledgerClosedAt) {
        throw new Error('operation is missing ledgerClosedAt; cannot assign a bucket');
      }
      const tsMs = Date.parse(op.ledgerClosedAt);
      if (Number.isNaN(tsMs)) {
        throw new Error(`operation has unparseable ledgerClosedAt: ${op.ledgerClosedAt}`);
      }
      accumulateInto(accumulators, op, tsMs, interval);
    }
    for (const entry of accumulators.values()) {
      deltas.push({
        interval,
        bucketStart: entry.bucketStart,
        assetCode: entry.assetCode,
        assetIssuer: entry.assetIssuer,
        volume: formatAmount(entry.volume),
        payments: entry.payments,
        netFlow: formatAmount(entry.netFlow),
      });
    }
  }
  // Deterministic order: bucket, then asset code/issuer.
  return deltas.sort((a, b) =>
    a.bucketStart.localeCompare(b.bucketStart) ||
    a.assetCode.localeCompare(b.assetCode) ||
    a.assetIssuer.localeCompare(b.assetIssuer) ||
    a.interval.localeCompare(b.interval),
  );
}

/** Maps a queue payment payload to its single leg. */
export function paymentPayloadToOperations(payload: PaymentPayload): AssetFlowOperation[] {
  return [
    {
      assetCode: payload.asset.code,
      assetIssuer: payload.asset.issuer ?? null,
      amount: payload.amount,
      fromAccount: payload.from,
      toAccount: payload.to,
      ledgerSequence: payload.ledger,
      ledgerClosedAt: payload.ledgerClosedAt,
    },
  ];
}

/**
 * Maps a queue path-payment payload to its two legs (send asset out of the
 * sender's account, destination asset into the recipient's).
 *
 * When Horizon only exposes a bound (send_max / destination_min_amount),
 * the bound is used as an approximation and a warning is logged; the queue
 * payloads produced by the live stream carry the exact amounts for
 * completed operations, so this fallback should never trigger in practice.
 */
export function pathPaymentPayloadToOperations(
  payload: PathPaymentPayload,
  log: typeof logger = logger,
): AssetFlowOperation[] {
  const common = {
    fromAccount: payload.from,
    toAccount: payload.to,
    ledgerSequence: payload.ledger,
    ledgerClosedAt: payload.ledgerClosedAt,
  };
  const legs: AssetFlowOperation[] = [];
  const sendAmount = payload.sendAmount ?? payload.sendMax;
  if (sendAmount !== undefined) {
    if (payload.sendAmount === undefined && payload.sendMax !== undefined) {
      log.warn(
        { id: payload.id },
        'aggregator: path payment missing send_amount, using send_max as approximation',
      );
    }
    legs.push({
      ...common,
      assetCode: payload.sendAsset.code,
      assetIssuer: payload.sendAsset.issuer ?? null,
      amount: sendAmount,
    });
  }
  const destAmount = payload.destinationAmount ?? payload.destinationMin;
  if (destAmount !== undefined) {
    if (payload.destinationAmount === undefined && payload.destinationMin !== undefined) {
      log.warn(
        { id: payload.id },
        'aggregator: path payment missing destination_amount, using destination_min_amount as approximation',
      );
    }
    legs.push({
      ...common,
      assetCode: payload.destinationAsset.code,
      assetIssuer: payload.destinationAsset.issuer ?? null,
      amount: destAmount,
    });
  }
  return legs;
}

/** Maps stored `ledger_operations` rows to aggregator operations. */
export function storedOperationsToAssetFlow(
  rows: readonly AssetFlowStoredOperation[],
): AssetFlowOperation[] {
  const operations: AssetFlowOperation[] = [];
  for (const row of rows) {
    if (!row.asset_code || !row.amount) continue;
    operations.push({
      assetCode: row.asset_code,
      assetIssuer: row.asset_issuer,
      amount: row.amount,
      fromAccount: row.source_account ?? '',
      toAccount: row.destination_account ?? '',
      ledgerSequence: row.ledger_sequence ?? undefined,
      ledgerClosedAt: row.ledger_closed_at,
    });
  }
  return operations;
}

/* ------------------------------------------------------------------ */
/* Store port (implemented by the Postgres adapter below)              */
/* ------------------------------------------------------------------ */

/**
 * Persistence port for the aggregates. Implementations must apply upserts
 * additively: `volume`, `payments` and `net_flow` add onto existing rows
 * (`INSERT ... ON CONFLICT ... DO UPDATE SET x = x + EXCLUDED.x`), which is
 * what makes the real-time path safe under concurrent workers.
 */
export interface AssetFlowStore {
  /** Successful payment/path-payment rows closed in `[fromIso, toIso)`. */
  readOperations(fromIso: string, toIso: string): Promise<AssetFlowStoredOperation[]>;
  /** Additive upsert; returns the persisted rows (with totals) per bucket. */
  upsertAggregates(deltas: readonly AssetFlowDelta[]): Promise<AssetFlowAggregate[]>;
  /** Deletes the given buckets for one interval; returns the row count. */
  deleteAggregates(interval: AssetFlowInterval, bucketStarts: readonly string[]): Promise<number>;
}

/** Minimal `postgres.Sql` surface used by the Postgres adapter. */
export interface AssetFlowSql {
  (strings: TemplateStringsArray, ...values: unknown[]): Promise<unknown>;
}

/**
 * Postgres implementation of {@link AssetFlowStore} against the shared
 * `postgres.Sql` pool. Upserts return the merged totals so the update
 * events carry the persisted state, not just the delta.
 */
export function createPostgresAssetFlowStore(sql: AssetFlowSql): AssetFlowStore {
  return {
    async readOperations(fromIso, toIso) {
      const rows = (await sql`
        SELECT asset_code, asset_issuer, amount, source_account, destination_account,
               ledger_sequence, ledger_closed_at
        FROM ledger_operations
        WHERE ledger_closed_at >= ${fromIso}
          AND ledger_closed_at < ${toIso}
          AND successful = true
          AND operation_type IN ('payment', 'path_payment')
          AND asset_code IS NOT NULL
          AND amount IS NOT NULL
      `) as AssetFlowStoredOperation[];
      return rows;
    },

    async upsertAggregates(deltas) {
      const persisted: AssetFlowAggregate[] = [];
      for (const delta of deltas) {
        const rows = (await sql`
          INSERT INTO asset_flow_aggregates
            (bucket_start, interval, asset_code, asset_issuer, volume, payments, net_flow)
          VALUES
            (${delta.bucketStart}, ${delta.interval}, ${delta.assetCode}, ${delta.assetIssuer},
             ${delta.volume}, ${delta.payments}, ${delta.netFlow})
          ON CONFLICT (bucket_start, interval, asset_code, asset_issuer)
          DO UPDATE SET
            volume   = asset_flow_aggregates.volume + EXCLUDED.volume,
            payments = asset_flow_aggregates.payments + EXCLUDED.payments,
            net_flow = asset_flow_aggregates.net_flow + EXCLUDED.net_flow
          RETURNING to_char(bucket_start AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.000"Z"') AS bucket_start,
                    interval, asset_code, asset_issuer,
                    volume::text AS volume, payments, net_flow::text AS net_flow
        `) as AssetFlowAggregate[];
        persisted.push(...rows);
      }
      return persisted;
    },

    async deleteAggregates(interval, bucketStarts) {
      if (bucketStarts.length === 0) return 0;
      const result = (await sql`
        DELETE FROM asset_flow_aggregates
        WHERE interval = ${interval}
          AND bucket_start = ANY(${bucketStarts as string[]})
      `) as { count?: number };
      return result.count ?? 0;
    },
  };
}

/**
 * In-memory {@link AssetFlowStore} with the same additive-upsert semantics
 * (issuers kept verbatim — `''` for the native asset), exported for tests
 * and local development without a database.
 */
export function createInMemoryAssetFlowStore(
  seed: readonly AssetFlowStoredOperation[] = [],
): AssetFlowStore & { rows(): AssetFlowAggregate[] } {
  const operations = [...seed];
  const aggregates = new Map<string, AssetFlowAggregate>();

  const keyOf = (row: {
    bucket_start: string;
    interval: string;
    asset_code: string;
    asset_issuer: string | null;
  }): string => `${row.bucket_start} ${row.interval} ${row.asset_code} ${row.asset_issuer ?? ''}`;

  return {
    async readOperations(fromIso, toIso) {
      const from = Date.parse(fromIso);
      const to = Date.parse(toIso);
      return operations.filter((op) => {
        const t = Date.parse(op.ledger_closed_at);
        return !Number.isNaN(t) && t >= from && t < to;
      });
    },
    async upsertAggregates(deltas) {
      const persisted: AssetFlowAggregate[] = [];
      for (const delta of deltas) {
        const candidate: AssetFlowAggregate = {
          bucket_start: delta.bucketStart,
          interval: delta.interval,
          asset_code: delta.assetCode,
          asset_issuer: delta.assetIssuer,
          volume: delta.volume,
          payments: delta.payments,
          net_flow: delta.netFlow,
        };
        const key = keyOf(candidate);
        const existing = aggregates.get(key);
        if (existing) {
          const merged: AssetFlowAggregate = {
            ...existing,
            volume: formatAmount(parseAmount(existing.volume) + parseAmount(candidate.volume)),
            payments: existing.payments + candidate.payments,
            net_flow: formatAmount(parseAmount(existing.net_flow) + parseAmount(candidate.net_flow)),
          };
          aggregates.set(key, merged);
          persisted.push(merged);
        } else {
          aggregates.set(key, candidate);
          persisted.push(candidate);
        }
      }
      return persisted;
    },
    async deleteAggregates(interval, bucketStarts) {
      const targets = new Set(bucketStarts);
      let removed = 0;
      for (const [key, row] of [...aggregates]) {
        if (row.interval === interval && targets.has(row.bucket_start)) {
          aggregates.delete(key);
          removed += 1;
        }
      }
      return removed;
    },
    rows() {
      return [...aggregates.values()].map((row) => ({ ...row }));
    },
  };
}

/* ------------------------------------------------------------------ */
/* Update events (SSE / webhooks)                                      */
/* ------------------------------------------------------------------ */

/** Emitted after each bucket upsert; consumed by SSE/webhook layers. */
export interface AssetFlowUpdatedEvent {
  kind: 'asset_flow_updated';
  interval: string;
  bucketStart: string;
  assetCode: string;
  assetIssuer: string;
  /** Deltas applied to the bucket by this upsert. */
  volumeDelta: string;
  paymentsDelta: number;
  netFlowDelta: string;
  /** Persisted totals after the upsert, when the store returns them. */
  volume?: string;
  payments?: number;
  netFlow?: string;
  updatedAt: string;
}

export interface AssetFlowEventSink {
  emit(event: AssetFlowUpdatedEvent): void | Promise<void>;
}

/** In-memory sink that records events; handy for tests. */
export function createInMemoryAssetFlowSink(): AssetFlowEventSink & {
  events: AssetFlowUpdatedEvent[];
} {
  const events: AssetFlowUpdatedEvent[] = [];
  return {
    events,
    emit(event) {
      events.push(event);
    },
  };
}

/**
 * Publishes update events to a Redis channel (default `eta:flows:assets`);
 * the SSE/webhook fan-out layers subscribe there when those land. Any
 * client with a `publish(channel, message)` method works.
 */
export function createRedisAssetFlowSink(
  client: { publish(channel: string, message: string): Promise<unknown> },
  channel = 'eta:flows:assets',
): AssetFlowEventSink {
  return {
    async emit(event) {
      await client.publish(channel, JSON.stringify(event));
    },
  };
}

/* ------------------------------------------------------------------ */
/* Prometheus metrics                                                  */
/* ------------------------------------------------------------------ */

export interface AssetFlowMetrics {
  /** Operations processed, by job kind. */
  operationsProcessed: Counter<'kind'>;
  /** Bucket rows upserted, by interval. */
  bucketsUpdated: Counter<'interval'>;
  /** Reorg detections followed by a trailing-window recompute, by interval. */
  reorgsDetected: Counter<'interval'>;
  /** Failures by pipeline stage (validation, recompute, upsert, events). */
  failuresTotal: Counter<'stage'>;
}

export const ASSET_FLOW_METRIC_PREFIX = 'eta_asset_flow_';

/**
 * Lazily created shared default metrics so multiple aggregators built
 * without explicit metrics (e.g. the live-handler factory and the
 * aggregation-jobs handler factory in production wiring) never register
 * duplicate collectors against the global registry.
 */
let defaultMetrics: AssetFlowMetrics | undefined;
function getDefaultAssetFlowMetrics(): AssetFlowMetrics {
  if (!defaultMetrics) {
    defaultMetrics = createAssetFlowMetrics();
  }
  return defaultMetrics;
}

export function createAssetFlowMetrics(registry: Registry = register): AssetFlowMetrics {
  const kinds = ['kind'] as const;
  const intervals = ['interval'] as const;
  const stages = ['stage'] as const;
  return {
    operationsProcessed: new Counter({
      name: `${ASSET_FLOW_METRIC_PREFIX}operations_processed_total`,
      help: 'Operations folded into asset flow aggregates, by ledger job kind',
      registers: [registry],
      labelNames: kinds,
    }),
    bucketsUpdated: new Counter({
      name: `${ASSET_FLOW_METRIC_PREFIX}buckets_updated_total`,
      help: 'Asset flow bucket rows upserted, by interval',
      registers: [registry],
      labelNames: intervals,
    }),
    reorgsDetected: new Counter({
      name: `${ASSET_FLOW_METRIC_PREFIX}reorgs_detected_total`,
      help: 'Ledger rollback detections that triggered a trailing recompute, by interval',
      registers: [registry],
      labelNames: intervals,
    }),
    failuresTotal: new Counter({
      name: `${ASSET_FLOW_METRIC_PREFIX}failures_total`,
      help: 'Asset flow aggregation failures, by pipeline stage',
      registers: [registry],
      labelNames: stages,
    }),
  };
}

/* ------------------------------------------------------------------ */
/* Aggregator                                                          */
/* ------------------------------------------------------------------ */

export interface AssetFlowAggregatorOptions {
  store: AssetFlowStore;
  /** Buckets to maintain per operation; defaults to minute, hour and day. */
  intervals?: readonly AssetFlowInterval[];
  metrics?: AssetFlowMetrics;
  /** Event sinks for SSE/webhook fan-out; failures are logged, not thrown. */
  events?: readonly AssetFlowEventSink[];
  /**
   * Optional durable high-water mark for ledger sequences (survives restarts).
   * Without it the high-water mark is per-process and resets on restart —
   * the trailing recompute still protects recent buckets.
   */
  highWaterStore?: CursorStore;
  /** Key for {@link AssetFlowAggregatorOptions.highWaterStore}. */
  highWaterKey?: string;
  /**
   * Buckets to recompute per interval when a reorg is detected. Defaults to
   * a trailing window of 60 × 1m, 24 × 1h and 7 × 1d.
   */
  reorgLookbackBuckets?: Partial<Record<AssetFlowInterval, number>>;
  log?: typeof logger;
}

const DEFAULT_REORG_LOOKBACK: Record<AssetFlowInterval, number> = {
  '1m': 60,
  '1h': 24,
  '1d': 7,
};

const DEFAULT_HIGH_WATER_KEY = 'asset-flow:last-ledger-sequence';

/**
 * Stateful aggregator: tracks the ledger high-water mark, applies operations
 * and recomputes trailing windows on reorgs. Build with
 * {@link createAssetFlowHandlers} / {@link createAssetFlowAggregationHandler},
 * or use directly for batch jobs.
 */
export class AssetFlowAggregator {
  private readonly store: AssetFlowStore;
  private readonly intervals: readonly AssetFlowInterval[];
  private readonly metrics: AssetFlowMetrics;
  private readonly events: readonly AssetFlowEventSink[];
  private readonly highWaterStore?: CursorStore;
  private readonly highWaterKey: string;
  private readonly reorgLookback: Record<AssetFlowInterval, number>;
  private readonly log: typeof logger;
  private lastLedgerSequence: number | null = null;

  constructor(options: AssetFlowAggregatorOptions) {
    if (!options.store) {
      throw new Error('AssetFlowAggregator requires a store');
    }
    this.store = options.store;
    this.intervals = options.intervals ?? ASSET_FLOW_INTERVALS;
    for (const interval of this.intervals) {
      if (!isAssetFlowInterval(interval)) {
        throw new Error(`unsupported interval: ${String(interval)}`);
      }
    }
    this.metrics = options.metrics ?? getDefaultAssetFlowMetrics();
    this.events = options.events ?? [];
    this.highWaterStore = options.highWaterStore;
    this.highWaterKey = options.highWaterKey ?? DEFAULT_HIGH_WATER_KEY;
    this.reorgLookback = { ...DEFAULT_REORG_LOOKBACK, ...options.reorgLookbackBuckets };
    this.log = options.log ?? logger;
  }

  /** Current in-memory ledger high-water mark (null until the first op). */
  get highWaterLedgerSequence(): number | null {
    return this.lastLedgerSequence;
  }

  /** Loads the persisted high-water mark, when a store is configured. */
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

  /**
   * Folds operations into the aggregate buckets, emitting update events.
   * Throws on invalid operation data — the queue layer retries the job.
   */
  async applyOperations(
    operations: readonly AssetFlowOperation[],
    context: { kind: string } = { kind: 'unknown' },
  ): Promise<AssetFlowAggregate[]> {
    if (operations.length === 0) return [];
    this.metrics.operationsProcessed.inc({ kind: context.kind });
    const deltas = computeAssetFlowDeltas(operations, this.intervals);
    if (deltas.length === 0) return [];
    const persisted = await this.upsertAndEmit(deltas);
    for (const delta of deltas) {
      this.metrics.bucketsUpdated.inc({ interval: delta.interval });
    }
    return persisted;
  }

  /** Recomputes the `[bucketStart, bucketEnd(bucketStart))` window from the stored operations. */
  async recomputeBucket(
    bucketStart: string,
    interval: AssetFlowInterval,
  ): Promise<AssetFlowAggregate[]> {
    const toIso = bucketEndIso(bucketStart, interval);
    const rows = await this.store.readOperations(bucketStart, toIso);
    const operations = storedOperationsToAssetFlow(rows);
    const deltas = computeAssetFlowDeltas(operations, [interval]);
    const deleted = await this.store.deleteAggregates(interval, [bucketStart]);
    this.log.info(
      { bucketStart, interval, deleted },
      'aggregator: recomputed bucket from ledger_operations',
    );
    if (deltas.length === 0) return [];
    const persisted = await this.upsertAndEmit(deltas);
    for (const delta of deltas) {
      this.metrics.bucketsUpdated.inc({ interval: delta.interval });
    }
    return persisted;
  }

  /** Recomputes the trailing window for every configured interval (used after reorgs). */
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
        const operations = storedOperationsToAssetFlow(rows);
        const deltas = computeAssetFlowDeltas(operations, [interval]);
        const bucketStarts = listBucketStarts(fromIso, toIso, interval);
        await this.store.deleteAggregates(interval, bucketStarts);
        if (deltas.length > 0) {
          await this.upsertAndEmit(deltas);
        }
        this.metrics.reorgsDetected.inc({ interval });
        this.log.info(
          { interval, from: fromIso, to: toIso, operations: operations.length },
          'aggregator: recomputed trailing window after ledger rollback',
        );
      } catch (error) {
        this.metrics.failuresTotal.inc({ stage: 'recompute' });
        this.log.error({ err: error, interval }, 'aggregator: trailing recompute failed');
        throw error;
      }
    }
  }

  /**
   * Observes the ledger sequence of an incoming batch; returns true when a
   * rollback was detected (and a trailing recompute was triggered). The
   * trailing window is anchored at `referenceTime` — typically the ledger
   * close time of the triggering operation — falling back to now.
   */
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
      // The fork may re-include higher sequences; keep the max of both.
      this.lastLedgerSequence = Math.max(last, ledgerSequence);
      await this.persistHighWaterMark(this.lastLedgerSequence);
      return true;
    }
    this.lastLedgerSequence = Math.max(last ?? 0, ledgerSequence);
    await this.persistHighWaterMark(this.lastLedgerSequence);
    return false;
  }

  private async upsertAndEmit(deltas: readonly AssetFlowDelta[]): Promise<AssetFlowAggregate[]> {
    let persisted: AssetFlowAggregate[];
    try {
      persisted = await this.store.upsertAggregates(deltas);
    } catch (error) {
      this.metrics.failuresTotal.inc({ stage: 'upsert' });
      throw error;
    }
    const byKey = new Map<string, AssetFlowAggregate>();
    for (const row of persisted) {
      byKey.set(`${row.bucket_start} ${row.interval} ${row.asset_code} ${row.asset_issuer ?? ''}`, row);
    }
    const updatedAt = new Date().toISOString();
    for (const delta of deltas) {
      const totals = byKey.get(
        `${delta.bucketStart} ${delta.interval} ${delta.assetCode} ${delta.assetIssuer}`,
      );
      const event: AssetFlowUpdatedEvent = {
        kind: 'asset_flow_updated',
        interval: delta.interval,
        bucketStart: delta.bucketStart,
        assetCode: delta.assetCode,
        assetIssuer: delta.assetIssuer,
        volumeDelta: delta.volume,
        paymentsDelta: delta.payments,
        netFlowDelta: delta.netFlow,
        volume: totals?.volume,
        payments: totals?.payments,
        netFlow: totals?.net_flow,
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

/* ------------------------------------------------------------------ */
/* Queue handler factories                                             */
/* ------------------------------------------------------------------ */

export type AssetFlowHandlers = {
  payment?: LedgerOperationHandler<'payment'>;
  path_payment?: LedgerOperationHandler<'path_payment'>;
};

/**
 * Builds the `ledger-operations` handlers for the aggregator (issue #18):
 * one for payments, one for path payments. Pass the result into
 * `startQueueWorkers({ handlers })` so the workers feed the aggregates.
 */
export function createAssetFlowHandlers(
  options: AssetFlowAggregatorOptions,
): AssetFlowHandlers {
  const aggregator = new AssetFlowAggregator(options);
  return {
    payment: async (payload) => {
      await aggregator.observeLedgerSequence(payload.ledger, payload.ledgerClosedAt);
      await aggregator.applyOperations(paymentPayloadToOperations(payload), {
        kind: 'payment',
      });
    },
    path_payment: async (payload) => {
      await aggregator.observeLedgerSequence(payload.ledger, payload.ledgerClosedAt);
      await aggregator.applyOperations(pathPaymentPayloadToOperations(payload), {
        kind: 'path_payment',
      });
    },
  };
}

/**
 * Builds the `asset-flow` aggregation-job handler: recomputes one closed
 * hour/minute bucket from `ledger_operations`. Registered on the
 * `aggregation-jobs` worker (`startQueueWorkers({ aggregationHandlers })`).
 */
export function createAssetFlowAggregationHandler(
  options: AssetFlowAggregatorOptions,
): AggregationJobHandler {
  const aggregator = new AssetFlowAggregator(options);
  return async (job) => {
    if (job.kind !== 'asset-flow') {
      throw new Error(`asset flow aggregator cannot handle aggregation kind ${job.kind}`);
    }
    const interval = job.interval;
    if (!isAssetFlowInterval(interval)) {
      throw new Error(`unsupported asset flow interval: ${String(interval)}`);
    }
    await aggregator.recomputeBucket(job.bucketStart, interval);
  };
}
