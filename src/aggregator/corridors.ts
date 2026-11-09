/**
 * Corridor analysis aggregator (issue #21).
 *
 * Consumes parsed path-payment and payment operations from the
 * `ledger-operations` queue and maintains the `corridor_aggregates`
 * hypertable: time-bucketed flows between anchor pairs, broken down by
 * source and destination assets.
 *
 * Real-time path: each path-payment job is folded into the minute, hour and
 * day buckets of the ledger close time and upserted with an additive
 * `INSERT ... ON CONFLICT ... DO UPDATE SET ... + EXCLUDED ...` so
 * concurrent workers never double-count or lose a flow. Regular payments are
 * also folded in as degenerate corridors where source and destination assets
 * (and anchors) are identical.
 *
 * Anchor identification: issuers are resolved through the local
 * {@link AnchorRegistry} seeded at startup (SEP-1 aware labeling is tracked
 * separately, issue #51). When a payload carries corridor metadata from the
 * ingestor that is preferred; otherwise the registry is consulted directly
 * and unknown issuers fall back to `null`.
 *
 * Semantics worth knowing before consuming `/v1/flows/corridors`:
 *
 * - `source_asset` / `dest_asset` — `CODE` for the native asset, otherwise
 *   `CODE:ISSUER` (stable asset key).
 * - `source_anchor` / `dest_anchor` — issuer account of the endpoint asset,
 *   or `null` for the native asset. When the registry knows the issuer the
 *   value remains the issuer account; labels are resolved at query time.
 * - `volume` — sum of destination-leg amounts that arrived (the value
 *   actually landed). For regular payments this is simply `amount`.
 * - `payments` — number of corridor legs (one per payment operation).
 *
 * Reorg path mirrors the other aggregators: ledger sequence drops trigger a
 * trailing-window recompute from `ledger_operations`, and
 * `corridor` aggregation jobs recompute one closed bucket.
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
  PaymentPayload,
  PathPaymentPayload,
  NormalizedAsset,
} from '../ingestor/types';
import { assetKey, type AnchorInfo } from '../ingestor/types';
import type { CorridorAggregate } from '../db/schema/aggregates';
import type { CursorStore } from '../ingestor/types';

// ----------------------------------------------------------------------
// Decimal arithmetic (7-decimal stroops, bigint)
// ----------------------------------------------------------------------

const STROOP_SCALE = 10_000_000n;
const AMOUNT_PATTERN = /^([+-]?)(\d+)(?:\.(\d{1,7}))?$/;

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

export type CorridorInterval = '1m' | '1h' | '1d';

const INTERVAL_MS: Record<CorridorInterval, number> = {
  '1m': 60_000,
  '1h': 3_600_000,
  '1d': 86_400_000,
};

export const CORRIDOR_INTERVALS: readonly CorridorInterval[] = ['1m', '1h', '1d'];

export function bucketStartIso(tsMs: number, interval: CorridorInterval): string {
  const size = INTERVAL_MS[interval];
  return new Date(Math.floor(tsMs / size) * size).toISOString();
}

export function bucketEndIso(bucketIso: string, interval: CorridorInterval): string {
  const start = Date.parse(bucketIso);
  if (Number.isNaN(start)) {
    throw new Error(`invalid bucketStart: ${JSON.stringify(bucketIso)}`);
  }
  return new Date(start + INTERVAL_MS[interval]).toISOString();
}

export function listBucketStarts(
  fromIso: string,
  toIso: string,
  interval: CorridorInterval,
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

export function isCorridorInterval(value: string): value is CorridorInterval {
  return value === '1m' || value === '1h' || value === '1d';
}

// ----------------------------------------------------------------------
// Anchor resolution (SEP-1 aware labeling lives in issue #51)
// ----------------------------------------------------------------------

/** Minimal registry surface the aggregator depends on. */
export interface AnchorResolver {
  resolve(issuer: string | undefined): AnchorInfo | undefined;
}

/** Built-in anchor registry keyed by issuer account. */
export class AnchorRegistry {
  private readonly anchors = new Map<string, AnchorInfo>();

  register(issuer: string, info: AnchorInfo | Omit<AnchorInfo, 'issuer'>): this {
    this.anchors.set(issuer, { issuer, ...info });
    return this;
  }

  resolve(issuer: string | undefined): AnchorInfo | undefined {
    return issuer ? this.anchors.get(issuer) : undefined;
  }

  get size(): number {
    return this.anchors.size;
  }
}

export const anchorRegistry = new AnchorRegistry();

/**
 * Resolves the anchor issuer for an asset, preferring an explicit
 * `AnchorInfo` from corridor metadata and falling back to the asset issuer.
 * Returns `null` for the native asset and unknown issuers.
 */
export function resolveAnchor(
  issuer: string | undefined,
  registry: AnchorResolver = anchorRegistry,
  explicit?: AnchorInfo,
): string | null {
  if (explicit?.issuer) return explicit.issuer;
  if (!issuer) return null;
  return registry.resolve(issuer)?.issuer ?? issuer;
}

export function describeAsset(asset: NormalizedAsset): string {
  return assetKey(asset);
}

// ----------------------------------------------------------------------
// Normalized corridor legs
// ----------------------------------------------------------------------

export interface CorridorOperation {
  sourceAsset: string;
  destAsset: string;
  sourceAnchor: string | null;
  destAnchor: string | null;
  amount: string;
  ledgerSequence?: number;
  ledgerClosedAt?: string;
}

export interface CorridorDelta {
  interval: CorridorInterval;
  bucketStart: string;
  sourceAsset: string;
  destAsset: string;
  sourceAnchor: string | null;
  destAnchor: string | null;
  volume: string;
  payments: number;
}

export interface CorridorStoredOperation {
  asset_code: string | null;
  asset_issuer: string | null;
  amount: string | null;
  source_account: string | null;
  destination_account: string | null;
  operation_type: string | null;
  ledger_sequence: number | null;
  ledger_closed_at: string;
}

// ----------------------------------------------------------------------
// Core computation (pure)
// ----------------------------------------------------------------------

interface Accumulator {
  bucketStart: string;
  sourceAsset: string;
  destAsset: string;
  sourceAnchor: string | null;
  destAnchor: string | null;
  volume: bigint;
  payments: number;
}

function parseTsMs(ledgerClosedAt: string | undefined): number {
  if (!ledgerClosedAt) {
    throw new Error('operation is missing ledgerClosedAt; cannot assign a bucket');
  }
  const tsMs = Date.parse(ledgerClosedAt);
  if (Number.isNaN(tsMs)) {
    throw new Error(`operation has unparseable ledgerClosedAt: ${ledgerClosedAt}`);
  }
  return tsMs;
}

export function computeCorridorDeltas(
  operations: readonly CorridorOperation[],
  intervals: readonly CorridorInterval[] = CORRIDOR_INTERVALS,
): CorridorDelta[] {
  const deltas: CorridorDelta[] = [];

  for (const interval of intervals) {
    const accumulators = new Map<string, Accumulator>();
    for (const op of operations) {
      const tsMs = parseTsMs(op.ledgerClosedAt);
      const bucketStart = bucketStartIso(tsMs, interval);
      const key = `${bucketStart} ${op.sourceAsset} ${op.destAsset} ${op.sourceAnchor ?? ''} ${op.destAnchor ?? ''}`;
      let entry = accumulators.get(key);
      if (!entry) {
        entry = {
          bucketStart,
          sourceAsset: op.sourceAsset,
          destAsset: op.destAsset,
          sourceAnchor: op.sourceAnchor,
          destAnchor: op.destAnchor,
          volume: 0n,
          payments: 0,
        };
        accumulators.set(key, entry);
      }
      entry.volume += parseAmount(op.amount);
      entry.payments += 1;
    }
    const intervalDeltas: CorridorDelta[] = [];
    for (const entry of accumulators.values()) {
      intervalDeltas.push({
        interval,
        bucketStart: entry.bucketStart,
        sourceAsset: entry.sourceAsset,
        destAsset: entry.destAsset,
        sourceAnchor: entry.sourceAnchor,
        destAnchor: entry.destAnchor,
        volume: formatAmount(entry.volume),
        payments: entry.payments,
      });
    }
    intervalDeltas.sort(
      (a, b) =>
        a.bucketStart.localeCompare(b.bucketStart) ||
        a.sourceAsset.localeCompare(b.sourceAsset) ||
        a.destAsset.localeCompare(b.destAsset) ||
        a.interval.localeCompare(b.interval),
    );
    deltas.push(...intervalDeltas);
  }

  return deltas.sort(
    (a, b) =>
      a.bucketStart.localeCompare(b.bucketStart) ||
      a.sourceAsset.localeCompare(b.sourceAsset) ||
      a.destAsset.localeCompare(b.destAsset) ||
      a.interval.localeCompare(b.interval),
  );
}

// ----------------------------------------------------------------------
// Payload mapping
// ----------------------------------------------------------------------

export function paymentPayloadToCorridor(
  payload: PaymentPayload,
  registry: AnchorResolver = anchorRegistry,
): CorridorOperation {
  const sourceAnchor = resolveAnchor(payload.asset.issuer, registry);
  return {
    sourceAsset: assetKey(payload.asset),
    destAsset: assetKey(payload.asset),
    sourceAnchor,
    destAnchor: sourceAnchor,
    amount: payload.amount,
    ledgerSequence: payload.ledger,
    ledgerClosedAt: payload.ledgerClosedAt,
  };
}

export function pathPaymentPayloadToCorridor(
  payload: PathPaymentPayload,
  registry: AnchorResolver = anchorRegistry,
  log: typeof logger = logger,
): CorridorOperation[] {
  const common = {
    ledgerSequence: payload.ledger,
    ledgerClosedAt: payload.ledgerClosedAt,
  };
  const operations: CorridorOperation[] = [];

  const destAmount = payload.destinationAmount ?? payload.destinationMin;
  if (destAmount !== undefined) {
    if (payload.destinationAmount === undefined && payload.destinationMin !== undefined) {
      log.warn(
        { id: payload.id },
        'aggregator: path payment missing destination_amount, using destination_min_amount as approximation',
      );
    }
    operations.push({
      ...common,
      sourceAsset: assetKey(payload.sendAsset),
      destAsset: assetKey(payload.destinationAsset),
      sourceAnchor: resolveAnchor(
        payload.sendAsset.issuer,
        registry,
        payload.corridor.sourceAnchor,
      ),
      destAnchor: resolveAnchor(
        payload.destinationAsset.issuer,
        registry,
        payload.corridor.destinationAnchor,
      ),
      amount: destAmount,
    });
  } else if (payload.sendAmount !== undefined || payload.sendMax !== undefined) {
    const fallback = payload.sendAmount ?? payload.sendMax;
    if (fallback !== undefined) {
      operations.push({
        ...common,
        sourceAsset: assetKey(payload.sendAsset),
        destAsset: assetKey(payload.destinationAsset),
        sourceAnchor: resolveAnchor(
          payload.sendAsset.issuer,
          registry,
          payload.corridor.sourceAnchor,
        ),
        destAnchor: resolveAnchor(
          payload.destinationAsset.issuer,
          registry,
          payload.corridor.destinationAnchor,
        ),
        amount: fallback,
      });
    }
  }

  return operations;
}

export function storedOperationsToCorridor(
  rows: readonly CorridorStoredOperation[],
): CorridorOperation[] {
  const operations: CorridorOperation[] = [];
  for (const row of rows) {
    if (!row.source_account || !row.asset_code || !row.amount) continue;
    // Stored rows are single-asset legs; treat the leg as a degenerate
    // corridor. Destination is the same asset for payments.
    const assetKeyValue = row.asset_issuer
      ? `${row.asset_code}:${row.asset_issuer}`
      : row.asset_code;
    operations.push({
      sourceAsset: assetKeyValue,
      destAsset: assetKeyValue,
      sourceAnchor: row.asset_issuer,
      destAnchor: row.asset_issuer,
      amount: row.amount,
      ledgerSequence: row.ledger_sequence ?? undefined,
      ledgerClosedAt: row.ledger_closed_at,
    });
  }
  return operations;
}

// ----------------------------------------------------------------------
// Store port
// ----------------------------------------------------------------------

export interface CorridorStore {
  readOperations(fromIso: string, toIso: string): Promise<CorridorStoredOperation[]>;
  upsertAggregates(deltas: readonly CorridorDelta[]): Promise<CorridorAggregate[]>;
  deleteAggregates(interval: CorridorInterval, bucketStarts: readonly string[]): Promise<number>;
}

export interface CorridorSql {
  (strings: TemplateStringsArray, ...values: unknown[]): Promise<unknown>;
}

export function createPostgresCorridorStore(sql: CorridorSql): CorridorStore {
  return {
    async readOperations(fromIso, toIso) {
      const rows = (await sql`
        SELECT asset_code, asset_issuer, amount, source_account, destination_account,
               operation_type, ledger_sequence, ledger_closed_at
        FROM ledger_operations
        WHERE ledger_closed_at >= ${fromIso}
          AND ledger_closed_at < ${toIso}
          AND successful = true
          AND operation_type IN ('payment', 'path_payment')
          AND asset_code IS NOT NULL
          AND amount IS NOT NULL
      `) as CorridorStoredOperation[];
      return rows;
    },

    async upsertAggregates(deltas) {
      const persisted: CorridorAggregate[] = [];
      for (const delta of deltas) {
        const rows = (await sql`
          INSERT INTO corridor_aggregates
            (bucket_start, interval, source_asset, dest_asset, source_anchor, dest_anchor, volume, payments)
          VALUES
            (${delta.bucketStart}, ${delta.interval}, ${delta.sourceAsset}, ${delta.destAsset},
             ${delta.sourceAnchor}, ${delta.destAnchor}, ${delta.volume}, ${delta.payments})
          ON CONFLICT (bucket_start, interval, source_asset, dest_asset, source_anchor, dest_anchor)
          DO UPDATE SET
            volume = corridor_aggregates.volume + EXCLUDED.volume,
            payments = corridor_aggregates.payments + EXCLUDED.payments
          RETURNING to_char(bucket_start AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.000"Z"') AS bucket_start,
                    interval, source_asset, dest_asset, source_anchor, dest_anchor,
                    volume::text AS volume, payments
        `) as CorridorAggregate[];
        persisted.push(...rows);
      }
      return persisted;
    },

    async deleteAggregates(interval, bucketStarts) {
      if (bucketStarts.length === 0) return 0;
      const result = (await sql`
        DELETE FROM corridor_aggregates
        WHERE interval = ${interval}
          AND bucket_start = ANY(${bucketStarts as string[]})
      `) as { count?: number };
      return result.count ?? 0;
    },
  };
}

export function createInMemoryCorridorStore(
  seed: readonly CorridorStoredOperation[] = [],
): CorridorStore & { rows(): CorridorAggregate[] } {
  const operations = [...seed];
  const aggregates = new Map<string, CorridorAggregate>();

  const keyOf = (row: {
    bucket_start: string;
    interval: string;
    source_asset: string;
    dest_asset: string;
    source_anchor: string | null;
    dest_anchor: string | null;
  }): string =>
    `${row.bucket_start} ${row.interval} ${row.source_asset} ${row.dest_asset} ${row.source_anchor ?? ''} ${row.dest_anchor ?? ''}`;

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
      const persisted: CorridorAggregate[] = [];
      for (const delta of deltas) {
        const candidate: CorridorAggregate = {
          bucket_start: delta.bucketStart,
          interval: delta.interval,
          source_asset: delta.sourceAsset,
          dest_asset: delta.destAsset,
          source_anchor: delta.sourceAnchor,
          dest_anchor: delta.destAnchor,
          volume: delta.volume,
          payments: delta.payments,
        };
        const key = keyOf(candidate);
        const existing = aggregates.get(key);
        if (existing) {
          const merged: CorridorAggregate = {
            ...existing,
            volume: formatAmount(parseAmount(existing.volume) + parseAmount(candidate.volume)),
            payments: existing.payments + candidate.payments,
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

export interface CorridorUpdatedEvent {
  kind: 'corridor_updated';
  interval: string;
  bucketStart: string;
  sourceAsset: string;
  destAsset: string;
  sourceAnchor: string | null;
  destAnchor: string | null;
  volumeDelta: string;
  paymentsDelta: number;
  volume?: string;
  payments?: number;
  updatedAt: string;
}

export interface CorridorEventSink {
  emit(event: CorridorUpdatedEvent): void | Promise<void>;
}

export function createInMemoryCorridorSink(): CorridorEventSink & {
  events: CorridorUpdatedEvent[];
} {
  const events: CorridorUpdatedEvent[] = [];
  return {
    events,
    emit(event) {
      events.push(event);
    },
  };
}

export function createRedisCorridorSink(
  client: { publish(channel: string, message: string): Promise<unknown> },
  channel = 'eta:flows:corridors',
): CorridorEventSink {
  return {
    async emit(event) {
      await client.publish(channel, JSON.stringify(event));
    },
  };
}

// ----------------------------------------------------------------------
// Prometheus metrics
// ----------------------------------------------------------------------

export interface CorridorMetrics {
  operationsProcessed: Counter<'kind'>;
  bucketsUpdated: Counter<'interval'>;
  reorgsDetected: Counter<'interval'>;
  failuresTotal: Counter<'stage'>;
}

export const CORRIDOR_METRIC_PREFIX = 'eta_corridor_';

let defaultMetrics: CorridorMetrics | undefined;
function getDefaultMetrics(): CorridorMetrics {
  if (!defaultMetrics) {
    defaultMetrics = createCorridorMetrics();
  }
  return defaultMetrics;
}

export function createCorridorMetrics(registry: Registry = register): CorridorMetrics {
  return {
    operationsProcessed: new Counter({
      name: `${CORRIDOR_METRIC_PREFIX}operations_processed_total`,
      help: 'Operations folded into corridor aggregates, by ledger job kind',
      registers: [registry],
      labelNames: ['kind'],
    }),
    bucketsUpdated: new Counter({
      name: `${CORRIDOR_METRIC_PREFIX}buckets_updated_total`,
      help: 'Corridor bucket rows upserted, by interval',
      registers: [registry],
      labelNames: ['interval'],
    }),
    reorgsDetected: new Counter({
      name: `${CORRIDOR_METRIC_PREFIX}reorgs_detected_total`,
      help: 'Ledger rollback detections that triggered a trailing recompute, by interval',
      registers: [registry],
      labelNames: ['interval'],
    }),
    failuresTotal: new Counter({
      name: `${CORRIDOR_METRIC_PREFIX}failures_total`,
      help: 'Corridor aggregation failures, by pipeline stage',
      registers: [registry],
      labelNames: ['stage'],
    }),
  };
}

// ----------------------------------------------------------------------
// Aggregator
// ----------------------------------------------------------------------

export interface CorridorAggregatorOptions {
  store: CorridorStore;
  intervals?: readonly CorridorInterval[];
  metrics?: CorridorMetrics;
  events?: readonly CorridorEventSink[];
  highWaterStore?: CursorStore;
  highWaterKey?: string;
  reorgLookbackBuckets?: Partial<Record<CorridorInterval, number>>;
  log?: typeof logger;
  registry?: AnchorResolver;
}

const DEFAULT_REORG_LOOKBACK: Record<CorridorInterval, number> = {
  '1m': 60,
  '1h': 24,
  '1d': 7,
};

const DEFAULT_HIGH_WATER_KEY = 'corridor:last-ledger-sequence';

export class CorridorAggregator {
  private readonly store: CorridorStore;
  private readonly intervals: readonly CorridorInterval[];
  private readonly metrics: CorridorMetrics;
  private readonly events: readonly CorridorEventSink[];
  private readonly highWaterStore?: CursorStore;
  private readonly highWaterKey: string;
  private readonly reorgLookback: Record<CorridorInterval, number>;
  private readonly log: typeof logger;
  private readonly registry: AnchorResolver;
  private lastLedgerSequence: number | null = null;

  constructor(options: CorridorAggregatorOptions) {
    if (!options.store) {
      throw new Error('CorridorAggregator requires a store');
    }
    this.store = options.store;
    this.intervals = options.intervals ?? CORRIDOR_INTERVALS;
    for (const interval of this.intervals) {
      if (!isCorridorInterval(interval)) {
        throw new Error(`unsupported interval: ${String(interval)}`);
      }
    }
    this.metrics = options.metrics ?? getDefaultMetrics();
    this.events = options.events ?? [];
    this.highWaterStore = options.highWaterStore;
    this.highWaterKey = options.highWaterKey ?? DEFAULT_HIGH_WATER_KEY;
    this.reorgLookback = { ...DEFAULT_REORG_LOOKBACK, ...options.reorgLookbackBuckets };
    this.log = options.log ?? logger;
    this.registry = options.registry ?? anchorRegistry;
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

  async applyOperations(
    operations: readonly CorridorOperation[],
    context: { kind: string } = { kind: 'unknown' },
  ): Promise<CorridorAggregate[]> {
    if (operations.length === 0) return [];
    this.metrics.operationsProcessed.inc({ kind: context.kind });
    const deltas = computeCorridorDeltas(operations, this.intervals);
    if (deltas.length === 0) return [];
    const persisted = await this.upsertAndEmit(deltas);
    for (const delta of deltas) {
      this.metrics.bucketsUpdated.inc({ interval: delta.interval });
    }
    return persisted;
  }

  async recomputeBucket(
    bucketStart: string,
    interval: CorridorInterval,
  ): Promise<CorridorAggregate[]> {
    const toIso = bucketEndIso(bucketStart, interval);
    const rows = await this.store.readOperations(bucketStart, toIso);
    const operations = storedOperationsToCorridor(rows);
    const deltas = computeCorridorDeltas(operations, [interval]);
    const deleted = await this.store.deleteAggregates(interval, [bucketStart]);
    this.log.info(
      { bucketStart, interval, deleted },
      'aggregator: recomputed corridor bucket from ledger_operations',
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
        const operations = storedOperationsToCorridor(rows);
        const deltas = computeCorridorDeltas(operations, [interval]);
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

  private async upsertAndEmit(deltas: readonly CorridorDelta[]): Promise<CorridorAggregate[]> {
    let persisted: CorridorAggregate[];
    try {
      persisted = await this.store.upsertAggregates(deltas);
    } catch (error) {
      this.metrics.failuresTotal.inc({ stage: 'upsert' });
      throw error;
    }
    const updatedAt = new Date().toISOString();
    for (const delta of deltas) {
      const totals = persisted.find(
        (p) =>
          p.bucket_start === delta.bucketStart &&
          p.source_asset === delta.sourceAsset &&
          p.dest_asset === delta.destAsset,
      );
      const event: CorridorUpdatedEvent = {
        kind: 'corridor_updated',
        interval: delta.interval,
        bucketStart: delta.bucketStart,
        sourceAsset: delta.sourceAsset,
        destAsset: delta.destAsset,
        sourceAnchor: delta.sourceAnchor,
        destAnchor: delta.destAnchor,
        volumeDelta: delta.volume,
        paymentsDelta: delta.payments,
        volume: totals?.volume,
        payments: totals?.payments,
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

export type CorridorHandlers = {
  payment?: LedgerOperationHandler<'payment'>;
  path_payment?: LedgerOperationHandler<'path_payment'>;
};

export function createCorridorHandlers(
  options: CorridorAggregatorOptions,
): CorridorHandlers {
  const aggregator = new CorridorAggregator(options);
  const registry = options.registry ?? anchorRegistry;
  return {
    payment: async (payload) => {
      await aggregator.observeLedgerSequence(payload.ledger, payload.ledgerClosedAt);
      await aggregator.applyOperations([paymentPayloadToCorridor(payload, registry)], {
        kind: 'payment',
      });
    },
    path_payment: async (payload) => {
      await aggregator.observeLedgerSequence(payload.ledger, payload.ledgerClosedAt);
      await aggregator.applyOperations(pathPaymentPayloadToCorridor(payload, registry), {
        kind: 'path_payment',
      });
    },
  };
}

export function createCorridorAggregationHandler(
  options: CorridorAggregatorOptions,
): AggregationJobHandler {
  const aggregator = new CorridorAggregator(options);
  return async (job) => {
    if (job.kind !== 'corridor') {
      throw new Error(`corridor aggregator cannot handle aggregation kind ${job.kind}`);
    }
    const interval = job.interval;
    if (!isCorridorInterval(interval)) {
      throw new Error(`unsupported corridor interval: ${String(interval)}`);
    }
    await aggregator.recomputeBucket(job.bucketStart, interval);
  };
}
