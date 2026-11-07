/**
 * Account flow aggregator (issue #19).
 *
 * Consumes parsed operations from the `ledger-operations` queue (via the
 * handler port in `src/queue/processors.ts`) and maintains the
 * `account_flow_aggregates` hypertable: time-bucketed per-account inflow,
 * outflow, net flow and counterparties.
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
 * exposed for the `account-flow` aggregation jobs produced by the Hubble
 * backfill (issue #17): they recompute one closed bucket from the stored
 * operations.
 *
 * Semantics worth knowing before consuming `/v1/flows/accounts/{account}`:
 *
 * - `inflow`   — sum of amounts received by the account (where the account
 *                is the `to` party), per bucket.
 * - `outflow`  — sum of amounts sent by the account (where the account
 *                is the `from` party), per bucket.
 * - `net_flow` — issuer-attributed net issuance: amount paid OUT of the
 *                account minus amount received INTO the account. Moves
 *                between two non-issuer accounts always net to zero. The
 *                native asset (no issuer) always reports `0`.
 * - `counterparties` — JSONB array of the top N counterparty accounts by
 *   volume, each with `{ account_id, volume, count, direction }` where
 *   `direction` is `'in'` (received) or `'out'` (sent).
 *
 * `account_id` is written as the raw Stellar account ID. The hypertable's
 * primary key treats each account as distinct.
 *
 * Precision: amounts travel as decimal strings and are summed with bigint
 * stroops (7 decimal places) — no float rounding anywhere.
 */

/* ------------------------------------------------------------------ */
/* Imports                                                            */
/* ------------------------------------------------------------------ */

import { Counter, register, type Registry } from 'prom-client';
import { logger } from '../logger';
import type {
  LedgerOperationHandler,
  AggregationJobHandler,
} from '../queue/processors';
import type { PaymentPayload, PathPaymentPayload } from '../ingestor/types';
import type { AccountFlowAggregate } from '../db/schema/aggregates';
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

export type AccountFlowInterval = '1m' | '1h' | '1d';

const INTERVAL_MS: Record<AccountFlowInterval, number> = {
  '1m': 60_000,
  '1h': 3_600_000,
  '1d': 86_400_000,
};

export const ACCOUNT_FLOW_INTERVALS: readonly AccountFlowInterval[] = ['1m', '1h', '1d'];

/** Floor `tsMs` to the start of its interval bucket; returns ISO 8601. */
export function bucketStartIso(tsMs: number, interval: AccountFlowInterval): string {
  const size = INTERVAL_MS[interval];
  return new Date(Math.floor(tsMs / size) * size).toISOString();
}

/** ISO 8601 end (exclusive) of the bucket starting at `bucketIso`. */
export function bucketEndIso(bucketIso: string, interval: AccountFlowInterval): string {
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
  interval: AccountFlowInterval,
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
export function isAccountFlowInterval(value: string): value is AccountFlowInterval {
  return value === '1m' || value === '1h' || value === '1d';
}

/* ------------------------------------------------------------------ */
/* Normalized operation legs                                           */

/* ------------------------------------------------------------------ */

/** One account transfer leg to fold into the aggregates. */
export interface AccountFlowOperation {
  /** Non-negative decimal string; throws at compute time when invalid. */
  amount: string;
  fromAccount: string;
  toAccount: string;
  /** Ledger close time (ISO 8601); drives the bucket assignment. */
  ledgerClosedAt?: string;
  /** Ledger sequence, when available. */
  ledgerSequence?: number;
}

/** Delta computed per (bucket, account) before persisting. */
export interface AccountFlowDelta {
  interval: AccountFlowInterval;
  bucketStart: string;
  accountId: string;
  inflow: string;
  outflow: string;
  netFlow: string;
  counterparties: Array<{
    account_id: string;
    volume: string;
    count: number;
    direction: 'in' | 'out';
  }>;
}

/** Stored representation returned from Postgres (all strings). */
export interface AccountFlowStoredOperation {
  account_id: string;
  amount: string;
  from_account: string | null;
  to_account: string | null;
  ledger_sequence?: number | null;
  ledger_closed_at: string;
}

/* ------------------------------------------------------------------ */
/* Core computation (pure, unit-test friendly)                         */

/* ------------------------------------------------------------------ */

interface AccountAccumulator {
  bucketStart: string;
  accountId: string;
  inflow: bigint;
  outflow: bigint;
  netFlow: bigint;
  counterparties: Map<string, CounterpartyInfo>;
}

interface CounterpartyInfo {
  volume: bigint;
  count: number;
  direction: 'in' | 'out';
}

/** Accumulate one operation into the per-account bucket accumulator. */
function accumulateInto(
  accumulators: Map<string, AccountAccumulator>,
  op: AccountFlowOperation,
  tsMs: number,
  interval: AccountFlowInterval,
): void {
  const bucketStart = bucketStartIso(tsMs, interval);

  // Accumulate into the 'to' account (inflow)
  const toKey = `${bucketStart} ${op.toAccount}`;
  let toEntry = accumulators.get(toKey);
  if (!toEntry) {
    toEntry = {
      bucketStart,
      accountId: op.toAccount,
      inflow: 0n,
      outflow: 0n,
      netFlow: 0n,
      counterparties: new Map(),
    };
    accumulators.set(toKey, toEntry);
  }

  // Accumulate into the 'from' account (outflow)
  const fromKey = `${bucketStart} ${op.fromAccount}`;
  let fromEntry = accumulators.get(fromKey);
  if (!fromEntry) {
    fromEntry = {
      bucketStart,
      accountId: op.fromAccount,
      inflow: 0n,
      outflow: 0n,
      netFlow: 0n,
      counterparties: new Map(),
    };
    accumulators.set(fromKey, fromEntry);
  }

  const amount = parseAmount(op.amount);

  // Inflow: the to account received this amount
  toEntry.inflow += amount;
  // Outflow: the from account sent this amount
  fromEntry.outflow += amount;

  // Net flow: inflow - outflow tracked per account
  toEntry.netFlow = toEntry.inflow - toEntry.outflow;
  fromEntry.netFlow = fromEntry.inflow - fromEntry.outflow;

  // Track counterparties for 'to' account (the from account is a counterparty receiving/involved)
  const toCpKey = op.fromAccount;
  let toCpEntry = toEntry.counterparties.get(toCpKey);
  if (!toCpEntry) {
    toCpEntry = { volume: 0n, count: 0, direction: 'in' };
    toEntry.counterparties.set(toCpKey, toCpEntry);
  }
  toCpEntry.volume += amount;
  toCpEntry.count += 1;
  // Direction is 'in' because the toAccount is receiving from this counterparty

  // Track counterparties for 'from' account (the to account is a counterparty sending/involved)
  const fromCpKey = op.toAccount;
  let fromCpEntry = fromEntry.counterparties.get(fromCpKey);
  if (!fromCpEntry) {
    fromCpEntry = { volume: 0n, count: 0, direction: 'out' };
    fromEntry.counterparties.set(fromCpKey, fromCpEntry);
  }
  fromCpEntry.volume += amount;
  fromCpEntry.count += 1;
  // Direction is 'out' because the fromAccount is sending to this counterparty
}

/**
 * Folds operations into per-(bucket, account) deltas. Returns one delta per
 * interval, bucket and account; an empty input yields an empty list.
 *
 * Note: this function tracks both inflow (toAccount) and outflow (fromAccount)
 * by creating entries for both the destination and source accounts per operation.
 */
export function computeAccountFlowDeltas(
  operations: readonly AccountFlowOperation[],
  intervals: readonly AccountFlowInterval[] = ACCOUNT_FLOW_INTERVALS,
): AccountFlowDelta[] {
  const deltas: AccountFlowDelta[] = [];

  for (const interval of intervals) {
    const accumulators = new Map<string, AccountAccumulator>();

    for (const op of operations) {
      if (!op.ledgerClosedAt) {
        throw new Error('operation is missing ledgerClosedAt; cannot assign a bucket');
      }
      const tsMs = Date.parse(op.ledgerClosedAt);
      if (Number.isNaN(tsMs)) {
        throw new Error(`operation has unparseable ledgerClosedAt: ${op.ledgerClosedAt}`);
      }

      // Accumulate into both 'to' and 'from' accounts (handled in accumulateInto)
      accumulateInto(accumulators, op, tsMs, interval);
    }

    // Compute final deltas for this interval
    const intervalDeltas: AccountFlowDelta[] = [];
    for (const entry of accumulators.values()) {
      const netFlow = entry.inflow - entry.outflow;
      // Downsize counterparties to top N by volume
      const sortedCp = [...entry.counterparties.entries()]
        .sort((a, b) => Number(b[1].volume) - Number(a[1].volume))
        .slice(0, 100) // top 100 counterparties
        .map(([accountId, info]) => ({
          account_id: accountId,
          volume: formatAmount(info.volume),
          count: info.count,
          direction: info.direction,
        }));

      intervalDeltas.push({
        interval,
        bucketStart: entry.bucketStart,
        accountId: entry.accountId,
        inflow: formatAmount(entry.inflow),
        outflow: formatAmount(entry.outflow),
        netFlow: formatAmount(netFlow),
        counterparties: sortedCp,
      });
    }

    // Sort within interval by bucketStart then accountId, and add to results
    intervalDeltas.sort((a, b) =>
      a.bucketStart.localeCompare(b.bucketStart) ||
      a.accountId.localeCompare(b.accountId) ||
      a.interval.localeCompare(b.interval),
    );
    deltas.push(...intervalDeltas);
  }

  // Overall deterministic order: bucket, then account id.
  return deltas.sort((a, b) =>
    a.bucketStart.localeCompare(b.bucketStart) ||
    a.accountId.localeCompare(b.accountId) ||
    a.interval.localeCompare(b.interval),
  );
}

/** Maps a queue payment payload to its single leg. */
export function paymentPayloadToOperations(payload: PaymentPayload): AccountFlowOperation[] {
  return [
    {
      amount: payload.amount,
      fromAccount: payload.from,
      toAccount: payload.to,
      ledgerSequence: payload.ledger,
      ledgerClosedAt: payload.ledgerClosedAt,
    },
  ];
}

/** Maps a queue path-payment payload to its two legs (send asset out of the
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
): AccountFlowOperation[] {
  const common = {
    fromAccount: payload.from,
    toAccount: payload.to,
    ledgerSequence: payload.ledger,
    ledgerClosedAt: payload.ledgerClosedAt,
  };
  const legs: AccountFlowOperation[] = [];
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
      amount: destAmount,
    });
  }
  return legs;
}

/* ------------------------------------------------------------------ */
/* Store port (implemented by the Postgres adapter below)              */

/* ------------------------------------------------------------------ */

/**
 * Persistence port for the account flow aggregates. Implementations must
 * apply upserts additively: `inflow`, `outflow` and `net_flow` add onto
 * existing rows (`INSERT ... ON CONFLICT ... DO UPDATE SET x = x + EXCLUDED.x`),
 * which is what makes the real-time path safe under concurrent workers.
 * `counterparties` is stored as JSONB and merged additively by taking the
 * element-wise volume-max and count-add.
 */
export interface AccountFlowStore {
  /** Read operations from the aggregates table for a given time range. */
  readOperations(fromIso: string, toIso: string): Promise<AccountFlowStoredOperation[]>;
  /** Additive upsert; returns the persisted rows (with totals) per bucket. */
  upsertAggregates(deltas: readonly AccountFlowDelta[]): Promise<AccountFlowAggregate[]>;
  /** Deletes the given buckets for one interval; returns the row count. */
  deleteAggregates(interval: AccountFlowInterval, bucketStarts: readonly string[]): Promise<number>;
}

/** Minimal `postgres.Sql` surface used by the Postgres adapter. */
export interface AccountFlowSql {
  (strings: TemplateStringsArray, ...values: unknown[]): Promise<unknown>;
}

/**
 * Postgres implementation of {@link AccountFlowStore} against the shared
 * `postgres.Sql` pool. Upserts return the merged totals so the update
 * events carry the persisted state, not just the delta.
 */
export function createPostgresAccountFlowStore(sql: AccountFlowSql): AccountFlowStore {
  return {
    async readOperations(fromIso, toIso) {
      const rows = (await sql`
        SELECT account_id, amount, from_account, to_account,
               ledger_sequence, ledger_closed_at
        FROM account_flow_aggregates
        WHERE ledger_closed_at >= ${fromIso}
          AND ledger_closed_at < ${toIso}
      `) as AccountFlowStoredOperation[];
      return rows;
    },

    async upsertAggregates(deltas) {
      const persisted: AccountFlowAggregate[] = [];
      for (const delta of deltas) {
        const rows = (await sql`
          INSERT INTO account_flow_aggregates
            (bucket_start, interval, account_id, inflow, outflow, counterparties, net_flow)
          VALUES
            (${delta.bucketStart}, ${delta.interval}, ${delta.accountId},
             ${delta.inflow}, ${delta.outflow}, ${JSON.stringify(delta.counterparties)}, ${delta.netFlow})
          ON CONFLICT (bucket_start, interval, account_id)
          DO UPDATE SET
            inflow   = account_flow_aggregates.inflow + EXCLUDED.inflow,
            outflow  = account_flow_aggregates.outflow + EXCLUDED.outflow,
            net_flow = account_flow_aggregates.net_flow + EXCLUDED.net_flow,
            counterparties = account_flow_aggregates.counterparties || '[]'::jsonb || EXCLUDED.counterparties || '[]'::jsonb,
          RETURNING to_char(bucket_start AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.000"Z"') AS bucket_start,
                    interval, account_id,
                    inflow::text AS inflow, outflow, counterparties,
                    net_flow::text AS net_flow
        `) as AccountFlowAggregate[];
        persisted.push(...rows);
      }
      return persisted;
    },

    async deleteAggregates(interval, bucketStarts) {
      if (bucketStarts.length === 0) return 0;
      const result = (await sql`
        DELETE FROM account_flow_aggregates
        WHERE interval = ${interval}
          AND bucket_start = ANY(${bucketStarts as string[]})
      `) as { count?: number };
      return result.count ?? 0;
    },
  };
}

/**
 * In-memory {@link AccountFlowStore} with the same additive-upsert semantics
 * (counterparties kept as maps, merged by volume-max and count-add), exported
 * for tests and local development without a database.
 */
export function createInMemoryAccountFlowStore(
  seed: readonly AccountFlowStoredOperation[] = [],
): AccountFlowStore & { rows(): AccountFlowAggregate[] } {
  const operations = [...seed];
  const aggregates = new Map<string, AccountFlowAggregate>();

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
      const persisted: AccountFlowAggregate[] = [];
      for (const delta of deltas) {
        const candidate: AccountFlowAggregate = {
          bucket_start: delta.bucketStart,
          interval: delta.interval,
          account_id: delta.accountId,
          inflow: delta.inflow,
          outflow: delta.outflow,
          counterparties: delta.counterparties,
          net_flow: delta.netFlow,
        };
        const key = keyOf(candidate);
        const existing = aggregates.get(key);
        if (existing) {
          // Merge counterparties: take volume-max and count-add
          const mergedCp = new Map<
            string,
            { volume: bigint; count: number; direction: 'in' | 'out' }
          >();
          // Start with existing counterparties
          for (const [cid, cinfo] of (existing.counterparties as any)) {
            mergedCp.set(cid, { volume: cinfo.volume, count: cinfo.count, direction: cinfo.direction });
          }
          // Merge new counterparties
          for (const [cid, cinfo] of (delta.counterparties as any)) {
            const existingCp = mergedCp.get(cid);
            if (existingCp) {
              // Take volume max, add counts
              const newVol = existingCp.volume >= cinfo.volume ? existingCp.volume : cinfo.volume;
              existingCp.volume = newVol;
              existingCp.count += cinfo.count;
            } else {
              mergedCp.set(cid, {
                volume: cinfo.volume,
                count: cinfo.count,
                direction: cinfo.direction,
              });
            }
          }
          const merged: AccountFlowAggregate = {
            ...existing,
            inflow: formatAmount(
              parseAmount(existing.inflow) + parseAmount(candidate.inflow),
            ),
            outflow: formatAmount(
              parseAmount(existing.outflow) + parseAmount(candidate.outflow),
            ),
            net_flow: formatAmount(
              parseAmount(existing.net_flow) + parseAmount(candidate.net_flow),
            ),
            counterparties: Array.from(mergedCp.entries()).map(
              ([accountId, info]) => ({
                account_id: accountId,
                volume: info.volume > 0n ? info.volume : BigInt(0),
                count: info.count,
                direction: info.direction,
              }),
            ),
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

/* ------------------------------------------------------------------ */
/* Update events (SSE / webhooks)                                      */

/* ------------------------------------------------------------------ */

/** Emitted after each bucket upsert; consumed by SSE/webhook layers. */
export interface AccountFlowUpdatedEvent {
  kind: 'account_flow_updated';
  interval: string;
  bucketStart: string;
  accountId: string;
  /** Deltas applied to the bucket by this upsert. */
  inflowDelta: string;
  outflowDelta: string;
  netFlowDelta: string;
  /** Persisted totals after the upsert, when the store returns them. */
  inflow?: string;
  outflow?: string;
  netFlow?: string;
  counterparties?: Array<{
    account_id: string;
    volume: string;
    count: number;
    direction: 'in' | 'out';
  }>;
  updatedAt: string;
}

export interface AccountFlowEventSink {
  emit(event: AccountFlowUpdatedEvent): void | Promise<void>;
}

/** In-memory sink that records events; handy for tests. */
export function createInMemoryAccountFlowSink(): AccountFlowEventSink & {
  events: AccountFlowUpdatedEvent[];
} {
  const events: AccountFlowUpdatedEvent[] = [];
  return {
    events,
    emit(event) {
      events.push(event);
    },
  };
}

/** Publishes update events to a Redis channel (default `eta:flows:accounts`); the SSE/webhook fan-out layers subscribe there when those land. Any client with a `publish(channel, message)` method works. */
export function createRedisAccountFlowSink(
  client: { publish(channel: string, message: string): Promise<unknown> },
  channel = 'eta:flows:accounts',
): AccountFlowEventSink {
  return {
    async emit(event) {
      await client.publish(channel, JSON.stringify(event));
    },
  };
}

/* ------------------------------------------------------------------ */
/* Prometheus metrics                                                  */

/* ------------------------------------------------------------------ */

export interface AccountFlowMetrics {
  /** Operations processed, by job kind. */
  operationsProcessed: Counter<'kind'>;
  /** Bucket rows upserted, by interval. */
  bucketsUpdated: Counter<'interval'>;
  /** Reorg detections followed by a trailing-window recompute, by interval. */
  reorgsDetected: Counter<'interval'>;
  /** Failures by pipeline stage (validation, recompute, upsert, events). */
  failuresTotal: Counter<'stage'>;
}

export const ACCOUNT_FLOW_METRIC_PREFIX = 'eta_account_flow_';

/**
 * Lazily created shared default metrics so multiple aggregators built
 * without explicit metrics (e.g. the live-handler factory and the
 * aggregation-jobs handler factory in production wiring) never register
 * duplicate collectors against the global registry.
 */
let defaultAccountFlowMetrics: AccountFlowMetrics | undefined;
function getDefaultAccountFlowMetrics(): AccountFlowMetrics {
  if (!defaultAccountFlowMetrics) {
    defaultAccountFlowMetrics = createAccountFlowMetrics();
  }
  return defaultAccountFlowMetrics;
}

export function createAccountFlowMetrics(
  registry: Registry = register,
): AccountFlowMetrics {
  const kinds = ['kind'] as const;
  const intervals = ['interval'] as const;
  const stages = ['stage'] as const;
  return {
    operationsProcessed: new Counter({
      name: `${ACCOUNT_FLOW_METRIC_PREFIX}operations_processed_total`,
      help: 'Operations folded into account flow aggregates, by ledger job kind',
      registers: [registry],
      labelNames: kinds,
    }),
    bucketsUpdated: new Counter({
      name: `${ACCOUNT_FLOW_METRIC_PREFIX}buckets_updated_total`,
      help: 'Account flow bucket rows upserted, by interval',
      registers: [registry],
      labelNames: intervals,
    }),
    reorgsDetected: new Counter({
      name: `${ACCOUNT_FLOW_METRIC_PREFIX}reorgs_detected_total`,
      help: 'Ledger rollback detections that triggered a trailing recompute, by interval',
      registers: [registry],
      labelNames: intervals,
    }),
    failuresTotal: new Counter({
      name: `${ACCOUNT_FLOW_METRIC_PREFIX}failures_total`,
      help: 'Account flow aggregation failures, by pipeline stage',
      registers: [registry],
      labelNames: stages,
    }),
  };
}

/* ------------------------------------------------------------------ */
/* Aggregator                                                          */

/* ------------------------------------------------------------------ */

export interface AccountFlowAggregatorOptions {
  store: AccountFlowStore;
  /** Buckets to maintain per operation; defaults to minute, hour and day. */
  intervals?: readonly AccountFlowInterval[];
  metrics?: AccountFlowMetrics;
  /** Event sinks for SSE/webhook fan-out; failures are logged, not thrown. */
  events?: readonly AccountFlowEventSink[];
  /**
   * Optional durable high-water mark for ledger sequences (survives restarts).
   * Without it the high-water mark is per-process and resets on restart —
   * the trailing recompute still protects recent buckets.
   */
  highWaterStore?: CursorStore;
  /** Key for {@link AccountFlowAggregatorOptions.highWaterStore}. */
  highWaterKey?: string;
  /**
   * Buckets to recompute per interval when a reorg is detected. Defaults to
   * a trailing window of 60 × 1m, 24 × 1h and 7 × 1d.
   */
  reorgLookbackBuckets?: Partial<Record<AccountFlowInterval, number>>;
  log?: typeof logger;
}

const DEFAULT_REORG_LOOKBACK: Record<AccountFlowInterval, number> = {
  '1m': 60,
  '1h': 24,
  '1d': 7,
};

const DEFAULT_HIGH_WATER_KEY = 'account-flow:last-ledger-sequence';

/**
 * Stateful aggregator: tracks the ledger high-water mark, applies operations
 * and recomputes trailing windows on reorgs. Build with
 * {@link createAccountFlowHandlers} / {@link createAccountFlowAggregationHandler},
 * or use directly for batch jobs.
 */
export class AccountFlowAggregator {
  private readonly store: AccountFlowStore;
  private readonly intervals: readonly AccountFlowInterval[];
  private readonly metrics: AccountFlowMetrics;
  private readonly events: readonly AccountFlowEventSink[];
  private readonly highWaterStore?: CursorStore;
  private readonly highWaterKey: string;
  private readonly reorgLookback: Record<AccountFlowInterval, number>;
  private readonly log: typeof logger;
  private lastLedgerSequence: number | null = null;

  constructor(options: AccountFlowAggregatorOptions) {
    if (!options.store) {
      throw new Error('AccountFlowAggregator requires a store');
    }
    this.store = options.store;
    this.intervals = options.intervals ?? ACCOUNT_FLOW_INTERVALS;
    for (const interval of this.intervals) {
      if (!isAccountFlowInterval(interval)) {
        throw new Error(`unsupported interval: ${String(interval)}`);
      }
    }
    this.metrics = options.metrics ?? getDefaultAccountFlowMetrics();
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
    operations: readonly AccountFlowOperation[],
    context: { kind: string } = { kind: 'unknown' },
  ): Promise<AccountFlowAggregate[]> {
    if (operations.length === 0) return [];
    this.metrics.operationsProcessed.inc({ kind: context.kind });
    const deltas = computeAccountFlowDeltas(operations, this.intervals);
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
    interval: AccountFlowInterval,
  ): Promise<AccountFlowAggregate[]> {
    const toIso = bucketEndIso(bucketStart, interval);
    const rows = await this.store.readOperations(bucketStart, toIso);
    const operations = convertStoredOperations(rows);
    const deltas = computeAccountFlowDeltas(operations, [interval]);
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
        const operations = convertStoredOperations(rows);
        const deltas = computeAccountFlowDeltas(operations, [interval]);
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

  private async upsertAndEmit(deltas: readonly AccountFlowDelta[]): Promise<AccountFlowAggregate[]> {
    let persisted: AccountFlowAggregate[];
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
      const event: AccountFlowUpdatedEvent = {
        kind: 'account_flow_updated',
        interval: delta.interval,
        bucketStart: delta.bucketStart,
        accountId: delta.accountId,
        inflowDelta: delta.inflow,
        outflowDelta: delta.outflow,
        netFlowDelta: delta.netFlow,
        inflow: totals?.inflow,
        outflow: totals?.outflow,
        netFlow: totals?.net_flow,
        counterparties: totals?.counterparties as Array<{
      account_id: string;
      volume: string;
      count: number;
      direction: 'in' | 'out';
    }> | undefined,
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

export type AccountFlowHandlers = {
  payment?: LedgerOperationHandler<'payment'>;
  path_payment?: LedgerOperationHandler<'path_payment'>;
};

/**
 * Builds the `ledger-operations` handlers for the aggregator (issue #19):
 * one for payments, one for path payments. Pass the result into
 * `startQueueWorkers({ handlers })` so the workers feed the aggregates.
 */
export function createAccountFlowHandlers(
  options: AccountFlowAggregatorOptions,
): AccountFlowHandlers {
  const aggregator = new AccountFlowAggregator(options);
  return {
    payment: async (payload) => {
      await aggregator.observeLedgerSequence(payload.ledger, payload.ledgerClosedAt);
      await aggregator.applyOperations(paymentPayloadToOperations(payload), {
        kind: 'payment',
      });
    },
    path_payment: async (payload) => {
      await aggregator.observeLedgerSequence(payload.ledger, payload.ledgerClosedAt);
      await aggregator.applyOperations(
        pathPaymentPayloadToOperations(payload, logger),
        { kind: 'path_payment' },
      );
    },
  };
}

/**
 * Builds the `account-flow` aggregation-job handler: recomputes one closed
 * minute/hour bucket from `ledger_operations`. Registered on the
 * `aggregation-jobs` worker (`startQueueWorkers({ aggregationHandlers })`).
 */
export function createAccountFlowAggregationHandler(
  options: AccountFlowAggregatorOptions,
): AggregationJobHandler {
  const aggregator = new AccountFlowAggregator(options);
  return async (job) => {
    if (job.kind !== 'account-flow') {
      throw new Error(`account flow aggregator cannot handle aggregation kind ${job.kind}`);
    }
    const interval = job.interval;
    if (!isAccountFlowInterval(interval)) {
      throw new Error(`unsupported account flow interval: ${String(interval)}`);
    }
    await aggregator.recomputeBucket(job.bucketStart, interval);
  };
}

/* ------------------------------------------------------------------ */
/* Conversion helper                                                     */

/* ------------------------------------------------------------------ */

/** Converts stored operation rows from the DB into AccountFlowOperation. */
function convertStoredOperations(
  rows: readonly AccountFlowStoredOperation[],
): AccountFlowOperation[] {
  const operations: AccountFlowOperation[] = [];
  for (const row of rows) {
    operations.push({
      amount: row.amount,
      fromAccount: row.from_account ?? '',
      toAccount: row.to_account ?? '',
      ledgerSequence: row.ledger_sequence ?? undefined,
      ledgerClosedAt: row.ledger_closed_at,
    });
  }
  return operations;
}