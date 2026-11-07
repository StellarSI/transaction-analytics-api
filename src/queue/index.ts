import type Redis from 'ioredis';
import { Redis as IORedis } from 'ioredis';
import { Queue, type ConnectionOptions, type JobsOptions } from 'bullmq';
import { z } from 'zod';
import { config } from '../config';
import type {
  ChangeTrustPayload,
  ClaimableBalancePayload,
  ClaimantInfo,
  CorridorMetadata,
  LiquidityPoolPayload,
  NormalizedAsset,
  OfferPayload,
  OperationQueue,
  PaymentPayload,
  PathPaymentPayload,
  SorobanEventPayload,
} from '../ingestor/types';
import { logger } from '../logger';

/**
 * BullMQ-backed queue layer for the ingestion pipeline (issue #15).
 *
 * Ingestors push parsed operations onto the `ledger-operations` queue; the
 * workers in `processors.ts` consume those and (for aggregation passes)
 * enqueue follow-up work onto `aggregation-jobs`. Keeping both queues here
 * means the streaming code never needs re-touching when workers grow — the
 * ingestors still only see the narrow `OperationQueue` port.
 *
 * Design notes:
 * - Redis Streams semantics come from BullMQ itself; a Redis connection is
 *   created from `REDIS_URL` (or injected) with `maxRetriesPerRequest: null`
 *   as BullMQ requires for blocking connections.
 * - Job payloads are validated with Zod at the boundary, so a malformed
 *   producer writes fail loudly instead of poisoning the aggregator.
 * - Retry policy: 3 attempts with exponential backoff (1s base). Failed jobs
 *   stay in the `ledger-operations` retention window until the dead letter
 *   queue from issue #98 takes over.
 */
export const LEDGER_OPERATIONS_QUEUE = 'ledger-operations';
export const AGGREGATION_JOBS_QUEUE = 'aggregation-jobs';

/** Attempts per job before BullMQ marks it failed (issue #15 acceptance). */
export const DEFAULT_JOB_ATTEMPTS = 3;
/** Base delay for the exponential retry backoff; the nth retry waits 2^n * this. */
export const DEFAULT_BACKOFF_BASE_MS = 1_000;
/** Workers process jobs concurrently with this default parallelism. */
export const DEFAULT_WORKER_CONCURRENCY = 4;
/** How often worker-side sweeps refresh queue depth / failure-rate gauges. */
export const QUEUE_METRICS_SWEEP_MS = 15_000;

/* ------------------------------------------------------------------ */
/* Job payload schemas                                                 */
/* ------------------------------------------------------------------ */

const NormalizedAssetSchema = z.object({
  type: z.string().min(1),
  code: z.string().min(1),
  issuer: z.string().min(1).optional(),
});

const OperationBaseSchema = z.object({
  id: z.string().min(1),
  cursor: z.string().min(1),
  ledger: z.number().int().nonnegative().optional(),
  ledgerClosedAt: z.string().optional(),
  txHash: z.string().optional(),
  sourceAccount: z.string().optional(),
});

const PaymentPayloadSchema = OperationBaseSchema.extend({
  kind: z.literal('payment'),
  from: z.string().min(1),
  to: z.string().min(1),
  asset: NormalizedAssetSchema,
  amount: z.string().min(1),
});

const AnchorInfoSchema = z.object({
  issuer: z.string().min(1),
  name: z.string().optional(),
  homeDomain: z.string().optional(),
});

const CorridorMetadataSchema = z.object({
  path: z.array(NormalizedAssetSchema),
  sourceAnchor: AnchorInfoSchema.optional(),
  destinationAnchor: AnchorInfoSchema.optional(),
  sameAnchor: z.boolean(),
  crossIssuer: z.boolean(),
});

const PathPaymentPayloadSchema = OperationBaseSchema.extend({
  kind: z.literal('path_payment'),
  variant: z.enum(['strict-send', 'strict-receive']),
  from: z.string().min(1),
  to: z.string().min(1),
  sendAsset: NormalizedAssetSchema,
  sendAmount: z.string().optional(),
  sendMax: z.string().optional(),
  destinationAsset: NormalizedAssetSchema,
  destinationAmount: z.string().optional(),
  destinationMin: z.string().optional(),
  corridor: CorridorMetadataSchema,
});

const OfferPayloadSchema = OperationBaseSchema.extend({
  kind: z.literal('offer'),
  variant: z.enum(['manage-buy', 'manage-sell', 'passive-sell']),
  sellingAsset: NormalizedAssetSchema,
  buyingAsset: NormalizedAssetSchema,
  amount: z.string().min(1),
  priceRatio: z.object({ n: z.number().int(), d: z.number().int().positive() }),
  priceDecimal: z.string().optional(),
  offerId: z.string().min(1),
  isDeletion: z.boolean(),
});

const LiquidityPoolPayloadSchema = OperationBaseSchema.extend({
  kind: z.literal('liquidity_pool'),
  variant: z.enum(['deposit', 'withdraw']),
  poolId: z.string().min(1),
  reserves: z.array(NormalizedAssetSchema),
  /** `${reserves[i]}*_max` for deposits / `*_min` for withdrawals. */
  reserveBounds: z.array(z.string()),
});

const ClaimantInfoSchema = z.object({
  destination: z.string().min(1),
  predicate: z.unknown().optional(),
});

const ClaimableBalancePayloadSchema = OperationBaseSchema.extend({
  kind: z.literal('claimable_balance'),
  variant: z.enum(['create', 'claim']),
  asset: NormalizedAssetSchema,
  amount: z.string().min(1),
  balanceId: z.string().min(1).optional(),
  claimants: z.array(ClaimantInfoSchema),
  claimant: z.string().optional(),
  sponsor: z.string().optional(),
});

const SorobanEventPayloadSchema = OperationBaseSchema.extend({
  kind: z.literal('soroban_event'),
  contractId: z.string().min(1),
  eventType: z.string().min(1),
  topics: z.array(z.string().min(1)),
  value: z.string().optional(),
});

const ChangeTrustPayloadSchema = OperationBaseSchema.extend({
  kind: z.literal('change_trust'),
  trustor: z.string().min(1),
  asset: NormalizedAssetSchema,
  limit: z.string().min(1),
  isDeletion: z.boolean(),
});

/** Union of every payload the ingestors can push (Zod-validated at the boundary). */
export const LedgerOperationJobSchema = z.discriminatedUnion('kind', [
  PaymentPayloadSchema,
  PathPaymentPayloadSchema,
  OfferPayloadSchema,
  LiquidityPoolPayloadSchema,
  ClaimableBalancePayloadSchema,
  SorobanEventPayloadSchema,
  ChangeTrustPayloadSchema,
]);

export type LedgerOperationJob = z.infer<typeof LedgerOperationJobSchema>;

/**
 * Follow-up work produced by the aggregration passes tracked in issues
 * #18/#19/#21 as an alternative to time-bucketed aggregate jobs.
 */
export const AGGREGATION_KINDS = ['asset-flow', 'account-flow', 'corridor', 'trustline'] as const;

export type AggregationJobKind = (typeof AGGREGATION_KINDS)[number];

/** One aggregation pass over a closed time bucket. */
export const AggregationJobSchema = z.object({
  kind: z.enum(AGGREGATION_KINDS),
  /** Left-closed bucket the aggregation covers, e.g. `2026-11-01T10:00:00Z`. */
  bucketStart: z.string().min(1),
  interval: z.enum(['1m', '1h']),
});

export type AggregationJob = z.infer<typeof AggregationJobSchema>;

/** Thrown when a job payload fails validation at the queue boundary. */
export class QueueValidationError extends Error {
  constructor(
    message: string,
    /** Zod issues (or an equivalent description) for structured logging. */
    readonly issues: unknown,
  ) {
    super(message);
    this.name = 'QueueValidationError';
  }
}

/* ------------------------------------------------------------------ */
/* Retry policy                                                        */
/* ------------------------------------------------------------------ */

/**
 * Shared retry policy: 3 attempts with exponential backoff so transient
 * Redis/Postgres hiccups self-heal, plus completion retention settings.
 * `removeOnFail: false` keeps failed jobs inspectable until the dead letter
 * queue lands (issue #98).
 */
export function defaultJobOptions(): JobsOptions {
  return {
    attempts: DEFAULT_JOB_ATTEMPTS,
    backoff: { type: 'exponential', delay: DEFAULT_BACKOFF_BASE_MS },
    removeOnComplete: { age: 3_600, count: 10_000 },
    removeOnFail: false,
  };
}

/* ------------------------------------------------------------------ */
/* Connections                                                         */
/* ------------------------------------------------------------------ */

/**
 * Creates a dedicated connection for BullMQ. BullMQ requires
 * `maxRetriesPerRequest: null` on blocking pipelines, so this connection is
 * intentionally separate from the shared `getRedis()` client (which caps
 * retries for regular commands).
 */
export function createQueueConnection(url: string = config.REDIS_URL): Redis {
  return new IORedis(url, { maxRetriesPerRequest: null, lazyConnect: false });
}

/** Anything BullMQ accepts as a connection (a URL-derived client or injected). */
export type QueueConnection = Redis | ConnectionOptions;

/**
 * The `ledger-operations` / `aggregation-jobs` pair plus typed producer
 * helpers. `close()` disconnects BullMQ's connections; call it on shutdown
 * after the workers have drained (see `processors.ts`).
 */
export interface QueueRegistry {
  readonly ledgerOperations: Queue<LedgerOperationJob>;
  readonly aggregationJobs: Queue<AggregationJob>;
  /** Validates and enqueues one parsed operation (weighted by kind). */
  enqueueLedgerOperation(job: LedgerOperationJob): Promise<void>;
  /** Enqueues one aggregation pass (validated). */
  enqueueAggregation(job: AggregationJob): Promise<void>;
  close(): Promise<void>;
}

export interface CreateQueueRegistryOptions {
  /** Injected for tests; defaults to a client built from `REDIS_URL`. */
  connection?: QueueConnection;
  /** URL used when no explicit connection is injected. */
  redisUrl?: string;
}

export function createQueueRegistry(
  options: CreateQueueRegistryOptions = {},
): QueueRegistry {
  const connection: QueueConnection =
    options.connection ?? createQueueConnection(options.redisUrl);

  const ledgerOperations = new Queue<LedgerOperationJob>(
    LEDGER_OPERATIONS_QUEUE,
    { connection, defaultJobOptions: defaultJobOptions() },
  );
  const aggregationJobs = new Queue<AggregationJob>(
    AGGREGATION_JOBS_QUEUE,
    { connection, defaultJobOptions: defaultJobOptions() },
  );

  return {
    ledgerOperations,
    aggregationJobs,
    async enqueueLedgerOperation(job) {
      const parsed = LedgerOperationJobSchema.safeParse(job);
      if (!parsed.success) {
        throw new QueueValidationError(
          `queue: invalid ledger operation payload (kind=${String((job as { kind?: string }).kind)})`,
          parsed.error.issues,
        );
      }
      await ledgerOperations.add(parsed.data.kind, parsed.data, {
        // Keep per-kind jobIds distinct: BullMQ dedupes on id, and different
        // operations must never collapse into one job.
        jobId: parsed.data.id,
      });
    },
    async enqueueAggregation(job) {
      const parsed = AggregationJobSchema.safeParse(job);
      if (!parsed.success) {
        throw new QueueValidationError(
          'queue: invalid aggregation job payload',
          parsed.error.issues,
        );
      }
      // Aggregation jobs must never dedupe or the retry of one bucket would
      // suppress a concurrent run; leave jobId unset.
      await aggregationJobs.add(parsed.data.kind, parsed.data);
    },
    async close() {
      await Promise.allSettled([ledgerOperations.close(), aggregationJobs.close()]);
    },
  };
}

/* ------------------------------------------------------------------ */
/* OperationQueue adapter                                              */
/* ------------------------------------------------------------------ */

/**
 * Adapts the `OperationQueue` port to BullMQ. All parsed operation kinds are
 * serialized into the single `ledger-operations` queue with their `kind` as
 * the job name, so the ingestor streaming code (issues #11–#14) stays
 * untouched: swapping the interim Redis-list producer for this adapter changes
 * only where the payload is handed to Redis, never who parses it.
 */
export function createBullMQOperationQueue(
  ledgerOperations: Queue<LedgerOperationJob>,
): OperationQueue {
  const push = async (payload: LedgerOperationJob): Promise<void> => {
    const parsed = LedgerOperationJobSchema.safeParse(payload);
    if (!parsed.success) {
      logger.error(
        { issues: parsed.error.issues, kind: payload.kind },
        'queue: dropping invalid operation payload',
      );
      throw new QueueValidationError(
        `queue: invalid operation payload (kind=${String(payload.kind)})`,
        parsed.error.issues,
      );
    }
    await ledgerOperations.add(parsed.data.kind, parsed.data, {
      jobId: parsed.data.id,
    });
  };

  return {
    pushPayment(payload: PaymentPayload) {
      return push(payload);
    },
    pushPathPayment(payload: PathPaymentPayload) {
      return push(payload);
    },
    pushOffer(payload: OfferPayload) {
      return push(payload);
    },
    pushLiquidityPool(payload: LiquidityPoolPayload) {
      return push(payload);
    },
    pushClaimableBalance(payload: ClaimableBalancePayload) {
      return push(payload);
    },
    pushSorobanEvent(payload: SorobanEventPayload) {
      return push(payload);
    },
    pushChangeTrust(payload: ChangeTrustPayload) {
      return push(payload);
    },
  };
}

/**
 * Convenience factory wiring the registry's ledger queue into the
 * `OperationQueue` port used by the ingestors.
 */
export function defaultOperationQueue(
  registry: Pick<QueueRegistry, 'ledgerOperations'>,
): OperationQueue {
  return createBullMQOperationQueue(registry.ledgerOperations);
}

/* Re-exported so downstream modules can type handlers without importing zod. */
export type {
  ChangeTrustPayload,
  ClaimableBalancePayload,
  ClaimantInfo,
  CorridorMetadata,
  LiquidityPoolPayload,
  NormalizedAsset,
  OfferPayload,
  PaymentPayload,
  PathPaymentPayload,
  SorobanEventPayload,
};
