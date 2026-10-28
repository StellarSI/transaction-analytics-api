/**
 * Shared types for the Horizon ingestion pipeline (issues #11, #12).
 *
 * The ingestors depend on the small ports defined here (`OperationQueue`,
 * `CursorStore`) rather than on concrete infrastructure, so the BullMQ-based
 * queue from issue #15 or a Redis-backed cursor store can be dropped in
 * without touching the streaming code.
 */

/** Normalized Stellar asset, as it appears on a parsed payload. */
export interface NormalizedAsset {
  /** Horizon asset type: `native`, `credit_alphanum4`, `credit_alphanum12`. */
  type: string;
  /** Asset code; `XLM` for native. */
  code: string;
  /** Issuer account; absent for the native asset. */
  issuer?: string;
}

/** Serialized form of an asset as it appears inside a Horizon operation. */
export interface HorizonAssetFields {
  asset_type?: string;
  asset_code?: string;
  asset_issuer?: string;
}

/**
 * Structural view of a Horizon operation record as delivered over the SSE
 * stream. Only the fields the ingestors read are listed; records are cast to
 * this shape at the streaming boundary so parsers stay pure and testable
 * with plain objects.
 */
export interface HorizonOperationRecord {
  id?: string;
  paging_token?: string;
  transaction_hash?: string;
  source_account?: string;
  type?: string;
  type_i?: number | string;
  created_at?: string;
  ledger?: number;
  // payment
  from?: string;
  to?: string;
  amount?: string;
  asset_type?: string;
  asset_code?: string;
  asset_issuer?: string;
  // path payment
  send_asset_type?: string;
  send_asset_code?: string;
  send_asset_issuer?: string;
  send_amount?: string;
  send_max?: string;
  source_asset_type?: string;
  source_asset_code?: string;
  source_asset_issuer?: string;
  source_amount?: string;
  destination_asset_type?: string;
  destination_asset_code?: string;
  destination_asset_issuer?: string;
  destination_amount?: string;
  destination_account?: string;
  destination_min_amount?: string;
  path?: HorizonAssetFields[];
}

/**
 * Horizon `type_i` values used by the ingestors (mirrors js-stellar-sdk's
 * `OperationResponseTypeInt`). Note these do not follow the XDR enum order:
 * strict send is 2 and strict receive is 13. Ingestors match on the canonical
 * `type` string as well, so records are recognized even if only one of the
 * two fields is present.
 */
export const OPERATION_TYPE_PAYMENT = 1;
export const OPERATION_TYPE_PATH_PAYMENT_STRICT_SEND = 2;
export const OPERATION_TYPE_PATH_PAYMENT_STRICT_RECEIVE = 13;

/** Canonical Horizon `type` strings for the operations we ingest. */
export const OPERATION_NAME_PAYMENT = 'payment';
export const OPERATION_NAME_PATH_PAYMENT_STRICT_SEND = 'path_payment_strict_send';
export const OPERATION_NAME_PATH_PAYMENT_STRICT_RECEIVE = 'path_payment_strict_receive';

/** Queue names shared by the ingestors and the future queue consumer. */
export const PAYMENT_QUEUE = 'ingest:payments';
export const PATH_PAYMENT_QUEUE = 'ingest:path-payments';

/**
 * Destination for parsed operations. The Redis list implementation here is a
 * deliberate stand-in until the BullMQ-based queue lands (issue #15); the
 * ingestors only ever see this interface.
 */
export interface OperationQueue {
  pushPayment(payload: PaymentPayload): Promise<void>;
  pushPathPayment(payload: PathPaymentPayload): Promise<void>;
}

/** Fields common to both payload kinds. */
export interface OperationBase {
  /** Horizon operation id. */
  id: string;
  /** Paging token, used as the resume cursor for the SSE stream. */
  cursor: string;
  /** Ledger the operation was included in, when Horizon provides it. */
  ledger?: number;
  /** Ledger close time as reported by Horizon (`created_at`). */
  ledgerClosedAt?: string;
  /** Hash of the transaction that produced the operation. */
  txHash?: string;
  /** Account that submitted the transaction. */
  sourceAccount?: string;
}

/** Payload pushed for `type_i === 1` (payment) operations. */
export interface PaymentPayload extends OperationBase {
  kind: 'payment';
  from: string;
  to: string;
  asset: NormalizedAsset;
  amount: string;
}

/** Which flavour of path payment an operation represents. */
export type PathPaymentVariant = 'strict-send' | 'strict-receive';

/**
 * Corridor metadata derived from a path payment: the ordered asset hops plus
 * best-effort anchor identification on both ends (SEP-1/SEP-6 aware labeling
 * is tracked separately on the roadmap; here issuers are matched against the
 * local anchor registry).
 */
export interface CorridorMetadata {
  /** Ordered asset hops, sender side first (may be empty). */
  path: NormalizedAsset[];
  /** Anchor serving the sending side, when known. */
  sourceAnchor?: AnchorInfo;
  /** Anchor serving the receiving side, when known. */
  destinationAnchor?: AnchorInfo;
  /** True when both ends resolve to the same anchor. */
  sameAnchor: boolean;
  /** True when the payment crossed at least two distinct issuers. */
  crossIssuer: boolean;
}

/** Best-effort description of an anchor (issuer organization). */
export interface AnchorInfo {
  /** Issuer account of the anchored asset. */
  issuer: string;
  /** Human-readable anchor name, when registered. */
  name?: string;
  /** Home domain taken from the asset's issuer account, when known. */
  homeDomain?: string;
}

/** Payload pushed for `type_i === 2` / `13` (path payment) operations. */
export interface PathPaymentPayload extends OperationBase {
  kind: 'path_payment';
  variant: PathPaymentVariant;
  from: string;
  to: string;
  /** Asset (and amount) leaving the sender. */
  sendAsset: NormalizedAsset;
  sendAmount?: string;
  /** For strict receive: the sender's cap on the amount spent. */
  sendMax?: string;
  /** Asset (and amount) arriving at the recipient. */
  destinationAsset: NormalizedAsset;
  destinationAmount?: string;
  /** For strict send: the recipient's floor on the amount received. */
  destinationMin?: string;
  corridor: CorridorMetadata;
}

/**
 * Resumable cursor storage for the SSE streams. The ingestor only persists
 * the last *seen* paging token; any store satisfying this port works — an
 * in-memory one for tests, Redis in production.
 */
export interface CursorStore {
  get(key: string): Promise<string | null>;
  set(key: string, value: string): Promise<void>;
}

/** Process-local cursor store; survives reconnects within one process only. */
export function createMemoryCursorStore(
  initial: Record<string, string> = {},
): CursorStore {
  const store = new Map(Object.entries(initial));
  // Deliberately non-async methods that resolve promises inline — a memory
  // lookup has nothing to await, but callers expect the async interface.
  return {
    get: (key) => Promise.resolve(store.get(key) ?? null),
    set: (key, value) => {
      store.set(key, value);
      return Promise.resolve();
    },
  };
}

function normalizeTypeI(value: number | string | undefined): number | undefined {
  if (value === undefined) return undefined;
  const parsed = typeof value === 'number' ? value : Number(value);
  return Number.isFinite(parsed) ? parsed : undefined;
}

/**
 * True when a record is a plain payment (`type_i === 1` / `type === 'payment'`).
 * Accepts either representation because Horizon serializes `type_i` as a
 * string over SSE but some SDKs expose it as a number.
 */
export function isPaymentOperation(record: HorizonOperationRecord): boolean {
  return (
    record.type === OPERATION_NAME_PAYMENT ||
    normalizeTypeI(record.type_i) === OPERATION_TYPE_PAYMENT
  );
}

/**
 * True when a record is a path payment (strict send or strict receive).
 */
export function isPathPaymentOperation(record: HorizonOperationRecord): boolean {
  return (
    record.type === OPERATION_NAME_PATH_PAYMENT_STRICT_SEND ||
    record.type === OPERATION_NAME_PATH_PAYMENT_STRICT_RECEIVE ||
    normalizeTypeI(record.type_i) === OPERATION_TYPE_PATH_PAYMENT_STRICT_SEND ||
    normalizeTypeI(record.type_i) === OPERATION_TYPE_PATH_PAYMENT_STRICT_RECEIVE
  );
}

/** Which flavour of path payment a record represents, or null if it isn't one. */
export function pathPaymentVariant(
  record: HorizonOperationRecord,
): PathPaymentVariant | null {
  const typeI = normalizeTypeI(record.type_i);
  if (
    record.type === OPERATION_NAME_PATH_PAYMENT_STRICT_SEND ||
    typeI === OPERATION_TYPE_PATH_PAYMENT_STRICT_SEND
  ) {
    return 'strict-send';
  }
  if (
    record.type === OPERATION_NAME_PATH_PAYMENT_STRICT_RECEIVE ||
    typeI === OPERATION_TYPE_PATH_PAYMENT_STRICT_RECEIVE
  ) {
    return 'strict-receive';
  }
  return null;
}

/**
 * Normalizes a set of Horizon asset fields into a `NormalizedAsset`, or null
 * if the fields are too incomplete to identify an asset (e.g. a credit asset
 * missing its issuer).
 */
export function normalizeAsset(
  fields: HorizonAssetFields | undefined,
): NormalizedAsset | null {
  if (!fields || !fields.asset_type) return null;
  if (fields.asset_type === 'native') {
    return { type: 'native', code: 'XLM' };
  }
  if (!fields.asset_code || !fields.asset_issuer) return null;
  return {
    type: fields.asset_type,
    code: fields.asset_code,
    issuer: fields.asset_issuer,
  };
}

/** Stable string key for an asset, e.g. `XLM` or `USDC:GAYZ...`. */
export function assetKey(asset: NormalizedAsset): string {
  return asset.issuer ? `${asset.code}:${asset.issuer}` : asset.code;
}
