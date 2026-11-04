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
  // offer / DEX
  selling_asset_type?: string;
  selling_asset_code?: string;
  selling_asset_issuer?: string;
  buying_asset_type?: string;
  buying_asset_code?: string;
  buying_asset_issuer?: string;
  price?: string;
  price_n?: number | string;
  price_d?: number | string;
  offer_id?: string | number;
  liquidity_pool_id?: string;
  /** Upper-bounded asset amounts for `liquidity_pool_deposit`. */
  reserves_max?: HorizonAssetAmount[];
  /** Lower-bounded asset amounts for `liquidity_pool_withdraw`. */
  reserves_min?: HorizonAssetAmount[];
  // claimable balances
  balance_id?: string;
  /** Claimant objects of a `create_claimable_balance` operation. */
  claimants?: unknown[];
  /** Claimant account of a `claim_claimable_balance` operation. */
  claimant?: string;
  /** Sponsor account that covered the create operation's reserve. */
  sponsor?: string;
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

// Offer / DEX operations (issue #14). Note the Horizon enum order here is
// contiguous for the offers (3-5) but jumps for the liquidity pool ops.
export const OPERATION_TYPE_MANAGE_BUY_OFFER = 3;
export const OPERATION_TYPE_MANAGE_SELL_OFFER = 4;
export const OPERATION_TYPE_CREATE_PASSIVE_SELL_OFFER = 5;
export const OPERATION_TYPE_LIQUIDITY_POOL_DEPOSIT = 22;
export const OPERATION_TYPE_LIQUIDITY_POOL_WITHDRAW = 23;

export const OPERATION_NAME_MANAGE_BUY_OFFER = 'manage_buy_offer';
export const OPERATION_NAME_MANAGE_SELL_OFFER = 'manage_sell_offer';
export const OPERATION_NAME_CREATE_PASSIVE_SELL_OFFER = 'create_passive_sell_offer';
export const OPERATION_NAME_LIQUIDITY_POOL_DEPOSIT = 'liquidity_pool_deposit';
export const OPERATION_NAME_LIQUIDITY_POOL_WITHDRAW = 'liquidity_pool_withdraw';

// Claimable balance operations (issue #13; SEP-11 life cycle).
export const OPERATION_TYPE_CREATE_CLAIMABLE_BALANCE = 14;
export const OPERATION_TYPE_CLAIM_CLAIMABLE_BALANCE = 15;
export const OPERATION_NAME_CREATE_CLAIMABLE_BALANCE = 'create_claimable_balance';
export const OPERATION_NAME_CLAIM_CLAIMABLE_BALANCE = 'claim_claimable_balance';

/** Queue names shared by the ingestors and the future queue consumer. */
export const PAYMENT_QUEUE = 'ingest:payments';
export const PATH_PAYMENT_QUEUE = 'ingest:path-payments';
export const OFFER_QUEUE = 'ingest:offers';
export const CLAIMABLE_BALANCE_QUEUE = 'ingest:claimable-balances';
export const SOROBAN_EVENT_QUEUE = 'ingest:soroban-events';

/**
 * Destination for parsed operations. The Redis list implementation here is a
 * deliberate stand-in until the BullMQ-based queue lands (issue #15); the
 * ingestors only ever see this interface.
 */
export interface OperationQueue {
  pushPayment(payload: PaymentPayload): Promise<void>;
  pushPathPayment(payload: PathPaymentPayload): Promise<void>;
  pushOffer(payload: OfferPayload): Promise<void>;
  pushLiquidityPool(payload: LiquidityPoolPayload): Promise<void>;
  pushClaimableBalance(payload: ClaimableBalancePayload): Promise<void>;
  pushSorobanEvent(payload: SorobanEventPayload): Promise<void>;
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

/** Horizon asset serialized together with its bound amount (reserves lists). */
export interface HorizonAssetAmount extends HorizonAssetFields {
  amount?: string;
}

/** Which flavour of offer an operation represents. */
export type OfferOperationVariant = 'manage-buy' | 'manage-sell' | 'passive-sell';

/** Payload pushed for offer operations (`type_i` 3, 4, 5) — issue #14. */
export interface OfferPayload extends OperationBase {
  kind: 'offer';
  variant: OfferOperationVariant;
  sellingAsset: NormalizedAsset;
  buyingAsset: NormalizedAsset;
  /** Offer amount after this operation; `0.0000000` marks a deletion. */
  amount: string;
  /** Price as Horizon's integer ratio (n / d), required for every offer op. */
  priceRatio: { n: number; d: number };
  /** Decimal price as reported by Horizon, when present. */
  priceDecimal?: string;
  /** Horizon offer id (`'0'` when the operation creates a new offer). */
  offerId: string;
  /** True when this operation zeroes out the offer (amount is 0). */
  isDeletion: boolean;
}

/** Which direction of liquidity pool movement an operation represents. */
export type LiquidityPoolVariant = 'deposit' | 'withdraw';

/**
 * Payload pushed for liquidity pool operations (`type_i` 22, 23). Deposits
 * carry upper bounds (`reserves_max`), withdrawals lower bounds
 * (`reserves_min`); both live in `reserveBounds` parallel to `reserves`.
 */
export interface LiquidityPoolPayload extends OperationBase {
  kind: 'liquidity_pool';
  variant: LiquidityPoolVariant;
  /** Hex pool identifier as allocated by Horizon. */
  poolId: string;
  /** Reserve assets, in canonical (assetA, assetB) order. */
  reserves: NormalizedAsset[];
  /** Bound amounts parallel to `reserves` (max for deposits, min for withdrawals). */
  reserveBounds: string[];
}

/** One claimant entry of a claimable balance (SEP-11). */
export interface ClaimantInfo {
  /** Account allowed to claim the balance. */
  destination: string;
  /** Claim predicate as sent by Horizon, when present. */
  predicate?: unknown;
}

/** Which step of the claimable balance life cycle an operation represents. */
export type ClaimableBalanceVariant = 'create' | 'claim';

/** Payload pushed for claimable balance operations (`type_i` 14, 15). */
export interface ClaimableBalancePayload extends OperationBase {
  kind: 'claimable_balance';
  variant: ClaimableBalanceVariant;
  asset: NormalizedAsset;
  amount: string;
  /**
   * Balance id for correlation across the life cycle. Horizon reports it on
   * claim operations; on create operations it is derived downstream.
   */
  balanceId?: string;
  /** Destinations allowed to claim (create operations). */
  claimants: ClaimantInfo[];
  /** Account that executed the claim (claim operations). */
  claimant?: string;
  /** Sponsor covering the held balance (create operations), when known. */
  sponsor?: string;
}

/** Payload pushed for Soroban contract events ingested via Stellar RPC.
 * Events are keyed by contract ID and event type; topics and value are
 * base64-encoded SCVals as returned by the RPC `getEvents` endpoint.
 */
export interface SorobanEventPayload extends OperationBase {
  kind: 'soroban_event';
  /** The contract that emitted the event. */
  contractId: string;
  /** The event type string (e.g. "transfer", "initialize"). */
  eventType: string;
  /** Base64-encoded SCVal topics from the event. */
  topics: string[];
  /** Base64-encoded SCVal event data, when present. */
  value?: string;
}

/**
 * Resumable cursor storage for the SSE streams. The ingestor only persists
 * the last *seen* paging token; any store satisfying this port works — an
 * in-memory one for tests, Redis in production.
 */export interface CursorStore {
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

/**
 * Which offer-side operation a record represents (`type_i` 3, 4, 5), or null
 * if it isn't one. Liquidity pool deposits/withdrawals are DEX too but get
 * their own classifier ({@link liquidityPoolVariant}) since their payloads
 * name a pool and reserve bounds instead of an offer id.
 */
export function offerOperationVariant(
  record: HorizonOperationRecord,
): OfferOperationVariant | null {
  const typeI = normalizeTypeI(record.type_i);
  if (
    record.type === OPERATION_NAME_MANAGE_BUY_OFFER ||
    typeI === OPERATION_TYPE_MANAGE_BUY_OFFER
  ) {
    return 'manage-buy';
  }
  if (
    record.type === OPERATION_NAME_MANAGE_SELL_OFFER ||
    typeI === OPERATION_TYPE_MANAGE_SELL_OFFER
  ) {
    return 'manage-sell';
  }
  if (
    record.type === OPERATION_NAME_CREATE_PASSIVE_SELL_OFFER ||
    typeI === OPERATION_TYPE_CREATE_PASSIVE_SELL_OFFER
  ) {
    return 'passive-sell';
  }
  return null;
}

/** True when a record is an offer-side operation (manage/passive offers). */
export function isOfferOperation(record: HorizonOperationRecord): boolean {
  return offerOperationVariant(record) !== null;
}

/**
 * Which liquidity pool movement a record represents (`type_i` 22, 23), or
 * null if it isn't one.
 */
export function liquidityPoolVariant(
  record: HorizonOperationRecord,
): LiquidityPoolVariant | null {
  const typeI = normalizeTypeI(record.type_i);
  if (
    record.type === OPERATION_NAME_LIQUIDITY_POOL_DEPOSIT ||
    typeI === OPERATION_TYPE_LIQUIDITY_POOL_DEPOSIT
  ) {
    return 'deposit';
  }
  if (
    record.type === OPERATION_NAME_LIQUIDITY_POOL_WITHDRAW ||
    typeI === OPERATION_TYPE_LIQUIDITY_POOL_WITHDRAW
  ) {
    return 'withdraw';
  }
  return null;
}

/** True when a record is a liquidity pool deposit or withdrawal. */
export function isLiquidityPoolOperation(record: HorizonOperationRecord): boolean {
  return liquidityPoolVariant(record) !== null;
}

/**
 * Parses Horizon's integer price ratio (`price_n` / `price_d`). Both fields
 * arrive as numbers over REST but may be stringified over SSE, so both
 * representations are accepted. Returns null when the ratio is missing or
 * unusable (zero denominator).
 */
export function parsePriceRatio(
  record: HorizonOperationRecord,
): { n: number; d: number } | null {
  const n = normalizeTypeI(record.price_n);
  const d = normalizeTypeI(record.price_d);
  if (
    n === undefined ||
    d === undefined ||
    d === 0 ||
    !Number.isInteger(n) ||
    !Number.isInteger(d)
  ) {
    return null;
  }
  return { n, d };
}

/**
 * Which step of the claimable balance life cycle a record represents
 * (`type_i` 14 create, 15 claim), or null if it isn't one.
 */
export function claimableBalanceVariant(
  record: HorizonOperationRecord,
): ClaimableBalanceVariant | null {
  const typeI = normalizeTypeI(record.type_i);
  if (
    record.type === OPERATION_NAME_CREATE_CLAIMABLE_BALANCE ||
    typeI === OPERATION_TYPE_CREATE_CLAIMABLE_BALANCE
  ) {
    return 'create';
  }
  if (
    record.type === OPERATION_NAME_CLAIM_CLAIMABLE_BALANCE ||
    typeI === OPERATION_TYPE_CLAIM_CLAIMABLE_BALANCE
  ) {
    return 'claim';
  }
  return null;
}

/** True when a record is a claimable balance create or claim operation. */
export function isClaimableBalanceOperation(record: HorizonOperationRecord): boolean {
  return claimableBalanceVariant(record) !== null;
}
