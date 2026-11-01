import type {
  HorizonAssetAmount,
  HorizonOperationRecord,
  LiquidityPoolPayload,
  NormalizedAsset,
  OfferPayload,
} from '../types';
import {
  liquidityPoolVariant,
  normalizeAsset,
  offerOperationVariant,
  parsePriceRatio,
} from '../types';
import { HorizonIngestor, type HorizonIngestorOptions } from './client';

/**
 * Horizon SSE client for offer / DEX operations (issue #14).
 *
 * Keeps the offer-side and liquidity pool operation types from the
 * operations stream and pushes structured payloads onto the offer queue:
 * - `manage_buy_offer` (`type_i === 3`) and `manage_sell_offer`
 *   (`type_i === 4`): create, update or delete a specific offer.
 * - `create_passive_sell_offer` (`type_i === 5`): create an offer that does
 *   not take and then make another offer if possible.
 * - `liquidity_pool_deposit` (`type_i === 22`) and
 *   `liquidity_pool_withdraw` (`type_i === 23`): AMM liquidity movement,
 *   tracked with the pool id and its (bounded) reserve assets since these
 *   are key DEX liquidity signals as well.
 *
 * Offer tracking: Horizon exposes the target offer as `offer_id` (`'0'` when
 * an operation creates a new offer) and the resulting amount; an amount of
 * zero marks a deletion, surfaced as `isDeletion` so aggregators can close
 * the offer's lifecycle. Prices are normalized into Horizon's integer ratio
 * (`price_n` / `price_d`) so comparisons do not depend on decimal rounding.
 *
 * Reconnection with cursor persistence is inherited from
 * {@link HorizonIngestor}; see `client.ts`.
 */

/** True when an offer amount marks the offer as deleted (zeroed out). */
export function isZeroAmount(amount: string): boolean {
  const value = Number(amount);
  return Number.isFinite(value) && value === 0;
}

/**
 * Parses a Horizon offer operation into an `OfferPayload`, or returns null
 * when the record is not an offer operation or lacks fields we cannot
 * reconstruct later (id, paging token, both assets, the amount or the price
 * ratio — an offer without a price is not an order).
 */
export function parseOffer(
  record: HorizonOperationRecord,
): OfferPayload | null {
  const variant = offerOperationVariant(record);
  if (!variant) return null;
  if (!record.id || !record.paging_token) return null;
  if (record.amount === undefined || record.offer_id == null) return null;

  const sellingAsset: NormalizedAsset | null = normalizeAsset({
    asset_type: record.selling_asset_type,
    asset_code: record.selling_asset_code,
    asset_issuer: record.selling_asset_issuer,
  });
  const buyingAsset: NormalizedAsset | null = normalizeAsset({
    asset_type: record.buying_asset_type,
    asset_code: record.buying_asset_code,
    asset_issuer: record.buying_asset_issuer,
  });
  if (!sellingAsset || !buyingAsset) return null;

  const priceRatio = parsePriceRatio(record);
  if (!priceRatio) return null;

  // Horizon stringifies offer ids over SSE ('0' marks a brand-new offer);
  // normalize to a stable string for correlation in the aggregator.
  const offerId = typeof record.offer_id === 'number'
    ? String(record.offer_id)
    : record.offer_id;

  return {
    kind: 'offer',
    variant,
    id: record.id,
    cursor: record.paging_token,
    ledger: typeof record.ledger === 'number' ? record.ledger : undefined,
    ledgerClosedAt: record.created_at,
    txHash: record.transaction_hash,
    sourceAccount: record.source_account,
    sellingAsset,
    buyingAsset,
    amount: record.amount,
    priceRatio,
    ...(record.price !== undefined ? { priceDecimal: record.price } : {}),
    offerId,
    isDeletion: isZeroAmount(record.amount),
  };
}

/**
 * Parses the asset/amount pairs of a liquidity pool op (`reserves_max` for
 * deposits, `reserves_min` for withdrawals). Pairs whose asset fields are too
 * incomplete are skipped; bounds are returned parallel to the reserves, so
 * both arrays shrink together.
 */
export function parseReserves(
  entries: HorizonAssetAmount[] | undefined,
): { reserves: NormalizedAsset[]; bounds: string[] } | null {
  if (!Array.isArray(entries)) return null;
  const reserves: NormalizedAsset[] = [];
  const bounds: string[] = [];
  for (const entry of entries) {
    if (!entry) continue;
    const asset = normalizeAsset(entry);
    if (!asset || entry.amount === undefined) continue;
    reserves.push(asset);
    bounds.push(entry.amount);
  }
  return reserves.length > 0 ? { reserves, bounds } : null;
}

/**
 * Parses a Horizon liquidity pool deposit/withdraw into a
 * `LiquidityPoolPayload`, or null when the record is not a liquidity pool
 * operation or lacks the pool id and at least one bounded reserve.
 */
export function parseLiquidityPool(
  record: HorizonOperationRecord,
): LiquidityPoolPayload | null {
  const variant = liquidityPoolVariant(record);
  if (!variant) return null;
  if (!record.id || !record.paging_token) return null;
  if (!record.liquidity_pool_id) return null;

  const parsedReserves = parseReserves(
    variant === 'deposit' ? record.reserves_max : record.reserves_min,
  );
  if (!parsedReserves) return null;

  return {
    kind: 'liquidity_pool',
    variant,
    id: record.id,
    cursor: record.paging_token,
    ledger: typeof record.ledger === 'number' ? record.ledger : undefined,
    ledgerClosedAt: record.created_at,
    txHash: record.transaction_hash,
    sourceAccount: record.source_account,
    poolId: record.liquidity_pool_id,
    reserves: parsedReserves.reserves,
    reserveBounds: parsedReserves.bounds,
  };
}

export type OfferIngestorOptions = Omit<HorizonIngestorOptions, 'cursorKey'> & {
  /** Cursor store key override; defaults to `horizon:cursor:offers`. */
  cursorKey?: string;
};

/**
 * Streams offer and liquidity pool operations from Horizon onto the offer
 * queue.
 *
 * Construct with the shared `opener` / `queue` (and optionally a
 * `cursorStore`), then `await start()`. `stop()` closes the stream; `start()`
 * may be called again to resume.
 */
export class OfferIngestor extends HorizonIngestor {
  constructor(options: OfferIngestorOptions) {
    super({ cursorKey: 'horizon:cursor:offers', ...options });
  }

  protected override get name(): string {
    return 'offer-ingestor';
  }

  protected override async processRecord(
    record: HorizonOperationRecord,
  ): Promise<void> {
    const offer = parseOffer(record);
    if (offer) {
      await this.options.queue.pushOffer(offer);
      return;
    }
    const pool = parseLiquidityPool(record);
    if (pool) {
      await this.options.queue.pushLiquidityPool(pool);
    }
  }
}
