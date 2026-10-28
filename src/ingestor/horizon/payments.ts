import type {
  HorizonOperationRecord,
  NormalizedAsset,
  PaymentPayload,
} from '../types';
import { assetKey, isPaymentOperation, normalizeAsset } from '../types';
import { HorizonIngestor, type HorizonIngestorOptions } from './client';

/**
 * Horizon SSE client for plain payments (issue #11).
 *
 * Connects to the operations stream, keeps `type_i === 1` (`payment`)
 * operations, extracts ledger / accounts / asset / amount / transaction hash,
 * and pushes the structured payload onto the payment queue. Path payments are
 * intentionally excluded here — they carry corridor metadata and are handled
 * by the dedicated `PathPaymentIngestor` (issue #12).
 *
 * Reconnection with cursor persistence is inherited from
 * {@link HorizonIngestor}; see `client.ts`.
 */

/**
 * Parses a Horizon payment operation into a `PaymentPayload`, or returns null
 * when the record is not a payment or lacks the fields we cannot reconstruct
 * later (id, paging token, sender, recipient or amount).
 */
export function parsePayment(
  record: HorizonOperationRecord,
): PaymentPayload | null {
  if (!isPaymentOperation(record)) return null;
  if (!record.id || !record.paging_token) return null;
  if (!record.from || !record.to || record.amount === undefined) return null;

  // A payment always names an asset; for the native asset Horizon only sets
  // `asset_type: 'native'`. Anything else is incomplete and skipped rather
  // than pushed in a shape the aggregator cannot consume.
  const asset: NormalizedAsset | null = normalizeAsset(record);
  if (!asset) return null;

  return {
    kind: 'payment',
    id: record.id,
    cursor: record.paging_token,
    ledger: typeof record.ledger === 'number' ? record.ledger : undefined,
    ledgerClosedAt: record.created_at,
    txHash: record.transaction_hash,
    sourceAccount: record.source_account,
    from: record.from,
    to: record.to,
    asset,
    amount: record.amount,
  };
}

/**
 * True when the asset is native XLM. Kept here (rather than in `types.ts`) so
 * the ingestion layer owns its own small conveniences.
 */
export function isNativeAsset(asset: NormalizedAsset): boolean {
  return asset.type === 'native' && assetKey(asset) === 'XLM';
}

export type PaymentIngestorOptions = Omit<
  HorizonIngestorOptions,
  'cursorKey'
> & {
  /** Cursor store key override; defaults to `horizon:cursor:payments`. */
  cursorKey?: string;
};

/**
 * Streams plain payments from Horizon onto the payment queue.
 *
 * Construct with the shared `opener` / `queue` (and optionally a
 * `cursorStore`), then `await start()`. `stop()` closes the stream; `start()`
 * may be called again to resume.
 */
export class PaymentIngestor extends HorizonIngestor {
  constructor(options: PaymentIngestorOptions) {
    super({ cursorKey: 'horizon:cursor:payments', ...options });
  }

  protected override get name(): string {
    return 'payment-ingestor';
  }

  protected override async processRecord(
    record: HorizonOperationRecord,
  ): Promise<void> {
    const payload = parsePayment(record);
    if (!payload) return; // Not a payment (or unparsable) — nothing to push.
    await this.options.queue.pushPayment(payload);
  }
}
