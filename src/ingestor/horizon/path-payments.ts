import type {
  AnchorInfo,
  CorridorMetadata,
  HorizonAssetFields,
  HorizonOperationRecord,
  NormalizedAsset,
  PathPaymentPayload,
  PathPaymentVariant,
} from '../types';
import {
  normalizeAsset,
  pathPaymentVariant,
} from '../types';
import { HorizonIngestor, type HorizonIngestorOptions } from './client';

/**
 * Horizon SSE client for path payment operations (issue #12).
 *
 * Path payments are the signal for cross-asset corridor analysis: a single
 * operation names a sender asset, a recipient asset and the intermediate
 * hops. This ingestor handles both flavours —
 * `path_payment_strict_send` (`type_i === 2`) and
 * `path_payment_strict_receive` (`type_i === 13`) — parses the full path, and
 * attaches corridor metadata including best-effort anchor identification on
 * both ends.
 *
 * Anchor labeling: SEP-1/SEP-6 aware resolution (fetching issuer home domains
 * over the network) is on the roadmap and deliberately out of scope here.
 * Instead issuers are matched against a local {@link AnchorRegistry} that can
 * be seeded with known anchors; unknown issuers still produce a valid payload
 * with `sourceAnchor`/`destinationAnchor` left undefined.
 */

/** Maps asset issuers to anchor descriptions; seeded by the caller. */
export class AnchorRegistry {
  private readonly anchors = new Map<string, AnchorInfo>();

  /** Registers (or replaces) the anchor for an issuer. */
  register(issuer: string, info: AnchorInfo | Omit<AnchorInfo, 'issuer'>): this {
    this.anchors.set(issuer, { issuer, ...info });
    return this;
  }

  /** Looks up an anchor by issuer; undefined when unknown. */
  resolve(issuer: string | undefined): AnchorInfo | undefined {
    return issuer ? this.anchors.get(issuer) : undefined;
  }

  /** Number of registered anchors. */
  get size(): number {
    return this.anchors.size;
  }
}

/** Shared registry; seed it at startup with the anchors you care about. */
export const anchorRegistry = new AnchorRegistry();

/**
 * Builds corridor metadata from the parsed send/destination assets and the
 * intermediate path. `sameAnchor` is true only when both ends resolve to the
 * same registered anchor; `crossIssuer` is true when the payment moved value
 * between two distinct issuers (the defining trait of a cross-asset corridor).
 */
export function describeCorridor(
  sendAsset: NormalizedAsset,
  destinationAsset: NormalizedAsset,
  path: NormalizedAsset[],
  registry: AnchorRegistry,
): CorridorMetadata {
  const issuers = new Set<string>();
  if (sendAsset.issuer) issuers.add(sendAsset.issuer);
  if (destinationAsset.issuer) issuers.add(destinationAsset.issuer);
  for (const hop of path) {
    if (hop.issuer) issuers.add(hop.issuer);
  }

  const sourceAnchor = registry.resolve(sendAsset.issuer);
  const destinationAnchor = registry.resolve(destinationAsset.issuer);
  const sameAnchor =
    !!sourceAnchor &&
    !!destinationAnchor &&
    sourceAnchor.issuer === destinationAnchor.issuer;

  return {
    path,
    sourceAnchor,
    destinationAnchor,
    sameAnchor,
    crossIssuer: issuers.size > 1,
  };
}

/**
 * Parses a Horizon path payment operation into a `PathPaymentPayload`, or
 * returns null when the record is not a path payment or is missing fields we
 * cannot reconstruct (id, token, endpoints or either asset).
 *
 * Horizon reports both flavours with slightly different field names:
 * - strict send exposes `send_asset`/`send_amount` and
 *   `destination_asset`/`destination_amount`
 * - strict receive exposes `source_asset`/`source_amount` and
 *   `destination_asset`/`destination_amount`
 * We read whichever pair is present, normalizing into a single shape.
 */
export function parsePathPayment(
  record: HorizonOperationRecord,
  registry: AnchorRegistry = anchorRegistry,
): PathPaymentPayload | null {
  const variant: PathPaymentVariant | null = pathPaymentVariant(record);
  if (!variant) return null;
  if (!record.id || !record.paging_token) return null;

  const from = record.from ?? record.source_account;
  const to = record.to ?? record.destination_account;
  if (!from || !to) return null;

  // Sender-side asset: strict send names it `send_asset`, strict receive
  // names it `source_asset`.
  const sendFields: HorizonAssetFields =
    variant === 'strict-send'
      ? {
          asset_type: record.send_asset_type,
          asset_code: record.send_asset_code,
          asset_issuer: record.send_asset_issuer,
        }
      : {
          asset_type: record.source_asset_type,
          asset_code: record.source_asset_code,
          asset_issuer: record.source_asset_issuer,
        };
  const sendAsset = normalizeAsset(sendFields);
  if (!sendAsset) return null;

  // Recipient-side asset is `destination_asset` for both flavours.
  const destinationAsset = normalizeAsset({
    asset_type: record.destination_asset_type,
    asset_code: record.destination_asset_code,
    asset_issuer: record.destination_asset_issuer,
  });
  if (!destinationAsset) return null;

  const path = (record.path ?? [])
    .map((hop) => normalizeAsset(hop))
    .filter((hop): hop is NormalizedAsset => hop !== null);

  const sendAmount =
    variant === 'strict-send' ? record.send_amount : record.source_amount;
  const sendMax = variant === 'strict-receive' ? record.send_max : undefined;
  const destinationAmount = record.destination_amount;
  const destinationMin =
    variant === 'strict-send' ? record.destination_min_amount : undefined;

  return {
    kind: 'path_payment',
    variant,
    id: record.id,
    cursor: record.paging_token,
    ledger: typeof record.ledger === 'number' ? record.ledger : undefined,
    ledgerClosedAt: record.created_at,
    txHash: record.transaction_hash,
    sourceAccount: record.source_account,
    from,
    to,
    sendAsset,
    sendAmount,
    sendMax,
    destinationAsset,
    destinationAmount,
    destinationMin,
    corridor: describeCorridor(sendAsset, destinationAsset, path, registry),
  };
}

export type PathPaymentIngestorOptions = Omit<
  HorizonIngestorOptions,
  'cursorKey'
> & {
  /** Cursor store key override; defaults to `horizon:cursor:path-payments`. */
  cursorKey?: string;
  /** Anchor registry to label corridor endpoints; defaults to the shared one. */
  registry?: AnchorRegistry;
};

/**
 * Streams path payments from Horizon onto the path-payment queue, enriching
 * each payload with corridor metadata.
 */
export class PathPaymentIngestor extends HorizonIngestor {
  private readonly registry: AnchorRegistry;

  constructor(options: PathPaymentIngestorOptions) {
    const { registry, ...rest } = options;
    super({ cursorKey: 'horizon:cursor:path-payments', ...rest });
    this.registry = registry ?? anchorRegistry;
  }

  protected override get name(): string {
    return 'path-payment-ingestor';
  }

  protected override async processRecord(
    record: HorizonOperationRecord,
  ): Promise<void> {
    const payload = parsePathPayment(record, this.registry);
    if (!payload) return; // Not a path payment (or unparsable) — skip.
    await this.options.queue.pushPathPayment(payload);
  }
}
