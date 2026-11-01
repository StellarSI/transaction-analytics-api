import type {
  ClaimableBalancePayload,
  ClaimantInfo,
  HorizonOperationRecord,
  NormalizedAsset,
} from '../types';
import { claimableBalanceVariant, normalizeAsset } from '../types';
import { HorizonIngestor, type HorizonIngestorOptions } from './client';

/**
 * Horizon SSE client for claimable balance operations (issue #13).
 *
 * Claimable balances (SEP-11) power aid disbursement and escrow flows, so
 * both ends of the life cycle are ingested:
 * - `create_claimable_balance` (`type_i === 14`): who may claim (`claimants`
 *   with their predicates), the asset, the amount and the sponsor.
 * - `claim_claimable_balance` (`type_i === 15`): the `balance_id` being
 *   claimed and the account that claimed it.
 *
 * Correlation across the life cycle happens on `balance_id`: claims carry the
 * id of the balance they consume, while creates carry the full claimant list
 * whose destinations receive the funds. Payload variants are distinguished by
 * `variant`, so downstream aggregators can replay the full life cycle.
 *
 * Reconnection with cursor persistence is inherited from
 * {@link HorizonIngestor}; see `client.ts`.
 */

/**
 * Normalizes the claimants of a `create_claimable_balance` operation. The
 * array holds `{ destination, predicate }` objects over SSE; entries without
 * a destination are unusable and dropped, and an empty result means the
 * record cannot be aggregated (nothing would ever be claimable).
 */
export function parseClaimants(
  record: HorizonOperationRecord,
): ClaimantInfo[] | null {
  if (!Array.isArray(record.claimants)) return null;
  const claimants: ClaimantInfo[] = [];
  for (const entry of record.claimants) {
    if (!entry || typeof entry !== 'object') continue;
    const destination = (entry as { destination?: unknown }).destination;
    if (typeof destination !== 'string' || destination.length === 0) continue;
    const predicate = (entry as { predicate?: unknown }).predicate;
    claimants.push({
      destination,
      ...(predicate !== undefined ? { predicate } : {}),
    });
  }
  return claimants.length > 0 ? claimants : null;
}

/**
 * Parses a Horizon claimable balance operation into a
 * `ClaimableBalancePayload`, or returns null when the record is not a
 * claimable balance operation or lacks fields we cannot reconstruct later
 * (id, paging token, asset, amount, and a balance_id for claim operations —
 * a claim without its balance_id cannot be correlated with its create).
 */
export function parseClaimableBalance(
  record: HorizonOperationRecord,
): ClaimableBalancePayload | null {
  const variant = claimableBalanceVariant(record);
  if (!variant) return null;
  if (!record.id || !record.paging_token) return null;

  const asset: NormalizedAsset | null = normalizeAsset(record);
  if (!asset) return null;
  if (record.amount === undefined) return null;

  // Claim operations must carry the balance id they consume.
  const balanceId = record.balance_id;
  if (variant === 'claim' && !balanceId) return null;
  // Create operations must name at least one valid claimant.
  const claimants = variant === 'create' ? parseClaimants(record) : [];
  if (variant === 'create' && !claimants) return null;

  const claimant = variant === 'claim' ? record.claimant : undefined;

  return {
    kind: 'claimable_balance',
    variant,
    id: record.id,
    cursor: record.paging_token,
    ledger: typeof record.ledger === 'number' ? record.ledger : undefined,
    ledgerClosedAt: record.created_at,
    txHash: record.transaction_hash,
    sourceAccount: record.source_account,
    asset,
    amount: record.amount,
    ...(balanceId !== undefined ? { balanceId } : {}),
    claimants: claimants as ClaimantInfo[],
    ...(claimant !== undefined ? { claimant } : {}),
    ...(record.sponsor !== undefined ? { sponsor: record.sponsor } : {}),
  };
}

export type ClaimableBalanceIngestorOptions = Omit<
  HorizonIngestorOptions,
  'cursorKey'
> & {
  /** Cursor store key override; defaults to `horizon:cursor:claimable-balances`. */
  cursorKey?: string;
};

/**
 * Streams claimable balance operations from Horizon onto the claimable
 * balance queue.
 *
 * Construct with the shared `opener` / `queue` (and optionally a
 * `cursorStore`), then `await start()`. `stop()` closes the stream; `start()`
 * may be called again to resume.
 */
export class ClaimableBalanceIngestor extends HorizonIngestor {
  constructor(options: ClaimableBalanceIngestorOptions) {
    super({ cursorKey: 'horizon:cursor:claimable-balances', ...options });
  }

  protected override get name(): string {
    return 'claimable-balance-ingestor';
  }

  protected override async processRecord(
    record: HorizonOperationRecord,
  ): Promise<void> {
    const payload = parseClaimableBalance(record);
    if (!payload) return; // Not a claimable balance op (or unparsable).
    await this.options.queue.pushClaimableBalance(payload);
  }
}
