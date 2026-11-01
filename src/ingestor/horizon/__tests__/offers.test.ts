import { describe, it, expect, vi } from 'vitest';
import {
  OfferIngestor,
  isZeroAmount,
  parseLiquidityPool,
  parseOffer,
  parseReserves,
} from '../offers';
import type { OperationStreamOpener, StreamCloser, StreamHandlers } from '../stream';
import type { HorizonOperationRecord, OperationQueue } from '../../types';

/** A controllable fake SSE stream: records the cursor it was opened at and
 * lets the test push operations / errors. */
function fakeOpener() {
  const opens: Array<{ cursor: string; handlers: StreamHandlers }> = [];
  const closers: StreamCloser[] = [];
  const opener: OperationStreamOpener = ({ cursor, handlers }) => {
    opens.push({ cursor, handlers });
    const closer: StreamCloser = vi.fn();
    closers.push(closer);
    return closer;
  };
  return { opener, opens, closers };
}

function fakeQueue() {
  return {
    pushPayment: vi.fn().mockResolvedValue(undefined),
    pushPathPayment: vi.fn().mockResolvedValue(undefined),
    pushOffer: vi.fn().mockResolvedValue(undefined),
    pushLiquidityPool: vi.fn().mockResolvedValue(undefined),
    pushClaimableBalance: vi.fn().mockResolvedValue(undefined),
  } as unknown as OperationQueue & {
    pushOffer: ReturnType<typeof vi.fn>;
    pushLiquidityPool: ReturnType<typeof vi.fn>;
  };
}

function offerRecord(
  overrides: Partial<HorizonOperationRecord> = {},
): HorizonOperationRecord {
  return {
    id: '00123456789-0000000004',
    paging_token: '123456789-4',
    type: 'manage_sell_offer',
    type_i: 4,
    source_account: 'GTRADER',
    selling_asset_type: 'native',
    buying_asset_type: 'credit_alphanum4',
    buying_asset_code: 'USDC',
    buying_asset_issuer: 'GISSUER',
    amount: '10.0000000',
    price: '2.5000000',
    price_n: 5,
    price_d: 2,
    offer_id: '7',
    transaction_hash: 'deadbeef',
    created_at: '2026-01-01T00:00:00Z',
    ...overrides,
  };
}

function lpDepositRecord(
  overrides: Partial<HorizonOperationRecord> = {},
): HorizonOperationRecord {
  return {
    id: '00123456789-0000000022',
    paging_token: '123456789-22',
    type: 'liquidity_pool_deposit',
    type_i: 22,
    source_account: 'GLP',
    liquidity_pool_id: 'abcdef01',
    reserves_max: [
      {
        asset_type: 'native',
        amount: '5000.0000000',
      },
      {
        asset_type: 'credit_alphanum4',
        asset_code: 'USDC',
        asset_issuer: 'GISSUER',
        amount: '1000.0000000',
      },
    ],
    transaction_hash: 'deadbeef',
    created_at: '2026-01-01T00:00:00Z',
    ...overrides,
  };
}

/** Resolves on the next macrotask so queued microtasks (cursor persistence,
 * processing) can flush. */
const flush = () => new Promise((r) => setTimeout(r, 0));

describe('isZeroAmount', () => {
  it('detects deletions encoded as zero amounts', () => {
    expect(isZeroAmount('0.0000000')).toBe(true);
    expect(isZeroAmount('0')).toBe(true);
    expect(isZeroAmount('9.0000000')).toBe(false);
    expect(isZeroAmount('garbage')).toBe(false);
  });
});

describe('parseReserves', () => {
  it('keeps reserves and bounds parallel, dropping unusable entries', () => {
    const parsed = parseReserves([
      { asset_type: 'native', amount: '5.0000000' },
      null,
      { asset_type: 'credit_alphanum4', asset_code: 'USDC', amount: '1.0000000' },
      { asset_type: 'credit_alphanum4', asset_code: 'ORPHAN', asset_issuer: 'GI' },
    ] as NonNullable<HorizonOperationRecord['reserves_max']>);
    expect(parsed).toEqual({
      reserves: [
        { type: 'native', code: 'XLM' },
        { type: 'credit_alphanum4', code: 'USDC' },
      ],
      bounds: ['5.0000000', '1.0000000'],
    });
  });

  it('returns null for missing or fully unusable reserve lists', () => {
    expect(parseReserves(undefined)).toBeNull();
    expect(parseReserves([])).toBeNull();
    expect(
      parseReserves([null, { asset_type: 'native' }] as HorizonOperationRecord['reserves_max']),
    ).toBeNull();
  });
});

describe('parseOffer', () => {
  it('extracts assets, ratio price, amount and offer id', () => {
    const payload = parseOffer(offerRecord({ ledger: 123456 }));
    expect(payload).not.toBeNull();
    expect(payload?.kind).toBe('offer');
    expect(payload?.variant).toBe('manage-sell');
    expect(payload?.sellingAsset).toEqual({ type: 'native', code: 'XLM' });
    expect(payload?.buyingAsset).toEqual({
      type: 'credit_alphanum4',
      code: 'USDC',
      issuer: 'GISSUER',
    });
    expect(payload?.amount).toBe('10.0000000');
    expect(payload?.priceRatio).toEqual({ n: 5, d: 2 });
    expect(payload?.priceDecimal).toBe('2.5000000');
    expect(payload?.offerId).toBe('7');
    expect(payload?.isDeletion).toBe(false);
    expect(payload?.txHash).toBe('deadbeef');
    expect(payload?.cursor).toBe('123456789-4');
  });

  it('handles all offer operation types and stringified type_i / offer_id', () => {
    expect(
      parseOffer(offerRecord({ type: 'manage_buy_offer', type_i: '3', offer_id: '12' }))
        ?.variant,
    ).toBe('manage-buy');
    expect(
      parseOffer(
        offerRecord({ type: 'create_passive_sell_offer', type_i: '5', offer_id: '0' }),
      )?.variant,
    ).toBe('passive-sell');
    expect(parseOffer(offerRecord({ type_i: '4' }))?.variant).toBe('manage-sell');
    expect(parseOffer(offerRecord({ offer_id: 99 }))?.offerId).toBe('99');
  });

  it('marks zero amounts as deletions', () => {
    const payload = parseOffer(offerRecord({ amount: '0.0000000' }));
    expect(payload?.isDeletion).toBe(true);
    expect(payload?.amount).toBe('0.0000000');
  });

  it('rejects offers with an unusable price ratio', () => {
    expect(parseOffer(offerRecord({ price_n: undefined }))).toBeNull();
    expect(parseOffer(offerRecord({ price_d: 0 }))).toBeNull();
    expect(parseOffer(offerRecord({ price_n: 1.5, price_d: 2 }))).toBeNull();
  });

  it('rejects offers missing the amount, an asset or the offer id', () => {
    expect(parseOffer(offerRecord({ amount: undefined }))).toBeNull();
    expect(parseOffer(offerRecord({ buying_asset_issuer: undefined }))).toBeNull();
    expect(parseOffer(offerRecord({ offer_id: undefined }))).toBeNull();
  });

  it('rejects non-offer operations', () => {
    expect(parseOffer(offerRecord({ type: 'payment', type_i: 1 }))).toBeNull();
    expect(parseOffer(lpDepositRecord())).toBeNull();
  });
});

describe('parseLiquidityPool', () => {
  it('parses deposits with pool id and bounded reserves', () => {
    const payload = parseLiquidityPool(lpDepositRecord({ ledger: 123456 }));
    expect(payload).not.toBeNull();
    expect(payload?.kind).toBe('liquidity_pool');
    expect(payload?.variant).toBe('deposit');
    expect(payload?.poolId).toBe('abcdef01');
    expect(payload?.reserves).toEqual([
      { type: 'native', code: 'XLM' },
      { type: 'credit_alphanum4', code: 'USDC', issuer: 'GISSUER' },
    ]);
    expect(payload?.reserveBounds).toEqual(['5000.0000000', '1000.0000000']);
  });

  it('reads withdrawals from reserves_min instead', () => {
    const payload = parseLiquidityPool(
      lpDepositRecord({ type: 'liquidity_pool_withdraw', type_i: 23, reserves_max: undefined }),
    );
    expect(payload?.variant).toBe('withdraw');
    expect(payload?.reserves).toHaveLength(2);
  });

  it('rejects pools missing the id or reserve bounds', () => {
    expect(parseLiquidityPool(lpDepositRecord({ liquidity_pool_id: undefined }))).toBeNull();
    expect(
      parseLiquidityPool(lpDepositRecord({ type: 'liquidity_pool_withdraw', type_i: 23 })),
    ).toBeNull();
  });
});

describe('OfferIngestor', () => {
  it('opens the stream at the cold-start cursor "now"', async () => {
    const { opener, opens } = fakeOpener();
    const ingestor = new OfferIngestor({ opener, queue: fakeQueue() });
    await ingestor.start();
    expect(opens).toHaveLength(1);
    expect(opens[0].cursor).toBe('now');
    expect(ingestor.isRunning).toBe(true);
    ingestor.stop();
  });

  it('routes offers and liquidity pools to their queue ports', async () => {
    const { opener, opens } = fakeOpener();
    const queue = fakeQueue();
    const ingestor = new OfferIngestor({ opener, queue });
    await ingestor.start();

    opens[0].handlers.onOperation(offerRecord());
    opens[0].handlers.onOperation(lpDepositRecord());
    opens[0].handlers.onOperation(
      offerRecord({ id: 'x', paging_token: '2', type: 'payment', type_i: 1 }),
    );
    await flush();

    expect(queue.pushOffer).toHaveBeenCalledTimes(1);
    expect(queue.pushOffer).toHaveBeenCalledWith(
      expect.objectContaining({ kind: 'offer', offerId: '7' }),
    );
    expect(queue.pushLiquidityPool).toHaveBeenCalledTimes(1);
    expect(queue.pushLiquidityPool).toHaveBeenCalledWith(
      expect.objectContaining({ kind: 'liquidity_pool', poolId: 'abcdef01' }),
    );
    ingestor.stop();
  });

  it('advances the cursor to the last seen paging token', async () => {
    const { opener, opens } = fakeOpener();
    const ingestor = new OfferIngestor({ opener, queue: fakeQueue() });
    await ingestor.start();

    opens[0].handlers.onOperation(offerRecord({ paging_token: '999-4' }));
    await flush();
    expect(ingestor.resumeCursor).toBe('999-4');
    ingestor.stop();
  });

  it('resumes from a persisted cursor on start', async () => {
    const { opener, opens } = fakeOpener();
    const store = new Map<string, string>([['horizon:cursor:offers', '777-5']]);
    const cursorStore = {
      get: (key: string) => Promise.resolve(store.get(key) ?? null),
      set: (key: string, value: string) => {
        store.set(key, value);
        return Promise.resolve();
      },
    };
    const ingestor = new OfferIngestor({ opener, queue: fakeQueue(), cursorStore });
    await ingestor.start();
    expect(opens[0].cursor).toBe('777-5');
    ingestor.stop();
  });
});
