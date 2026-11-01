import { describe, it, expect, vi } from 'vitest';
import {
  ClaimableBalanceIngestor,
  parseClaimableBalance,
  parseClaimants,
} from '../claimable-balances';
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
    pushClaimableBalance: ReturnType<typeof vi.fn>;
  };
}

function createRecord(
  overrides: Partial<HorizonOperationRecord> = {},
): HorizonOperationRecord {
  return {
    id: '00123456789-0000000014',
    paging_token: '123456789-14',
    type: 'create_claimable_balance',
    type_i: 14,
    source_account: 'GSPONSOR',
    amount: '500.0000000',
    asset_type: 'credit_alphanum4',
    asset_code: 'USDC',
    asset_issuer: 'GISSUER',
    claimants: [
      { destination: 'GBENEFICIARY', predicate: { unconditional: true } },
      { destination: 'GFALLBACK', predicate: { not: { unconditional: true } } },
    ],
    transaction_hash: 'deadbeef',
    created_at: '2026-01-01T00:00:00Z',
    ...overrides,
  };
}

function claimRecord(
  overrides: Partial<HorizonOperationRecord> = {},
): HorizonOperationRecord {
  return {
    id: '00123456789-0000000015',
    paging_token: '123456789-15',
    type: 'claim_claimable_balance',
    type_i: 15,
    source_account: 'GBENEFICIARY',
    balance_id: '0000000099112233445566778899aabbccddeeff00112233445566778899aabb',
    claimant: 'GBENEFICIARY',
    amount: '500.0000000',
    asset_type: 'credit_alphanum4',
    asset_code: 'USDC',
    asset_issuer: 'GISSUER',
    transaction_hash: 'deadbeef',
    created_at: '2026-01-02T00:00:00Z',
    ...overrides,
  };
}

/** Resolves on the next macrotask so queued microtasks (cursor persistence,
 * processing) can flush. */
const flush = () => new Promise((r) => setTimeout(r, 0));

describe('parseClaimants', () => {
  it('keeps destination and predicate for every valid claimant', () => {
    const claimants = parseClaimants(createRecord());
    expect(claimants).toEqual([
      { destination: 'GBENEFICIARY', predicate: { unconditional: true } },
      { destination: 'GFALLBACK', predicate: { not: { unconditional: true } } },
    ]);
  });

  it('drops entries without a usable destination', () => {
    const claimants = parseClaimants(
      createRecord({
        claimants: [
          { destination: 'GKEEP', predicate: { absBefore: '2026-01-01T00:00:00Z' } },
          {},
          null,
          { destination: 42 },
        ],
      }),
    );
    expect(claimants).toEqual([
      { destination: 'GKEEP', predicate: { absBefore: '2026-01-01T00:00:00Z' } },
    ]);
  });

  it('returns null for a missing or empty claimants array', () => {
    expect(parseClaimants(createRecord({ claimants: undefined }))).toBeNull();
    expect(parseClaimants(createRecord({ claimants: [] }))).toBeNull();
    expect(parseClaimants(createRecord({ claimants: [null, 1] }))).toBeNull();
  });
});

describe('parseClaimableBalance (create)', () => {
  it('extracts asset, amount, claimants and sponsor', () => {
    const payload = parseClaimableBalance(
      createRecord({ ledger: 123456, sponsor: 'GSPONSOR' }),
    );
    expect(payload).not.toBeNull();
    expect(payload?.kind).toBe('claimable_balance');
    expect(payload?.variant).toBe('create');
    expect(payload?.asset).toEqual({
      type: 'credit_alphanum4',
      code: 'USDC',
      issuer: 'GISSUER',
    });
    expect(payload?.amount).toBe('500.0000000');
    expect(payload?.claimants?.[0]?.destination).toBe('GBENEFICIARY');
    expect(payload?.sponsor).toBe('GSPONSOR');
    expect(payload?.txHash).toBe('deadbeef');
    expect(payload?.cursor).toBe('123456789-14');
  });

  it('normalizes the native asset', () => {
    const payload = parseClaimableBalance(
      createRecord({ asset_type: 'native', asset_code: undefined, asset_issuer: undefined }),
    );
    expect(payload?.asset).toEqual({ type: 'native', code: 'XLM' });
  });

  it('accepts type_i delivered as a string over SSE', () => {
    const payload = parseClaimableBalance(createRecord({ type: undefined, type_i: '14' }));
    expect(payload?.variant).toBe('create');
  });

  it('rejects creates whose claimants array is unusable', () => {
    expect(
      parseClaimableBalance(createRecord({ claimants: undefined })),
    ).toBeNull();
    expect(parseClaimableBalance(createRecord({ claimants: [] }))).toBeNull();
  });

  it('rejects creates with an incomplete asset or missing amount', () => {
    expect(parseClaimableBalance(createRecord({ asset_issuer: undefined }))).toBeNull();
    expect(parseClaimableBalance(createRecord({ amount: undefined }))).toBeNull();
  });
});

describe('parseClaimableBalance (claim)', () => {
  it('correlates on balance_id and records the claiming account', () => {
    const payload = parseClaimableBalance(claimRecord());
    expect(payload?.variant).toBe('claim');
    expect(payload?.balanceId).toBe(
      '0000000099112233445566778899aabbccddeeff00112233445566778899aabb',
    );
    expect(payload?.claimant).toBe('GBENEFICIARY');
    expect(payload?.claimants).toEqual([]);
  });

  it('rejects claims missing the balance id (cannot be correlated)', () => {
    expect(parseClaimableBalance(claimRecord({ balance_id: undefined }))).toBeNull();
  });

  it('rejects non-claimable-balance operations', () => {
    expect(parseClaimableBalance(createRecord({ type: 'payment', type_i: 1 }))).toBeNull();
  });

  it('rejects records missing the id or paging token', () => {
    expect(parseClaimableBalance(claimRecord({ id: undefined }))).toBeNull();
    expect(parseClaimableBalance(claimRecord({ paging_token: undefined }))).toBeNull();
  });
});

describe('ClaimableBalanceIngestor', () => {
  it('opens the stream at the cold-start cursor "now"', async () => {
    const { opener, opens } = fakeOpener();
    const ingestor = new ClaimableBalanceIngestor({ opener, queue: fakeQueue() });
    await ingestor.start();
    expect(opens).toHaveLength(1);
    expect(opens[0].cursor).toBe('now');
    expect(ingestor.isRunning).toBe(true);
    ingestor.stop();
  });

  it('filters to claimable balance ops and pushes parsed payloads', async () => {
    const { opener, opens } = fakeOpener();
    const queue = fakeQueue();
    const ingestor = new ClaimableBalanceIngestor({ opener, queue });
    await ingestor.start();

    opens[0].handlers.onOperation(createRecord());
    opens[0].handlers.onOperation(claimRecord());
    opens[0].handlers.onOperation(
      createRecord({ id: 'x', paging_token: '2', type: 'payment', type_i: 1 }),
    );
    await flush();

    expect(queue.pushClaimableBalance).toHaveBeenCalledTimes(2);
    expect(queue.pushClaimableBalance).toHaveBeenCalledWith(
      expect.objectContaining({ kind: 'claimable_balance', variant: 'create' }),
    );
    ingestor.stop();
  });

  it('advances the cursor to the last seen paging token', async () => {
    const { opener, opens } = fakeOpener();
    const ingestor = new ClaimableBalanceIngestor({ opener, queue: fakeQueue() });
    await ingestor.start();

    opens[0].handlers.onOperation(createRecord({ paging_token: '999-14' }));
    await flush();
    expect(ingestor.resumeCursor).toBe('999-14');
    ingestor.stop();
  });

  it('resumes from a persisted cursor on start', async () => {
    const { opener, opens } = fakeOpener();
    const store = new Map<string, string>([
      ['horizon:cursor:claimable-balances', '777-15'],
    ]);
    const cursorStore = {
      get: (key: string) => Promise.resolve(store.get(key) ?? null),
      set: (key: string, value: string) => {
        store.set(key, value);
        return Promise.resolve();
      },
    };
    const ingestor = new ClaimableBalanceIngestor({
      opener,
      queue: fakeQueue(),
      cursorStore,
    });
    await ingestor.start();
    expect(opens[0].cursor).toBe('777-15');
    ingestor.stop();
  });
});
