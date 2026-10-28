import { describe, it, expect, vi } from 'vitest';
import { PaymentIngestor, parsePayment } from '../payments';
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
  } as unknown as OperationQueue & {
    pushPayment: ReturnType<typeof vi.fn>;
    pushPathPayment: ReturnType<typeof vi.fn>;
  };
}

function paymentRecord(overrides: Partial<HorizonOperationRecord> = {}): HorizonOperationRecord {
  return {
    id: '00123456789-0000000001',
    paging_token: '123456789-1',
    type: 'payment',
    type_i: 1,
    source_account: 'GSENDER',
    from: 'GSENDER',
    to: 'GRECIPIENT',
    amount: '12.5000000',
    asset_type: 'credit_alphanum4',
    asset_code: 'USDC',
    asset_issuer: 'GISSUER',
    transaction_hash: 'deadbeef',
    created_at: '2026-01-01T00:00:00Z',
    ...overrides,
  };
}

/** Resolves on the next macrotask so queued microtasks (cursor persistence,
 * processing) can flush. */
const flush = () => new Promise((r) => setTimeout(r, 0));

/** Waits long enough for a 1ms backoff reconnect to have fired. */
const settle = () => new Promise((r) => setTimeout(r, 25));

describe('parsePayment', () => {
  it('extracts ledger, accounts, asset, amount and tx hash', () => {
    const payload = parsePayment(paymentRecord({ ledger: 123456 }));
    expect(payload).not.toBeNull();
    expect(payload?.kind).toBe('payment');
    expect(payload?.ledger).toBe(123456);
    expect(payload?.from).toBe('GSENDER');
    expect(payload?.to).toBe('GRECIPIENT');
    expect(payload?.asset).toEqual({
      type: 'credit_alphanum4',
      code: 'USDC',
      issuer: 'GISSUER',
    });
    expect(payload?.amount).toBe('12.5000000');
    expect(payload?.txHash).toBe('deadbeef');
    expect(payload?.cursor).toBe('123456789-1');
  });

  it('normalizes the native asset', () => {
    const payload = parsePayment(
      paymentRecord({ asset_type: 'native', asset_code: undefined, asset_issuer: undefined }),
    );
    expect(payload?.asset).toEqual({ type: 'native', code: 'XLM' });
  });

  it('accepts type_i delivered as a string over SSE', () => {
    const payload = parsePayment(paymentRecord({ type: undefined, type_i: '1' }));
    expect(payload?.kind).toBe('payment');
  });

  it('ignores non-payment operations', () => {
    expect(parsePayment(paymentRecord({ type: 'create_account', type_i: 0 }))).toBeNull();
  });

  it('ignores payments missing the recipient or amount', () => {
    expect(parsePayment(paymentRecord({ to: undefined }))).toBeNull();
    expect(parsePayment(paymentRecord({ amount: undefined }))).toBeNull();
  });

  it('ignores payments whose asset is incomplete', () => {
    expect(parsePayment(paymentRecord({ asset_issuer: undefined }))).toBeNull();
  });
});

describe('PaymentIngestor', () => {
  it('opens the stream at the cold-start cursor "now"', async () => {
    const { opener, opens } = fakeOpener();
    const ingestor = new PaymentIngestor({ opener, queue: fakeQueue() });
    await ingestor.start();
    expect(opens).toHaveLength(1);
    expect(opens[0].cursor).toBe('now');
    expect(ingestor.isRunning).toBe(true);
    ingestor.stop();
  });

  it('filters to payments and pushes a parsed payload to the queue', async () => {
    const { opener, opens } = fakeOpener();
    const queue = fakeQueue();
    const ingestor = new PaymentIngestor({ opener, queue });
    await ingestor.start();

    opens[0].handlers.onOperation(paymentRecord());
    opens[0].handlers.onOperation(
      paymentRecord({ id: 'x', paging_token: '2', type: 'create_account', type_i: 0 }),
    );
    await flush();

    expect(queue.pushPayment).toHaveBeenCalledTimes(1);
    expect(queue.pushPayment).toHaveBeenCalledWith(
      expect.objectContaining({ kind: 'payment', amount: '12.5000000' }),
    );
    ingestor.stop();
  });

  it('advances the cursor to the last seen paging token', async () => {
    const { opener, opens } = fakeOpener();
    const ingestor = new PaymentIngestor({ opener, queue: fakeQueue() });
    await ingestor.start();

    opens[0].handlers.onOperation(paymentRecord({ paging_token: '999-9' }));
    await flush();
    expect(ingestor.resumeCursor).toBe('999-9');
    ingestor.stop();
  });

  it('reconnects at the last cursor after a stream error', async () => {
    const { opener, opens } = fakeOpener();
    const ingestor = new PaymentIngestor({
      opener,
      queue: fakeQueue(),
      baseBackoffMs: 1, // keep the test fast
    });
    await ingestor.start();

    opens[0].handlers.onOperation(paymentRecord({ paging_token: '555-1' }));
    await flush();
    opens[0].handlers.onError(new Error('SSE dropped'));
    await settle();

    expect(opens.length).toBeGreaterThanOrEqual(2);
    expect(opens[1].cursor).toBe('555-1');
    ingestor.stop();
  });

  it('closes the previous stream when reconnecting', async () => {
    const { opener, opens, closers } = fakeOpener();
    const ingestor = new PaymentIngestor({ opener, queue: fakeQueue(), baseBackoffMs: 1 });
    await ingestor.start();
    opens[0].handlers.onError(new Error('drop'));
    await settle();
    expect(closers[0]).toHaveBeenCalled();
    ingestor.stop();
  });

  it('does not reconnect after stop()', async () => {
    const { opener, opens } = fakeOpener();
    const ingestor = new PaymentIngestor({ opener, queue: fakeQueue(), baseBackoffMs: 1 });
    await ingestor.start();
    ingestor.stop();
    opens[0].handlers.onError(new Error('late error'));
    await flush();
    expect(opens).toHaveLength(1);
    expect(ingestor.isRunning).toBe(false);
  });

  it('resumes from a persisted cursor on start', async () => {
    const { opener, opens } = fakeOpener();
    const store = new Map<string, string>([['horizon:cursor:payments', '777-3']]);
    const cursorStore = {
      get: async (k: string) => store.get(k) ?? null,
      set: async (k: string, v: string) => void store.set(k, v),
    };
    const ingestor = new PaymentIngestor({ opener, queue: fakeQueue(), cursorStore });
    await ingestor.start();
    expect(opens[0].cursor).toBe('777-3');
    ingestor.stop();
  });
});
