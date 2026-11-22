import { describe, expect, it, vi } from 'vitest';
import { ClaimableBalanceIngestor } from '../claimable-balances';
import { OfferIngestor } from '../offers';
import { PathPaymentIngestor } from '../path-payments';
import { PaymentIngestor } from '../payments';
import type { HorizonOperationRecord, OperationQueue } from '../../types';
import type { OperationStreamOpener, StreamCloser, StreamHandlers } from '../stream';

function fakeOpener() {
  const opens: Array<{ cursor: string; handlers: StreamHandlers }> = [];
  const opener: OperationStreamOpener = ({ cursor, handlers }) => {
    opens.push({ cursor, handlers });
    const closer: StreamCloser = vi.fn();
    return closer;
  };
  return { opener, opens };
}

function fakeQueue() {
  return {
    pushPayment: vi.fn().mockResolvedValue(undefined),
    pushPathPayment: vi.fn().mockResolvedValue(undefined),
    pushOffer: vi.fn().mockResolvedValue(undefined),
    pushLiquidityPool: vi.fn().mockResolvedValue(undefined),
    pushClaimableBalance: vi.fn().mockResolvedValue(undefined),
  } as unknown as OperationQueue & Record<string, ReturnType<typeof vi.fn>>;
}

const flush = () => new Promise((r) => setTimeout(r, 0));

describe('Horizon SSE malformed data handling', () => {
  it('payment ingestor ignores malformed records', async () => {
    const { opener, opens } = fakeOpener();
    const queue = fakeQueue();
    const ingestor = new PaymentIngestor({ opener, queue, baseBackoffMs: 1 });
    await ingestor.start();

    opens[0].handlers.onOperation({ type: 'payment', type_i: 1 } as unknown as HorizonOperationRecord);
    opens[0].handlers.onOperation({} as unknown as HorizonOperationRecord);
    await flush();

    expect(queue.pushPayment.mock.calls).toHaveLength(0);
    expect(ingestor.isRunning).toBe(true);
    ingestor.stop();
  });

  it('path payment ingestor ignores malformed records', async () => {
    const { opener, opens } = fakeOpener();
    const queue = fakeQueue();
    const ingestor = new PathPaymentIngestor({ opener, queue, baseBackoffMs: 1 });
    await ingestor.start();

    opens[0].handlers.onOperation({ type: 'path_payment_strict_receive', type_i: 2 } as unknown as HorizonOperationRecord);
    opens[0].handlers.onOperation({ bad: true } as unknown as HorizonOperationRecord);
    await flush();

    expect(queue.pushPathPayment.mock.calls).toHaveLength(0);
    ingestor.stop();
  });

  it('offer ingestor ignores malformed records', async () => {
    const { opener, opens } = fakeOpener();
    const queue = fakeQueue();
    const ingestor = new OfferIngestor({ opener, queue, baseBackoffMs: 1 });
    await ingestor.start();

    opens[0].handlers.onOperation({ type: 'manage_sell_offer', type_i: 3 } as unknown as HorizonOperationRecord);
    await flush();

    expect(queue.pushOffer.mock.calls).toHaveLength(0);
    ingestor.stop();
  });

  it('claimable balance ingestor ignores malformed records', async () => {
    const { opener, opens } = fakeOpener();
    const queue = fakeQueue();
    const ingestor = new ClaimableBalanceIngestor({ opener, queue, baseBackoffMs: 1 });
    await ingestor.start();

    opens[0].handlers.onOperation({ type: 'create_claimable_balance', type_i: 14 } as unknown as HorizonOperationRecord);
    opens[0].handlers.onOperation({ type: 'claim_claimable_balance', type_i: 15 } as unknown as HorizonOperationRecord);
    await flush();

    expect(queue.pushClaimableBalance.mock.calls).toHaveLength(0);
    ingestor.stop();
  });
});
