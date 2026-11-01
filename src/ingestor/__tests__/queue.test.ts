import { describe, it, expect, vi } from 'vitest';
import { createRedisListQueue } from '../queue';
import {
  CLAIMABLE_BALANCE_QUEUE,
  OFFER_QUEUE,
  PAYMENT_QUEUE,
  PATH_PAYMENT_QUEUE,
  type ClaimableBalancePayload,
  type OfferPayload,
  type PaymentPayload,
  type PathPaymentPayload,
} from '../types';

function fakeClient() {
  return { rpush: vi.fn().mockResolvedValue(1) };
}

const payment: PaymentPayload = {
  kind: 'payment',
  id: '1',
  cursor: '1-1',
  from: 'GA',
  to: 'GB',
  asset: { type: 'native', code: 'XLM' },
  amount: '1.0000000',
};

const pathPayment: PathPaymentPayload = {
  kind: 'path_payment',
  variant: 'strict-send',
  id: '2',
  cursor: '2-1',
  from: 'GA',
  to: 'GB',
  sendAsset: { type: 'native', code: 'XLM' },
  destinationAsset: { type: 'native', code: 'XLM' },
  corridor: { path: [], sameAnchor: false, crossIssuer: false },
};

describe('createRedisListQueue', () => {
  it('pushes payments onto the payment queue as JSON', async () => {
    const client = fakeClient();
    const queue = createRedisListQueue(client);
    await queue.pushPayment(payment);
    expect(client.rpush).toHaveBeenCalledWith(PAYMENT_QUEUE, JSON.stringify(payment));
  });

  it('pushes path payments onto the path-payment queue as JSON', async () => {
    const client = fakeClient();
    const queue = createRedisListQueue(client);
    await queue.pushPathPayment(pathPayment);
    expect(client.rpush).toHaveBeenCalledWith(
      PATH_PAYMENT_QUEUE,
      JSON.stringify(pathPayment),
    );
  });

  it('rethrows enqueue failures so callers can retry', async () => {
    const client = { rpush: vi.fn().mockRejectedValue(new Error('down')) };
    const queue = createRedisListQueue(client);
    await expect(queue.pushPayment(payment)).rejects.toThrow('down');
  });

  describe('offer payloads (issue #14)', () => {
    const offer: OfferPayload = {
      kind: 'offer',
      variant: 'manage-sell',
      id: '3',
      cursor: '3-1',
      sellingAsset: { type: 'native', code: 'XLM' },
      buyingAsset: { type: 'credit_alphanum4', code: 'USDC', issuer: 'GI' },
      amount: '5.0000000',
      priceRatio: { n: 1, d: 2 },
      offerId: '7',
      isDeletion: false,
    };

    it('pushes offers onto the offer queue as JSON', async () => {
      const client = fakeClient();
      const queue = createRedisListQueue(client);
      await queue.pushOffer(offer);
      expect(client.rpush).toHaveBeenCalledWith(OFFER_QUEUE, JSON.stringify(offer));
    });
  });

  describe('claimable balance payloads (issue #13)', () => {
    const claimable: ClaimableBalancePayload = {
      kind: 'claimable_balance',
      variant: 'create',
      id: '4',
      cursor: '4-1',
      asset: { type: 'native', code: 'XLM' },
      amount: '3.0000000',
      claimants: [{ destination: 'GCLAIMANT' }],
    };

    it('pushes claimable balance operations as JSON', async () => {
      const client = fakeClient();
      const queue = createRedisListQueue(client);
      await queue.pushClaimableBalance(claimable);
      expect(client.rpush).toHaveBeenCalledWith(
        CLAIMABLE_BALANCE_QUEUE,
        JSON.stringify(claimable),
      );
    });
  });
});
