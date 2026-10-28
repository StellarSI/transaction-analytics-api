import { describe, it, expect, vi } from 'vitest';
import { createRedisListQueue } from '../queue';
import {
  PAYMENT_QUEUE,
  PATH_PAYMENT_QUEUE,
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
});
