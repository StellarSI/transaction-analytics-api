import type {
  OperationQueue,
  PaymentPayload,
  PathPaymentPayload,
} from './types';
import { PATH_PAYMENT_QUEUE, PAYMENT_QUEUE } from './types';
import { logger } from '../logger';

/**
 * Redis list-backed queue (issues #11, #12).
 *
 * This is a deliberately minimal producer that satisfies the `OperationQueue`
 * port so the ingestors can ship before the BullMQ-based queue (issue #15)
 * exists: payloads are JSON-serialized and appended to one Redis list per
 * queue with `RPUSH`. When #15 lands, its consumer only needs to `BLPOP` the
 * same keys (or this class can be swapped for a BullMQ producer) without any
 * change to the streaming code, because the ingestors depend solely on the
 * interface.
 *
 * A failing enqueue is logged and re-thrown; the ingestor treats it as a
 * transient error and retries on the next reconnect cycle rather than losing
 * the operation silently.
 */
export function createRedisListQueue(client: {
  rpush(key: string, value: string): Promise<number>;
}): OperationQueue {
  const push = async (key: string, payload: object): Promise<void> => {
    try {
      await client.rpush(key, JSON.stringify(payload));
    } catch (error) {
      logger.error({ err: error, key }, 'queue: failed to push operation');
      throw error;
    }
  };

  return {
    pushPayment(payload: PaymentPayload) {
      return push(PAYMENT_QUEUE, payload);
    },
    pushPathPayment(payload: PathPaymentPayload) {
      return push(PATH_PAYMENT_QUEUE, payload);
    },
  };
}

/**
 * Convenience factory wiring the shared Redis client (issue #10) into the
 * list-backed queue. Import lazily so unit tests never construct a client.
 */
export async function defaultQueue(): Promise<OperationQueue> {
  const { getRedis } = await import('../redis/client.js');
  return createRedisListQueue(getRedis());
}
