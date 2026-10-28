import { Horizon } from '@stellar/stellar-sdk';
import type { HorizonOperationRecord } from '../types';

/** Callbacks handed to a stream opener for each delivered record / error. */
export interface StreamHandlers {
  onOperation: (record: HorizonOperationRecord) => void;
  onError: (error: Error) => void;
}

/** Stops an opened stream. Idempotent. */
export type StreamCloser = () => void;

/**
 * Port for "open an SSE stream of operations starting at `cursor`".
 *
 * The default implementation (see {@link createHorizonStreamOpener}) wraps the
 * stellar-sdk Horizon server; tests inject a fake opener that replays recorded
 * operations, so the reconnect/cursor logic is exercised without a network.
 */
export interface OperationStreamOpener {
  (options: { cursor: string; handlers: StreamHandlers }): StreamCloser;
}

/**
 * Builds the default stream opener backed by `@stellar/stellar-sdk`.
 *
 * `cursor` is forwarded to Horizon; the special value `now` (the default for a
 * first, cold start) means "only operations that arrive from this point on".
 * After the first operation the ingestor resumes from its persisted paging
 * token, so a restart replays anything that happened while it was down.
 */
export function createHorizonStreamOpener(horizonUrl: string): OperationStreamOpener {
  const server = new Horizon.Server(horizonUrl);
  return ({ cursor, handlers }) => {
    const close = server
      .operations()
      .stream({
        cursor,
        onmessage: (record: unknown) =>
          handlers.onOperation(record as HorizonOperationRecord),
        onerror: (error: unknown) =>
          handlers.onError(error instanceof Error ? error : new Error(String(error))),
      });
    return () => close();
  };
}
