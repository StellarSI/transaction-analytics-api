/**
 * Stellar RPC client for Soroban contract events (issue #16).
 *
 * Polls `getEvents` on a Stellar RPC endpoint for configured contracts and
 * pushes parsed Soroban events onto the Soroban event queue. Used for smart
 * contract analytics alongside the Horizon ingestors that cover classic
 * ledger operations.
 *
 * Unlike the Horizon SSE stream, RPC event delivery is poll-based: the
 * ingestor tracks the last ledger seen as a cursor and fetches events from
 * that ledger onward on each poll cycle.
 *
 * The RPC client interface is structural (not tied to a specific SDK class)
 * so unit tests can inject a mock without touching the network.
 */

import type {
  CursorStore,
  OperationQueue,
  SorobanEventPayload,
} from '../types';
import { logger as defaultLogger } from '../../logger';

/** Shape of a Soroban event as returned by `RpcClient.getEvents`. */
interface RpcSorobanEvent {
  type: 'contract' | 'system' | 'diagnostic';
  ledger: number;
  ledgerClosedAt: string;
  transactionHash: string;
  eventIndex: number;
  contractId?: string;
  /** Event type string for contract events (e.g. "transfer"). */
  eventType?: string;
  /** Base64-encoded SCVal topics. */
  topics?: string[];
  /** Base64-encoded SCVal event data, when present. */
  value?: string;
}

/** Minimal structural interface for the RPC client's `getEvents` method. */
interface RpcClientLike {
  getEvents(
    params: unknown,
  ): Promise<{ events: RpcSorobanEvent[]; latestLedger: number }>;
}

/** Options for {@link SorobanEventIngestor}. */
export interface SorobanEventIngestorOptions {
  /** Stellar RPC endpoint URL (e.g. https://soroban-testnet.stellar.org). */
  rpcUrl: string;
  /** Contract IDs to watch for events. */
  contractIds: string[];
  /** Event types to fetch: 'contract', 'system', 'diagnostic', or 'all'. */
  eventTypes: 'contract' | 'system' | 'diagnostic' | 'all';
  /** Destination for parsed events. */
  queue: OperationQueue;
  /** Optional durable cursor storage; without it, cursors live in-process. */
  cursorStore?: CursorStore;
  /** Key under which this ingestor's cursor is persisted. */
  cursorKey: string;
  /** First cold-start ledger; defaults to the latest ledger. */
  initialLedger?: number;
  /** Poll interval in milliseconds. Default: 5_000. */
  pollIntervalMs?: number;
  /** Maximum events per poll. Default: 100. */
  limit?: number;
  /** Injected for tests so poll delays are instantaneous. */
  wait?: (ms: number) => Promise<void>;
  /** Injected RPC client for tests; defaults to a real client. */
  rpc?: RpcClientLike;
}

type ResolvedOptions = {
  rpcUrl: string;
  contractIds: string[];
  eventTypes: 'contract' | 'system' | 'diagnostic' | 'all';
  queue: OperationQueue;
  cursorStore?: CursorStore;
  cursorKey: string;
  initialLedger: number | undefined;
  pollIntervalMs: number;
  limit: number;
  wait: (ms: number) => Promise<void>;
  rpc?: RpcClientLike;
};

const defaultWait = (ms: number) =>
  new Promise<void>((resolve) => setTimeout(resolve, ms));

/**
 * Polls Stellar RPC for Soroban contract events and pushes them to the
 * event queue.
 *
 * On each poll:
 * 1. Loads the last processed ledger from the cursor store (or uses
 *    `initialLedger` / latest ledger on cold start).
 * 2. Calls `getEvents` with the configured contract IDs and start ledger.
 * 3. Parses each event into a `SorobanEventPayload` and pushes it to the
 *    queue.
 * 4. Advances the cursor to `latestLedger`.
 * 5. Waits `pollIntervalMs` before the next poll.
 *
 * Errors during a poll are logged and the ingestor retries on the next cycle
 * rather than stopping.
 */
export class SorobanEventIngestor {
  private readonly options: ResolvedOptions;
  private running = false;
  private pollTimer: Promise<void> | null = null;
  private cursor: number | null = null;

  constructor(options: SorobanEventIngestorOptions) {
    this.options = {
      rpcUrl: options.rpcUrl,
      contractIds: options.contractIds,
      eventTypes: options.eventTypes,
      queue: options.queue,
      cursorStore: options.cursorStore,
      cursorKey: options.cursorKey,
      initialLedger: options.initialLedger,
      pollIntervalMs: options.pollIntervalMs ?? 5_000,
      limit: options.limit ?? 100,
      wait: options.wait ?? defaultWait,
      rpc: options.rpc,
    };
    this.cursor = null;
  }

  /** Human-readable ingestor name used in logs. */
  get name(): string {
    return 'soroban-event-ingestor';
  }

  /** Current resume cursor (last processed ledger, or null on cold start). */
  get resumeCursor(): number | null {
    return this.cursor;
  }

  /** True while the ingestor is supposed to be polling. */
  get isRunning(): boolean {
    return this.running;
  }

  /** Starts polling. Safe to call again after {@link stop}. */
  async start(): Promise<void> {
    if (this.running) return;
    this.running = true;
    this.cursor = await this.loadCursor();
    void this.pollOnce();
  }

  /** Stops polling and cancels any pending poll timer. */
  stop(): void {
    this.running = false;
    this.pollTimer = null;
  }

  /** Performs one poll cycle: fetch events, push to queue, advance cursor. */
  private async pollOnce(): Promise<void> {
    if (!this.running) return;

    try {
      const startLedger = this.cursor ?? this.options.initialLedger;
      const params: Record<string, unknown> = {
        contractIds: this.options.contractIds,
        startLedger,
        limit: this.options.limit,
      };
      if (this.options.eventTypes !== 'all') {
        params.eventTypes = [this.options.eventTypes];
      }

      const response = await this.options.rpc?.getEvents(params);
      const events = response?.events ?? [];
      for (const event of events) {
        const payload = this.parseEvent(event);
        if (payload) {
          await this.options.queue.pushSorobanEvent(payload);
        }
      }

      const latestLedger = response?.latestLedger;
      if (typeof latestLedger === 'number') {
        this.cursor = latestLedger;
        await this.persistCursor();
      }
    } catch (error) {
      defaultLogger.error(
        { err: error },
        `${this.name}: poll failed, retrying on next cycle`,
      );
    }

    if (this.running) {
      this.pollTimer = this.options
        .wait(this.options.pollIntervalMs)
        .then(() => void this.pollOnce())
        .catch(() => {
          this.pollTimer = null;
        });
    }
  }

  /** Parses an RPC Soroban event into a `SorobanEventPayload`, or null when
   * the event lacks the fields we need (contract ID or event type). */
  private parseEvent(event: RpcSorobanEvent): SorobanEventPayload | null {
    if (!event.contractId || !event.eventType) return null;

    return {
      kind: 'soroban_event',
      id: `${event.ledger}-${event.eventIndex}`,
      cursor: String(event.ledger),
      ledger: event.ledger,
      ledgerClosedAt: event.ledgerClosedAt,
      txHash: event.transactionHash,
      sourceAccount: undefined,
      contractId: event.contractId,
      eventType: event.eventType,
      topics: event.topics ?? [],
      value: event.value,
    };
  }

  /** Loads the last processed ledger from the cursor store. */
  private async loadCursor(): Promise<number | null> {
    if (!this.options.cursorStore) return null;
    try {
      const raw = await this.options.cursorStore.get(this.options.cursorKey);
      if (raw === null) return null;
      const parsed = Number(raw);
      return Number.isFinite(parsed) ? parsed : null;
    } catch (error) {
      defaultLogger.warn(
        { err: error },
        `${this.name}: could not load cursor, starting fresh`,
      );
      return null;
    }
  }

  /** Persists the last processed ledger to the cursor store. */
  private async persistCursor(): Promise<void> {
    if (!this.options.cursorStore || this.cursor === null) return;
    try {
      await this.options.cursorStore.set(
        this.options.cursorKey,
        String(this.cursor),
      );
    } catch (error) {
      defaultLogger.warn({ err: error }, `${this.name}: could not persist cursor`);
    }
  }
}

/**
 * Convenience factory wiring the RPC client from config into the ingestor.
 * Import lazily so unit tests never construct a real client.
 */
export async function defaultSorobanEventIngestor(
  options: Omit<SorobanEventIngestorOptions, 'rpc'>,
): Promise<SorobanEventIngestor> {
  const sdk = await import('@stellar/stellar-sdk');
  const RpcClient = (sdk as { default?: { RpcClient: new (url: string) => unknown } })
    .default?.RpcClient;
  return new SorobanEventIngestor({
    ...options,
    rpc: RpcClient
      ? (new RpcClient(options.rpcUrl) as unknown as RpcClientLike)
      : undefined,
  });
}
