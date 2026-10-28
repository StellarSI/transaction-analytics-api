import type {
  CursorStore,
  HorizonOperationRecord,
  OperationQueue,
} from '../types';
import type { OperationStreamOpener, StreamCloser } from './stream';
import { logger as defaultLogger } from '../../logger';

export interface HorizonIngestorOptions {
  /** Where the SSE stream comes from. Injected so tests avoid the network. */
  opener: OperationStreamOpener;
  /** Destination for parsed operations. */
  queue: OperationQueue;
  /** Optional durable cursor storage; without it, cursors live in-process. */
  cursorStore?: CursorStore;
  /** Key under which this ingestor's cursor is persisted. */
  cursorKey: string;
  /** Cold-start cursor; `now` streams only new operations. */
  initialCursor?: string;
  /** First reconnect delay; doubles each attempt up to `maxBackoffMs`. */
  baseBackoffMs?: number;
  /** Ceiling for the reconnect backoff. */
  maxBackoffMs?: number;
  /** Injected for tests so reconnect delays are instantaneous. */
  wait?: (ms: number) => Promise<void>;
}

const defaultWait = (ms: number) =>
  new Promise<void>((resolve) => setTimeout(resolve, ms));

/**
 * Options after defaults are applied. The constructor fills in every optional
 * field so the streaming loop can use them without null checks.
 */
type ResolvedOptions = Pick<HorizonIngestorOptions, 'cursorStore'> & {
  opener: OperationStreamOpener;
  queue: OperationQueue;
  cursorKey: string;
  initialCursor: string;
  baseBackoffMs: number;
  maxBackoffMs: number;
  wait: (ms: number) => Promise<void>;
};

/**
 * Shared streaming loop for the Horizon ingestors (issues #11, #12).
 *
 * Responsibilities that are identical for payments and path payments live
 * here: opening the SSE stream at a cursor, persisting the last *seen* paging
 * token, and transparently reconnecting with exponential backoff after a
 * disconnect. Subclasses implement only {@link processRecord} — deciding which
 * operations to keep, how to parse them, and which queue to push to.
 *
 * The stream delivers one operation at a time and reports disconnects via
 * `onError`; this client treats any error as "reconnect at the last cursor" so
 * no operation is dropped across a restart. A successful record resets the
 * backoff, so a stable stream never grows its retry delay.
 */
export abstract class HorizonIngestor {
  protected readonly options: ResolvedOptions;

  private closer: StreamCloser | null = null;
  private reconnectTimer: Promise<void> | null = null;
  private running = false;
  private cursor: string;
  private attempts = 0;

  constructor(options: HorizonIngestorOptions) {
    this.options = {
      opener: options.opener,
      queue: options.queue,
      cursorKey: options.cursorKey,
      cursorStore: options.cursorStore,
      initialCursor: options.initialCursor ?? 'now',
      baseBackoffMs: options.baseBackoffMs ?? 1_000,
      maxBackoffMs: options.maxBackoffMs ?? 60_000,
      wait: options.wait ?? defaultWait,
    };
    this.cursor = this.options.initialCursor;
  }

  /** Human-readable ingestor name used in logs. */
  protected abstract get name(): string;

  /**
   * Decide whether to keep a record and, if so, push the parsed payload to the
   * appropriate queue. Return value is ignored; thrown errors are logged.
   */
  protected abstract processRecord(record: HorizonOperationRecord): Promise<void>;

  /** Current resume cursor (persisted paging token, or the cold-start value). */
  get resumeCursor(): string {
    return this.cursor;
  }

  /** True while the ingestor is supposed to be streaming. */
  get isRunning(): boolean {
    return this.running;
  }

  /**
   * Opens the stream and resolves once it is attached. Safe to call again
   * after {@link stop}.
   */
  async start(): Promise<void> {
    if (this.running) return;
    this.running = true;
    this.attempts = 0;
    this.cursor = (await this.loadCursor()) ?? this.options.initialCursor;
    this.openStream();
  }

  /** Closes the stream and cancels any pending reconnect. */
  stop(): void {
    this.running = false;
    this.closeStream();
    this.reconnectTimer = null;
  }

  private openStream(): void {
    if (!this.running) return;
    this.closer = this.options.opener({
      cursor: this.cursor,
      handlers: {
        onOperation: (record) => {
          // Advance the cursor before processing so a crash mid-processing
          // still resumes at the next operation on restart.
          if (record.paging_token) this.cursor = record.paging_token;
          void this.persistCursor();
          void this.onRecord(record);
        },
        onError: (error) => {
          defaultLogger.error(
            { err: error, attempts: this.attempts },
            `${this.name}: stream error, reconnecting`,
          );
          this.scheduleReconnect();
        },
      },
    });
  }

  private closeStream(): void {
    if (this.closer) {
      try {
        this.closer();
      } catch {
        // A closed/failed stream may throw on close; nothing to do about it.
      }
      this.closer = null;
    }
  }

  private async onRecord(record: HorizonOperationRecord): Promise<void> {
    // Any successfully delivered operation proves the stream is healthy, so
    // reset the backoff before doing work that might itself fail.
    this.attempts = 0;
    try {
      await this.processRecord(record);
    } catch (error) {
      defaultLogger.error(
        { err: error, id: record.id },
        `${this.name}: failed to process operation`,
      );
    }
  }

  private scheduleReconnect(): void {
    if (!this.running || this.reconnectTimer) return;
    this.closeStream();
    const delay = Math.min(
      this.options.baseBackoffMs * 2 ** this.attempts,
      this.options.maxBackoffMs,
    );
    this.attempts += 1;
    this.reconnectTimer = this.options
      .wait(delay)
      .then(() => {
        this.reconnectTimer = null;
        this.openStream();
      })
      .catch(() => {
        this.reconnectTimer = null;
      });
  }

  private async loadCursor(): Promise<string | null> {
    if (!this.options.cursorStore) return null;
    try {
      return await this.options.cursorStore.get(this.options.cursorKey);
    } catch (error) {
      defaultLogger.warn(
        { err: error },
        `${this.name}: could not load cursor, starting fresh`,
      );
      return null;
    }
  }

  private async persistCursor(): Promise<void> {
    if (!this.options.cursorStore) return;
    try {
      await this.options.cursorStore.set(this.options.cursorKey, this.cursor);
    } catch (error) {
      defaultLogger.warn({ err: error }, `${this.name}: could not persist cursor`);
    }
  }
}