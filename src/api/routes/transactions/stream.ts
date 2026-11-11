/**
 * Transaction SSE stream route (issue #27): `GET /v1/transactions/stream`.
 *
 * Streams live transactions as Server-Sent Events.
 *
 * Query parameters:
 * - `accounts[]` / `accounts` — filter by account (source/destination/from/to).
 * - `assets[]` / `assets`     — filter by asset key (e.g. `USDC:G...`, `XLM`).
 * - `types[]` / `types`       — filter by operation/transaction type or kind.
 * - `cursor`                  — resume cursor; clients may also send
 *                                `Last-Event-ID` for resume.
 *
 * Events:
 * - `transaction` with `id` and JSON `data`.
 * - `retry: <ms>` hint for client reconnection.
 * - periodic `: heartbeat` comments to keep connections alive.
 *
 * Real-time updates are consumed from a Redis pub/sub channel (default
 * `eta:transactions`, emitted by the aggregator/ingestor pipeline). The
 * subscriber is injectable for tests; when none is provided the endpoint
 * returns 503 (stream source unavailable).
 */

import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import { createRedisClient } from '../../../redis/client';
import { ValidationError, zodToFields } from '../../errors';
import { z } from 'zod';

export const TRANSACTIONS_STREAM_CHANNEL = 'eta:transactions';
export const DEFAULT_STREAM_RETRY_MS = 5_000;
export const DEFAULT_HEARTBEAT_MS = 15_000;
export const DEFAULT_MAX_BUFFERED_EVENTS = 100;

export interface TransactionStreamEvent {
  /** Event/cursor id used for SSE `id` and resume. */
  id: string;
  /** Transaction/operation payload sent as `data`. */
  data: unknown;
}

export interface TransactionStreamSubscriber {
  /** Subscribe and invoke `onEvent` for each incoming stream event. */
  subscribe(onEvent: (event: TransactionStreamEvent) => void): Promise<void> | void;
  /** Stop receiving events and release resources. */
  close(): Promise<void> | void;
}

export interface TransactionStreamOptions {
  /** Factory for a per-connection subscriber (injectable for tests). */
  createTransactionStreamSubscriber?: () => TransactionStreamSubscriber;
  /** Redis pub/sub channel to subscribe to when using the default subscriber. */
  transactionStreamChannel?: string;
  retryMs?: number;
  heartbeatMs?: number;
  maxBufferedEvents?: number;
}

interface NormalizedStreamQuery {
  accounts: string[];
  assets: string[];
  types: string[];
  cursor?: string;
}

const querySchema = z.object({
  accounts: z.union([z.string(), z.array(z.string())]).optional(),
  'accounts[]': z.union([z.string(), z.array(z.string())]).optional(),
  assets: z.union([z.string(), z.array(z.string())]).optional(),
  'assets[]': z.union([z.string(), z.array(z.string())]).optional(),
  types: z.union([z.string(), z.array(z.string())]).optional(),
  'types[]': z.union([z.string(), z.array(z.string())]).optional(),
  cursor: z.string().min(1).optional(),
});

function toStringArray(value: string | string[] | undefined): string[] {
  if (value === undefined) return [];
  const list = Array.isArray(value) ? value : [value];
  return list
    .flatMap((entry) => entry.split(','))
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0);
}

function unique(values: string[]): string[] {
  return [...new Set(values)];
}

export function normalizeStreamQuery(raw: unknown): NormalizedStreamQuery {
  const parsed = querySchema.safeParse(raw ?? {});
  if (!parsed.success) {
    throw new ValidationError('Invalid transaction stream query', zodToFields(parsed.error));
  }
  const q = parsed.data as Record<string, string | string[] | undefined>;
  return {
    accounts: unique([...toStringArray(q.accounts), ...toStringArray(q['accounts[]'])]),
    assets: unique([...toStringArray(q.assets), ...toStringArray(q['assets[]'])]),
    types: unique([...toStringArray(q.types), ...toStringArray(q['types[]'])]),
    cursor: typeof q.cursor === 'string' ? q.cursor : undefined,
  };
}

function extractString(value: unknown): string | undefined {
  return typeof value === 'string' && value.length > 0 ? value : undefined;
}

function collectStrings(value: unknown, out: Set<string>): void {
  if (typeof value === 'string') {
    if (value) out.add(value);
    return;
  }
  if (Array.isArray(value)) {
    for (const entry of value) collectStrings(entry, out);
    return;
  }
  if (value && typeof value === 'object') {
    for (const entry of Object.values(value as Record<string, unknown>)) {
      collectStrings(entry, out);
    }
  }
}

function assetKeysFromData(data: unknown): Set<string> {
  const keys = new Set<string>();
  if (!data || typeof data !== 'object') return keys;
  const record = data as Record<string, unknown>;
  const direct = [
    record.asset,
    record.sendAsset,
    record.send_asset,
    record.destinationAsset,
    record.destination_asset,
    record.sellingAsset,
    record.buyingAsset,
  ];
  for (const candidate of direct) {
    if (typeof candidate === 'string') {
      keys.add(candidate);
    } else if (candidate && typeof candidate === 'object') {
      const c = candidate as Record<string, unknown>;
      const code = extractString(c.code);
      const issuer = extractString(c.issuer);
      if (code) keys.add(issuer ? `${code}:${issuer}` : code);
      const type = extractString(c.type);
      if (type === 'native') keys.add('XLM');
    }
  }
  if (Array.isArray(record.path)) {
    for (const hop of record.path) {
      if (hop && typeof hop === 'object') {
        const c = hop as Record<string, unknown>;
        const code = extractString(c.code);
        const issuer = extractString(c.issuer);
        if (code) keys.add(issuer ? `${code}:${issuer}` : code);
      }
    }
  }
  if (typeof record.asset_code === 'string') {
    const code = record.asset_code;
    const issuer = typeof record.asset_issuer === 'string' ? record.asset_issuer : undefined;
    keys.add(issuer ? `${code}:${issuer}` : code);
  }
  return keys;
}

function accountsFromData(data: unknown): Set<string> {
  const accounts = new Set<string>();
  if (!data || typeof data !== 'object') return accounts;
  const record = data as Record<string, unknown>;
  const fields = [
    'account',
    'account_id',
    'from',
    'to',
    'source',
    'destination',
    'source_account',
    'destination_account',
    'funder',
    'claimant',
    'sponsor',
    'trustor',
  ];
  for (const field of fields) {
    const value = record[field];
    if (typeof value === 'string' && value) accounts.add(value);
  }
  // include nested known shapes lightly
  for (const nestedKey of ['payment', 'operation', 'transaction']) {
    const nested = record[nestedKey];
    if (nested && typeof nested === 'object') {
      for (const account of accountsFromData(nested)) accounts.add(account);
    }
  }
  return accounts;
}

function typesFromData(data: unknown): Set<string> {
  const types = new Set<string>();
  if (!data || typeof data !== 'object') return types;
  const record = data as Record<string, unknown>;
  for (const key of ['type', 'type_i', 'kind', 'operation_type', 'variant']) {
    const value = record[key];
    if (typeof value === 'string' || typeof value === 'number') {
      types.add(String(value));
    }
  }
  return types;
}

/** Returns true when the event matches the requested filters. */
export function matchesTransactionFilters(
  event: TransactionStreamEvent,
  filters: NormalizedStreamQuery,
): boolean {
  if (filters.cursor && event.id <= filters.cursor) return false;

  if (filters.accounts.length > 0) {
    const accounts = accountsFromData(event.data);
    const matchesAccount = filters.accounts.some((account) => accounts.has(account));
    if (!matchesAccount) return false;
  }

  if (filters.assets.length > 0) {
    const assets = assetKeysFromData(event.data);
    const matchesAsset = filters.assets.some((asset) => assets.has(asset));
    if (!matchesAsset) return false;
  }

  if (filters.types.length > 0) {
    const types = typesFromData(event.data);
    const matchesType = filters.types.some((type) => types.has(type));
    if (!matchesType) return false;
  }

  return true;
}

function parseStreamEvent(message: string): TransactionStreamEvent | null {
  try {
    const parsed = JSON.parse(message) as unknown;
    if (parsed && typeof parsed === 'object') {
      const record = parsed as Record<string, unknown>;
      const id =
        extractString(record.id) ??
        extractString(record.cursor) ??
        extractString(record.paging_token) ??
        extractString((record.data as Record<string, unknown> | undefined)?.id);
      if (id) {
        const data = 'data' in record && record.data !== undefined ? record.data : parsed;
        return { id, data };
      }
    }
  } catch {
    // ignore malformed message
  }
  return null;
}

/** Default Redis-backed subscriber: listens on a pub/sub channel. */
export function createRedisTransactionStreamSubscriber(
  channel: string = TRANSACTIONS_STREAM_CHANNEL,
): TransactionStreamSubscriber {
  let subscriber: ReturnType<typeof createRedisClient> | null = null;
  let handler: ((channelName: string, message: string) => void) | null = null;

  return {
    async subscribe(onEvent) {
      subscriber = createRedisClient({ maxRetriesPerRequest: null });
      handler = (_channelName, message) => {
        const event = parseStreamEvent(message);
        if (event) onEvent(event);
      };
      subscriber.on('message', handler);
      await subscriber.subscribe(channel);
    },
    async close() {
      try {
        if (subscriber && handler) subscriber.off('message', handler);
        if (subscriber) await subscriber.quit();
      } catch {
        subscriber?.disconnect();
      } finally {
        subscriber = null;
        handler = null;
      }
    },
  };
}

function writeSse(reply: FastifyReply, chunk: string): boolean {
  const raw = reply.raw;
  if (raw.writableEnded || raw.destroyed) return false;
  // Backpressure: if the client can't keep up, drop this write and let the
  // caller decide (we drop buffered/live events rather than growing memory).
  const ok = raw.write(chunk);
  return ok !== false;
}

export function registerTransactionStreamRoutes(
  app: FastifyInstance,
  options: TransactionStreamOptions = {},
): void {
  const retryMs = options.retryMs ?? DEFAULT_STREAM_RETRY_MS;
  const heartbeatMs = options.heartbeatMs ?? DEFAULT_HEARTBEAT_MS;
  const maxBuffered = options.maxBufferedEvents ?? DEFAULT_MAX_BUFFERED_EVENTS;
  const channel = options.transactionStreamChannel ?? TRANSACTIONS_STREAM_CHANNEL;

  app.get('/transactions/stream', async (request: FastifyRequest, reply: FastifyReply) => {
    const filters = normalizeStreamQuery(request.query);
    const lastEventIdHeader = request.headers['last-event-id'];
    if (!filters.cursor && typeof lastEventIdHeader === 'string' && lastEventIdHeader) {
      filters.cursor = lastEventIdHeader;
    }

    const createSubscriber =
      options.createTransactionStreamSubscriber ??
      (() => createRedisTransactionStreamSubscriber(channel));

    let subscriber: TransactionStreamSubscriber;
    try {
      subscriber = createSubscriber();
    } catch (error) {
      request.log.error({ err: error }, 'failed to create transaction stream subscriber');
      return reply.code(503).send({
        type: 'about:blank',
        title: 'Service Unavailable',
        status: 503,
        detail: 'Transaction stream unavailable',
        code: 'internal_error',
        instance: request.url,
      });
    }

    reply.raw.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache, no-transform',
      Connection: 'keep-alive',
      'X-Accel-Buffering': 'no',
    });
    // Hint Fastify we've taken over the response.
    reply.hijack();

    writeSse(reply, `retry: ${retryMs}\n\n`);

    let closed = false;
    let lastCursor = filters.cursor;
    let buffered = 0;

    const cleanup = async (): Promise<void> => {
      if (closed) return;
      closed = true;
      clearInterval(heartbeat);
      try {
        await subscriber.close();
      } catch {
        // ignore cleanup errors
      }
      if (!reply.raw.writableEnded) {
        reply.raw.end();
      }
    };

    const heartbeat = setInterval(() => {
      if (closed) return;
      if (!writeSse(reply, ': heartbeat\n\n')) {
        // Client/socket backpressured or stalled; drop keepalive write.
      }
    }, heartbeatMs);
    heartbeat.unref?.();

    const onEvent = (event: TransactionStreamEvent): void => {
      if (closed) return;
      // Cursor-based resume: ignore events at/before the resume cursor.
      if (lastCursor && event.id <= lastCursor) return;
      if (!matchesTransactionFilters(event, filters)) return;

      if (buffered >= maxBuffered) {
        // Backpressure policy: drop when the client can't keep up.
        request.log.warn({ maxBuffered }, 'transaction stream buffer full, dropping event');
        return;
      }
      buffered += 1;
      const payload = JSON.stringify(event.data);
      const chunk = `id: ${event.id}\nevent: transaction\ndata: ${payload}\n\n`;
      buffered -= 1;
      lastCursor = event.id;
      if (!writeSse(reply, chunk)) {
        // If the socket is saturated, rely on backpressure/drop behavior.
      }
    };

    try {
      await subscriber.subscribe(onEvent);
    } catch (error) {
      request.log.error({ err: error }, 'failed to subscribe to transaction stream');
      await cleanup();
      return;
    }

    request.raw.on('close', () => {
      void cleanup();
    });
  });
}
