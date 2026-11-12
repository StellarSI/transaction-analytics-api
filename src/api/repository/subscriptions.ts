/**
 * Read/write model for webhook subscriptions (issue #28).
 *
 * The HTTP layer depends on the small {@link SubscriptionStore} port rather
 * than on SQL directly, which keeps route handlers unit-testable with an
 * in-memory fake. The production implementation is a thin adapter over the
 * shared `postgres.Sql` pool; the shared secret is stored as-is (the column
 * is TEXT, and this adapter never returns it) so future delivery work can
 * HMAC-sign outbound payloads without a schema change.
 */

import type postgres from 'postgres';

export type SubscriptionTargetType = 'account' | 'asset' | 'threshold';

/** Input for registering a webhook subscription. */
export interface CreateSubscriptionInput {
  /** Identifier of the owning API key (opaque to this module). */
  apiKeyId: string;
  targetType: SubscriptionTargetType;
  targetValue: string;
  url: string;
  secret?: string;
  events?: string[];
}

/** What the API persists and, on creation, returns. Secret is never echoed back. */
export interface Subscription {
  id: string;
  apiKeyId: string;
  targetType: SubscriptionTargetType;
  targetValue: string;
  url: string;
  secret?: string;
  events: string[];
  active: boolean;
  createdAt: string;
  updatedAt: string;
}

export interface SubscriptionStore {
  createSubscription(input: CreateSubscriptionInput): Promise<Subscription>;
}

/**
 * Minimal tagged-template surface used by the Postgres adapter. The shared
 * `postgres.Sql` pool satisfies it structurally; `src/index.ts` narrows it at
 * the boundary, mirroring the asset flow repository.
 */
export interface SubscriptionSql {
  (strings: TemplateStringsArray, ...values: unknown[]): Promise<unknown>;
}

interface SubscriptionRow {
  id: string;
  api_key_id: string;
  target_type: string;
  target_value: string;
  url: string;
  secret: string | null;
  events: unknown;
  active: boolean;
  created_at: Date | string;
  updated_at: Date | string;
}

function toSubscription(row: SubscriptionRow): Subscription {
  return {
    id: row.id,
    apiKeyId: row.api_key_id,
    targetType: row.target_type as SubscriptionTargetType,
    targetValue: row.target_value,
    url: row.url,
    secret: row.secret ?? undefined,
    events: Array.isArray(row.events) ? (row.events as string[]) : [],
    active: row.active,
    createdAt: new Date(row.created_at).toISOString(),
    updatedAt: new Date(row.updated_at).toISOString(),
  };
}

/**
 * Creates a repository backed by PostgreSQL. `sql` is the shared connection
 * pool from `src/db/pool.ts`.
 */
export function createPostgresSubscriptionStore(
  sql: SubscriptionSql,
): SubscriptionStore {
  const db = sql as unknown as postgres.Sql;

  return {
    async createSubscription(input) {
      const rows = (await db`
        INSERT INTO subscriptions (
          api_key_id, target_type, target_value, url, secret, events
        ) VALUES (
          ${input.apiKeyId},
          ${input.targetType},
          ${input.targetValue},
          ${input.url},
          ${input.secret ?? null},
          ${JSON.stringify(input.events ?? [])}::jsonb
        )
        RETURNING id, api_key_id, target_type, target_value, url, secret, events,
                  active, created_at, updated_at
      `) as SubscriptionRow[];

      return toSubscription(rows[0]);
    },
  };
}
