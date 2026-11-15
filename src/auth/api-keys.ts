/**
 * API key lifecycle (issue #30).
 *
 * Keys are issued as `sk_live_<random>` and are never stored: only an argon2
 * hash of the key is persisted. The hash uses the configured `API_KEY_SALT`
 * as its salt, which makes hashing deterministic — the middleware can hash
 * the presented key and look the row up by hash instead of scanning every
 * key in the table.
 *
 * A key carries
 *
 * - a set of permissions (`flows:read`, `subscriptions:write`, …), and
 * - a rate limit tier (`free`, `pro`, `enterprise`) used by the limiter in
 *   `src/auth/rate-limit.ts`.
 *
 * The HTTP layer depends on the small {@link ApiKeyStore} port rather than on
 * SQL directly, which keeps the middleware unit-testable with an in-memory
 * fake. The production implementation is a thin adapter over the shared
 * `postgres.Sql` pool.
 */

import { randomBytes } from 'node:crypto';
import * as argon2 from 'argon2';
import type postgres from 'postgres';
import { config } from '../config';

/** Prefix of every issued key. */
export const API_KEY_PREFIX = 'sk_live_';

/** Random bytes behind an issued key. */
export const API_KEY_ENTROPY_BYTES = 32;

/** Rate limit tiers, from the most restrictive to the least. */
export const API_KEY_TIERS = ['free', 'pro', 'enterprise'] as const;
export type ApiKeyTier = (typeof API_KEY_TIERS)[number];

/** Permissions a key can be granted. */
export const API_KEY_PERMISSIONS = [
  'flows:read',
  'subscriptions:read',
  'subscriptions:write',
  'streams:read',
] as const;
export type ApiKeyPermission = (typeof API_KEY_PERMISSIONS)[number];

/** Wildcard permission: grants every other permission. */
export const API_KEY_WILDCARD_PERMISSION = '*';

/** A permission as stored on a key: a named permission or the wildcard. */
export type ApiKeyPermissionGrant = ApiKeyPermission | typeof API_KEY_WILDCARD_PERMISSION;

/**
 * Argon2id parameters used for API key hashes.
 *
 * The plaintext key carries 256 bits of entropy, so guessing it is infeasible
 * regardless of the cost factor. The parameters below are deliberately
 * cheaper than the argon2 defaults: the hash is recomputed on every
 * authenticated request, and the plaintext is not a user password but a
 * random secret that cannot be brute-forced offline.
 */
export const API_KEY_HASH_OPTIONS = {
  memoryCost: 19_456,
  timeCost: 2,
  parallelism: 1,
} as const;

/** How often `last_used_at` is refreshed for a key, in milliseconds. */
export const LAST_USED_REFRESH_INTERVAL_MS = 60_000;

/** A key as stored in the database. */
export interface ApiKey {
  id: string;
  name: string;
  permissions: ApiKeyPermissionGrant[];
  tier: ApiKeyTier;
  createdAt: string;
  lastUsedAt: string | null;
  revokedAt: string | null;
}

/** The key information attached to an authenticated request. */
export interface AuthenticatedApiKey {
  id: string;
  name: string;
  permissions: ApiKeyPermissionGrant[];
  tier: ApiKeyTier;
}

/** Input for issuing a key. */
export interface IssueApiKeyInput {
  /** Human readable label, e.g. "trading-bot". */
  name: string;
  permissions: ApiKeyPermissionGrant[];
  tier: ApiKeyTier;
}

/** A newly issued key. The plaintext is shown once and never stored. */
export interface IssuedApiKey {
  apiKey: ApiKey;
  /** The plaintext key; it cannot be recovered from the database later. */
  plaintext: string;
}

/** Input for persisting a key. */
export interface CreateApiKeyInput {
  name: string;
  /** argon2 hash of the plaintext key. */
  hash: string;
  permissions: ApiKeyPermissionGrant[];
  tier: ApiKeyTier;
}

export interface ApiKeyStore {
  createApiKey(input: CreateApiKeyInput): Promise<ApiKey>;
  /** Looks a key up by its hash; returns null when the hash is unknown. */
  findApiKeyByHash(hash: string): Promise<ApiKey | null>;
  /** Records that the key was used (best effort). */
  markApiKeyUsed(id: string): Promise<void>;
}

/**
 * Minimal tagged-template surface used by the Postgres adapter. The shared
 * `postgres.Sql` pool satisfies it structurally; `src/index.ts` narrows it at
 * the boundary, mirroring the other stores.
 */
export interface ApiKeySql {
  (strings: TemplateStringsArray, ...values: unknown[]): Promise<unknown>;
}

/** Generates the plaintext of a new key. */
export function generateApiKeyPlaintext(): string {
  return `${API_KEY_PREFIX}${randomBytes(API_KEY_ENTROPY_BYTES).toString('base64url')}`;
}

/**
 * Hashes a plaintext key with argon2id, salted with `API_KEY_SALT`.
 *
 * The salt is fixed by configuration, so hashing the same key twice returns
 * the same digest — which is what allows the lookup by hash.
 */
export async function hashApiKey(
  plaintext: string,
  salt: string = config.API_KEY_SALT,
): Promise<string> {
  return argon2.hash(plaintext, { ...API_KEY_HASH_OPTIONS, salt });
}

/**
 * Verifies a plaintext key against a stored hash.
 *
 * Returns false (instead of throwing) for malformed hashes so a corrupt row
 * can never crash the request path.
 */
export async function verifyApiKey(
  hash: string,
  plaintext: string,
): Promise<boolean> {
  try {
    return await argon2.verify(hash, plaintext);
  } catch {
    return false;
  }
}

/**
 * Issues a key: generates the plaintext, hashes it and stores the hash.
 *
 * The caller receives the plaintext once; only the hash is persisted.
 */
export async function issueApiKey(
  store: ApiKeyStore,
  input: IssueApiKeyInput,
): Promise<IssuedApiKey> {
  const plaintext = generateApiKeyPlaintext();
  const hash = await hashApiKey(plaintext);
  const apiKey = await store.createApiKey({
    name: input.name,
    hash,
    permissions: input.permissions,
    tier: input.tier,
  });
  return { apiKey, plaintext };
}

/**
 * Authenticates a presented key.
 *
 * The plaintext is hashed with the configured salt and looked up by hash;
 * revoked keys are rejected. Returns null for an unknown or revoked key.
 */
export async function authenticateApiKey(
  store: ApiKeyStore,
  plaintext: string,
): Promise<AuthenticatedApiKey | null> {
  const hash = await hashApiKey(plaintext);
  const apiKey = await store.findApiKeyByHash(hash);
  if (!apiKey || apiKey.revokedAt) {
    return null;
  }

  const lastUsed = apiKey.lastUsedAt ? new Date(apiKey.lastUsedAt).getTime() : 0;
  if (Date.now() - lastUsed > LAST_USED_REFRESH_INTERVAL_MS) {
    // Best effort: a failed bookkeeping update must not fail the request.
    void store.markApiKeyUsed(apiKey.id).catch(() => undefined);
  }

  return {
    id: apiKey.id,
    name: apiKey.name,
    permissions: apiKey.permissions,
    tier: apiKey.tier,
  };
}

/** True when the key holds the given permission (or the wildcard). */
export function hasPermission(
  apiKey: AuthenticatedApiKey,
  permission: ApiKeyPermission,
): boolean {
  return (
    apiKey.permissions.includes(API_KEY_WILDCARD_PERMISSION) ||
    apiKey.permissions.includes(permission)
  );
}

interface ApiKeyRow {
  id: string;
  name: string;
  key_hash: string;
  permissions: unknown;
  rate_limit_tier: string;
  created_at: Date | string;
  last_used_at: Date | string | null;
  revoked_at: Date | string | null;
}

function toIso(value: Date | string | null): string | null {
  if (value === null) return null;
  return new Date(value).toISOString();
}

function toPermissionGrants(value: unknown): ApiKeyPermissionGrant[] {
  if (!Array.isArray(value)) return [];
  return value.filter(
    (entry): entry is ApiKeyPermissionGrant =>
      typeof entry === 'string' &&
      (entry === API_KEY_WILDCARD_PERMISSION ||
        (API_KEY_PERMISSIONS as readonly string[]).includes(entry)),
  );
}

function toTier(value: string): ApiKeyTier {
  return (API_KEY_TIERS as readonly string[]).includes(value)
    ? (value as ApiKeyTier)
    : 'free';
}

function toApiKey(row: ApiKeyRow): ApiKey {
  return {
    id: row.id,
    name: row.name,
    permissions: toPermissionGrants(row.permissions),
    tier: toTier(row.rate_limit_tier),
    createdAt: toIso(row.created_at) ?? new Date(0).toISOString(),
    lastUsedAt: toIso(row.last_used_at),
    revokedAt: toIso(row.revoked_at),
  };
}

/**
 * Creates a store backed by PostgreSQL. `sql` is the shared connection pool
 * from `src/db/pool.ts`.
 */
export function createPostgresApiKeyStore(sql: ApiKeySql): ApiKeyStore {
  const db = sql as unknown as postgres.Sql;

  return {
    async createApiKey(input) {
      const rows = (await db`
        INSERT INTO api_keys (name, key_hash, permissions, rate_limit_tier)
        VALUES (
          ${input.name},
          ${input.hash},
          ${JSON.stringify(input.permissions)}::jsonb,
          ${input.tier}
        )
        RETURNING id, name, key_hash, permissions, rate_limit_tier, created_at,
                  last_used_at, revoked_at
      `) as ApiKeyRow[];

      return toApiKey(rows[0]);
    },

    async findApiKeyByHash(hash) {
      const rows = (await db`
        SELECT id, name, key_hash, permissions, rate_limit_tier, created_at,
               last_used_at, revoked_at
        FROM api_keys
        WHERE key_hash = ${hash}
        LIMIT 1
      `) as ApiKeyRow[];

      return rows.length > 0 ? toApiKey(rows[0]) : null;
    },

    async markApiKeyUsed(id) {
      await db`
        UPDATE api_keys
        SET last_used_at = NOW()
        WHERE id = ${id}
      `;
    },
  };
}
