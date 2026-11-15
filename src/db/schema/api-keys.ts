/**
 * Schema for the `api_keys` table (issue #30).
 *
 * Mirrors `migrations/0007_api_keys.sql` so the authentication middleware and
 * the database agree on column names and types. Only the argon2 hash of a
 * key is stored: the plaintext key is shown once when it is issued and can
 * never be recovered from the database.
 *
 * Timestamps are ISO 8601 strings (`YYYY-MM-DDTHH:MM:SSZ`) and the
 * permission list is a JSON array of permission names.
 */

export interface ApiKeyRow {
  id: string;
  name: string;
  /** argon2 hash of the key; the plaintext is never stored. */
  key_hash: string;
  /** Permissions granted to the key. */
  permissions: string[];
  /** Rate limit tier of the key. */
  rate_limit_tier: string;
  created_at: string;
  last_used_at: string | null;
  revoked_at: string | null;
}

export interface ApiKeyInsert {
  name: string;
  key_hash: string;
  permissions?: string[];
  rate_limit_tier?: string;
}

export const API_KEYS_TABLE = 'api_keys';

export const API_KEYS_COLUMNS = [
  'id',
  'name',
  'key_hash',
  'permissions',
  'rate_limit_tier',
  'created_at',
  'last_used_at',
  'revoked_at',
] as const;

/** Columns returned when a key is looked up by its hash. */
export const API_KEY_SELECT_COLUMNS = API_KEYS_COLUMNS.join(', ');
