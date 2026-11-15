import { describe, it, expect } from 'vitest';
import {
  API_KEYS_TABLE,
  API_KEYS_COLUMNS,
  API_KEY_SELECT_COLUMNS,
  type ApiKeyRow,
} from '../api-keys';

describe('api key schema', () => {
  it('exports the api_keys table name', () => {
    expect(API_KEYS_TABLE).toBe('api_keys');
  });

  it('exports all api_keys columns', () => {
    expect(API_KEYS_COLUMNS).toContain('id');
    expect(API_KEYS_COLUMNS).toContain('name');
    expect(API_KEYS_COLUMNS).toContain('key_hash');
    expect(API_KEYS_COLUMNS).toContain('permissions');
    expect(API_KEYS_COLUMNS).toContain('rate_limit_tier');
    expect(API_KEYS_COLUMNS).toContain('created_at');
    expect(API_KEYS_COLUMNS).toContain('last_used_at');
    expect(API_KEYS_COLUMNS).toContain('revoked_at');
    expect(API_KEYS_COLUMNS.length).toBe(8);
  });

  it('selects every column when a key is looked up', () => {
    for (const column of API_KEYS_COLUMNS) {
      expect(API_KEY_SELECT_COLUMNS).toContain(column);
    }
  });

  it('describes the row shape returned by the database', () => {
    const row: ApiKeyRow = {
      id: '0f8f3d3e-4a1a-4c6a-9b1a-9a1b2c3d4e5f',
      name: 'trading-bot',
      key_hash: '$argon2id$v=19$m=19456,t=2,p=1$...',
      permissions: ['flows:read'],
      rate_limit_tier: 'pro',
      created_at: '2026-11-15T00:00:00.000Z',
      last_used_at: null,
      revoked_at: null,
    };

    expect(row.key_hash).not.toContain('sk_live_');
    expect(row.revoked_at).toBeNull();
  });
});
