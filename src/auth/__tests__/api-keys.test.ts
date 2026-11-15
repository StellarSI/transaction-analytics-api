import { describe, it, expect } from 'vitest';
import {
  API_KEY_PREFIX,
  API_KEY_TIERS,
  API_KEY_WILDCARD_PERMISSION,
  authenticateApiKey,
  generateApiKeyPlaintext,
  hashApiKey,
  hasPermission,
  issueApiKey,
  verifyApiKey,
  type ApiKey,
  type ApiKeyStore,
  type CreateApiKeyInput,
} from '../api-keys';

const SALT = 'unit-test-salt-min-16-chars';

/** In-memory stand-in for the `api_keys` table. */
function createStore(): ApiKeyStore & { rows: Map<string, ApiKey> } {
  const rows = new Map<string, ApiKey>();

  return {
    rows,
    createApiKey(input: CreateApiKeyInput) {
      const apiKey: ApiKey = {
        id: `key-${rows.size + 1}`,
        name: input.name,
        permissions: input.permissions,
        tier: input.tier,
        createdAt: new Date().toISOString(),
        lastUsedAt: null,
        revokedAt: null,
      };
      rows.set(input.hash, apiKey);
      return Promise.resolve(apiKey);
    },
    findApiKeyByHash(hash: string) {
      return Promise.resolve(rows.get(hash) ?? null);
    },
    markApiKeyUsed(id: string) {
      for (const [hash, apiKey] of rows) {
        if (apiKey.id === id) {
          rows.set(hash, { ...apiKey, lastUsedAt: new Date().toISOString() });
        }
      }
      return Promise.resolve();
    },
  };
}

describe('API key generation', () => {
  it('issues keys with the sk_live_ prefix', () => {
    expect(generateApiKeyPlaintext().startsWith(API_KEY_PREFIX)).toBe(true);
  });

  it('issues a different key every time', () => {
    const keys = new Set([
      generateApiKeyPlaintext(),
      generateApiKeyPlaintext(),
      generateApiKeyPlaintext(),
    ]);

    expect(keys.size).toBe(3);
  });

  it('encodes at least 256 bits of entropy', () => {
    const secret = generateApiKeyPlaintext().slice(API_KEY_PREFIX.length);
    // base64url: 6 bits per character.
    expect(secret.length * 6).toBeGreaterThanOrEqual(256);
  });
});

describe('API key hashing', () => {
  it('is deterministic for the same key and salt', async () => {
    const plaintext = generateApiKeyPlaintext();

    const first = await hashApiKey(plaintext, SALT);
    const second = await hashApiKey(plaintext, SALT);

    expect(first).toBe(second);
  });

  it('differs for a different salt', async () => {
    const plaintext = generateApiKeyPlaintext();

    const first = await hashApiKey(plaintext, SALT);
    const other = await hashApiKey(plaintext, 'another-salt-min-16-chars');

    expect(first).not.toBe(other);
  });

  it('verifies a key against its hash and rejects another key', async () => {
    const plaintext = generateApiKeyPlaintext();
    const hash = await hashApiKey(plaintext, SALT);

    await expect(verifyApiKey(hash, plaintext)).resolves.toBe(true);
    await expect(verifyApiKey(hash, generateApiKeyPlaintext())).resolves.toBe(
      false,
    );
  });

  it('reports a malformed hash as invalid instead of throwing', async () => {
    await expect(verifyApiKey('not-a-hash', 'whatever')).resolves.toBe(false);
  });
});

describe('issueApiKey', () => {
  it('returns the plaintext once and stores only its hash', async () => {
    const store = createStore();

    const issued = await issueApiKey(store, {
      name: 'trading-bot',
      permissions: ['flows:read'],
      tier: 'pro',
    });

    expect(issued.plaintext.startsWith(API_KEY_PREFIX)).toBe(true);
    expect(issued.apiKey.name).toBe('trading-bot');
    expect(issued.apiKey.tier).toBe('pro');

    const stored = [...store.rows.values()];
    expect(stored).toHaveLength(1);
    expect(store.rows.has(issued.plaintext)).toBe(false);
  });
});

describe('authenticateApiKey', () => {
  it('resolves the key behind a presented plaintext', async () => {
    const store = createStore();
    const issued = await issueApiKey(store, {
      name: 'trading-bot',
      permissions: ['flows:read', 'streams:read'],
      tier: 'enterprise',
    });

    const authenticated = await authenticateApiKey(store, issued.plaintext);

    expect(authenticated).toEqual({
      id: issued.apiKey.id,
      name: 'trading-bot',
      permissions: ['flows:read', 'streams:read'],
      tier: 'enterprise',
    });
  });

  it('rejects an unknown key', async () => {
    const store = createStore();

    await expect(
      authenticateApiKey(store, generateApiKeyPlaintext()),
    ).resolves.toBeNull();
  });

  it('rejects a revoked key', async () => {
    const store = createStore();
    const issued = await issueApiKey(store, {
      name: 'trading-bot',
      permissions: ['flows:read'],
      tier: 'free',
    });
    const hash = [...store.rows.keys()][0];
    const revoked = store.rows.get(hash);
    if (revoked) {
      store.rows.set(hash, { ...revoked, revokedAt: new Date().toISOString() });
    }

    await expect(
      authenticateApiKey(store, issued.plaintext),
    ).resolves.toBeNull();
  });
});

describe('hasPermission', () => {
  const key = {
    id: 'key-1',
    name: 'test',
    permissions: ['flows:read'] as const,
    tier: 'free' as const,
  };

  it('grants a permission the key holds', () => {
    expect(hasPermission({ ...key, permissions: ['flows:read'] }, 'flows:read')).toBe(
      true,
    );
  });

  it('denies a permission the key does not hold', () => {
    expect(
      hasPermission({ ...key, permissions: ['flows:read'] }, 'subscriptions:write'),
    ).toBe(false);
  });

  it('grants everything to the wildcard permission', () => {
    expect(
      hasPermission(
        { ...key, permissions: [API_KEY_WILDCARD_PERMISSION] },
        'subscriptions:write',
      ),
    ).toBe(true);
  });
});

describe('API key tiers', () => {
  it('exposes the free, pro and enterprise tiers', () => {
    expect(API_KEY_TIERS).toEqual(['free', 'pro', 'enterprise']);
  });
});
