import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const ORIGINAL_ENV = process.env;

async function loadConfig(env: Record<string, string | undefined>) {
  vi.resetModules();
  process.env = { ...env };

  const exit = vi.spyOn(process, 'exit').mockImplementation(((code?: number) => {
    throw new Error(`process.exit:${code}`);
  }) as never);
  const error = vi.spyOn(console, 'error').mockImplementation(() => {});

  const mod = await import('../index');
  return { mod, exit, error };
}

describe('config', () => {
  beforeEach(() => {
    vi.resetModules();
    process.env = { ...ORIGINAL_ENV };
  });

  afterEach(() => {
    process.env = ORIGINAL_ENV;
    vi.restoreAllMocks();
  });

  it('parses valid config with defaults', async () => {
    const { mod, exit } = await loadConfig({
      DATABASE_URL: 'postgres://user:password@localhost:5432/db',
      API_KEY_SALT: 'a-very-long-salt-value',
    });

    expect(exit).not.toHaveBeenCalled();
    expect(mod.config).toMatchObject({
      NODE_ENV: 'development',
      PORT: 3000,
      LOG_LEVEL: 'info',
      STELLAR_NETWORK: 'testnet',
      HORIZON_URL: 'https://horizon-testnet.stellar.org',
      STELLAR_RPC_URL: 'https://soroban-testnet.stellar.org',
      DATABASE_URL: 'postgres://user:password@localhost:5432/db',
      REDIS_URL: 'redis://localhost:6379',
      BIGQUERY_PROJECT_ID: '',
      BIGQUERY_KEY_FILE: '',
      API_KEY_SALT: 'a-very-long-salt-value',
    });
  });

  it('accepts valid overrides', async () => {
    const { mod } = await loadConfig({
      NODE_ENV: 'test',
      PORT: '8080',
      LOG_LEVEL: 'debug',
      STELLAR_NETWORK: 'public',
      HORIZON_URL: 'https://horizon.stellar.org',
      STELLAR_RPC_URL: 'https://rpc.stellar.org',
      DATABASE_URL: 'postgres://user:password@localhost:5432/other',
      REDIS_URL: 'redis://redis:6379/1',
      BIGQUERY_PROJECT_ID: 'my-project',
      BIGQUERY_KEY_FILE: '/tmp/key.json',
      API_KEY_SALT: 'test-overrides-salt-1234',
    });

    expect(mod.config.PORT).toBe(8080);
    expect(mod.config.NODE_ENV).toBe('test');
    expect(mod.config.STELLAR_NETWORK).toBe('public');
    expect(mod.config.BIGQUERY_PROJECT_ID).toBe('my-project');
  });

  it('throws descriptive errors for missing required vars', async () => {
    await expect(loadConfig({})).rejects.toThrow('process.exit:1');
  });

  it('reports missing required vars to console.error', async () => {
    const exit = vi.spyOn(process, 'exit').mockImplementation(((code?: number) => {
      throw new Error(`process.exit:${code}`);
    }) as never);
    const error = vi.spyOn(console, 'error').mockImplementation(() => {});
    vi.resetModules();
    process.env = {} as any;

    await expect(import('../index')).rejects.toThrow('process.exit:1');
    expect(exit).toHaveBeenCalledWith(1);
    expect(error).toHaveBeenCalled();
    const allErrors = error.mock.calls.flat().join(' ');
    expect(allErrors).toContain('DATABASE_URL');
    expect(allErrors).toContain('API_KEY_SALT');
  });

  it('rejects invalid values (bad URL, non-numeric port, short salt)', async () => {
    await expect(
      loadConfig({
        DATABASE_URL: '',
        API_KEY_SALT: 'short',
        HORIZON_URL: 'not-a-url',
        PORT: 'abc',
      }),
    ).rejects.toThrow('process.exit:1');
  });

  it('rejects invalid enum values', async () => {
    await expect(
      loadConfig({
        DATABASE_URL: 'postgres://user:password@localhost:5432/db',
        API_KEY_SALT: 'a-very-long-salt-value',
        NODE_ENV: 'staging',
        STELLAR_NETWORK: 'devnet',
        LOG_LEVEL: 'loud',
      }),
    ).rejects.toThrow('process.exit:1');
  });
});
