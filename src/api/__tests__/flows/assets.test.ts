import { describe, it, expect, afterEach } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { createApp } from '../../server';
import type {
  AssetFlowBucket,
  AssetFlowQuery,
  AssetFlowRepository,
} from '../../repository/asset-flows';

const ISSUER = 'GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN';
const ASSET = `USDC:${ISSUER}`;

function bucket(overrides: Partial<AssetFlowBucket> = {}): AssetFlowBucket {
  return {
    start: '2026-10-01T00:00:00Z',
    volume: '184220.55',
    payments: 1243,
    net_flow: '-1200.00',
    ...overrides,
  };
}

describe('GET /v1/flows/assets/:asset (issue #24)', () => {
  let app: FastifyInstance | undefined;
  let queries: AssetFlowQuery[] = [];

  afterEach(async () => {
    await app?.close();
    app = undefined;
    queries = [];
  });

  async function buildApp(repository: AssetFlowRepository): Promise<FastifyInstance> {
    const instance = await createApp({ logger: false, repository });
    app = instance;
    return instance;
  }

  function recordingRepository(buckets: AssetFlowBucket[] = []): AssetFlowRepository {
    return {
      getAssetFlowBuckets(query) {
        queries.push(query);
        return Promise.resolve(buckets);
      },
    };
  }

  it('returns buckets for a valid asset', async () => {
    const instance = await buildApp(recordingRepository([bucket()]));

    const response = await instance.inject({
      method: 'GET',
      url: `/v1/flows/assets/${ASSET}?interval=1h`,
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({
      asset: ASSET,
      interval: '1h',
      buckets: [bucket()],
    });
    expect(queries).toHaveLength(1);
    expect(queries[0]).toMatchObject({
      assetCode: 'USDC',
      assetIssuer: ISSUER,
      interval: '1h',
      limit: 100,
    });
  });

  it('supports the native XLM asset without an issuer', async () => {
    const instance = await buildApp(recordingRepository([]));

    const response = await instance.inject({ method: 'GET', url: '/v1/flows/assets/XLM' });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({ asset: 'XLM', interval: '1h', buckets: [] });
    expect(queries[0]).toMatchObject({ assetCode: 'XLM', assetIssuer: '' });
  });

  it('passes from, to and limit through to the repository', async () => {
    const instance = await buildApp(recordingRepository([]));

    const response = await instance.inject({
      method: 'GET',
      url: `/v1/flows/assets/${ASSET}?interval=1m&from=2026-10-01&to=2026-10-02&limit=5`,
    });

    expect(response.statusCode).toBe(200);
    expect(queries[0]).toMatchObject({
      assetCode: 'USDC',
      assetIssuer: ISSUER,
      interval: '1m',
      limit: 5,
    });
    expect(queries[0].from?.toISOString()).toBe('2026-10-01T00:00:00.000Z');
    expect(queries[0].to?.toISOString()).toBe('2026-10-02T00:00:00.000Z');
  });

  it('rejects an asset without an issuer', async () => {
    const instance = await buildApp(recordingRepository());

    const response = await instance.inject({ method: 'GET', url: '/v1/flows/assets/USDC' });

    expect(response.statusCode).toBe(400);
    expect(response.headers['content-type']).toContain('application/problem+json');
    expect(response.json()).toMatchObject({ status: 400, code: 'validation_error' });
    expect(queries).toHaveLength(0);
  });

  it('rejects an unsupported interval', async () => {
    const instance = await buildApp(recordingRepository());

    const response = await instance.inject({
      method: 'GET',
      url: `/v1/flows/assets/${ASSET}?interval=1s`,
    });

    expect(response.statusCode).toBe(400);
    expect(response.json()).toMatchObject({ status: 400, code: 'validation_error' });
  });

  it('rejects a limit above the maximum', async () => {
    const instance = await buildApp(recordingRepository());

    const response = await instance.inject({
      method: 'GET',
      url: `/v1/flows/assets/${ASSET}?limit=1001`,
    });

    expect(response.statusCode).toBe(400);
    expect(response.json()).toMatchObject({ status: 400, code: 'validation_error' });
  });

  it('rejects a from bound after the to bound', async () => {
    const instance = await buildApp(recordingRepository());

    const response = await instance.inject({
      method: 'GET',
      url: `/v1/flows/assets/${ASSET}?from=2026-10-02&to=2026-10-01`,
    });

    expect(response.statusCode).toBe(400);
    expect(response.json()).toMatchObject({ status: 400, code: 'validation_error' });
  });

  it('returns an empty bucket list when there is no data', async () => {
    const instance = await buildApp(recordingRepository([]));

    const response = await instance.inject({ method: 'GET', url: `/v1/flows/assets/${ASSET}` });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({ asset: ASSET, interval: '1h', buckets: [] });
  });

  it('maps repository failures to RFC 7807 problems', async () => {
    const instance = await buildApp({
      getAssetFlowBuckets() {
        return Promise.reject(new Error('database unavailable'));
      },
    });

    const response = await instance.inject({ method: 'GET', url: `/v1/flows/assets/${ASSET}` });

    expect(response.statusCode).toBe(500);
    expect(response.headers['content-type']).toContain('application/problem+json');
    expect(response.json()).toMatchObject({ status: 500, code: 'internal_error' });
  });
});
