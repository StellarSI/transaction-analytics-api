/** Unit tests for GET /v1/flows/assets/:asset (issue #40). */

import { afterEach, describe, expect, it } from 'vitest';
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
    volume: '184220.5500000',
    payments: 1243,
    net_flow: '-1200.0000000',
    ...overrides,
  };
}

describe('GET /v1/flows/assets/:asset (issue #40)', () => {
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

  it('returns 200 with the documented shape and forwards the query', async () => {
    const instance = await buildApp(recordingRepository([bucket()]));

    const response = await instance.inject({
      method: 'GET',
      url: `/v1/flows/assets/${ASSET}?interval=1h&limit=5`,
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({
      asset: ASSET,
      interval: '1h',
      buckets: [bucket()],
    });
    expect(queries[0]).toMatchObject({
      assetCode: 'USDC',
      assetIssuer: ISSUER,
      interval: '1h',
      limit: 5,
    });
  });

  it('supports XLM without an issuer and needs no API key', async () => {
    const instance = await buildApp(recordingRepository([]));

    const response = await instance.inject({
      method: 'GET',
      url: '/v1/flows/assets/XLM',
      headers: { authorization: 'Bearer sk_live_invalid' },
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({ asset: 'XLM', interval: '1h', buckets: [] });
    expect(queries[0]).toMatchObject({ assetCode: 'XLM', assetIssuer: '' });
  });

  it('returns an RFC 7807 problem for invalid parameters', async () => {
    const instance = await buildApp(recordingRepository());

    const invalid = await instance.inject({ method: 'GET', url: '/v1/flows/assets/USDC' });
    const badInterval = await instance.inject({
      method: 'GET',
      url: `/v1/flows/assets/${ASSET}?interval=1s`,
    });
    const badLimit = await instance.inject({
      method: 'GET',
      url: `/v1/flows/assets/${ASSET}?limit=1001`,
    });
    const unknown = await instance.inject({
      method: 'GET',
      url: `/v1/flows/assets/${ASSET}?surprise=1`,
    });

    for (const response of [invalid, badInterval, badLimit, unknown]) {
      expect(response.statusCode).toBe(400);
      expect(response.headers['content-type']).toContain('application/problem+json');
      expect(response.json()).toMatchObject({ status: 400, code: 'validation_error' });
    }
    expect(queries).toHaveLength(0);
  });

  it('maps repository failures to RFC 7807 problems', async () => {
    const instance = await buildApp({
      getAssetFlowBuckets() {
        return Promise.reject(new Error('database unavailable'));
      },
    });

    const response = await instance.inject({ method: 'GET', url: `/v1/flows/assets/${ASSET}` });

    expect(response.statusCode).toBe(500);
    expect(response.json()).toMatchObject({ status: 500, code: 'internal_error' });
  });
});
