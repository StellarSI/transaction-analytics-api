/** Unit tests for GET /v1/flows/corridors (issue #40). */

import { afterEach, describe, expect, it } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { createApp } from '../../server';
import type {
  CorridorBucket,
  CorridorFlowQuery,
  CorridorFlowRepository,
} from '../../repository/corridor-flows';

const ISSUER = 'GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN';
const ASSET = `USDC:${ISSUER}`;

function bucket(overrides: Partial<CorridorBucket> = {}): CorridorBucket {
  return {
    start: '2026-10-01T00:00:00Z',
    source_asset: 'XLM',
    dest_asset: ASSET,
    source_anchor: null,
    dest_anchor: ISSUER,
    volume: '184220.5500000',
    payments: 1243,
    ...overrides,
  };
}

describe('GET /v1/flows/corridors (issue #40)', () => {
  let app: FastifyInstance | undefined;
  let queries: CorridorFlowQuery[] = [];

  afterEach(async () => {
    await app?.close();
    app = undefined;
    queries = [];
  });

  async function buildApp(repository: CorridorFlowRepository): Promise<FastifyInstance> {
    const instance = await createApp({ logger: false, corridorFlowRepository: repository });
    app = instance;
    return instance;
  }

  function recordingRepository(buckets: CorridorBucket[] = []): CorridorFlowRepository {
    return {
      getCorridorBuckets(query) {
        queries.push(query);
        return Promise.resolve(buckets);
      },
    };
  }

  it('returns 200 with the documented shape and pagination', async () => {
    const instance = await buildApp(recordingRepository([bucket()]));

    const response = await instance.inject({
      method: 'GET',
      url: `/v1/flows/corridors?interval=1h&limit=5&offset=2`,
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({
      interval: '1h',
      corridors: [bucket()],
      pagination: { limit: 5, offset: 2 },
    });
    expect(queries[0]).toMatchObject({ interval: '1h', limit: 5, offset: 2 });
  });

  it('forwards all filters and needs no API key', async () => {
    const instance = await buildApp(recordingRepository([]));

    const response = await instance.inject({
      method: 'GET',
      url:
        `/v1/flows/corridors?source_asset=XLM&dest_asset=${ASSET}` +
        `&source_anchor=GSOURCE&dest_anchor=${ISSUER}&from=2026-10-01&to=2026-10-02`,
      headers: { authorization: 'Bearer sk_live_invalid' },
    });

    expect(response.statusCode).toBe(200);
    expect(queries[0]).toMatchObject({
      sourceAsset: 'XLM',
      destAsset: ASSET,
      sourceAnchor: 'GSOURCE',
      destAnchor: ISSUER,
    });
    expect(queries[0]?.from?.toISOString()).toBe('2026-10-01T00:00:00.000Z');
    expect(queries[0]?.to?.toISOString()).toBe('2026-10-02T00:00:00.000Z');
  });

  it('returns RFC 7807 problems for invalid query parameters', async () => {
    const instance = await buildApp(recordingRepository());

    const responses = await Promise.all([
      instance.inject({ method: 'GET', url: '/v1/flows/corridors?interval=1s' }),
      instance.inject({ method: 'GET', url: '/v1/flows/corridors?limit=1001' }),
      instance.inject({ method: 'GET', url: '/v1/flows/corridors?offset=-1' }),
      instance.inject({ method: 'GET', url: '/v1/flows/corridors?source_asset=USDC' }),
      instance.inject({ method: 'GET', url: '/v1/flows/corridors?unexpected=1' }),
    ]);

    for (const response of responses) {
      expect(response.statusCode).toBe(400);
      expect(response.headers['content-type']).toContain('application/problem+json');
      expect(response.json()).toMatchObject({ status: 400, code: 'validation_error' });
    }
    expect(queries).toHaveLength(0);
  });

  it('wraps repository failures in an RFC 7807 problem', async () => {
    const instance = await buildApp({
      getCorridorBuckets() {
        return Promise.reject(new Error('database unavailable'));
      },
    });

    const response = await instance.inject({ method: 'GET', url: '/v1/flows/corridors' });

    expect(response.statusCode).toBe(500);
    expect(response.json()).toMatchObject({ status: 500, code: 'internal_error' });
  });
});
