/** Unit tests for GET /v1/flows/accounts/:account (issue #40). */

import { afterEach, describe, expect, it } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { createApp } from '../../server';
import type {
  AccountFlowBucket,
  AccountFlowQuery,
  AccountFlowRepository,
} from '../../repository/account-flows';

const ACCOUNT = `G${'A'.repeat(55)}`;

function bucket(overrides: Partial<AccountFlowBucket> = {}): AccountFlowBucket {
  return {
    start: '2026-10-01T00:00:00Z',
    inflow: '250.5000000',
    outflow: '120.2500000',
    net_flow: '130.2500000',
    counterparties: [
      {
        account_id: `G${'B'.repeat(55)}`,
        volume: '250.5000000',
        count: 3,
        direction: 'in',
      },
    ],
    ...overrides,
  };
}

describe('GET /v1/flows/accounts/:account (issue #40)', () => {
  let app: FastifyInstance | undefined;
  let queries: AccountFlowQuery[] = [];

  afterEach(async () => {
    await app?.close();
    app = undefined;
    queries = [];
  });

  async function buildApp(repository: AccountFlowRepository): Promise<FastifyInstance> {
    const instance = await createApp({ logger: false, accountFlowRepository: repository });
    app = instance;
    return instance;
  }

  function recordingRepository(buckets: AccountFlowBucket[] = []): AccountFlowRepository {
    return {
      getAccountFlowBuckets(query) {
        queries.push(query);
        return Promise.resolve(buckets);
      },
    };
  }

  it('returns 200 with string-precision flows and counterparties', async () => {
    const instance = await buildApp(recordingRepository([bucket()]));

    const response = await instance.inject({
      method: 'GET',
      url: `/v1/flows/accounts/${ACCOUNT}?interval=1h`,
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({
      account: ACCOUNT,
      interval: '1h',
      buckets: [bucket()],
    });
    expect(queries[0]).toMatchObject({ accountId: ACCOUNT, interval: '1h', limit: 100 });
  });

  it('forwards from, to and limit and needs no API key', async () => {
    const instance = await buildApp(recordingRepository([]));

    const response = await instance.inject({
      method: 'GET',
      url: `/v1/flows/accounts/${ACCOUNT}?interval=1m&from=2026-10-01&to=2026-10-02&limit=7`,
      headers: { authorization: 'Bearer sk_live_invalid' },
    });

    expect(response.statusCode).toBe(200);
    expect(queries[0]?.interval).toBe('1m');
    expect(queries[0]?.limit).toBe(7);
    expect(queries[0]?.from?.toISOString()).toBe('2026-10-01T00:00:00.000Z');
    expect(queries[0]?.to?.toISOString()).toBe('2026-10-02T00:00:00.000Z');
  });

  it('returns RFC 7807 problems for invalid requests', async () => {
    const instance = await buildApp(recordingRepository());

    const responses = await Promise.all([
      instance.inject({ method: 'GET', url: '/v1/flows/accounts/not-an-account' }),
      instance.inject({
        method: 'GET',
        url: `/v1/flows/accounts/${ACCOUNT}?interval=1s`,
      }),
      instance.inject({
        method: 'GET',
        url: `/v1/flows/accounts/${ACCOUNT}?limit=1001`,
      }),
      instance.inject({
        method: 'GET',
        url: `/v1/flows/accounts/${ACCOUNT}?unexpected=1`,
      }),
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
      getAccountFlowBuckets() {
        return Promise.reject(new Error('database unavailable'));
      },
    });

    const response = await instance.inject({
      method: 'GET',
      url: `/v1/flows/accounts/${ACCOUNT}`,
    });

    expect(response.statusCode).toBe(500);
    expect(response.json()).toMatchObject({ status: 500, code: 'internal_error' });
  });
});
