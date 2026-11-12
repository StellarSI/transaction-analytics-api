import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import type { FastifyInstance } from 'fastify';
import { pool } from '@/db/pool';
import { runMigrations } from '@/db/migrate';
import { createApp } from '@/api/server';
import {
  createPostgresAssetFlowRepository,
  type AssetFlowSql,
} from '@/api/repository/asset-flows';

/**
 * Integration coverage for `GET /v1/flows/assets/:asset` (issue #24).
 *
 * Requires the TimescaleDB instance provided by CI (DATABASE_URL). Minute
 * buckets are inserted into the base hypertable; the hourly/daily continuous
 * aggregates are refreshed explicitly so the test does not depend on the
 * refresh policy's schedule.
 */

const ASSET_CODE = 'TSTU';
const ASSET_ISSUER = 'GB5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN';
const ASSET = `${ASSET_CODE}:${ASSET_ISSUER}`;

const MINUTE_ROWS = [
  { bucket_start: '2026-10-01T00:00:00Z', volume: 100.5, payments: 10, net_flow: 5 },
  { bucket_start: '2026-10-01T00:01:00Z', volume: 200.25, payments: 20, net_flow: -5 },
  { bucket_start: '2026-10-01T01:00:00Z', volume: 50, payments: 5, net_flow: 0 },
];

describe('GET /v1/flows/assets/:asset (integration)', () => {
  let app: FastifyInstance;

  beforeAll(async () => {
    await runMigrations('up');
    await pool`DELETE FROM asset_flow_aggregates WHERE asset_code = ${ASSET_CODE} AND asset_issuer = ${ASSET_ISSUER}`;
    await pool`
      INSERT INTO asset_flow_aggregates
        (bucket_start, interval, asset_code, asset_issuer, volume, payments, net_flow)
      VALUES
        (${MINUTE_ROWS[0].bucket_start}, '1m', ${ASSET_CODE}, ${ASSET_ISSUER}, ${MINUTE_ROWS[0].volume}, ${MINUTE_ROWS[0].payments}, ${MINUTE_ROWS[0].net_flow}),
        (${MINUTE_ROWS[1].bucket_start}, '1m', ${ASSET_CODE}, ${ASSET_ISSUER}, ${MINUTE_ROWS[1].volume}, ${MINUTE_ROWS[1].payments}, ${MINUTE_ROWS[1].net_flow}),
        (${MINUTE_ROWS[2].bucket_start}, '1m', ${ASSET_CODE}, ${ASSET_ISSUER}, ${MINUTE_ROWS[2].volume}, ${MINUTE_ROWS[2].payments}, ${MINUTE_ROWS[2].net_flow})
    `;
    await pool`CALL refresh_continuous_aggregate('asset_flow_aggregates_1h', NULL, NULL)`;
    await pool`CALL refresh_continuous_aggregate('asset_flow_aggregates_1d', NULL, NULL)`;

    app = await createApp({
      logger: false,
      repository: createPostgresAssetFlowRepository(pool as unknown as AssetFlowSql),
      // The health route is covered by unit tests; keep this suite focused on
      // the asset flow read model and independent of Redis, Horizon, the
      // Soroban RPC and the ingestion lag calculation.
      checkDatabase: async () => ({ ok: true, latencyMs: 1, detail: 'postgres reachable' }),
      checkRedis: async () => ({ ok: true, latencyMs: 1, detail: 'redis reachable' }),
      checkHorizon: async () => ({
        ok: true,
        latencyMs: 1,
        detail: 'horizon reachable',
        latest_ledger: 10,
      }),
      checkRpc: async () => ({
        ok: true,
        latencyMs: 1,
        detail: 'stellar rpc reachable',
        latest_ledger: 10,
      }),
      getLatestLedger: async () => 10,
    });
  }, 60000);

  afterAll(async () => {
    await app?.close();
    await pool`DELETE FROM asset_flow_aggregates WHERE asset_code = ${ASSET_CODE} AND asset_issuer = ${ASSET_ISSUER}`;
    await pool`CALL refresh_continuous_aggregate('asset_flow_aggregates_1h', NULL, NULL)`;
    await pool`CALL refresh_continuous_aggregate('asset_flow_aggregates_1d', NULL, NULL)`;
    await pool.end();
  });

  it('returns minute buckets from the base hypertable', async () => {
    const response = await app.inject({
      method: 'GET',
      url: `/v1/flows/assets/${ASSET}?interval=1m&from=2026-10-01T00:00:00Z&to=2026-10-01T02:00:00Z`,
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({
      asset: ASSET,
      interval: '1m',
      buckets: [
        { start: '2026-10-01T00:00:00Z', volume: '100.5', payments: 10, net_flow: '5' },
        { start: '2026-10-01T00:01:00Z', volume: '200.25', payments: 20, net_flow: '-5' },
        { start: '2026-10-01T01:00:00Z', volume: '50', payments: 5, net_flow: '0' },
      ],
    });
  });

  it('returns hourly buckets from the continuous aggregate', async () => {
    const response = await app.inject({
      method: 'GET',
      url: `/v1/flows/assets/${ASSET}?interval=1h&from=2026-10-01T00:00:00Z&to=2026-10-02T00:00:00Z`,
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({
      asset: ASSET,
      interval: '1h',
      buckets: [
        { start: '2026-10-01T00:00:00Z', volume: '300.75', payments: 30, net_flow: '0' },
        { start: '2026-10-01T01:00:00Z', volume: '50', payments: 5, net_flow: '0' },
      ],
    });
  });

  it('returns daily buckets from the continuous aggregate', async () => {
    const response = await app.inject({
      method: 'GET',
      url: `/v1/flows/assets/${ASSET}?interval=1d&from=2026-10-01T00:00:00Z&to=2026-10-03T00:00:00Z`,
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({
      asset: ASSET,
      interval: '1d',
      buckets: [{ start: '2026-10-01T00:00:00Z', volume: '350.75', payments: 35, net_flow: '0' }],
    });
  });

  it('applies the limit parameter', async () => {
    const response = await app.inject({
      method: 'GET',
      url: `/v1/flows/assets/${ASSET}?interval=1m&limit=2`,
    });

    expect(response.statusCode).toBe(200);
    expect(response.json().buckets).toHaveLength(2);
  });

  it('returns an empty list for an unknown asset', async () => {
    const response = await app.inject({
      method: 'GET',
      url: '/v1/flows/assets/USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN?interval=1m',
    });

    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({
      asset: 'USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN',
      interval: '1m',
      buckets: [],
    });
  });

  it('rejects an invalid asset format', async () => {
    const response = await app.inject({ method: 'GET', url: '/v1/flows/assets/USDC' });

    expect(response.statusCode).toBe(400);
    expect(response.headers['content-type']).toContain('application/problem+json');
    expect(response.json()).toMatchObject({ status: 400, code: 'validation_error' });
  });
});
