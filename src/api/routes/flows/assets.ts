/**
 * Asset flow route (issue #24): `GET /v1/flows/assets/{asset}`.
 *
 * Returns time-bucketed volume, payment counts and net flow for a single
 * Stellar asset. The asset path parameter uses the `CODE:ISSUER` form shown
 * in the README (`USDC:GA5Z...`); the native asset can be requested as `XLM`.
 *
 * Request validation (params/query, coercion, unknown-param rejection) is
 * handled by Zod schemas/middleware from `src/api/validation/schemas.ts`
 * before the handler runs (issue #32).
 */

import type { FastifyInstance } from 'fastify';
import { InternalError } from '../../errors';
import type { AssetFlowInterval, AssetFlowRepository } from '../../repository/asset-flows';
import {
  assetFlowParamsSchema,
  assetFlowQuerySchema,
  validateRequest,
} from '../../validation/schemas';

export const DEFAULT_ASSET_FLOW_LIMIT = 100;
export const MAX_ASSET_FLOW_LIMIT = 1000;

export interface AssetFlowRouteOptions {
  repository?: AssetFlowRepository;
}

interface ParsedAsset {
  /** The asset as requested, echoed back in the response. */
  asset: string;
  assetCode: string;
  assetIssuer: string;
}

interface AssetFlowQuery {
  interval: AssetFlowInterval;
  from?: Date;
  to?: Date;
  limit: number;
}

/** Splits a validated `CODE:ISSUER` parameter; `XLM` maps to an empty issuer. */
export function parseAsset(asset: string): ParsedAsset {
  if (asset === 'XLM') {
    return { asset, assetCode: 'XLM', assetIssuer: '' };
  }
  const separator = asset.indexOf(':');
  return {
    asset,
    assetCode: asset.slice(0, separator),
    assetIssuer: asset.slice(separator + 1),
  };
}

export function registerAssetFlowRoutes(
  app: FastifyInstance,
  options: AssetFlowRouteOptions,
  done: (err?: Error) => void,
): void {
  const repository = options.repository;

  app.get<{ Params: { asset: string }; Querystring: Record<string, unknown> }>(
    '/assets/:asset',
    {
      preValidation: validateRequest({
        params: assetFlowParamsSchema,
        query: assetFlowQuerySchema,
      }),
    },
    async (request) => {
      const { asset, assetCode, assetIssuer } = parseAsset(
        (request.params as { asset: string }).asset,
      );
      const query = request.query as unknown as AssetFlowQuery;

      if (!repository) {
        throw new InternalError('Asset flow repository is not configured');
      }

      const buckets = await repository.getAssetFlowBuckets({
        assetCode,
        assetIssuer,
        interval: query.interval,
        from: query.from,
        to: query.to,
        limit: query.limit,
      });

      return { asset, interval: query.interval, buckets };
    },
  );

  done();
}
