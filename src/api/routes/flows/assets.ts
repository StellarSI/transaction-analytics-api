/**
 * Asset flow route (issue #24): `GET /v1/flows/assets/{asset}`.
 *
 * Returns time-bucketed volume, payment counts and net flow for a single
 * Stellar asset. The asset path parameter uses the `CODE:ISSUER` form shown
 * in the README (`USDC:GA5Z...`); the native asset can be requested as `XLM`.
 *
 * Query parameters:
 * - `interval` — `1m` (default `1h`), `1h` or `1d`.
 * - `from`     — inclusive ISO 8601 lower bound.
 * - `to`       — exclusive ISO 8601 upper bound.
 * - `limit`    — maximum number of buckets to return (default 100, max 1000).
 *
 * All inputs are validated with Zod; failures are reported as RFC 7807
 * validation problems by the global error handler.
 */

import type { FastifyInstance } from 'fastify';
import { z } from 'zod';
import { InternalError, ValidationError, zodToFields } from '../../errors';
import type { AssetFlowInterval, AssetFlowRepository } from '../../repository/asset-flows';

/** Stellar asset code, or `CODE:ISSUER`; the native asset is plain `XLM`. */
const ASSET_PATTERN = /^(?:XLM|[A-Z0-9]{1,12}:[A-Za-z0-9]{1,64})$/;

export const DEFAULT_ASSET_FLOW_LIMIT = 100;
export const MAX_ASSET_FLOW_LIMIT = 1000;

const assetParamSchema = z
  .string({ required_error: 'asset is required' })
  .regex(ASSET_PATTERN, 'asset must use the CODE:ISSUER format (or XLM for the native asset)');

const querySchema = z
  .object({
    interval: z.enum(['1m', '1h', '1d']).default('1h'),
    from: z.coerce.date().optional(),
    to: z.coerce.date().optional(),
    limit: z.coerce
      .number()
      .int()
      .min(1)
      .max(MAX_ASSET_FLOW_LIMIT)
      .default(DEFAULT_ASSET_FLOW_LIMIT),
  })
  .refine((value) => !value.from || !value.to || value.from.getTime() <= value.to.getTime(), {
    message: 'from must be earlier than or equal to to',
    path: ['from'],
  });

export interface AssetFlowRouteOptions {
  repository?: AssetFlowRepository;
}

interface ParsedAsset {
  /** The asset as requested, echoed back in the response. */
  asset: string;
  assetCode: string;
  assetIssuer: string;
}

interface ParsedQuery {
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

function parseAssetParam(asset: string): ParsedAsset {
  const result = assetParamSchema.safeParse(asset);
  if (!result.success) {
    throw new ValidationError('Invalid asset parameter', zodToFields(result.error));
  }
  return parseAsset(result.data);
}

function parseAssetQuery(query: Record<string, string>): ParsedQuery {
  const result = querySchema.safeParse(query);
  if (!result.success) {
    throw new ValidationError('Invalid query parameters', zodToFields(result.error));
  }
  return result.data;
}

export function registerAssetFlowRoutes(
  app: FastifyInstance,
  options: AssetFlowRouteOptions,
  done: (err?: Error) => void,
): void {
  const repository = options.repository;

  app.get<{ Params: { asset: string }; Querystring: Record<string, string> }>(
    '/assets/:asset',
    async (request) => {
      const { asset, assetCode, assetIssuer } = parseAssetParam(request.params.asset);
      const query = parseAssetQuery(request.query);

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
