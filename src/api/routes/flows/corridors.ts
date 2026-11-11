/**
 * Corridor flow route (issue #26): `GET /v1/flows/corridors`.
 *
 * Returns time-bucketed corridor aggregates (flows between asset pairs/anchors).
 *
 * Query parameters:
 * - `interval`      — `1m`, `1h` (default) or `1d`.
 * - `from`          — inclusive ISO 8601 lower bound.
 * - `to`            — exclusive ISO 8601 upper bound.
 * - `source_anchor` — filter by source anchor issuer.
 * - `dest_anchor`   — filter by destination anchor issuer.
 * - `source_asset`  — filter by source asset key (`XLM` or `CODE:ISSUER`).
 * - `dest_asset`    — filter by destination asset key.
 * - `limit`         — maximum number of corridors to return (default 100, max 1000).
 * - `offset`        — pagination offset (default 0).
 *
 * All inputs are validated with Zod; failures are reported as RFC 7807
 * validation problems by the global error handler.
 */

import type { FastifyInstance } from 'fastify';
import { z } from 'zod';
import { InternalError, ValidationError, zodToFields } from '../../errors';
import type { CorridorFlowInterval, CorridorFlowRepository } from '../../repository/corridor-flows';

const ASSET_PATTERN = /^(?:XLM|[A-Z0-9]{1,12}:[A-Za-z0-9]{1,64})$/;

export const DEFAULT_CORRIDOR_FLOW_LIMIT = 100;
export const MAX_CORRIDOR_FLOW_LIMIT = 1000;

const optionalAsset = z
  .string()
  .regex(ASSET_PATTERN, 'asset must use the CODE:ISSUER format (or XLM for the native asset)')
  .optional();

const querySchema = z
  .object({
    interval: z.enum(['1m', '1h', '1d']).default('1h'),
    from: z.coerce.date().optional(),
    to: z.coerce.date().optional(),
    source_anchor: z.string().min(1).optional(),
    dest_anchor: z.string().min(1).optional(),
    source_asset: optionalAsset,
    dest_asset: optionalAsset,
    limit: z.coerce
      .number()
      .int()
      .min(1)
      .max(MAX_CORRIDOR_FLOW_LIMIT)
      .default(DEFAULT_CORRIDOR_FLOW_LIMIT),
    offset: z.coerce.number().int().min(0).default(0),
  })
  .refine((value) => !value.from || !value.to || value.from.getTime() <= value.to.getTime(), {
    message: '`from` must be before or equal to `to`',
    path: ['from'],
  });

export interface RegisterCorridorFlowRoutesOptions {
  repository: CorridorFlowRepository;
}

export function registerCorridorFlowRoutes(
  app: FastifyInstance,
  options: RegisterCorridorFlowRoutesOptions,
): void {
  app.get('/flows/corridors', async (request, reply) => {
    const query = querySchema.safeParse(request.query);
    if (!query.success) {
      throw new ValidationError('Invalid corridor flow query', zodToFields(query.error));
    }

    let corridors;
    try {
      corridors = await options.repository.getCorridorBuckets({
        interval: query.data.interval as CorridorFlowInterval,
        from: query.data.from,
        to: query.data.to,
        sourceAnchor: query.data.source_anchor,
        destAnchor: query.data.dest_anchor,
        sourceAsset: query.data.source_asset,
        destAsset: query.data.dest_asset,
        limit: query.data.limit,
        offset: query.data.offset,
      });
    } catch (error) {
      request.log.error({ err: error }, 'failed to read corridor flow buckets');
      throw new InternalError('Failed to read corridor flow data');
    }

    return reply.send({
      interval: query.data.interval,
      corridors,
      pagination: {
        limit: query.data.limit,
        offset: query.data.offset,
      },
    });
  });
}
