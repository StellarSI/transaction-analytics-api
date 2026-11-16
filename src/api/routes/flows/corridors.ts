import type { FastifyInstance } from 'fastify';
import { InternalError } from '../../errors';
import type { CorridorFlowInterval, CorridorFlowRepository } from '../../repository/corridor-flows';
import { corridorFlowQuerySchema, validateRequest } from '../../validation/schemas';

export const DEFAULT_CORRIDOR_FLOW_LIMIT = 100;
export const MAX_CORRIDOR_FLOW_LIMIT = 1000;

export interface CorridorFlowRouteOptions {
  repository?: CorridorFlowRepository;
}

interface CorridorFlowQuery {
  interval: CorridorFlowInterval;
  from?: Date;
  to?: Date;
  source_anchor?: string;
  dest_anchor?: string;
  source_asset?: string;
  dest_asset?: string;
  limit: number;
  offset: number;
}

export function registerCorridorFlowRoutes(
  app: FastifyInstance,
  options: CorridorFlowRouteOptions,
  done: (err?: Error) => void,
): void {
  const repository = options.repository;

  // eslint-disable-next-line @typescript-eslint/no-misused-promises
  app.get('/corridors', {
    preValidation: validateRequest({ query: corridorFlowQuerySchema }),
  }, async (request, reply) => {
    const query = request.query as CorridorFlowQuery;

    if (!repository) {
      throw new InternalError('Corridor flow repository is not configured');
    }

    let corridors;
    try {
      corridors = await repository.getCorridorBuckets({
        interval: query.interval,
        from: query.from,
        to: query.to,
        sourceAnchor: query.source_anchor,
        destAnchor: query.dest_anchor,
        sourceAsset: query.source_asset,
        destAsset: query.dest_asset,
        limit: query.limit,
        offset: query.offset,
      });
    } catch (error) {
      request.log.error({ err: error }, 'failed to read corridor flow buckets');
      throw new InternalError('Failed to read corridor flow data');
    }

    return reply.send({
      interval: query.interval,
      corridors,
      pagination: {
        limit: query.limit,
        offset: query.offset,
      },
    });
  });

  done();
}
