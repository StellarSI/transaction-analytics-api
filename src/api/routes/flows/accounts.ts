import type { FastifyInstance } from 'fastify';
import { InternalError } from '../../errors';
import type { AccountFlowInterval, AccountFlowRepository } from '../../repository/account-flows';
import {
  accountFlowParamsSchema,
  accountFlowQuerySchema,
  validateRequest,
} from '../../validation/schemas';

export const DEFAULT_ACCOUNT_FLOW_LIMIT = 100;
export const MAX_ACCOUNT_FLOW_LIMIT = 1000;

export interface AccountFlowRouteOptions {
  repository?: AccountFlowRepository;
}

interface AccountFlowQuery {
  interval: AccountFlowInterval;
  from?: Date;
  to?: Date;
  limit: number;
}

export function registerAccountFlowRoutes(
  app: FastifyInstance,
  options: AccountFlowRouteOptions,
  done: (err?: Error) => void,
): void {
  const repository = options.repository;

  // eslint-disable-next-line @typescript-eslint/no-misused-promises
  app.get<{ Params: { account: string } }>('/accounts/:account', {
    preValidation: validateRequest({
      params: accountFlowParamsSchema,
      query: accountFlowQuerySchema,
    }),
  }, async (request, reply) => {
    const params = request.params as { account: string };
    const query = request.query as unknown as AccountFlowQuery;

    if (!repository) {
      throw new InternalError('Account flow repository is not configured');
    }

    let buckets;
    try {
      buckets = await repository.getAccountFlowBuckets({
        accountId: params.account,
        interval: query.interval,
        from: query.from,
        to: query.to,
        limit: query.limit,
      });
    } catch (error) {
      request.log.error({ err: error, account: params.account }, 'failed to read account flow buckets');
      throw new InternalError('Failed to read account flow data');
    }

    return reply.send({
      account: params.account,
      interval: query.interval,
      buckets,
    });
  });

  done();
}
