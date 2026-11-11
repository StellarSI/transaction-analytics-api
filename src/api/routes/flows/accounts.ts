/**
 * Account flow route (issue #25): `GET /v1/flows/accounts/{account}`.
 *
 * Returns time-bucketed inflow, outflow, net flow and top counterparties for a
 * single Stellar account.
 *
 * Query parameters:
 * - `interval` — `1m`, `1h` (default) or `1d`.
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
import type { AccountFlowInterval, AccountFlowRepository } from '../../repository/account-flows';

const STELLAR_ACCOUNT_PATTERN = /^G[A-Z2-7]{55}$/;

export const DEFAULT_ACCOUNT_FLOW_LIMIT = 100;
export const MAX_ACCOUNT_FLOW_LIMIT = 1000;

const accountParamSchema = z
  .string({ required_error: 'account is required' })
  .regex(STELLAR_ACCOUNT_PATTERN, 'account must be a valid Stellar account ID (G...)');

const querySchema = z
  .object({
    interval: z.enum(['1m', '1h', '1d']).default('1h'),
    from: z.coerce.date().optional(),
    to: z.coerce.date().optional(),
    limit: z.coerce
      .number()
      .int()
      .min(1)
      .max(MAX_ACCOUNT_FLOW_LIMIT)
      .default(DEFAULT_ACCOUNT_FLOW_LIMIT),
  })
  .refine((value) => !value.from || !value.to || value.from.getTime() <= value.to.getTime(), {
    message: '`from` must be before or equal to `to`',
    path: ['from'],
  });

export interface AccountFlowRouteOptions {
  repository?: AccountFlowRepository;
}

export function registerAccountFlowRoutes(
  app: FastifyInstance,
  options: AccountFlowRouteOptions,
  done: (err?: Error) => void,
): void {
  const repository = options.repository;

  app.get<{ Params: { account: string } }>('/accounts/:account', async (request, reply) => {
    const params = accountParamSchema.safeParse((request.params as { account?: string }).account);
    if (!params.success) {
      throw new ValidationError('Invalid account identifier', zodToFields(params.error));
    }

    const query = querySchema.safeParse(request.query);
    if (!query.success) {
      throw new ValidationError('Invalid account flow query', zodToFields(query.error));
    }

    if (!repository) {
      throw new InternalError('Account flow repository is not configured');
    }

    let buckets;
    try {
      buckets = await repository.getAccountFlowBuckets({
        accountId: params.data,
        interval: query.data.interval as AccountFlowInterval,
        from: query.data.from,
        to: query.data.to,
        limit: query.data.limit,
      });
    } catch (error) {
      request.log.error({ err: error, account: params.data }, 'failed to read account flow buckets');
      throw new InternalError('Failed to read account flow data');
    }

    return reply.send({
      account: params.data,
      interval: query.data.interval,
      buckets,
    });
  });

  done();
}
