import type { FastifyInstance } from 'fastify';
import { InternalError, UnauthorizedError } from '../errors';
import type { SubscriptionStore, SubscriptionTargetType } from '../repository/subscriptions';
import type { ApiKeyGuard } from '../../auth/middleware';
import type { RateLimitGuard } from '../../auth/rate-limit';
import { subscriptionBodySchema, validateRequest } from '../validation/schemas';
import type { z } from 'zod';

type SubscriptionBody = z.infer<typeof subscriptionBodySchema>;

export interface SubscriptionRouteOptions {
  /** Storage for subscriptions; REQUIRED in production (wired from src/index.ts). */
  store?: SubscriptionStore;
  /** Authenticates the request with an API key (issue #30). */
  requireApiKey?: ApiKeyGuard;
  /** Applies the per-API-key rate limit (issue #31). */
  rateLimit?: RateLimitGuard;
}

export function registerSubscriptionRoutes(
  app: FastifyInstance,
  options: SubscriptionRouteOptions,
  done: (err?: Error) => void,
): void {
  // eslint-disable-next-line @typescript-eslint/no-misused-promises
  app.post(
    '/subscriptions',
    { preValidation: validateRequest({ body: subscriptionBodySchema }) },
    async (request, reply) => {
      const requireApiKey = options.requireApiKey;
      if (!requireApiKey) {
        throw new InternalError('API key authentication is not configured');
      }

      const key = await requireApiKey(request);
      if (!key) {
        throw new UnauthorizedError('A valid API key is required');
      }

      if (options.rateLimit) {
        await options.rateLimit(request, reply);
      }

      const body = request.body as SubscriptionBody;
      const store = options.store;
      if (!store) {
        throw new InternalError('Subscription store is not configured');
      }

      const targetType: SubscriptionTargetType = body.target_type;
      const subscription = await store.createSubscription({
        apiKeyId: key.id,
        targetType,
        targetValue: body.target_value,
        url: body.url,
        secret: body.secret,
        events: body.events,
      });

      void reply.code(201);
      return {
        id: subscription.id,
        target_type: subscription.targetType,
        target_value: subscription.targetValue,
        url: subscription.url,
        events: subscription.events,
        created_at: subscription.createdAt,
      };
    },
  );

  done();
}
