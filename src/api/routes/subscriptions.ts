/**
 * Subscription route (issue #28): `POST /v1/subscriptions`.
 *
 * Registers a webhook for an account, an asset or a threshold. The body is
 * validated with Zod (`url` must be HTTPS; `target_value` must match the
 * format of its `target_type`), and each subscription is stored against the
 * authenticated API key so a key only sees its own subscriptions.
 *
 * Body:
 * ```json
 * {
 *   "target_type": "account" | "asset" | "threshold",
 *   "target_value": "G..." | "XLM" | "USDC:GA5Z..." | "1000000.5",
 *   "url": "https://example.com/webhook",
 *   "secret": "optional signing secret, min 16 chars",
 *   "events": ["account_credited"]
 * }
 * ```
 *
 * ## Authentication
 *
 * The route is protected by the API key middleware (issue #30): the guard
 * verifies the presented key against the `api_keys` table and attaches the
 * key to the request. The per-key rate limit (issue #31) is applied on top
 * of it. Both are injected by `src/api/server.ts`, which builds them from
 * the configured store and limiter.
 */

import type { FastifyInstance } from 'fastify';
import { z } from 'zod';
import { InternalError, UnauthorizedError, ValidationError, zodToFields } from '../errors';
import type {
  SubscriptionStore,
  SubscriptionTargetType,
} from '../repository/subscriptions';
import type { ApiKeyGuard } from '../../auth/middleware';

/** Stellar account id (G....). */
const STELLAR_ACCOUNT = /^G[A-Z2-7]{55}$/;
/** `XLM` or `CODE:ISSUER`. */
const STELLAR_ASSET = /^(?:XLM|[A-Za-z0-9]{1,12}:[A-Za-z0-9]{2,64})$/;
/** Positive decimal, up to 7 fraction digits (Stellar stroops precision). */
const THRESHOLD = /^\d+(\.\d{1,7})?$/;
/** Webhook event names: lowercase dotted identifiers. */
const EVENT_NAME = /^[a-z0-9_.]{1,64}$/;

const httpsUrl = z
  .string()
  .url('url must be a valid URL')
  .max(2048, 'url is too long')
  .refine(
    (value) => {
      try {
        return new URL(value).protocol === 'https:';
      } catch {
        return false;
      }
    },
    { message: 'url must use https' },
  );

const baseFields = {
  url: httpsUrl,
  secret: z.string().min(16, 'secret must be at least 16 characters').max(256).optional(),
  events: z.array(z.string().regex(EVENT_NAME, 'event names must be lowercase dotted identifiers')).max(32).optional().default([]),
};

const bodySchema = z.discriminatedUnion('target_type', [
  z.object({
    target_type: z.literal('account'),
    target_value: z.string().regex(STELLAR_ACCOUNT, 'account must be a Stellar account id (G...)'),
    ...baseFields,
  }),
  z.object({
    target_type: z.literal('asset'),
    target_value: z.string().regex(STELLAR_ASSET, 'asset must be XLM or CODE:ISSUER'),
    ...baseFields,
  }),
  z.object({
    target_type: z.literal('threshold'),
    target_value: z.string().regex(THRESHOLD, 'threshold must be a positive decimal'),
    ...baseFields,
  }),
]);

type SubscriptionBody = z.infer<typeof bodySchema>;

export interface SubscriptionRouteOptions {
  /** Storage for subscriptions; REQUIRED in production (wired from src/index.ts). */
  store?: SubscriptionStore;
  /** Authenticates the request with an API key (issue #30). */
  requireApiKey?: ApiKeyGuard;
}

function parseBody(body: unknown): SubscriptionBody {
  const result = bodySchema.safeParse(body);
  if (!result.success) {
    throw new ValidationError('Invalid subscription payload', zodToFields(result.error));
  }
  return result.data;
}

export function registerSubscriptionRoutes(
  app: FastifyInstance,
  options: SubscriptionRouteOptions,
  done: (err?: Error) => void,
): void {
  app.post('/subscriptions', async (request, reply) => {
    const requireApiKey = options.requireApiKey;
    if (!requireApiKey) {
      throw new InternalError('API key authentication is not configured');
    }

    const key = await requireApiKey(request);
    if (!key) {
      throw new UnauthorizedError('A valid API key is required');
    }

    const body = parseBody(request.body);
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

    reply.code(201);
    return {
      id: subscription.id,
      target_type: subscription.targetType,
      target_value: subscription.targetValue,
      url: subscription.url,
      events: subscription.events,
      created_at: subscription.createdAt,
    };
  });

  done();
}
