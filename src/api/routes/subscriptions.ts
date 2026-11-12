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
 * ## API key authentication — temporary stand-in, TODO(#30)
 *
 * Issue #30 (`src/auth/api-keys.ts`, `src/auth/middleware.ts`) will ship the
 * real API key middleware: `sk_live_` keys hashed with argon2 against the
 * configured `API_KEY_SALT`, stored in an `api_keys` table, looked up on
 * every request. Until that merges, {@link defaultRequireApiKey} is a
 * **stand-in**: it reads `Authorization: Bearer <token>`, hashes the token
 * with `API_KEY_SALT`, and uses the digest as an opaque tenant id attached
 * to the subscription row. It performs NO verification that the key exists or
 * is authorized; it only namespaces subscriptions to the presented token.
 *
 * The dependency is explicit at the wiring boundary via the `requireApiKey`
 * option — swap in the real middleware there (or set `options.requireApiKey`)
 * without touching this route.
 */

import { createHash } from 'node:crypto';
import type { FastifyInstance, FastifyRequest } from 'fastify';
import { z } from 'zod';
import { config } from '../../config';
import { InternalError, UnauthorizedError, ValidationError, zodToFields } from '../../errors';
import type {
  SubscriptionStore,
  SubscriptionTargetType,
} from '../../repository/subscriptions';

export interface AuthenticatedApiKey {
  /** Opaque identifier of the authenticated API key (stored on the row). */
  id: string;
}

export type RequireApiKey = (request: FastifyRequest) => Promise<AuthenticatedApiKey | null>;

/**
 * TEMPORARY stand-in for the #30 middleware (read the module docstring).
 * Returns null unless the request carries a `sk_live_`-shaped Bearer token;
 * the returned `id` is a salted digest of the token, not the token itself.
 */
export const defaultRequireApiKey: RequireApiKey = async (request) => {
  const header = request.headers.authorization;
  const match = typeof header === 'string' ? /^Bearer ([A-Za-z0-9_-]{16,512})$/.exec(header) : null;
  if (!match) {
    return null;
  }
  const digest = createHash('sha256')
    .update(`${config.API_KEY_SALT}:${match[1]}`)
    .digest('hex');
  return { id: digest };
};

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
  /** Override the API key guard (defaults to the temporary stand-in, see TODO(#30)). */
  requireApiKey?: RequireApiKey;
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
  const requireApiKey = options.requireApiKey ?? defaultRequireApiKey;

  app.post('/subscriptions', async (request, reply) => {
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
