/**
 * Per-API-key rate limiting (issue #31).
 *
 * Every API key has a tier, and every tier has a request budget per minute:
 *
 * | Tier        | Requests / minute |
 * | ----------- | ----------------- |
 * | `free`      | 100               |
 * | `pro`       | 1 000             |
 * | `enterprise`| 10 000            |
 *
 * The counter lives in Redis, keyed by API key id and window, so the budget
 * is shared by every instance of the API. Responses carry the standard
 * `X-RateLimit-Limit`, `X-RateLimit-Remaining` and `X-RateLimit-Reset`
 * headers; a request that exhausts its budget is rejected with `429` and a
 * `Retry-After` header.
 *
 * The limiter is exposed as a small port so the guard can be unit-tested
 * without Redis; the production implementation is a thin adapter over the
 * shared ioredis client.
 */

import type { FastifyReply, FastifyRequest } from 'fastify';
import { RateLimitError } from '../api/errors';
import type { ApiKeyTier } from './api-keys';

/** Length of the rate limit window, in seconds. */
export const RATE_LIMIT_WINDOW_SECONDS = 60;

/** Request budgets per minute, per tier. */
export const RATE_LIMIT_TIERS: Readonly<Record<ApiKeyTier, number>> = {
  free: 100,
  pro: 1_000,
  enterprise: 10_000,
};

/** Response headers set by the limiter. */
export const RATE_LIMIT_HEADERS = {
  limit: 'x-ratelimit-limit',
  remaining: 'x-ratelimit-remaining',
  reset: 'x-ratelimit-reset',
  retryAfter: 'retry-after',
} as const;

/** The outcome of consuming one request from a key's budget. */
export interface RateLimitDecision {
  /** False when the request exhausted the budget. */
  allowed: boolean;
  /** Budget of the tier, in requests per window. */
  limit: number;
  /** Requests left in the current window. */
  remaining: number;
  /** Unix timestamp (seconds) at which the current window ends. */
  resetAt: number;
  /** Seconds until the window resets; reported as `Retry-After`. */
  retryAfter: number;
}

export interface RateLimiter {
  /**
   * Consumes one request from the budget of `keyId`.
   *
   * `now` is injectable so the window boundaries can be tested.
   */
  consume(
    keyId: string,
    tier: ApiKeyTier,
    now?: number,
  ): Promise<RateLimitDecision>;
}

/**
 * Minimal Redis surface used by the limiter.
 *
 * The shared ioredis client satisfies it structurally.
 */
export interface RateLimitRedis {
  incr(key: string): Promise<number>;
  expire(key: string, seconds: number, mode?: 'NX'): Promise<number>;
}

export interface RateLimiterOptions {
  /** Window length in seconds; defaults to {@link RATE_LIMIT_WINDOW_SECONDS}. */
  windowSeconds?: number;
  /** Prefix of the Redis keys; defaults to `ratelimit`. */
  keyPrefix?: string;
}

/**
 * Creates a fixed-window limiter backed by Redis.
 *
 * The counter key contains the window index, so a new window starts with a
 * fresh budget. The TTL is only set once per window (`EXPIRE ... NX`), which
 * keeps the key from outliving its window and avoids a crash between `INCR`
 * and `EXPIRE` leaking the key forever.
 */
export function createRedisRateLimiter(
  redis: RateLimitRedis,
  options: RateLimiterOptions = {},
): RateLimiter {
  const windowSeconds = options.windowSeconds ?? RATE_LIMIT_WINDOW_SECONDS;
  const keyPrefix = options.keyPrefix ?? 'ratelimit';
  const windowMs = windowSeconds * 1000;

  return {
    async consume(keyId, tier, now = Date.now()) {
      const limit = RATE_LIMIT_TIERS[tier] ?? RATE_LIMIT_TIERS.free;
      const window = Math.floor(now / windowMs);
      const key = `${keyPrefix}:${keyId}:${window}`;

      const count = await redis.incr(key);
      await redis.expire(key, windowSeconds, 'NX');

      const resetAt = Math.ceil(((window + 1) * windowMs) / 1000);
      const retryAfter = Math.max(1, resetAt - Math.floor(now / 1000));

      return {
        allowed: count <= limit,
        limit,
        remaining: Math.max(0, limit - count),
        resetAt,
        retryAfter,
      };
    },
  };
}

/**
 * A route guard: consumes the request budget of the authenticated key.
 *
 * Requests without an API key are not counted — the limiter protects API
 * keys, and unauthenticated routes are protected by their own means.
 */
export type RateLimitGuard = (
  request: FastifyRequest,
  reply: FastifyReply,
) => Promise<void>;

export interface RateLimitGuardOptions {
  limiter: RateLimiter;
}

/**
 * Creates the guard that applies the per-API-key rate limit.
 *
 * The guard sets the rate limit headers on every response and rejects a
 * request that exhausted its budget with `429` and a `Retry-After` header.
 */
export function createRateLimitGuard(
  options: RateLimitGuardOptions,
): RateLimitGuard {
  return async (request, reply) => {
    const apiKey = request.apiKey;
    if (!apiKey) {
      return;
    }

    const decision = await options.limiter.consume(apiKey.id, apiKey.tier);

    void reply.header(RATE_LIMIT_HEADERS.limit, decision.limit);
    void reply.header(RATE_LIMIT_HEADERS.remaining, decision.remaining);
    void reply.header(RATE_LIMIT_HEADERS.reset, decision.resetAt);

    if (!decision.allowed) {
      void reply.header(RATE_LIMIT_HEADERS.retryAfter, decision.retryAfter);
      throw new RateLimitError(
        `Rate limit exceeded: ${decision.limit} requests per window`,
      );
    }
  };
}
