/**
 * API key authentication middleware (issue #30).
 *
 * The middleware authenticates a request from its `Authorization: Bearer
 * <key>` header, looks the key up in the `api_keys` table and attaches the
 * key information to the request so downstream handlers (and the rate
 * limiter) can use it.
 *
 * - a missing or unknown key is rejected with `401`,
 * - a key without the permission a route requires is rejected with `403`,
 * - an accepted key is attached to `request.apiKey`.
 *
 * The middleware is exposed as a guard function so routes stay free of
 * Fastify plugin plumbing and stay unit-testable: a route receives the guard
 * and calls it, exactly like the other ports in this codebase.
 */

import type { FastifyRequest } from 'fastify';
import { ForbiddenError, UnauthorizedError } from '../api/errors';
import {
  authenticateApiKey,
  hasPermission,
  type ApiKeyPermission,
  type ApiKeyStore,
  type AuthenticatedApiKey,
} from './api-keys';

declare module 'fastify' {
  interface FastifyRequest {
    /** The API key that authenticated the request, when there is one. */
    apiKey?: AuthenticatedApiKey;
  }
}

export interface ApiKeyAuthOptions {
  /** Store used to look the presented key up. */
  store: ApiKeyStore;
  /** Permissions the route requires; omitted means "any valid key". */
  permissions?: ApiKeyPermission[];
}

/**
 * A route guard: resolves the API key of the request or throws.
 *
 * On success the key is attached to `request.apiKey` and returned.
 */
export type ApiKeyGuard = (
  request: FastifyRequest,
) => Promise<AuthenticatedApiKey>;

/**
 * Extracts the bearer token from the `Authorization` header.
 *
 * Returns null when the header is missing or does not carry a bearer token.
 */
export function extractBearerToken(request: FastifyRequest): string | null {
  const header = request.headers.authorization;
  if (typeof header !== 'string') {
    return null;
  }
  const match = /^Bearer\s+(\S+)$/i.exec(header.trim());
  return match ? match[1] : null;
}

/**
 * Creates the guard that authenticates a request with an API key.
 *
 * The guard throws {@link UnauthorizedError} for a missing or unknown key and
 * {@link ForbiddenError} when the key lacks a permission the route requires.
 */
export function createApiKeyGuard(options: ApiKeyAuthOptions): ApiKeyGuard {
  const required = options.permissions ?? [];

  return async (request) => {
    const token = extractBearerToken(request);
    if (!token) {
      throw new UnauthorizedError('A valid API key is required');
    }

    const apiKey = await authenticateApiKey(options.store, token);
    if (!apiKey) {
      throw new UnauthorizedError('A valid API key is required');
    }

    const missing = required.filter(
      (permission) => !hasPermission(apiKey, permission),
    );
    if (missing.length > 0) {
      throw new ForbiddenError(
        `The API key is missing the required permission: ${missing.join(', ')}`,
      );
    }

    request.apiKey = apiKey;
    return apiKey;
  };
}
