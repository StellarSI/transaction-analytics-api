import type { FastifyCorsOptions } from '@fastify/cors';
import type { FastifyHelmetOptions } from '@fastify/helmet';
import { config } from '../config';

function parseOrigins(value: string | undefined): string[] {
  if (!value) return [];
  return value
    .split(',')
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0);
}

/**
 * Resolves allowed CORS origins for API and webhook clients.
 *
 * - `CORS_ORIGINS` / `WEBHOOK_CORS_ORIGINS`: comma-separated allowlists.
 * - If no explicit origins are configured, requests are reflected (useful in
 *   development/test) while still allowing credentialed requests for webhooks.
 *   In production you should set explicit origins.
 */
export function getCorsOriginConfig(): FastifyCorsOptions['origin'] {
  const apiOrigins = parseOrigins(process.env.CORS_ORIGINS);
  const webhookOrigins = parseOrigins(process.env.WEBHOOK_CORS_ORIGINS);
  const allowlist = [...new Set([...apiOrigins, ...webhookOrigins])];

  if (allowlist.length === 0) {
    return true; // reflect request origin
  }

  return (origin, cb) => {
    // Non-browser/server-to-server requests may omit Origin.
    if (!origin) {
      cb(null, true);
      return;
    }
    cb(null, allowlist.includes(origin));
  };
}

/** Helmet options with secure defaults for the API (issue #33). */
export function getHelmetOptions(): FastifyHelmetOptions {
  const isProd = config.NODE_ENV === 'production';
  return {
    global: true,
    // Basic security hygiene.
    contentSecurityPolicy: {
      useDefaults: true,
      directives: {
        // Restrictive default; allow SSE/EventSource connections to same origin.
        "connect-src": ["'self'"],
      },
    },
    // HSTS is only meaningful over HTTPS in production deployments.
    hsts: isProd
      ? {
          maxAge: 15552000, // 180 days
          includeSubDomains: true,
          preload: false,
        }
      : false,
    noSniff: true,
    frameguard: { action: 'deny' },
    referrerPolicy: { policy: 'no-referrer' },
  };
}

/** @fastify/cors options for API and webhook origins. */
export function getCorsOptions(): FastifyCorsOptions {
  return {
    origin: getCorsOriginConfig(),
    // Credentials enabled to support authenticated/webhook browser flows when
    // origins are explicitly configured (and reflected in dev by default).
    credentials: true,
  };
}

/**
 * Registers security plugins (helmet + CORS) on the Fastify instance.
 * Extracted so header/security configuration can be tested and reused.
 */
export async function registerSecurity(app: import('fastify').FastifyInstance): Promise<void> {
  const helmet = (await import('@fastify/helmet')).default;
  const cors = (await import('@fastify/cors')).default;
  await app.register(helmet, getHelmetOptions());
  await app.register(cors, getCorsOptions());
}
