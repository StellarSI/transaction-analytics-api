/**
 * RFC 7807 Problem Details for the HTTP API.
 *
 * Every error the API returns — validation failures, missing routes, auth
 * problems, rate limiting and unexpected faults — is serialized as a
 * Problem Details document (`application/problem+json`) so clients can parse
 * failures reliably instead of scraping HTML or ad-hoc JSON envelopes.
 *
 * The document carries the standard `type` / `title` / `status` / `detail` /
 * `instance` members plus two extension members used across the API:
 *
 * - `code`   — a stable machine-readable identifier (`validation_error`,
 *             `not_found`, `unauthorized`, `rate_limit`, `internal_error`).
 * - `fields` — for validation errors, a map of request field to the list of
 *             problems found on that field.
 */

import type { FastifyInstance } from 'fastify';
import { ZodError } from 'zod';

/** Media type used for every Problem Details response. */
export const PROBLEM_JSON_CONTENT_TYPE = 'application/problem+json';

export type ProblemCode =
  | 'validation_error'
  | 'not_found'
  | 'unauthorized'
  | 'rate_limit'
  | 'internal_error';

export interface ProblemDetails {
  /** URI identifying the error type; clients should treat it as an opaque string. */
  type: string;
  /** Short, human-readable summary of the error type. */
  title: string;
  /** HTTP status code. */
  status: number;
  /** Human-readable explanation specific to this occurrence. */
  detail?: string;
  /** URI reference identifying the request that produced the error. */
  instance?: string;
  /** Stable machine-readable error identifier. */
  code?: ProblemCode;
  /** Validation errors keyed by request field. */
  fields?: Record<string, string[]>;
}

const PROBLEM_TYPE_BASE = 'https://transaction-analytics-api.stellarsi/errors';

const PROBLEM_TITLES: Record<ProblemCode, string> = {
  validation_error: 'Validation Error',
  not_found: 'Not Found',
  unauthorized: 'Unauthorized',
  rate_limit: 'Too Many Requests',
  internal_error: 'Internal Server Error',
};

const PROBLEM_STATUS: Record<ProblemCode, number> = {
  validation_error: 400,
  not_found: 404,
  unauthorized: 401,
  rate_limit: 429,
  internal_error: 500,
};

/**
 * Base class for errors that map directly onto a Problem Details document.
 * Throwing an `AppError` (or a subclass) from a route is enough — the global
 * error handler installed by {@link setErrorHandler} serializes it.
 */
export class AppError extends Error {
  readonly status: number;
  readonly code: ProblemCode;
  readonly title: string;
  readonly fields?: Record<string, string[]>;

  constructor(code: ProblemCode, message: string, fields?: Record<string, string[]>) {
    super(message);
    this.name = 'AppError';
    this.code = code;
    this.status = PROBLEM_STATUS[code];
    this.title = PROBLEM_TITLES[code];
    if (fields) {
      this.fields = fields;
    }
  }
}

/** 400 — the request was malformed or failed validation. */
export class ValidationError extends AppError {
  constructor(message = 'Request validation failed', fields?: Record<string, string[]>) {
    super('validation_error', message, fields);
    this.name = 'ValidationError';
  }
}

/** 404 — the requested resource or route does not exist. */
export class NotFoundError extends AppError {
  constructor(message = 'Resource not found') {
    super('not_found', message);
    this.name = 'NotFoundError';
  }
}

/** 401 — the request is missing valid authentication. */
export class UnauthorizedError extends AppError {
  constructor(message = 'Authentication required') {
    super('unauthorized', message);
    this.name = 'UnauthorizedError';
  }
}

/** 429 — the client has sent too many requests. */
export class RateLimitError extends AppError {
  constructor(message = 'Too many requests') {
    super('rate_limit', message);
    this.name = 'RateLimitError';
  }
}

/** 500 — an unexpected server-side failure. */
export class InternalError extends AppError {
  constructor(message = 'Internal server error') {
    super('internal_error', message);
    this.name = 'InternalError';
  }
}

/** Flattens a Zod error into the `fields` extension member. */
export function zodToFields(error: ZodError): Record<string, string[]> {
  const fields: Record<string, string[]> = {};
  for (const issue of error.issues) {
    const path = issue.path.length > 0 ? issue.path.join('.') : '(root)';
    if (!fields[path]) {
      fields[path] = [];
    }
    fields[path].push(issue.message);
  }
  return fields;
}

/** Maps an HTTP status to the stable problem code used in responses. */
function problemCodeForStatus(status: number): ProblemCode | undefined {
  switch (status) {
    case 400:
      return 'validation_error';
    case 401:
      return 'unauthorized';
    case 404:
      return 'not_found';
    case 429:
      return 'rate_limit';
    case 500:
      return 'internal_error';
    default:
      return undefined;
  }
}

/**
 * Converts any thrown value into a Problem Details document.
 *
 * `AppError`s keep their status/code/fields, Zod errors become validation
 * problems with per-field details, Fastify errors keep their HTTP status and
 * everything else is reported as a generic 500.
 */
export function toProblemDetails(error: unknown, instance?: string): ProblemDetails {
  if (error instanceof AppError) {
    const details: ProblemDetails = {
      type: `${PROBLEM_TYPE_BASE}/${error.code}`,
      title: error.title,
      status: error.status,
      detail: error.message,
      code: error.code,
    };
    if (instance) {
      details.instance = instance;
    }
    if (error.fields) {
      details.fields = error.fields;
    }
    return details;
  }

  if (error instanceof ZodError) {
    const validation = new ValidationError('Request validation failed', zodToFields(error));
    return toProblemDetails(validation, instance);
  }

  const statusCode =
    typeof (error as { statusCode?: unknown })?.statusCode === 'number'
      ? (error as { statusCode: number }).statusCode
      : 500;
  const status = statusCode >= 400 && statusCode < 600 ? statusCode : 500;
  const message = error instanceof Error ? error.message : 'Internal server error';
  const code = problemCodeForStatus(status);
  const details: ProblemDetails = {
    type: `${PROBLEM_TYPE_BASE}/${code ?? 'error'}`,
    title: code ? PROBLEM_TITLES[code] : `HTTP ${status}`,
    status,
    detail: message,
  };
  if (code) {
    details.code = code;
  }
  if (instance) {
    details.instance = instance;
  }
  return details;
}

/**
 * Installs the global error handler and 404 handler on a Fastify instance.
 *
 * The error handler guarantees that even unexpected exceptions produce a
 * Problem Details body with `Content-Type: application/problem+json` instead
 * of Fastify's default JSON envelope.
 */
export function setErrorHandler(app: FastifyInstance): void {
  app.setErrorHandler((error, request, reply) => {
    const problem = toProblemDetails(error, request.url);
    if (problem.status >= 500) {
      request.log.error({ err: error, status: problem.status }, 'request failed');
    } else {
      request.log.warn({ status: problem.status, code: problem.code }, 'request rejected');
    }
    void reply
      .code(problem.status)
      .header('content-type', PROBLEM_JSON_CONTENT_TYPE)
      .send(problem);
  });

  app.setNotFoundHandler((request, reply) => {
    const problem = toProblemDetails(
      new NotFoundError(`Route ${request.method} ${request.url} not found`),
      request.url,
    );
    request.log.warn({ status: problem.status }, 'route not found');
    void reply
      .code(problem.status)
      .header('content-type', PROBLEM_JSON_CONTENT_TYPE)
      .send(problem);
  });
}
