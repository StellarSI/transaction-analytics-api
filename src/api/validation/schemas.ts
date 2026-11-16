import type { FastifyReply, FastifyRequest } from 'fastify';
import { z, type ZodTypeAny } from 'zod';
import { ValidationError, zodToFields } from '../errors';

/** Shared primitives */
export const ASSET_PARAM_PATTERN = /^(?:XLM|[A-Z0-9]{1,12}:[A-Za-z0-9]{1,64})$/;
export const STELLAR_ACCOUNT_PATTERN = /^G[A-Z2-7]{55}$/;
export const SUBSCRIPTION_ASSET_PATTERN = /^(?:XLM|[A-Za-z0-9]{1,12}:[A-Za-z0-9]{2,64})$/;
export const THRESHOLD_PATTERN = /^\d+(\.\d{1,7})?$/;
export const EVENT_NAME_PATTERN = /^[a-z0-9_.]{1,64}$/;

export const DEFAULT_FLOW_LIMIT = 100;
export const MAX_FLOW_LIMIT = 1000;
export const DEFAULT_CORRIDOR_LIMIT = 100;
export const MAX_CORRIDOR_LIMIT = 1000;

/** GET /v1/flows/assets/:asset */
export const assetFlowParamsSchema = z
  .object({
    asset: z
      .string({ required_error: 'asset is required' })
      .regex(ASSET_PARAM_PATTERN, 'asset must use the CODE:ISSUER format (or XLM for the native asset)'),
  })
  .strict();

export const assetFlowQuerySchema = z
  .object({
    interval: z.enum(['1m', '1h', '1d']).default('1h'),
    from: z.coerce.date().optional(),
    to: z.coerce.date().optional(),
    limit: z.coerce.number().int().min(1).max(MAX_FLOW_LIMIT).default(DEFAULT_FLOW_LIMIT),
  })
  .strict()
  .refine((value) => !value.from || !value.to || value.from.getTime() <= value.to.getTime(), {
    message: 'from must be earlier than or equal to to',
    path: ['from'],
  });

/** GET /v1/flows/accounts/:account */
export const accountFlowParamsSchema = z
  .object({
    account: z
      .string({ required_error: 'account is required' })
      .regex(STELLAR_ACCOUNT_PATTERN, 'account must be a valid Stellar account ID (G...)'),
  })
  .strict();

export const accountFlowQuerySchema = z
  .object({
    interval: z.enum(['1m', '1h', '1d']).default('1h'),
    from: z.coerce.date().optional(),
    to: z.coerce.date().optional(),
    limit: z.coerce.number().int().min(1).max(MAX_FLOW_LIMIT).default(DEFAULT_FLOW_LIMIT),
  })
  .strict()
  .refine((value) => !value.from || !value.to || value.from.getTime() <= value.to.getTime(), {
    message: '`from` must be before or equal to `to`',
    path: ['from'],
  });

/** GET /v1/flows/corridors */
const corridorAssetFilter = z
  .string()
  .regex(ASSET_PARAM_PATTERN, 'asset must use the CODE:ISSUER format (or XLM for the native asset)')
  .optional();

export const corridorFlowQuerySchema = z
  .object({
    interval: z.enum(['1m', '1h', '1d']).default('1h'),
    from: z.coerce.date().optional(),
    to: z.coerce.date().optional(),
    source_anchor: z.string().min(1).optional(),
    dest_anchor: z.string().min(1).optional(),
    source_asset: corridorAssetFilter,
    dest_asset: corridorAssetFilter,
    limit: z.coerce.number().int().min(1).max(MAX_CORRIDOR_LIMIT).default(DEFAULT_CORRIDOR_LIMIT),
    offset: z.coerce.number().int().min(0).default(0),
  })
  .strict()
  .refine((value) => !value.from || !value.to || value.from.getTime() <= value.to.getTime(), {
    message: '`from` must be before or equal to `to`',
    path: ['from'],
  });

/** GET /v1/transactions/stream */
const stringOrArray = z.union([z.string(), z.array(z.string())]).optional();
export const transactionStreamQuerySchema = z
  .object({
    accounts: stringOrArray,
    'accounts[]': stringOrArray,
    assets: stringOrArray,
    'assets[]': stringOrArray,
    types: stringOrArray,
    'types[]': stringOrArray,
    cursor: z.string().min(1).optional(),
  })
  .strict();

/** POST /v1/subscriptions */
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

const subscriptionBaseFields = {
  url: httpsUrl,
  secret: z.string().min(16, 'secret must be at least 16 characters').max(256).optional(),
  events: z
    .array(z.string().regex(EVENT_NAME_PATTERN, 'event names must be lowercase dotted identifiers'))
    .max(32)
    .optional()
    .default([]),
};

export const subscriptionBodySchema = z.discriminatedUnion('target_type', [
  z
    .object({
      target_type: z.literal('account'),
      target_value: z.string().regex(STELLAR_ACCOUNT_PATTERN, 'account must be a Stellar account id (G...)'),
      ...subscriptionBaseFields,
    })
    .strict(),
  z
    .object({
      target_type: z.literal('asset'),
      target_value: z.string().regex(SUBSCRIPTION_ASSET_PATTERN, 'asset must be XLM or CODE:ISSUER'),
      ...subscriptionBaseFields,
    })
    .strict(),
  z
    .object({
      target_type: z.literal('threshold'),
      target_value: z.string().regex(THRESHOLD_PATTERN, 'threshold must be a positive decimal'),
      ...subscriptionBaseFields,
    })
    .strict(),
]);

export interface RequestValidationSchemas {
  params?: ZodTypeAny;
  query?: ZodTypeAny;
  body?: ZodTypeAny;
}

export interface ValidatedRequestParts {
  params?: unknown;
  query?: unknown;
  body?: unknown;
}

declare module 'fastify' {
  interface FastifyRequest {
    validated?: ValidatedRequestParts;
  }
}

function parseOrThrow(schema: ZodTypeAny, value: unknown, detail: string): unknown {
  const result = schema.safeParse(value);
  if (!result.success) {
    throw new ValidationError(detail, zodToFields(result.error));
  }
  return result.data;
}

/**
 * Middleware factory: validates params/query/body before the route handler.
 * Parsed/coerced values replace the raw request parts and are also exposed on
 * `request.validated` for handlers that prefer explicit access.
 */
export function validateRequest(schemas: RequestValidationSchemas) {
  return async (request: FastifyRequest, _reply: FastifyReply): Promise<void> => {
    const validated: ValidatedRequestParts = request.validated ?? {};
    if (schemas.params) {
      const parsed = parseOrThrow(schemas.params, request.params, 'Invalid request parameters');
      (request as FastifyRequest & { params: unknown }).params = parsed;
      validated.params = parsed;
    }
    if (schemas.query) {
      const parsed = parseOrThrow(schemas.query, request.query, 'Invalid query parameters');
      (request as FastifyRequest & { query: unknown }).query = parsed;
      validated.query = parsed;
    }
    if (schemas.body) {
      const parsed = parseOrThrow(schemas.body, request.body, 'Invalid request body');
      (request as FastifyRequest & { body: unknown }).body = parsed;
      validated.body = parsed;
    }
    request.validated = validated;
  };
}

/** Compatibility export for schema-only tests. */
export function validateSchema<T>(schema: z.ZodType<T>, value: unknown, detail = 'Request validation failed'): T {
  return parseOrThrow(schema, value, detail) as T;
}
