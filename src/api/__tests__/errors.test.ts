import { describe, it, expect } from 'vitest';
import { z } from 'zod';
import {
  InternalError,
  NotFoundError,
  ValidationError,
  toProblemDetails,
  zodToFields,
} from '../errors';

describe('RFC 7807 problem details', () => {
  it('maps an AppError onto a problem document', () => {
    const problem = toProblemDetails(
      new ValidationError('Invalid asset parameter', { asset: ['asset must use CODE:ISSUER'] }),
      '/v1/flows/assets/USDC',
    );

    expect(problem).toEqual({
      type: 'https://transaction-analytics-api.stellarsi/errors/validation_error',
      title: 'Validation Error',
      status: 400,
      detail: 'Invalid asset parameter',
      code: 'validation_error',
      instance: '/v1/flows/assets/USDC',
      fields: { asset: ['asset must use CODE:ISSUER'] },
    });
  });

  it('maps a not-found error to a 404 problem', () => {
    const problem = toProblemDetails(new NotFoundError('Route /v1/nope not found'), '/v1/nope');

    expect(problem.status).toBe(404);
    expect(problem.code).toBe('not_found');
    expect(problem.title).toBe('Not Found');
    expect(problem.instance).toBe('/v1/nope');
  });

  it('flattens Zod issues into per-field messages', () => {
    const schema = z.object({
      interval: z.enum(['1m', '1h', '1d']),
      limit: z.number().int().min(1),
    });
    const result = schema.safeParse({ interval: '1s', limit: 0 });

    expect(result.success).toBe(false);
    if (!result.success) {
      const fields = zodToFields(result.error);
      expect(fields.interval?.length).toBeGreaterThan(0);
      expect(fields.limit?.length).toBeGreaterThan(0);
    }
  });

  it('converts a Zod error into a validation problem', () => {
    const result = z.object({ interval: z.enum(['1m', '1h', '1d']) }).safeParse({ interval: '1s' });

    expect(result.success).toBe(false);
    if (!result.success) {
      const problem = toProblemDetails(result.error, '/v1/flows/assets/USDC:GISS');
      expect(problem.status).toBe(400);
      expect(problem.code).toBe('validation_error');
      expect(problem.fields?.interval?.length).toBeGreaterThan(0);
    }
  });

  it('falls back to a 500 problem for unknown errors', () => {
    const problem = toProblemDetails(new Error('boom'), '/v1/health');

    expect(problem.status).toBe(500);
    expect(problem.code).toBe('internal_error');
    expect(problem.detail).toBe('boom');
  });

  it('keeps Fastify error statuses', () => {
    const problem = toProblemDetails(
      Object.assign(new Error('not found'), { statusCode: 404 }),
      '/v1/nope',
    );

    expect(problem.status).toBe(404);
  });

  it('exposes the internal error class', () => {
    const problem = toProblemDetails(new InternalError('repository unavailable'));
    expect(problem.status).toBe(500);
    expect(problem.code).toBe('internal_error');
  });
});
