import { describe, expect, it } from 'vitest';
import {
  accountFlowParamsSchema,
  accountFlowQuerySchema,
  assetFlowParamsSchema,
  assetFlowQuerySchema,
  corridorFlowQuerySchema,
  subscriptionBodySchema,
  transactionStreamQuerySchema,
  validateSchema,
} from '../../validation/schemas';

const ACCOUNT = 'GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN';
const ASSET = `USDC:${ACCOUNT}`;

describe('request validation schemas (issue #32)', () => {
  describe('assets endpoint schemas', () => {
    it('validates and accepts asset params', () => {
      expect(assetFlowParamsSchema.parse({ asset: ASSET })).toEqual({ asset: ASSET });
      expect(assetFlowParamsSchema.parse({ asset: 'XLM' })).toEqual({ asset: 'XLM' });
    });

    it('coerces query types and applies defaults', () => {
      const parsed = assetFlowQuerySchema.parse({ interval: '1m', from: '2026-10-01', to: '2026-10-02', limit: '5' });
      expect(parsed.interval).toBe('1m');
      expect(parsed.limit).toBe(5);
      expect(parsed.from?.toISOString()).toBe('2026-10-01T00:00:00.000Z');
      expect(parsed.to?.toISOString()).toBe('2026-10-02T00:00:00.000Z');
    });

    it('rejects unknown query params and invalid values with field errors', () => {
      const unknown = assetFlowQuerySchema.safeParse({ interval: '1h', extra: 'nope' });
      expect(unknown.success).toBe(false);
      try {
        validateSchema(assetFlowQuerySchema, { limit: '1001' });
        throw new Error('expected validation error');
      } catch (error) {
        const fields = (error as { fields?: Record<string, string[]> }).fields ?? {};
        expect(Object.keys(fields).length).toBeGreaterThan(0);
      }
    });
  });

  describe('accounts endpoint schemas', () => {
    it('validates account params', () => {
      expect(accountFlowParamsSchema.parse({ account: ACCOUNT })).toEqual({ account: ACCOUNT });
      expect(accountFlowParamsSchema.safeParse({ account: 'bad' }).success).toBe(false);
    });

    it('coerces and validates account query', () => {
      const parsed = accountFlowQuerySchema.parse({ limit: '10', from: '2026-01-01T00:00:00Z' });
      expect(parsed.interval).toBe('1h');
      expect(parsed.limit).toBe(10);
      expect(parsed.from).toBeInstanceOf(Date);
      expect(accountFlowQuerySchema.safeParse({ unknown: 'x' }).success).toBe(false);
      expect(accountFlowQuerySchema.safeParse({ from: '2026-02-01', to: '2026-01-01' }).success).toBe(false);
    });
  });

  describe('corridors endpoint schema', () => {
    it('coerces numeric/date query params and validates filters', () => {
      const parsed = corridorFlowQuerySchema.parse({
        from: '2026-01-01',
        to: '2026-01-02',
        limit: '25',
        offset: '2',
        source_asset: 'XLM',
        source_anchor: 'GA',
      });
      expect(parsed.limit).toBe(25);
      expect(parsed.offset).toBe(2);
      expect(parsed.source_asset).toBe('XLM');
      expect(parsed.from).toBeInstanceOf(Date);
    });

    it('rejects unknown query params and invalid pagination/assets', () => {
      expect(corridorFlowQuerySchema.safeParse({ nope: '1' }).success).toBe(false);
      expect(corridorFlowQuerySchema.safeParse({ limit: '1001' }).success).toBe(false);
      expect(corridorFlowQuerySchema.safeParse({ source_asset: 'bad' }).success).toBe(false);
      expect(corridorFlowQuerySchema.safeParse({ offset: '-1' }).success).toBe(false);
    });
  });

  describe('stream endpoint schema', () => {
    it('accepts supported query params and array forms', () => {
      expect(
        transactionStreamQuerySchema.parse({
          accounts: ['A', 'B'],
          'assets[]': 'XLM',
          types: 'payment',
          cursor: 'abc',
        }),
      ).toMatchObject({ cursor: 'abc' });
    });

    it('rejects unknown query params', () => {
      expect(transactionStreamQuerySchema.safeParse({ unknown: 'x' }).success).toBe(false);
      expect(transactionStreamQuerySchema.safeParse({ cursor: '' }).success).toBe(false);
    });
  });

  describe('subscriptions endpoint schema', () => {
    it('validates account, asset and threshold payloads', () => {
      expect(
        subscriptionBodySchema.parse({
          target_type: 'account',
          target_value: ACCOUNT,
          url: 'https://example.com/hook',
        }).events,
      ).toEqual([]);
      expect(
        subscriptionBodySchema.parse({
          target_type: 'asset',
          target_value: 'XLM',
          url: 'https://example.com/hook',
          events: ['payment.received'],
        }).target_value,
      ).toBe('XLM');
      expect(
        subscriptionBodySchema.parse({
          target_type: 'threshold',
          target_value: '1000.5',
          url: 'https://example.com/hook',
        }).target_type,
      ).toBe('threshold');
    });

    it('rejects unknown body properties and invalid field values', () => {
      expect(
        subscriptionBodySchema.safeParse({
          target_type: 'account',
          target_value: ACCOUNT,
          url: 'https://example.com/hook',
          extra: true,
        }).success,
      ).toBe(false);
      expect(
        subscriptionBodySchema.safeParse({
          target_type: 'account',
          target_value: 'bad',
          url: 'http://example.com/hook',
        }).success,
      ).toBe(false);
      expect(
        subscriptionBodySchema.safeParse({
          target_type: 'threshold',
          target_value: '-1',
          url: 'https://example.com/hook',
        }).success,
      ).toBe(false);
    });
  });
});
