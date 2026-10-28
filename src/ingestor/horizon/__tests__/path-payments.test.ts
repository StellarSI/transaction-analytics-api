import { describe, it, expect, vi } from 'vitest';
import {
  AnchorRegistry,
  describeCorridor,
  parsePathPayment,
  PathPaymentIngestor,
} from '../path-payments';
import type { OperationStreamOpener, StreamHandlers } from '../stream';
import type { HorizonOperationRecord, OperationQueue } from '../../types';

function fakeOpener() {
  const opens: Array<{ cursor: string; handlers: StreamHandlers }> = [];
  const opener: OperationStreamOpener = ({ cursor, handlers }) => {
    opens.push({ cursor, handlers });
    return vi.fn();
  };
  return { opener, opens };
}

function fakeQueue() {
  return {
    pushPayment: vi.fn().mockResolvedValue(undefined),
    pushPathPayment: vi.fn().mockResolvedValue(undefined),
  } as unknown as OperationQueue & { pushPathPayment: ReturnType<typeof vi.fn> };
}

const usdc = { asset_type: 'credit_alphanum4', asset_code: 'USDC', asset_issuer: 'GISSUER_A' };
const eurt = { asset_type: 'credit_alphanum4', asset_code: 'EURT', asset_issuer: 'GISSUER_B' };
const xlm = { asset_type: 'native' };

/** Sample strict-send operation: sender pays USDC, recipient gets EURT, with
 * an XLM intermediate hop. */
function strictSend(overrides: Partial<HorizonOperationRecord> = {}): HorizonOperationRecord {
  return {
    id: '00111-0000000001',
    paging_token: '111-1',
    type: 'path_payment_strict_send',
    type_i: 2,
    source_account: 'GSENDER',
    from: 'GSENDER',
    to: 'GRECIPIENT',
    send_asset_type: usdc.asset_type,
    send_asset_code: usdc.asset_code,
    send_asset_issuer: usdc.asset_issuer,
    send_amount: '100.0000000',
    destination_asset_type: eurt.asset_type,
    destination_asset_code: eurt.asset_code,
    destination_asset_issuer: eurt.asset_issuer,
    destination_amount: '90.0000000',
    destination_min_amount: '89.0000000',
    path: [xlm],
    transaction_hash: 'cafe',
    ...overrides,
  };
}

/** Sample strict-receive operation: sender spends XLM (capped), recipient
 * gets USDC. */
function strictReceive(overrides: Partial<HorizonOperationRecord> = {}): HorizonOperationRecord {
  return {
    id: '00222-0000000002',
    paging_token: '222-2',
    type: 'path_payment_strict_receive',
    type_i: 13,
    source_account: 'GSENDER',
    from: 'GSENDER',
    to: 'GRECIPIENT',
    source_asset_type: xlm.asset_type,
    source_amount: '50.0000000',
    send_max: '51.0000000',
    destination_asset_type: usdc.asset_type,
    destination_asset_code: usdc.asset_code,
    destination_asset_issuer: usdc.asset_issuer,
    destination_amount: '10.0000000',
    path: [],
    transaction_hash: 'beef',
    ...overrides,
  };
}

describe('parsePathPayment', () => {
  it('handles strict send and parses the full path', () => {
    const payload = parsePathPayment(strictSend());
    expect(payload).not.toBeNull();
    expect(payload?.variant).toBe('strict-send');
    expect(payload?.sendAsset).toEqual({
      type: 'credit_alphanum4',
      code: 'USDC',
      issuer: 'GISSUER_A',
    });
    expect(payload?.sendAmount).toBe('100.0000000');
    expect(payload?.destinationAsset).toEqual({
      type: 'credit_alphanum4',
      code: 'EURT',
      issuer: 'GISSUER_B',
    });
    expect(payload?.destinationAmount).toBe('90.0000000');
    expect(payload?.destinationMin).toBe('89.0000000');
    expect(payload?.corridor.path).toEqual([{ type: 'native', code: 'XLM' }]);
  });

  it('handles strict receive and reads source asset + send max', () => {
    const payload = parsePathPayment(strictReceive());
    expect(payload?.variant).toBe('strict-receive');
    expect(payload?.sendAsset).toEqual({ type: 'native', code: 'XLM' });
    expect(payload?.sendAmount).toBe('50.0000000');
    expect(payload?.sendMax).toBe('51.0000000');
    expect(payload?.destinationAsset.code).toBe('USDC');
  });

  it('identifies source and destination anchors from the registry', () => {
    const registry = new AnchorRegistry()
      .register('GISSUER_A', { name: 'Anchor A', homeDomain: 'a.example' })
      .register('GISSUER_B', { name: 'Anchor B', homeDomain: 'b.example' });
    const payload = parsePathPayment(strictSend(), registry);
    expect(payload?.corridor.sourceAnchor).toEqual({
      issuer: 'GISSUER_A',
      name: 'Anchor A',
      homeDomain: 'a.example',
    });
    expect(payload?.corridor.destinationAnchor?.name).toBe('Anchor B');
    expect(payload?.corridor.sameAnchor).toBe(false);
  });

  it('flags same-anchor payments', () => {
    const registry = new AnchorRegistry()
      .register('GISSUER_A', { name: 'Anchor A' })
      .register('GISSUER_B', { name: 'Anchor B' });
    const same = strictSend({
      destination_asset_issuer: 'GISSUER_A',
      destination_asset_code: 'USDC',
    });
    expect(parsePathPayment(same, registry)?.corridor.sameAnchor).toBe(true);
  });

  it('marks cross-issuer corridors and single-issuer ones', () => {
    const registry = new AnchorRegistry();
    expect(describeCorridor(
      { type: 'native', code: 'XLM' },
      { type: 'native', code: 'XLM' },
      [],
      registry,
    ).crossIssuer).toBe(false);

    expect(describeCorridor(
      { type: 'credit_alphanum4', code: 'USDC', issuer: 'GISSUER_A' },
      { type: 'credit_alphanum4', code: 'EURT', issuer: 'GISSUER_B' },
      [],
      registry,
    ).crossIssuer).toBe(true);
  });

  it('ignores non-path-payment operations', () => {
    expect(parsePathPayment(strictSend({ type: 'payment', type_i: 1 }))).toBeNull();
  });

  it('ignores records missing an endpoint or asset', () => {
    expect(parsePathPayment(strictSend({ to: undefined }))).toBeNull();
    expect(parsePathPayment(strictSend({ send_asset_issuer: undefined }))).toBeNull();
  });
});

describe('PathPaymentIngestor', () => {
  it('pushes parsed path payments with corridor metadata to the queue', async () => {
    const { opener, opens } = fakeOpener();
    const queue = fakeQueue();
    const registry = new AnchorRegistry().register('GISSUER_A', { name: 'Anchor A' });
    const ingestor = new PathPaymentIngestor({ opener, queue, registry });
    await ingestor.start();

    opens[0].handlers.onOperation(strictSend());
    await new Promise((r) => setTimeout(r, 0));

    expect(queue.pushPathPayment).toHaveBeenCalledTimes(1);
    const payload = queue.pushPathPayment.mock.calls[0][0] as {
      kind: string;
      variant: string;
      corridor: { sourceAnchor?: { name: string }; crossIssuer: boolean };
    };
    expect(payload.kind).toBe('path_payment');
    expect(payload.variant).toBe('strict-send');
    expect(payload.corridor.sourceAnchor?.name).toBe('Anchor A');
    expect(payload.corridor.crossIssuer).toBe(true);
    ingestor.stop();
  });

  it('skips plain payments (handled by the payment ingestor)', async () => {
    const { opener, opens } = fakeOpener();
    const queue = fakeQueue();
    const ingestor = new PathPaymentIngestor({ opener, queue });
    await ingestor.start();

    opens[0].handlers.onOperation(strictSend({ type: 'payment', type_i: 1 }));
    await new Promise((r) => setTimeout(r, 0));
    expect(queue.pushPathPayment).not.toHaveBeenCalled();
    ingestor.stop();
  });

  it('uses a dedicated cursor key so it streams independently of payments', async () => {
    const { opener, opens } = fakeOpener();
    const store = new Map<string, string>();
    const cursorStore = {
      get: (key: string) => Promise.resolve(store.get(key) ?? null),
      set: (key: string, value: string) => {
        store.set(key, value);
        return Promise.resolve();
      },
    };
    const ingestor = new PathPaymentIngestor({ opener, queue: fakeQueue(), cursorStore });
    await ingestor.start();

    opens[0].handlers.onOperation(strictReceive());
    await new Promise((r) => setTimeout(r, 0));

    expect(store.get('horizon:cursor:path-payments')).toBe('222-2');
    expect(store.has('horizon:cursor:payments')).toBe(false);
    ingestor.stop();
  });
});
