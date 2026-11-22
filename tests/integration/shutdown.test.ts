import { describe, it, expect, afterEach, vi } from 'vitest';
import type { FastifyInstance } from 'fastify';
import {
  getDrainingStatus,
  registerWorkerDrain,
  resetShutdownState,
  triggerShutdown,
} from '@/common/shutdown';

/**
 * Integration coverage for graceful shutdown (issue #36).
 *
 * The drain is exercised through the public API in common/shutdown.ts: the
 * draining flag flips during the sequence, registered worker drains run after
 * the HTTP server closes, and the force-exit timer fires when the drain
 * wedges. No real signals are sent: triggerShutdown is called on a stand-in
 * Fastify app.
 */

/** A Fastify stand-in: logs are no-ops, close resolves by default. */
function fakeApp(options: { close?: () => Promise<void>; calls?: string[] } = {}): FastifyInstance {
  const calls = options.calls ?? [];
  return {
    log: {
      info: () => undefined,
      warn: () => undefined,
      error: () => undefined,
    },
    close: async () => {
      calls.push('app.close');
      await options.close?.();
    },
  } as unknown as FastifyInstance;
}

/** Poll until condition is true or the timeout elapses. */
async function waitFor(condition: () => boolean, timeoutMs = 1_000): Promise<void> {
  const start = Date.now();
  while (!condition()) {
    if (Date.now() - start > timeoutMs) {
      throw new Error('waitFor: condition not met in time');
    }
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

afterEach(() => {
  resetShutdownState();
  vi.restoreAllMocks();
});

describe('graceful shutdown (issue #36)', () => {
  it('flips the draining flag during triggerShutdown and resets it', async () => {
    expect(getDrainingStatus()).toBe(false);
    const drain = triggerShutdown(fakeApp());
    expect(getDrainingStatus()).toBe(true);
    await drain;
    resetShutdownState();
    expect(getDrainingStatus()).toBe(false);
  });

  it('is idempotent: a second triggerShutdown while draining is a no-op', async () => {
    const app = fakeApp();
    const first = triggerShutdown(app);
    const second = triggerShutdown(app);
    expect(getDrainingStatus()).toBe(true);
    await first;
    await second;
    resetShutdownState();
  });

  it('runs registered worker drains after the HTTP server closes', async () => {
    const calls: string[] = [];
    registerWorkerDrain(async () => {
      calls.push('worker-drain');
    });
    const drain = triggerShutdown(fakeApp({ calls }));
    await drain;
    expect(calls).toEqual(['app.close', 'worker-drain']);
  });

  it('exits the process when the drain wedges past the timeout', async () => {
    const exitSpy = vi.spyOn(process, 'exit').mockImplementation((() => undefined) as never);
    const app = fakeApp({ close: () => new Promise<void>(() => undefined) }); // wedge
    const drain = triggerShutdown(app, { timeoutMs: 20 });
    await waitFor(() => exitSpy.mock.calls.length > 0, 1_000);
    expect(exitSpy).toHaveBeenCalledWith(1);
    resetShutdownState();
    // The wedged drain never resolves; do not await it.
    void drain;
  });
});
