import { describe, it, expect, vi } from 'vitest';
import { redisHealthCheck } from '../health';

function fakeClient(ping: () => Promise<string>) {
  return { ping };
}

describe('redisHealthCheck', () => {
  it('returns OK with latency when PING succeeds', async () => {
    const health = await redisHealthCheck(fakeClient(() => Promise.resolve('PONG')));
    expect(health.ok).toBe(true);
    expect(health.detail).toBe('redis reachable');
    expect(health.latencyMs).toBeGreaterThanOrEqual(0);
  });

  it('returns ERROR with the message when PING fails', async () => {
    const health = await redisHealthCheck(
      fakeClient(() => Promise.reject(new Error('ECONNREFUSED'))),
    );
    expect(health.ok).toBe(false);
    expect(health.detail).toBe('ECONNREFUSED');
  });

  it('stringifies non-Error failures', async () => {
    const health = await redisHealthCheck(
      fakeClient(() => Promise.reject('raw failure')),
    );
    expect(health.ok).toBe(false);
    expect(health.detail).toBe('raw failure');
  });

  it('measures round-trip latency', async () => {
    const ping = vi.fn(async () => {
      await new Promise((r) => setTimeout(r, 5));
      return 'PONG';
    });
    const health = await redisHealthCheck(fakeClient(ping));
    expect(ping).toHaveBeenCalledTimes(1);
    expect(health.latencyMs).toBeGreaterThanOrEqual(4);
  });
});
