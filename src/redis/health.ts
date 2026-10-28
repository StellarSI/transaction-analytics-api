import type Redis from 'ioredis';
import { getRedis } from './client';

/** Minimal surface needed for the health check — makes tests trivial to fake. */
export interface PingingRedis {
  ping(): Promise<string>;
}

export interface RedisHealth {
  ok: boolean;
  latencyMs: number;
  detail: string;
}

/**
 * Health check for the shared Redis connection (issue #10).
 *
 * Runs `PING` and reports reachability together with the round-trip latency,
 * mirroring the shape of `dbHealthCheck()` so `/v1/health` can serve both
 * under `services`. A failed ping is reported as `{ ok: false }` — the
 * connection keeps retrying in the background and recovers on its own.
 */
export async function redisHealthCheck(
  client: PingingRedis | Redis = getRedis(),
): Promise<RedisHealth> {
  const startedAt = Date.now();
  try {
    await client.ping();
    return {
      ok: true,
      latencyMs: Date.now() - startedAt,
      detail: 'redis reachable',
    };
  } catch (error) {
    return {
      ok: false,
      latencyMs: Date.now() - startedAt,
      detail: error instanceof Error ? error.message : String(error),
    };
  }
}
