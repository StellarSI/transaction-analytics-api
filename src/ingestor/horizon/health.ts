/**
 * Horizon reachability and lag check (issue #29).
 *
 * `GET /v1/health` uses this to report whether the Horizon endpoint the API
 * depends on is reachable and, critically, which ledger is the current head.
 * The ingestion lag reported by the endpoint is computed from the difference
 * between the Horizon head and the newest ledger stored in the database.
 *
 * The HTTP transport is injectable so unit tests can exercise the checker
 * without network access; production defaults to the global `fetch`.
 */

import { config } from '../../config';

export interface HorizonHealth {
  ok: boolean;
  latencyMs: number;
  detail: string;
  /** Horizon head sequence, when reachable. */
  latest_ledger?: number;
}

export async function horizonHealthCheck(
  fetchImpl: typeof fetch = globalThis.fetch,
): Promise<HorizonHealth> {
  const startedAt = Date.now();
  try {
    const response = await fetchImpl(
      `${config.HORIZON_URL}/ledgers?order=desc&limit=1`,
      { signal: AbortSignal.timeout(4000) },
    );
    if (!response.ok) {
      return {
        ok: false,
        latencyMs: Date.now() - startedAt,
        detail: `horizon responded with status ${response.status}`,
      };
    }

    const body = (await response.json()) as {
      records?: Array<{ sequence?: number }>;
    };
    const sequence = body.records?.[0]?.sequence;
    if (typeof sequence !== 'number') {
      return {
        ok: false,
        latencyMs: Date.now() - startedAt,
        detail: 'horizon responded with no ledger sequence',
      };
    }

    return {
      ok: true,
      latencyMs: Date.now() - startedAt,
      detail: 'horizon reachable',
      latest_ledger: sequence,
    };
  } catch (error) {
    return {
      ok: false,
      latencyMs: Date.now() - startedAt,
      detail: error instanceof Error ? error.message : String(error),
    };
  }
}
