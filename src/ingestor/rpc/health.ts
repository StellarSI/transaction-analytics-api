/**
 * Stellar RPC reachability check (issue #29).
 *
 * `GET /v1/health` uses this to report whether the Soroban RPC endpoint is
 * reachable and which ledger is the current head. `getLatestLedger` is the
 * minimal JSON-RPC method with no auth and no side effects.
 *
 * The HTTP transport is injectable so unit tests can exercise the checker
 * without network access; production defaults to the global `fetch`.
 */

import { config } from '../../config';

export interface RpcHealth {
  ok: boolean;
  latencyMs: number;
  detail: string;
  /** RPC head sequence, when reachable. */
  latest_ledger?: number;
}

export async function rpcHealthCheck(
  fetchImpl: typeof fetch = globalThis.fetch,
): Promise<RpcHealth> {
  const startedAt = Date.now();
  try {
    const response = await fetchImpl(config.STELLAR_RPC_URL, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ jsonrpc: '2.0', id: 1, method: 'getLatestLedger' }),
      signal: AbortSignal.timeout(4000),
    });
    if (!response.ok) {
      return {
        ok: false,
        latencyMs: Date.now() - startedAt,
        detail: `stellar rpc responded with status ${response.status}`,
      };
    }

    const body = (await response.json()) as {
      result?: { sequence?: number };
      error?: { message?: string; code?: number };
    };
    if (body.error) {
      return {
        ok: false,
        latencyMs: Date.now() - startedAt,
        detail: body.error.message ?? 'stellar rpc returned an error',
      };
    }

    const sequence = body.result?.sequence;
    if (typeof sequence !== 'number') {
      return {
        ok: false,
        latencyMs: Date.now() - startedAt,
        detail: 'stellar rpc responded with no ledger sequence',
      };
    }

    return {
      ok: true,
      latencyMs: Date.now() - startedAt,
      detail: 'stellar rpc reachable',
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
