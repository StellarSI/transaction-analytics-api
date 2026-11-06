# transaction-analytics-api

> APIs for real-time transaction flow insights on the Stellar network.

![Network](https://img.shields.io/badge/network-Stellar-black)
![Status](https://img.shields.io/badge/status-in%20development-orange)
![License](https://img.shields.io/badge/license-MIT-blue)

`transaction-analytics-api` turns raw Stellar ledger activity into queryable, real-time insight: who is moving what, where value is flowing, and how fast. It is built for wallets, anchors, NGOs, compliance teams and researchers who need more than a block explorer.

Part of the **StellarSI** toolkit, alongside [`funding-optimizer`](https://github.com/StellarSI/funding-optimizer) and [`fraud-detection-sdk`](https://github.com/StellarSI/fraud-detection-sdk).

## Why Stellar

Stellar closes a ledger roughly every 5 seconds, supports native multi-asset payments (USDC, local-currency stablecoins, anchored assets), path payments, and a built-in DEX. That makes payment *flow* analysis unusually rich, and unusually valuable for cross-border and humanitarian use cases.

## Features

- **Live flow streams**: payments, path payments, claimable balances and offers, streamed as ledgers close
- **Asset flow analytics**: volume, velocity and net flow per asset (e.g. `USDC:G...`), with issuer-level breakdowns
- **Account insights**: inflow/outflow, counterparties, trustline growth, activity patterns
- **Corridor analysis**: aggregate flows between anchors and regions via path payments
- **Historical queries**: time-bucketed aggregates (minute, hour, day)
- **Webhooks and SSE**: subscribe to accounts, assets or thresholds
- **Feeds the StellarSI stack**: the data layer used by the fraud SDK and funding optimizer

## Architecture

```
Stellar Network
   │
   ├── Horizon (SSE streams)  ┐
   ├── Stellar RPC            ├─► Ingestor ─► Queue ─► Aggregator ─► Time-series store
   └── Hubble (history)       ┘                                          │
                                                                         ▼
                                                      REST + SSE API  ◄── Clients
```

> Horizon covers classic operations. Use Stellar RPC where you need Soroban contract events. Hubble (SDF's public BigQuery dataset) is used for historical backfill.

## Getting started

### Prerequisites

- Node.js 20+
- A Stellar data source: a public Horizon endpoint or your own node
- PostgreSQL 15+ (TimescaleDB recommended) and Redis

### Install

```bash
git clone https://github.com/StellarSI/transaction-analytics-api.git
cd transaction-analytics-api
npm install
cp .env.example .env
npm run dev
```

### Configuration

| Variable | Description | Example |
| --- | --- | --- |
| `STELLAR_NETWORK` | `public` or `testnet` | `testnet` |
| `HORIZON_URL` | Horizon endpoint | `https://horizon-testnet.stellar.org` |
| `STELLAR_RPC_URL` | Stellar RPC endpoint (Soroban events) | `https://soroban-testnet.stellar.org` |
| `DATABASE_URL` | Postgres/Timescale connection string | `postgres://...` |
| `REDIS_URL` | Redis connection string | `redis://localhost:6379` |
| `API_KEY_SALT` | Salt for hashing API keys | — |

Start on **testnet**. Fund test accounts with [Friendbot](https://developers.stellar.org/docs/learn/fundamentals/networks#friendbot).

## Database schema

Data is stored in TimescaleDB hypertables managed by version-controlled SQL
migrations in `migrations/`. Run `npm run dev` to apply pending migrations
automatically on startup.

| Table | Type | Time column | Chunk interval | Description |
| --- | --- | --- | --- | --- |
| `ledger_operations` | Hypertable | `ledger_closed_at` | 1 day | Raw ledger operations ingested from Horizon / RPC |
| `asset_flow_aggregates` | Hypertable | `bucket_start` | 1 hour | Per-asset volume, payments, net flow |
| `account_flow_aggregates` | Hypertable | `bucket_start` | 1 hour | Per-account inflow, outflow, counterparties |
| `corridor_aggregates` | Hypertable | `bucket_start` | 1 hour | Per-corridor volume and payments between anchors |

All hypertables use compression with a 30-day policy on older chunks, and
aggregate tables enforce uniqueness on `(bucket_start, interval, ...)` to
prevent duplicate buckets.

Schema TypeScript definitions live in `src/db/schema/` (`ledger.ts`,
`aggregates.ts`) and are shared between ingestion and query code.

## API overview

Base path: `/v1`. Endpoints below are the planned surface.

| Method | Endpoint | Description |
| --- | --- | --- |
| `GET` | `/v1/flows/assets/{asset}` | Volume, velocity and net flow for an asset |
| `GET` | `/v1/flows/accounts/{account}` | Inflow/outflow and top counterparties |
| `GET` | `/v1/flows/corridors` | Aggregated flows between asset pairs and anchors |
| `GET` | `/v1/transactions/stream` | SSE stream of live transactions (filterable) |
| `POST` | `/v1/subscriptions` | Register a webhook for an account, asset or threshold |
| `GET` | `/v1/health` | Ingestion lag and service status |

### Example

```bash
curl -H "Authorization: Bearer $API_KEY" \
  "http://localhost:3000/v1/flows/assets/USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN?interval=1h&from=2026-10-01"
```

```json
{
  "asset": "USDC:GA5Z...KZVN",
  "interval": "1h",
  "buckets": [
    { "start": "2026-10-01T00:00:00Z", "volume": "184220.55", "payments": 1243, "net_flow": "-1200.00" }
  ]
}
```

## Use cases

- Anchors monitoring settlement and liquidity corridors
- NGOs tracking disbursement flow and cash-out behavior
- Compliance teams spotting unusual movement patterns
- Researchers and analysts studying network-level payment activity

## Ingestion

The Horizon ingestors stream operations over SSE and push structured payloads
onto a Redis queue:

- `PaymentIngestor` (`src/ingestor/horizon/payments.ts`) keeps `type_i === 1`
  operations and extracts ledger, accounts, asset, amount and transaction hash.
- `PathPaymentIngestor` (`src/ingestor/horizon/path-payments.ts`) handles both
  `path_payment_strict_send` (`type_i === 2`) and `path_payment_strict_receive`
  (`type_i === 13`), parsing the full path and attaching corridor metadata with
  best-effort anchor identification from a local registry.

Both share a streaming loop (`src/ingestor/horizon/client.ts`) that persists the
last seen paging token and reconnects with exponential backoff, so no operation
is dropped across a restart. Claimable balances (SEP-11) and offer / DEX
operations are ingested by dedicated clients (`src/ingestor/horizon/claimable-balances.ts`,
`src/ingestor/horizon/offers.ts`). Offer payloads carry the Horizon price as an
integer ratio (`price_n`/`price_d`) plus the `offer_id`, and a zero amount marks
a deletion, so an offer's life cycle can be followed from creation to removal.

The queue is BullMQ over Redis Streams (`src/queue/`): parsed operations land on
the `ledger-operations` queue, aggregation passes on `aggregation-jobs`, both
with Zod-validated payloads, 3-attempt exponential-backoff retries, workers
that run concurrently, Prometheus metrics (`eta_queue_*` for queue depth,
processing time and failure rate) and a drain-on-shutdown worker close.
Payloads not yet consumed by an aggregator are validated and acknowledged; the
dead letter queue is tracked in issue #98. Redis connection and health checks
live in `src/redis/`.

### Asset flow aggregates (issue #18)

`src/aggregator/asset-flows.ts` maintains the `asset_flow_aggregates`
hypertable: per asset code + issuer and per time bucket (`1m`, `1h`, `1d`) it
keeps `volume` (sum of leg amounts), `payments` (legs that moved the asset)
and `net_flow` (issuer-attributed net issuance: amount paid out of the
issuer minus amount received by the issuer; always `0` for the native asset
and for movements between non-issuer accounts).

- **Real-time path**: the `ledger-operations` worker folds every payment /
  path-payment job into the minute, hour and day buckets of the ledger
  close time and upserts additively (`ON CONFLICT ... DO UPDATE SET ... =
  existing + EXCLUDED`), so concurrent workers never double-count.
- **Reorg path**: if an incoming operation carries a ledger sequence lower
  than the high-water mark (kept per process and optionally persisted via
  the shared cursor store), the aggregator deletes the trailing window of
  buckets and re-aggregates them from `ledger_operations` — the fork-safe
  source of truth. The same recompute runs for the `asset-flow` jobs the
  Hubble backfill enqueues (one per closed bucket via the
  `aggregation-jobs` worker).
- **Precision**: amounts are summed as bigint stroops (7 decimals); no
  floating point anywhere. When Horizon exposes only a bound for a path
  payment leg (`send_max` / `destination_min_amount`), that bound is used as
  an approximation and a warning is logged.
- **`asset_issuer` convention**: the native asset (and rows re-aggregated
  from `ledger_operations`) are stored as `''` because the hypertable
  primary key treats NULLs as distinct — upserts need a real value to
  conflict on.
- **Events**: every upsert emits an `asset_flow_updated` event via a small
  sink interface (`createRedisAssetFlowSink` publishes to the
  `eta:flows:assets` channel for the future SSE/webhook fan-out). Sink
  failures are logged and counted, never thrown.
- **Metrics**: `eta_asset_flow_operations_processed_total{kind}`,
  `eta_asset_flow_buckets_updated_total{interval}`,
  `eta_asset_flow_reorgs_detected_total{interval}` and
  `eta_asset_flow_failures_total{stage}`.

Unit tests live in `src/aggregator/__tests__/asset-flows.test.ts` and run on
the in-memory store (`createInMemoryAssetFlowStore`) — no database or Redis
required. Path payments are treated as two legs (send asset out of the
sender, destination asset into the recipient); the batch re-aggregation
path counts the single asset recorded in `ledger_operations`.

### Historical backfill (Hubble / BigQuery)

The live Horizon/RPC ingestors only cover data from the moment they start.
For historical analytics, `src/ingestor/hubble/backfill.ts` pulls past
operations from SDF's public BigQuery dataset (`crypto_stellar`) and
batch-inserts them into the `ledger_operations` hypertable:

- **Idempotent**: rows whose `(transaction_hash, operation_index)` key
  already exists are skipped, so re-running a date range is safe.
- **Resumable**: the last completed day is persisted to a `CursorStore`;
  after a failure the job resumes from the day after the cursor.
- **Filtered**: optional `assetCode`, `assetIssuer` and `account` filters
  are passed as bound BigQuery parameters.
- **Aggregation-aware**: each inserted day enqueues `asset-flow`,
  `account-flow` and `corridor` aggregation jobs per hour bucket
  (issues #18, #19, #21).

The BigQuery client is injected as a structural port (`BigQueryClientLike`),
so unit tests run without the `@google-cloud/bigquery` SDK. In production,
`createBigQueryClient()` lazily imports it using the `BIGQUERY_PROJECT_ID`
and `BIGQUERY_KEY_FILE` environment variables (see `.env.example`). The
Postgres-backed store lives in `src/ingestor/hubble/store.ts`.

## Roadmap

- [ ] Ingestor for payments and path payments
- [ ] Asset and account aggregates
- [ ] SSE streaming endpoint
- [ ] Webhooks
- [ ] Soroban contract event analytics
- [ ] SEP-aware anchor labeling
- [ ] Public hosted instance

## Security

Please report vulnerabilities privately — see [SECURITY.md](SECURITY.md). Do not
open a public issue for security reports.

## Contributing

Issues and PRs are welcome. Please open an issue first for larger changes. See
[CONTRIBUTING.md](CONTRIBUTING.md).

## Related projects

- [`funding-optimizer`](https://github.com/StellarSI/funding-optimizer): grant allocation and NGO disbursement analytics
- [`fraud-detection-sdk`](https://github.com/StellarSI/fraud-detection-sdk): anomaly detection for Stellar transactions
- [Stellar Developers](https://developers.stellar.org) · [Horizon docs](https://developers.stellar.org/docs/data/apis/horizon) · [Stellar Expert](https://stellar.expert)

## License

MIT. See [LICENSE](LICENSE).
