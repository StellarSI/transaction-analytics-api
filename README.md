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
