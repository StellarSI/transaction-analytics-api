# Security Policy

## Supported Versions

`transaction-analytics-api` is pre-1.0 and moves quickly. Security fixes are
applied to the latest commit on `main` and included in the next release.

| Version | Supported |
| --- | --- |
| `main` (0.0.x) | :white_check_mark: |
| Older commits and forks | :x: |

## Reporting a Vulnerability

**Please do not open a public issue, pull request, or discussion for a security
problem.**

Report it privately through GitHub's private vulnerability reporting:

1. Open
   <https://github.com/StellarSI/transaction-analytics-api/security/advisories/new>.
2. Include the affected version or commit, impact, and reproduction steps.
3. If you have a suggested fix, describe it in the report rather than opening a
   public PR first.

If GitHub Security Advisories are unavailable to you, contact the maintainers
through the StellarSI organization (<https://github.com/StellarSI>) and ask for
a private channel. Do not include exploit details in a public message.

## What to Expect

- **Acknowledgement** within 2 business days.
- **Triage** within 7 days, including a severity assessment and remediation
  plan.
- **Coordinated disclosure**: we will agree on a disclosure date with you and
  will not publish details until a fix is available.
- **Credit**: reporters who want credit will be named in the advisory.

## Scope

In scope:

- The HTTP API, including request validation and authentication
- The ingestion and aggregation pipeline
- Webhook and SSE delivery
- Deployment and configuration files in this repository

Out of scope: vulnerabilities in third-party services (Stellar Horizon, Stellar
RPC, PostgreSQL, TimescaleDB, Redis) and social-engineering attacks. Report
those to the relevant vendor.

## Safe Harbor

We will not pursue legal action against researchers who act in good faith,
avoid privacy violations and service disruption, and give us reasonable time to
fix an issue before public disclosure.
