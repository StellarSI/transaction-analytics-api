# Contributing to transaction-analytics-api

Thank you for your interest in contributing! This document outlines the process for contributing to this project.

## Code of Conduct

By participating in this project, you agree to abide by our [Code of Conduct](CODE_OF_CONDUCT.md).

## Getting Started

### Prerequisites

- Node.js 20+
- PostgreSQL 15+ with TimescaleDB extension
- Redis 7+

### Development Setup

1. Fork the repository
2. Clone your fork: `git clone https://github.com/YOUR_USERNAME/transaction-analytics-api.git`
3. Add upstream remote: `git remote add upstream https://github.com/StellarSI/transaction-analytics-api.git`
4. Install dependencies: `npm ci`
5. Copy environment file: `cp .env.example .env`
6. Start development server: `npm run dev`

## Development Workflow

### Branch Naming

- Feature branches: `feature/short-description`
- Bug fixes: `fix/short-description`
- Documentation: `docs/short-description`
- Refactoring: `refactor/short-description`

### Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
type(scope): short description

[optional body]

[optional footer]
```

Types: `feat`, `fix`, `docs`, `style`, `refactor`, `test`, `chore`, `perf`

Examples:
```
feat(api): add asset flow endpoint
fix(ingestor): handle horizon reconnection
docs: update configuration reference
```

### Pull Request Process

1. **Open an issue first** for significant changes (per project policy)
2. Create a feature branch from `upstream/main`
3. Make your changes with tests
4. Run the full check suite locally:
   ```bash
   npm run lint
   npm run typecheck
   npm run test
   npm run format
   ```
5. Push to your fork and open a PR against `upstream/main`
6. Ensure all CI checks pass
7. Request review from maintainers

## Code Style

- **TypeScript**: Strict mode enabled, use type-safe patterns
- **ESLint**: Run `npm run lint` - no errors allowed
- **Prettier**: Run `npm run format` - auto-formats code
- **Imports**: Use `@/` path alias for `src/` imports

## Testing

- **Unit tests**: `npm run test:unit` - test individual functions/modules
- **Integration tests**: `npm run test:integration` - test with real DB/Redis
- **E2E tests**: `npm run test:e2e` - test full API flows
- Write tests for new features
- Maintain or improve coverage

## Project Structure

```
src/
├── api/           # REST API routes and server
├── config/        # Configuration management
├── ingestor/      # Horizon/RPC/Hubble ingestion
├── aggregator/    # Data aggregation workers
├── queue/         # BullMQ job queues
├── db/            # Database schema and migrations
├── redis/         # Redis client
├── auth/          # API key authentication
├── webhooks/      # Webhook delivery
├── observability/ # Logging, metrics, tracing
└── common/        # Shared utilities
```

## Issue Templates

See [GitHub Issues](https://github.com/StellarSI/transaction-analytics-api/issues/new/choose) for:
- Bug reports
- Feature requests
- Questions

## Security

For security vulnerabilities, please see [SECURITY.md](SECURITY.md) for responsible disclosure.

## License

By contributing, you agree that your contributions will be licensed under the [MIT License](LICENSE).