/**
 * Backwards-compatible re-export (issue #34).
 *
 * The implementation lives in `src/observability/logger.ts`; this module
 * remains so existing imports of `../logger` keep working. New code should
 * import from `../observability/logger` directly.
 */

export { AppLogger, logger } from './observability/logger';
