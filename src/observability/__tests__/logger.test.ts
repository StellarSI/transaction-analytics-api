/**
 * Tests for the shared structured logger (issue #34).
 *
 * Covers: JSON output in production, level from env, request id generation,
 * child loggers with module bindings and redaction of sensitive fields.
 * A Writable destination is injected to capture lines without touching
 * stdout.
 */

import { describe, it, expect } from 'vitest';
import { Writable } from 'node:stream';
import {
  createLogger,
  generateRequestId,
  REDACT_PATHS,
} from '../logger';
import { config } from '../../config';

interface Capture {
  stream: Writable;
  lines: string[];
}

function capture(): Capture {
  const lines: string[] = [];
  const stream = new Writable({
    write(chunk, _encoding, callback) {
      lines.push(String(chunk));
      callback();
    },
  });
  return { stream, lines };
}

function parseLines(lines: string[]): Array<Record<string, unknown>> {
  return lines
    .map((line) => line.trim())
    .filter((line) => line.length > 0)
    .map((line) => JSON.parse(line) as Record<string, unknown>);
}

describe('createLogger (issue #34)', () => {
  it('emits single-line JSON with the configured level', () => {
    const { stream, lines } = capture();
    const log = createLogger({ level: 'info', pretty: false, destination: stream });
    log.info('hello world');

    const [entry] = parseLines(lines);
    expect(entry.level).toBe(30);
    expect(entry.msg).toBe('hello world');
    expect(typeof entry.time).toBe('number');
  });

  it('honors the environment LOG_LEVEL by default', () => {
    const { stream, lines } = capture();
    const log = createLogger({ destination: stream });
    log.debug('debug line should be filtered out');
    log.warn('warn line stays');

    const parsed = parseLines(lines);
    expect(parsed.some((l) => l.msg === 'debug line should be filtered out')).toBe(false);
    expect(parsed.some((l) => l.msg === 'warn line stays')).toBe(config.LOG_LEVEL !== 'fatal');
  });

  it('redacts sensitive fields instead of logging them', () => {
    const { stream, lines } = capture();
    const log = createLogger({ level: 'info', pretty: false, destination: stream });
    log.info(
      {
        apiKey: 'super-secret-key',
        req: { headers: { authorization: 'Bearer sk_live_123', 'x-api-key': 'abc' } },
        note: 'keep me',
      },
      'sensitive payload',
    );

    const [entry] = parseLines(lines);
    expect(entry.apiKey).toBe('[redacted]');
    const req = entry.req as { headers: Record<string, unknown> };
    expect(req.headers.authorization).toBe('[redacted]');
    expect(req.headers['x-api-key']).toBe('[redacted]');
    expect(entry.note).toBe('keep me');
    expect(JSON.stringify(entry)).not.toContain('super-secret-key');
  });

  it('covers every documented redact path with a censor', () => {
    expect(REDACT_PATHS).toContain('authorization');
    expect(REDACT_PATHS).toContain('x-api-key');
    expect(REDACT_PATHS).toContain('password');
    expect(REDACT_PATHS).toContain('req.headers.authorization');
  });

  it('child logger carries the module binding', () => {
    const { stream, lines } = capture();
    const base = createLogger({ level: 'info', pretty: false, destination: stream });
    const child = base.child({ module: 'ingestor' });
    child.info('child line');

    const [entry] = parseLines(lines);
    expect(entry.module).toBe('ingestor');
    expect(entry.msg).toBe('child line');
  });

  it('module-bound child loggers carry the module name and extra bindings', () => {
    // createModuleLogger derives from the shared root logger (stdout); only
    // assert the resulting binding shape via a child of a captured logger.
    const { stream, lines } = capture();
    const base = createLogger({ level: 'info', pretty: false, destination: stream });
    const child = base.child({ module: 'aggregator', scope: 'test' });
    child.info('aggregator tick');

    const [entry] = parseLines(lines);
    expect(entry.module).toBe('aggregator');
    expect(entry.scope).toBe('test');
  });

  it('generateRequestId returns a UUID v4-shaped string and is unique', () => {
    const id = generateRequestId();
    expect(id).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
    );
    expect(generateRequestId()).not.toBe(id);
  });
});
