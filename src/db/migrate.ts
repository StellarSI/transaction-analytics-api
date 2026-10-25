import { promises as fs } from 'fs';
import path from 'path';
import { pool } from './pool';

/**
 * Directory containing migration SQL files. Resolved relative to this file
 * so it works both under tsx (`src/db`) and after a `tsc` build (`dist/db`).
 */
const MIGRATIONS_DIR = path.join(__dirname, '../../migrations');

export interface MigrationEntry {
  version: string;
  applied: boolean;
  appliedAt?: Date;
}

async function ensureMigrationsTable(): Promise<void> {
  await pool`
    CREATE TABLE IF NOT EXISTS schema_migrations (
      version TEXT PRIMARY KEY,
      applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `;
}

/**
 * Splits a migration file into named sections (`up`, `down`) separated by
 * `-- up` / `-- down` comment markers.
 */
function splitMigrationSections(sql: string): Map<string, string> {
  const sections = new Map<string, string>();
  let current: string | null = null;
  const parts: string[] = [];
  const markerRe = /^--\s*(up|down)\s*$/;
  for (const line of sql.split(/\r?\n/)) {
    const marker = line.match(markerRe);
    if (marker) {
      if (current !== null) {
        sections.set(current, parts.join('\n').trim());
      }
      current = marker[1];
      parts.length = 0;
    } else {
      parts.push(line);
    }
  }
  if (current !== null) {
    sections.set(current, parts.join('\n').trim());
  }
  return sections;
}

/**
 * Run pending migrations, or roll the most recently applied migration back.
 *
 * `up` applies the `-- up` section of every `.sql` migration that has not
 * been applied yet, in file name order. `down` runs the `-- down` section
 * of the most recently applied migration. Each migration runs inside a
 * transaction so a failure never leaves the schema half-applied.
 */
export async function runMigrations(
  direction: 'up' | 'down' = 'up'
): Promise<{ applied: string[]; skipped: string[] }> {
  await ensureMigrationsTable();

  const files = (await fs.readdir(MIGRATIONS_DIR)).filter((file) => file.endsWith('.sql')).sort();

  const rows = await pool<{ version: string }[]>`
    SELECT version FROM schema_migrations ORDER BY version
  `;
  const applied = new Set(rows.map((row) => row.version));

  const appliedNow: string[] = [];
  const skipped: string[] = [];

  // Rollbacks walk the history backwards and stop after the newest migration.
  const orderedFiles = direction === 'down' ? [...files].reverse() : files;

  for (const file of orderedFiles) {
    const isApplied = applied.has(file);

    if (direction === 'up' && isApplied) {
      skipped.push(file);
      continue;
    }
    if (direction === 'down' && !isApplied) {
      skipped.push(file);
      continue;
    }

    const sections = splitMigrationSections(
      await fs.readFile(path.join(MIGRATIONS_DIR, file), 'utf8')
    );
    const sql = sections.get(direction);

    if (!sql) {
      skipped.push(file);
      continue;
    }

    await pool.begin(async (tx) => {
      if (sql.trim().length > 0) {
        await tx.unsafe(sql);
      }
      if (direction === 'up') {
        await tx`
          INSERT INTO schema_migrations (version) VALUES (${file})
        `;
      } else {
        await tx`
          DELETE FROM schema_migrations WHERE version = ${file}
        `;
      }
    });

    appliedNow.push(file);

    if (direction === 'down') {
      break;
    }
  }

  return { applied: appliedNow, skipped };
}

/**
 * List every migration file and whether it has been applied. Used by the
 * health check and future admin endpoints.
 */
export async function migrationStatus(): Promise<MigrationEntry[]> {
  await ensureMigrationsTable();

  const files = (await fs.readdir(MIGRATIONS_DIR)).filter((file) => file.endsWith('.sql')).sort();

  const rows = await pool<{ version: string; applied_at: Date }[]>`
    SELECT version, applied_at FROM schema_migrations ORDER BY version
  `;
  const appliedAtByVersion = new Map(rows.map((row) => [row.version, row.applied_at] as const));

  return files.map((version) => ({
    version,
    applied: appliedAtByVersion.has(version),
    appliedAt: appliedAtByVersion.get(version),
  }));
}
