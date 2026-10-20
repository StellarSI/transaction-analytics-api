import { promises as fs } from 'fs';
import path from 'path';
import { pool } from './pool';

/**
 * Directory containing migration SQL files. Resolved relative to this file
 * so it works both under tsx (`src/db`) and after a `tsc` build (`dist/db`).
 */
const MIGRATIONS_DIR = path.join(import.meta.dirname, '../../migrations');

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
  const markerRe = /^--\s*(up|down)\s*$/m;
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

  const rows = await pool`
    SELECT version FROM schema_migrations ORDER BY version
  `;
  const applied = new Set(rows.map((row) => row.version as string));

  const appliedNow: string[] = [];
  const skipped: string[] = [];

  for (const file of files) {
    const sections = splitMigrationSections(
      await fs.readFile(path.join(MIGRATIONS_DIR, file), 'utf8')
    );
    const sql = sections.get(direction);

    if (!sql) {
      skipped.push(file);
      continue;
    }
    if (direction === 'up' && applied.has(file)) {
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

  const rows = await pool`
    SELECT version, applied_at FROM schema_migrations ORDER BY version
  `;
  const appliedAtByVersion = new Map(
    rows.map((row) => [row.version as string, row.applied_at as Date])
  );

  return files.map((version) => ({
    version,
    applied: appliedAtByVersion.has(version),
    appliedAt: appliedAtByVersion.get(version),
  }));
}
