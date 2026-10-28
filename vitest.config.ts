import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { defineConfig } from 'vitest/config';

const srcDir = fileURLToPath(new URL('./src', import.meta.url));

export default defineConfig({
  // Path alias defined in tsconfig.json paths (@/* -> src/*). Vitest does not
  // read tsconfig paths natively, so mirror the mapping explicitly.
  resolve: {
    alias: {
      '@': srcDir,
    },
  },
  test: {
    // Allow test suite to pass when no test files are found
    // This is a greenfield project without test files yet
    passWithNoTests: true,
    // Project configuration for different test types
    projects: [
      {
        name: 'unit',
        test: {
          include: ['src/**/*.test.ts'],
          environment: 'node',
          // Unit tests must not require real infrastructure. Several modules
          // import the validated `config`, which reads these vars at load and
          // exits if they are missing, so supply inert defaults here. This is
          // scoped to the unit project only; integration/e2e keep the real
          // environment.
          env: {
            NODE_ENV: 'test',
            DATABASE_URL: 'postgres://user:password@localhost:5432/test',
            API_KEY_SALT: 'unit-test-salt-min-16-chars',
            REDIS_URL: 'redis://localhost:6379',
          },
        },
      },
      {
        name: 'integration',
        test: {
          include: ['tests/integration/**/*.test.ts'],
          environment: 'node',
        },
      },
      {
        name: 'e2e',
        test: {
          include: ['tests/e2e/**/*.test.ts'],
          environment: 'node',
        },
      },
    ],
  },
});