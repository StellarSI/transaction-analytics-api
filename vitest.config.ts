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