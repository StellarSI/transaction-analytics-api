import { defineConfig } from 'vitest/config';

export default defineConfig({
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