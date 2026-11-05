import { z } from 'zod';

const envSchema = z.object({
  NODE_ENV: z.enum(['development', 'production', 'test']).default('development'),
  PORT: z.coerce.number().default(3000),
  LOG_LEVEL: z.enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace']).default('info'),
  
  STELLAR_NETWORK: z.enum(['public', 'testnet']).default('testnet'),
  HORIZON_URL: z.string().url().default('https://horizon-testnet.stellar.org'),
  STELLAR_RPC_URL: z.string().url().default('https://soroban-testnet.stellar.org'),
  
  DATABASE_URL: z.string().min(1),
  REDIS_URL: z.string().url().default('redis://localhost:6379'),

  // Hubble/BigQuery historical backfill (issue #17). Both are optional —
  // only needed when running the backfill job; empty values fall back to
  // Application Default Credentials / the gcloud SDK configuration.
  BIGQUERY_PROJECT_ID: z.string().default(''),
  BIGQUERY_KEY_FILE: z.string().default(''),

  API_KEY_SALT: z.string().min(16),
});

const parsed = envSchema.safeParse(process.env);

if (!parsed.success) {
  console.error('❌ Invalid environment variables:');
  console.error(parsed.error.flatten().fieldErrors);
  process.exit(1);
}

export const config = parsed.data;
export type Config = z.infer<typeof envSchema>;