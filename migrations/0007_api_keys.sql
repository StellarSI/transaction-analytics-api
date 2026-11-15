-- up
-- API keys (issue #30). Only the argon2 hash is stored: the plaintext key is
-- shown once when it is issued and can never be recovered from the database.
CREATE TABLE IF NOT EXISTS api_keys (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT NOT NULL,
  key_hash TEXT NOT NULL UNIQUE,
  permissions JSONB NOT NULL DEFAULT '[]'::jsonb,
  rate_limit_tier TEXT NOT NULL DEFAULT 'free'
    CHECK (rate_limit_tier IN ('free', 'pro', 'enterprise')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_used_at TIMESTAMPTZ,
  revoked_at TIMESTAMPTZ
);

-- Lookups by hash are served by the unique index on key_hash; this index
-- keeps "list the active keys of an account" cheap.
CREATE INDEX IF NOT EXISTS idx_api_keys_active ON api_keys (revoked_at) WHERE revoked_at IS NULL;

-- down
DROP TABLE IF EXISTS api_keys;
