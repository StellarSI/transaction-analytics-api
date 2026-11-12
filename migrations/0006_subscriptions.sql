-- up
CREATE TABLE IF NOT EXISTS subscriptions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  api_key_id TEXT NOT NULL,
  target_type TEXT NOT NULL CHECK (target_type IN ('account', 'asset', 'threshold')),
  target_value TEXT NOT NULL,
  url TEXT NOT NULL,
  secret TEXT,
  events JSONB NOT NULL DEFAULT '[]'::jsonb,
  active BOOLEAN NOT NULL DEFAULT true,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_subscriptions_api_key ON subscriptions (api_key_id);
CREATE INDEX IF NOT EXISTS idx_subscriptions_target ON subscriptions (target_type, target_value);

-- down
DROP TABLE IF EXISTS subscriptions;
