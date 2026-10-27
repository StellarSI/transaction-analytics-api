-- up
CREATE TABLE IF NOT EXISTS ledger_operations (
  id BIGSERIAL PRIMARY KEY,
  ledger_sequence BIGINT NOT NULL,
  ledger_closed_at TIMESTAMPTZ NOT NULL,
  operation_type TEXT NOT NULL,
  source_account TEXT,
  destination_account TEXT,
  asset_code TEXT,
  asset_issuer TEXT,
  asset_type TEXT,
  amount NUMERIC,
  fee_charged NUMERIC,
  transaction_hash TEXT,
  operation_index INT,
  successful BOOLEAN NOT NULL DEFAULT true
);

SELECT create_hypertable('ledger_operations', 'ledger_closed_at', chunk_time_interval => INTERVAL '1 day', migrate_data => true);

CREATE INDEX IF NOT EXISTS idx_ledger_operations_ledger_sequence ON ledger_operations (ledger_sequence);
CREATE INDEX IF NOT EXISTS idx_ledger_operations_source_account ON ledger_operations (source_account);
CREATE INDEX IF NOT EXISTS idx_ledger_operations_asset ON ledger_operations (asset_code, asset_issuer);
CREATE INDEX IF NOT EXISTS idx_ledger_operations_ledger_closed_at ON ledger_operations (ledger_closed_at);

ALTER TABLE ledger_operations SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'source_account, asset_code, asset_issuer'
);

SELECT add_compression_policy('ledger_operations', INTERVAL '30 days');

-- down
DROP TABLE IF EXISTS ledger_operations;
