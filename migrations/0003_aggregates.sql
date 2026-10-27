-- up
CREATE TABLE IF NOT EXISTS asset_flow_aggregates (
  bucket_start TIMESTAMPTZ NOT NULL,
  interval TEXT NOT NULL,
  asset_code TEXT NOT NULL,
  asset_issuer TEXT,
  volume NUMERIC NOT NULL DEFAULT 0,
  payments BIGINT NOT NULL DEFAULT 0,
  net_flow NUMERIC NOT NULL DEFAULT 0,
  PRIMARY KEY (bucket_start, interval, asset_code, asset_issuer)
);

SELECT create_hypertable('asset_flow_aggregates', 'bucket_start', chunk_time_interval => INTERVAL '1 hour', migrate_data => true);

CREATE TABLE IF NOT EXISTS account_flow_aggregates (
  bucket_start TIMESTAMPTZ NOT NULL,
  interval TEXT NOT NULL,
  account_id TEXT NOT NULL,
  inflow NUMERIC NOT NULL DEFAULT 0,
  outflow NUMERIC NOT NULL DEFAULT 0,
  counterparties JSONB NOT NULL DEFAULT '[]'::jsonb,
  net_flow NUMERIC NOT NULL DEFAULT 0,
  PRIMARY KEY (bucket_start, interval, account_id)
);

SELECT create_hypertable('account_flow_aggregates', 'bucket_start', chunk_time_interval => INTERVAL '1 hour', migrate_data => true);

CREATE TABLE IF NOT EXISTS corridor_aggregates (
  bucket_start TIMESTAMPTZ NOT NULL,
  interval TEXT NOT NULL,
  source_asset TEXT NOT NULL,
  dest_asset TEXT NOT NULL,
  source_anchor TEXT,
  dest_anchor TEXT,
  volume NUMERIC NOT NULL DEFAULT 0,
  payments BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (bucket_start, interval, source_asset, dest_asset)
);

SELECT create_hypertable('corridor_aggregates', 'bucket_start', chunk_time_interval => INTERVAL '1 hour', migrate_data => true);

ALTER TABLE asset_flow_aggregates SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'asset_code, asset_issuer'
);

ALTER TABLE account_flow_aggregates SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'account_id'
);

ALTER TABLE corridor_aggregates SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'source_asset, dest_asset'
);

SELECT add_compression_policy('asset_flow_aggregates', INTERVAL '30 days');
SELECT add_compression_policy('account_flow_aggregates', INTERVAL '30 days');
SELECT add_compression_policy('corridor_aggregates', INTERVAL '30 days');

-- down
DROP TABLE IF EXISTS corridor_aggregates;
DROP TABLE IF EXISTS account_flow_aggregates;
DROP TABLE IF EXISTS asset_flow_aggregates;
