-- up
CREATE TABLE IF NOT EXISTS trustline_aggregates (
  bucket_start TIMESTAMPTZ NOT NULL,
  interval TEXT NOT NULL,
  account_id TEXT NOT NULL,
  trustline_count BIGINT NOT NULL DEFAULT 0,
  new_trustlines BIGINT NOT NULL DEFAULT 0,
  removed_trustlines BIGINT NOT NULL DEFAULT 0,
  trust_limit_changes BIGINT NOT NULL DEFAULT 0,
  total_trust_limit NUMERIC NOT NULL DEFAULT 0,
  payments_count BIGINT NOT NULL DEFAULT 0,
  active_hours BIGINT NOT NULL DEFAULT 0,
  dormancy_days BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (bucket_start, interval, account_id)
);

SELECT create_hypertable('trustline_aggregates', 'bucket_start', chunk_time_interval => INTERVAL '1 hour', migrate_data => true);

ALTER TABLE trustline_aggregates SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'account_id'
);

SELECT add_compression_policy('trustline_aggregates', INTERVAL '30 days');

-- down
DROP TABLE IF EXISTS trustline_aggregates;
