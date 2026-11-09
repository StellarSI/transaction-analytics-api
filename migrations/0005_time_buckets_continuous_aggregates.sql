-- up
-- Continuous aggregates roll up minute buckets into hour/day buckets for
-- fast historical queries (issue #22). The minute buckets are materialized
-- by the aggregators themselves; these views are additive and idempotent.

CREATE MATERIALIZED VIEW IF NOT EXISTS asset_flow_aggregates_1h WITH (timescaledb.continuous) AS
SELECT time_bucket('1 hour', bucket_start) AS bucket_start,
       '1h' AS interval,
       asset_code, asset_issuer,
       SUM(volume) AS volume, SUM(payments) AS payments, SUM(net_flow) AS net_flow
FROM asset_flow_aggregates
WHERE interval = '1m'
GROUP BY bucket_start, asset_code, asset_issuer
WITH NO DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS asset_flow_aggregates_1d WITH (timescaledb.continuous) AS
SELECT time_bucket('1 day', bucket_start) AS bucket_start,
       '1d' AS interval,
       asset_code, asset_issuer,
       SUM(volume) AS volume, SUM(payments) AS payments, SUM(net_flow) AS net_flow
FROM asset_flow_aggregates
WHERE interval = '1m'
GROUP BY bucket_start, asset_code, asset_issuer
WITH NO DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS account_flow_aggregates_1h WITH (timescaledb.continuous) AS
SELECT time_bucket('1 hour', bucket_start) AS bucket_start,
       '1h' AS interval,
       account_id,
       SUM(inflow) AS inflow, SUM(outflow) AS outflow, SUM(net_flow) AS net_flow
FROM account_flow_aggregates
WHERE interval = '1m'
GROUP BY bucket_start, account_id
WITH NO DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS account_flow_aggregates_1d WITH (timescaledb.continuous) AS
SELECT time_bucket('1 day', bucket_start) AS bucket_start,
       '1d' AS interval,
       account_id,
       SUM(inflow) AS inflow, SUM(outflow) AS outflow, SUM(net_flow) AS net_flow
FROM account_flow_aggregates
WHERE interval = '1m'
GROUP BY bucket_start, account_id
WITH NO DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS corridor_aggregates_1h WITH (timescaledb.continuous) AS
SELECT time_bucket('1 hour', bucket_start) AS bucket_start,
       '1h' AS interval,
       source_asset, dest_asset, source_anchor, dest_anchor,
       SUM(volume) AS volume, SUM(payments) AS payments
FROM corridor_aggregates
WHERE interval = '1m'
GROUP BY bucket_start, source_asset, dest_asset, source_anchor, dest_anchor
WITH NO DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS corridor_aggregates_1d WITH (timescaledb.continuous) AS
SELECT time_bucket('1 day', bucket_start) AS bucket_start,
       '1d' AS interval,
       source_asset, dest_asset, source_anchor, dest_anchor,
       SUM(volume) AS volume, SUM(payments) AS payments
FROM corridor_aggregates
WHERE interval = '1m'
GROUP BY bucket_start, source_asset, dest_asset, source_anchor, dest_anchor
WITH NO DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS trustline_aggregates_1h WITH (timescaledb.continuous) AS
SELECT time_bucket('1 hour', bucket_start) AS bucket_start,
       '1h' AS interval,
       account_id,
       SUM(trustline_count) AS trustline_count,
       SUM(new_trustlines) AS new_trustlines,
       SUM(removed_trustlines) AS removed_trustlines,
       SUM(payments_count) AS payments_count,
       MAX(active_hours) AS active_hours,
       MAX(dormancy_days) AS dormancy_days
FROM trustline_aggregates
WHERE interval = '1m'
GROUP BY bucket_start, account_id
WITH NO DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS trustline_aggregates_1d WITH (timescaledb.continuous) AS
SELECT time_bucket('1 day', bucket_start) AS bucket_start,
       '1d' AS interval,
       account_id,
       SUM(trustline_count) AS trustline_count,
       SUM(new_trustlines) AS new_trustlines,
       SUM(removed_trustlines) AS removed_trustlines,
       SUM(payments_count) AS payments_count,
       MAX(active_hours) AS active_hours,
       MAX(dormancy_days) AS dormancy_days
FROM trustline_aggregates
WHERE interval = '1m'
GROUP BY bucket_start, account_id
WITH NO DATA;

-- Refresh policies: continuous for the recent hour, scheduled for older data.
SELECT add_continuous_aggregate_policy('asset_flow_aggregates_1h', start_offset => INTERVAL '1 day', end_offset => INTERVAL '1 hour', schedule_interval => INTERVAL '30 minutes');
SELECT add_continuous_aggregate_policy('asset_flow_aggregates_1d', start_offset => INTERVAL '1 day', end_offset => INTERVAL '1 day', schedule_interval => INTERVAL '1 day');
SELECT add_continuous_aggregate_policy('account_flow_aggregates_1h', start_offset => INTERVAL '1 day', end_offset => INTERVAL '1 hour', schedule_interval => INTERVAL '30 minutes');
SELECT add_continuous_aggregate_policy('account_flow_aggregates_1d', start_offset => INTERVAL '1 day', end_offset => INTERVAL '1 day', schedule_interval => INTERVAL '1 day');
SELECT add_continuous_aggregate_policy('corridor_aggregates_1h', start_offset => INTERVAL '1 day', end_offset => INTERVAL '1 hour', schedule_interval => INTERVAL '30 minutes');
SELECT add_continuous_aggregate_policy('corridor_aggregates_1d', start_offset => INTERVAL '1 day', end_offset => INTERVAL '1 day', schedule_interval => INTERVAL '1 day');
SELECT add_continuous_aggregate_policy('trustline_aggregates_1h', start_offset => INTERVAL '1 day', end_offset => INTERVAL '1 hour', schedule_interval => INTERVAL '30 minutes');
SELECT add_continuous_aggregate_policy('trustline_aggregates_1d', start_offset => INTERVAL '1 day', end_offset => INTERVAL '1 day', schedule_interval => INTERVAL '1 day');

-- down
DROP MATERIALIZED VIEW IF EXISTS trustline_aggregates_1d;
DROP MATERIALIZED VIEW IF EXISTS trustline_aggregates_1h;
DROP MATERIALIZED VIEW IF EXISTS corridor_aggregates_1d;
DROP MATERIALIZED VIEW IF EXISTS corridor_aggregates_1h;
DROP MATERIALIZED VIEW IF EXISTS account_flow_aggregates_1d;
DROP MATERIALIZED VIEW IF EXISTS account_flow_aggregates_1h;
DROP MATERIALIZED VIEW IF EXISTS asset_flow_aggregates_1d;
DROP MATERIALIZED VIEW IF EXISTS asset_flow_aggregates_1h;
