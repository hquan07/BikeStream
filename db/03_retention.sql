-- Auto-delete raw snapshots older than 30 days
SELECT add_retention_policy('station_snapshots', INTERVAL '30 days',
    if_not_exists => TRUE);

-- Keep aggregated metrics for 1 year
SELECT add_retention_policy('system_metrics', INTERVAL '365 days',
    if_not_exists => TRUE);
