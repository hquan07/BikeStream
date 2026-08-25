-- Hourly summary per city (auto-maintained by TimescaleDB)
CREATE MATERIALIZED VIEW IF NOT EXISTS hourly_city_summary
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS bucket,
    city,
    AVG(fill_ratio)             AS avg_fill,
    MAX(num_bikes_available)    AS max_bikes,
    MIN(num_bikes_available)    AS min_bikes,
    COUNT(DISTINCT station_id)  AS active_stations
FROM station_snapshots
GROUP BY bucket, city
WITH NO DATA;

-- Refresh policy: update every 30 minutes, look back 3 hours
SELECT add_continuous_aggregate_policy('hourly_city_summary',
    start_offset    => INTERVAL '3 hours',
    end_offset      => INTERVAL '30 minutes',
    schedule_interval => INTERVAL '30 minutes',
    if_not_exists   => TRUE
);
