-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;
-- Enable PostGIS extension (for vector tiles)
CREATE EXTENSION IF NOT EXISTS postgis;

-- ==========================================
-- Dimension: Station master data
-- ==========================================
CREATE TABLE IF NOT EXISTS stations (
    station_id    TEXT NOT NULL,
    city          TEXT NOT NULL,
    station_name  TEXT,
    lat           DOUBLE PRECISION,
    lon           DOUBLE PRECISION,
    capacity      INTEGER,
    updated_at    TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (city, station_id)
);

-- ==========================================
-- Fact: Station snapshots (raw time-series)
-- ==========================================
CREATE TABLE IF NOT EXISTS station_snapshots (
    time                    TIMESTAMPTZ     NOT NULL,
    city                    TEXT            NOT NULL,
    station_id              TEXT            NOT NULL,
    station_name            TEXT,
    lat                     DOUBLE PRECISION,
    lon                     DOUBLE PRECISION,
    capacity                INTEGER,
    num_bikes_available     INTEGER,
    num_docks_available     INTEGER,
    num_ebikes_available    INTEGER,
    num_scooters_available  INTEGER,
    fill_ratio              REAL,
    status                  TEXT,
    needs_rebalancing       BOOLEAN
);

-- Convert to hypertable (auto-partitioned by time, 1 chunk per day)
SELECT create_hypertable('station_snapshots', 'time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Indexes for dashboard queries
CREATE INDEX IF NOT EXISTS idx_ss_city_time
    ON station_snapshots (city, time DESC);
CREATE INDEX IF NOT EXISTS idx_ss_station_time
    ON station_snapshots (station_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_ss_rebalancing
    ON station_snapshots (needs_rebalancing, city, time DESC)
    WHERE needs_rebalancing = TRUE;

-- ==========================================
-- Fact: System metrics (5-min aggregates)
-- ==========================================
CREATE TABLE IF NOT EXISTS system_metrics (
    time                TIMESTAMPTZ     NOT NULL,
    city                TEXT            NOT NULL,
    total_bikes         INTEGER,
    total_docks         INTEGER,
    total_ebikes        INTEGER,
    avg_fill_ratio      REAL,
    num_empty_stations  INTEGER,
    num_full_stations   INTEGER,
    utilization_pct     REAL
);

SELECT create_hypertable('system_metrics', 'time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- ==========================================
-- Dead Letter Queue: Failed/malformed records
-- ==========================================
CREATE TABLE IF NOT EXISTS dlq_events (
    time                TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    source_topic        TEXT,
    source_partition    INTEGER,
    source_offset       BIGINT,
    raw_payload         TEXT,
    error_type          TEXT            NOT NULL,
    error_message       TEXT
);

SELECT create_hypertable('dlq_events', 'time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_dlq_error_type
    ON dlq_events (error_type, time DESC);

-- ==========================================
-- ML: Station fill ratio predictions
-- ==========================================
CREATE TABLE IF NOT EXISTS station_predictions (
    prediction_time             TIMESTAMPTZ     NOT NULL,
    target_time                 TIMESTAMPTZ     NOT NULL,
    city                        TEXT            NOT NULL,
    station_id                  TEXT            NOT NULL,
    station_name                TEXT,
    lat                         DOUBLE PRECISION,
    lon                         DOUBLE PRECISION,
    current_fill_ratio          REAL,
    predicted_fill_ratio        REAL,
    predicted_status            TEXT,
    needs_rebalancing_predicted BOOLEAN
);

SELECT create_hypertable('station_predictions', 'prediction_time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_sp_city_target
    ON station_predictions (city, target_time DESC);
CREATE INDEX IF NOT EXISTS idx_sp_rebalancing
    ON station_predictions (needs_rebalancing_predicted, city, target_time DESC)
    WHERE needs_rebalancing_predicted = TRUE;

-- ==========================================
-- Weather: City weather snapshots
-- ==========================================
CREATE TABLE IF NOT EXISTS weather_snapshots (
    time                TIMESTAMPTZ     NOT NULL,
    city                TEXT            NOT NULL,
    temp_celsius        DOUBLE PRECISION,
    feels_like          DOUBLE PRECISION,
    humidity            INTEGER,
    pressure            INTEGER,
    wind_speed          DOUBLE PRECISION,
    weather_main        TEXT,
    weather_desc        TEXT,
    rain_1h_mm          DOUBLE PRECISION DEFAULT 0,
    snow_1h_mm          DOUBLE PRECISION DEFAULT 0,
    clouds_pct          INTEGER,
    visibility_m        INTEGER
);

SELECT create_hypertable('weather_snapshots', 'time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_weather_city_time
    ON weather_snapshots (city, time DESC);

-- ==========================================
-- Spatial View for pg_tileserv (Vector Tiles)
-- ==========================================
CREATE OR REPLACE VIEW public.spatial_station_status AS
SELECT DISTINCT ON (city, station_id)
    city,
    station_id,
    station_name,
    num_bikes_available,
    num_docks_available,
    capacity,
    fill_ratio,
    status,
    needs_rebalancing,
    time,
    ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geometry(Point, 4326) AS geom
FROM station_snapshots
WHERE lat IS NOT NULL AND lon IS NOT NULL
  AND lat != 0 AND lon != 0
ORDER BY city, station_id, time DESC;

COMMENT ON VIEW public.spatial_station_status IS 'Latest station status with PostGIS geometry for pg_tileserv vector tile serving';
