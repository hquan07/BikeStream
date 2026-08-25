-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

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
