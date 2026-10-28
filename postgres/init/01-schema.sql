-- Runs once, on first initialisation of the data volume.
CREATE TABLE IF NOT EXISTS field_metrics_1m (
    field_id          VARCHAR(32)  NOT NULL,
    window_start      TIMESTAMP(3) NOT NULL,   -- UTC
    window_end        TIMESTAMP(3) NOT NULL,   -- UTC
    reading_count     BIGINT       NOT NULL,
    avg_soil_moisture DOUBLE PRECISION,
    min_soil_moisture DOUBLE PRECISION,
    max_soil_moisture DOUBLE PRECISION,
    avg_soil_temp_c   DOUBLE PRECISION,
    avg_air_temp_c    DOUBLE PRECISION,
    avg_humidity      DOUBLE PRECISION,
    avg_ph            DOUBLE PRECISION,
    PRIMARY KEY (field_id, window_start)       -- enables idempotent upserts from Flink
);

CREATE INDEX IF NOT EXISTS idx_fm1m_window ON field_metrics_1m (window_start DESC);
