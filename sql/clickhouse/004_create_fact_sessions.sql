CREATE TABLE IF NOT EXISTS fact_sessions
(
    session_id String,
    station_id String,
    connector_id String,
    operator_id String,
    started_at DateTime64(3, 'UTC'),
    ended_at DateTime64(3, 'UTC'),
    finalized_reason LowCardinality(String),
    duration_seconds UInt32,
    total_energy_kwh Float64,
    start_event_id String,
    stop_event_id Nullable(String),
    finalized_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(started_at)
ORDER BY (operator_id, station_id, started_at, session_id);
