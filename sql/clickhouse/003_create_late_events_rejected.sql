CREATE TABLE IF NOT EXISTS late_events_rejected
(
    event_id String,
    event_type LowCardinality(String),
    event_time DateTime64(3, 'UTC'),
    ingest_time DateTime64(3, 'UTC'),
    station_id String,
    connector_id String,
    operator_id String,
    session_id Nullable(String),
    schema_version String,
    producer_id String,
    sequence_no UInt64,
    lateness_seconds UInt32,
    payload_json String,
    rejected_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(rejected_at)
ORDER BY (rejected_at, station_id, event_id);
