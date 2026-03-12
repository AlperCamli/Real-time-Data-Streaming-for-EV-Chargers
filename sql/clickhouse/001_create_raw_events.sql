CREATE TABLE IF NOT EXISTS raw_events
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
    location_city Nullable(String),
    location_country Nullable(String),
    location_latitude Nullable(Float64),
    location_longitude Nullable(Float64),
    payload_json String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (station_id, event_time, event_id);
