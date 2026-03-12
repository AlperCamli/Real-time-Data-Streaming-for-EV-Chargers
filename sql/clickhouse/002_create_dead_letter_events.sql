CREATE TABLE IF NOT EXISTS dead_letter_events
(
    event_id String,
    event_type LowCardinality(String),
    event_time Nullable(DateTime64(3, 'UTC')),
    ingest_time DateTime64(3, 'UTC'),
    station_id Nullable(String),
    connector_id Nullable(String),
    operator_id Nullable(String),
    session_id Nullable(String),
    schema_version Nullable(String),
    producer_id Nullable(String),
    sequence_no Nullable(UInt64),
    error_reason String,
    raw_payload_json String,
    failed_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(failed_at)
ORDER BY (failed_at, event_id);
