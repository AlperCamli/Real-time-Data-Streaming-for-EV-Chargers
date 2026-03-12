CREATE TABLE IF NOT EXISTS agg_operator_hour
(
    bucket_hour DateTime('UTC'),
    operator_id String,
    sessions_total UInt64,
    energy_kwh_total Float64,
    avg_session_duration_seconds Float64,
    fault_events UInt64,
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(bucket_hour)
ORDER BY (bucket_hour, operator_id);
