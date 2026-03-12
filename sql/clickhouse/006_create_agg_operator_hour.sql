CREATE TABLE IF NOT EXISTS agg_operator_hour
(
    bucket_hour DateTime('UTC'),
    operator_id String,
    energy_kwh_sum Float64,
    revenue_eur_sum Float64,
    sessions_completed UInt64,
    sessions_incomplete UInt64,
    fault_count UInt64,
    distinct_station_count UInt64,
    avg_session_duration_seconds Float64,
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(bucket_hour)
ORDER BY (bucket_hour, operator_id);
