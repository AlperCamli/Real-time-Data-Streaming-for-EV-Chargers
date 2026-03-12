CREATE TABLE IF NOT EXISTS agg_city_day_faults
(
    bucket_day Date,
    city String,
    operator_id String,
    fault_events UInt64,
    affected_stations UInt64,
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(bucket_day)
ORDER BY (bucket_day, operator_id, city);
