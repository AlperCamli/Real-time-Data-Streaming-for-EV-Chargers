CREATE TABLE IF NOT EXISTS agg_city_day_faults
(
    bucket_day Date,
    operator_id String,
    country_code String,
    city String,
    fault_count UInt64,
    distinct_station_count UInt64,
    most_common_fault_code Nullable(String),
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(bucket_day)
ORDER BY (bucket_day, operator_id, country_code, city);
