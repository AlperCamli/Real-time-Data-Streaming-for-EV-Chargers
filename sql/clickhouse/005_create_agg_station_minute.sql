CREATE TABLE IF NOT EXISTS agg_station_minute
(
    bucket_minute DateTime('UTC'),
    station_id String,
    operator_id String,
    events_total UInt64,
    energy_kwh Float64,
    active_sessions UInt32,
    fault_events UInt64,
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(bucket_minute)
ORDER BY (bucket_minute, operator_id, station_id);
