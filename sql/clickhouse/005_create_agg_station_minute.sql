CREATE TABLE IF NOT EXISTS agg_station_minute
(
    bucket_minute DateTime('UTC'),
    operator_id String,
    station_id String,
    city Nullable(String),
    country_code Nullable(String),
    events_total UInt64,
    meter_update_count UInt64,
    session_start_count UInt64,
    session_stop_count UInt64,
    fault_count UInt64,
    heartbeat_count UInt64,
    energy_kwh_sum Float64,
    revenue_eur_sum Float64,
    avg_power_kw Float64,
    max_power_kw Float64,
    active_connector_estimate UInt32,
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(bucket_minute)
ORDER BY (bucket_minute, operator_id, station_id);
