CREATE TABLE IF NOT EXISTS fact_sessions
(
    session_id String,
    operator_id String,
    station_id String,
    connector_id String,
    session_start_time DateTime64(3, 'UTC'),
    session_end_time DateTime64(3, 'UTC'),
    location_city Nullable(String),
    location_country Nullable(String),
    vehicle_brand Nullable(String),
    vehicle_model Nullable(String),
    tariff_id String,
    duration_seconds UInt32,
    energy_kwh_total Float64,
    revenue_eur_total Float64,
    meter_update_count UInt32,
    peak_power_kw Float64,
    avg_power_kw Float64,
    session_completion_status LowCardinality(String),
    final_station_status LowCardinality(String),
    stop_reason String,
    is_complete UInt8,
    is_timeout_finalized UInt8,
    peak_hour_flag UInt8,
    revenue_per_kwh Float64,
    start_event_id String,
    stop_event_id Nullable(String),
    finalized_reason LowCardinality(String),
    finalized_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(session_start_time)
ORDER BY (operator_id, station_id, session_start_time, session_id);
