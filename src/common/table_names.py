"""Frozen ClickHouse table names."""

TABLE_RAW_EVENTS = "raw_events"
TABLE_DEAD_LETTER_EVENTS = "dead_letter_events"
TABLE_LATE_EVENTS_REJECTED = "late_events_rejected"
TABLE_FACT_SESSIONS = "fact_sessions"
TABLE_AGG_STATION_MINUTE = "agg_station_minute"
TABLE_AGG_OPERATOR_HOUR = "agg_operator_hour"
TABLE_AGG_CITY_DAY_FAULTS = "agg_city_day_faults"

ALL_TABLES: tuple[str, ...] = (
    TABLE_RAW_EVENTS,
    TABLE_DEAD_LETTER_EVENTS,
    TABLE_LATE_EVENTS_REJECTED,
    TABLE_FACT_SESSIONS,
    TABLE_AGG_STATION_MINUTE,
    TABLE_AGG_OPERATOR_HOUR,
    TABLE_AGG_CITY_DAY_FAULTS,
)
