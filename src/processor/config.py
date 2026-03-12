"""Processor configuration model and parsing."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from src.common.settings import ServiceSettings


@dataclass(frozen=True)
class ConsumerConfig:
    poll_timeout_ms: int
    max_poll_records: int
    auto_offset_reset: str


@dataclass(frozen=True)
class ProcessingConfig:
    event_time_field: str
    dedup_key_field: str
    dedup_ttl_seconds: int
    count_duplicates_and_discard: bool
    allowed_lateness_seconds: int
    late_events_enabled: bool


@dataclass(frozen=True)
class SessionRulesConfig:
    finalize_on_session_stop: bool
    inactivity_timeout_seconds: int
    timeout_sweeper_interval_seconds: float
    finalize_on_fault_termination: bool
    fact_sessions_append_once: bool
    retro_correction_from_ultra_late: bool
    default_tariff_eur_per_kwh: float


@dataclass(frozen=True)
class SinkConfig:
    clickhouse_batch_size: int
    clickhouse_flush_interval_seconds: float
    kafka_batch_size: int
    flush_interval_seconds: float
    redis_session_state_ttl_seconds: int
    redis_finalized_session_ttl_seconds: int


@dataclass(frozen=True)
class ProcessorConfig:
    consumer: ConsumerConfig
    processing: ProcessingConfig
    session_rules: SessionRulesConfig
    sinks: SinkConfig


def build_processor_config(raw: Mapping[str, Any], service_settings: ServiceSettings) -> ProcessorConfig:
    consumer_raw = _as_mapping(raw.get("consumer"))
    processing_raw = _as_mapping(raw.get("processing"))
    session_rules_raw = _as_mapping(raw.get("session_rules"))
    sinks_raw = _as_mapping(raw.get("sinks"))

    dedup_ttl_seconds = _as_int(
        processing_raw.get("dedup_ttl_seconds"),
        service_settings.redis.dedup_ttl_seconds,
    )
    too_late_threshold = _as_int(processing_raw.get("too_late_threshold_seconds"), 600)
    allowed_lateness = _as_int(processing_raw.get("allowed_lateness_seconds"), too_late_threshold)

    return ProcessorConfig(
        consumer=ConsumerConfig(
            poll_timeout_ms=max(100, _as_int(consumer_raw.get("poll_timeout_ms"), 1000)),
            max_poll_records=max(1, _as_int(consumer_raw.get("max_poll_records"), 500)),
            auto_offset_reset=str(consumer_raw.get("auto_offset_reset", "earliest")),
        ),
        processing=ProcessingConfig(
            event_time_field=str(processing_raw.get("event_time_field", "event_time")),
            dedup_key_field=str(processing_raw.get("dedup_key_field", "event_id")),
            dedup_ttl_seconds=max(60, dedup_ttl_seconds),
            count_duplicates_and_discard=_as_bool(processing_raw.get("count_duplicates_and_discard"), True),
            allowed_lateness_seconds=max(1, allowed_lateness),
            late_events_enabled=_as_bool(processing_raw.get("late_events_enabled"), True),
        ),
        session_rules=SessionRulesConfig(
            finalize_on_session_stop=_as_bool(session_rules_raw.get("finalize_on_session_stop"), True),
            inactivity_timeout_seconds=max(30, _as_int(session_rules_raw.get("inactivity_timeout_seconds"), 900)),
            timeout_sweeper_interval_seconds=max(
                0.5,
                _as_float(session_rules_raw.get("timeout_sweeper_interval_seconds"), 5.0),
            ),
            finalize_on_fault_termination=_as_bool(session_rules_raw.get("finalize_on_fault_termination"), True),
            fact_sessions_append_once=_as_bool(session_rules_raw.get("fact_sessions_append_once"), True),
            retro_correction_from_ultra_late=_as_bool(session_rules_raw.get("retro_correction_from_ultra_late"), False),
            default_tariff_eur_per_kwh=max(0.0, _as_float(session_rules_raw.get("default_tariff_eur_per_kwh"), 0.35)),
        ),
        sinks=SinkConfig(
            clickhouse_batch_size=max(1, _as_int(sinks_raw.get("clickhouse_batch_size"), 500)),
            clickhouse_flush_interval_seconds=max(
                0.1,
                _as_float(sinks_raw.get("clickhouse_flush_interval_seconds"), _as_float(sinks_raw.get("flush_interval_seconds"), 1.0)),
            ),
            kafka_batch_size=max(1, _as_int(sinks_raw.get("kafka_batch_size"), 200)),
            flush_interval_seconds=max(0.1, _as_float(sinks_raw.get("flush_interval_seconds"), 1.0)),
            redis_session_state_ttl_seconds=max(60, _as_int(sinks_raw.get("redis_session_state_ttl_seconds"), 86400)),
            redis_finalized_session_ttl_seconds=max(
                60,
                _as_int(sinks_raw.get("redis_finalized_session_ttl_seconds"), 3600),
            ),
        ),
    )


def _as_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}


def _as_int(value: Any, default: int) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_float(value: Any, default: float) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    raw = str(value).strip().lower()
    if raw in {"1", "true", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "no", "n", "off"}:
        return False
    return default
