"""Shared processor models for routing and sink payloads."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Mapping

from src.common.schemas.validation import parse_timestamp


@dataclass(slots=True)
class DeadLetterRecord:
    error_reason: str
    raw_payload_json: str
    failed_at: datetime
    event_id: str = "unknown"
    event_type: str = "UNKNOWN"
    event_time: datetime | None = None
    ingest_time: datetime | None = None
    station_id: str | None = None
    connector_id: str | None = None
    operator_id: str | None = None
    session_id: str | None = None
    schema_version: str | None = None
    producer_id: str | None = None
    sequence_no: int | None = None
    source_topic: str | None = None
    source_partition: int | None = None
    source_offset: int | None = None


@dataclass(slots=True)
class BatchOutcome:
    processed_count: int = 0
    accepted_count: int = 0
    invalid_count: int = 0
    duplicate_count: int = 0
    late_rejected_count: int = 0
    stale_redis_count: int = 0
    accepted_late_count: int = 0
    errors: list[str] = field(default_factory=list)


@dataclass(slots=True)
class MessageContext:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    timestamp_ms: int | None = None


def dead_letter_from_raw(
    error_reason: str,
    raw_payload_json: str,
    failed_at: datetime,
    raw_event: Mapping[str, Any] | None,
    source_topic: str,
    source_partition: int,
    source_offset: int,
) -> DeadLetterRecord:
    raw = raw_event or {}
    return DeadLetterRecord(
        error_reason=error_reason,
        raw_payload_json=raw_payload_json,
        failed_at=failed_at,
        event_id=str(raw.get("event_id") or "unknown"),
        event_type=str(raw.get("event_type") or "UNKNOWN"),
        event_time=_as_datetime(raw.get("event_time")),
        ingest_time=_as_datetime(raw.get("ingest_time")),
        station_id=_as_optional_str(raw.get("station_id")),
        connector_id=_as_optional_str(raw.get("connector_id")),
        operator_id=_as_optional_str(raw.get("operator_id")),
        session_id=_as_optional_str(raw.get("session_id")),
        schema_version=_as_optional_str(raw.get("schema_version")),
        producer_id=_as_optional_str(raw.get("producer_id")),
        sequence_no=_as_optional_int(raw.get("sequence_no")),
        source_topic=source_topic,
        source_partition=source_partition,
        source_offset=source_offset,
    )


def _as_optional_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        parsed = int(value)
        return parsed if parsed >= 0 else None
    except (TypeError, ValueError):
        return None


def _as_optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _as_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return parse_timestamp(value, "dlq_timestamp")
        except ValueError:
            return None
    return None
