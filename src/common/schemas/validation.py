"""Validation helpers for canonical envelope ingestion."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping

from src.common.event_types import EventType
from src.common.schemas.event_payloads import build_payload


REQUIRED_ENVELOPE_FIELDS: tuple[str, ...] = (
    "event_id",
    "event_type",
    "event_time",
    "ingest_time",
    "station_id",
    "connector_id",
    "operator_id",
    "session_id",
    "schema_version",
    "producer_id",
    "sequence_no",
    "location",
    "payload",
)


class ValidationError(ValueError):
    """Raised when event contract validation fails."""


def parse_timestamp(value: Any, field_name: str) -> datetime:
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        raw = value.strip()
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(raw)
        except ValueError as exc:
            raise ValidationError(f"Invalid timestamp for {field_name}: {value}") from exc
    else:
        raise ValidationError(f"Timestamp for {field_name} must be a string or datetime")

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)


def validate_required_fields(event: Mapping[str, Any]) -> None:
    missing_fields = [field for field in REQUIRED_ENVELOPE_FIELDS if field not in event]
    if missing_fields:
        raise ValidationError(f"Missing required fields: {', '.join(missing_fields)}")


def validate_event_type(value: Any) -> EventType:
    if isinstance(value, EventType):
        return value

    try:
        return EventType(str(value))
    except ValueError as exc:
        raise ValidationError(f"Invalid event_type: {value}") from exc


def validate_payload(event_type: EventType, payload: Any) -> None:
    if not isinstance(payload, Mapping):
        raise ValidationError("payload must be a mapping")

    try:
        build_payload(event_type, payload)
    except ValueError as exc:
        raise ValidationError(str(exc)) from exc


def run_semantic_validation_hooks(event: Mapping[str, Any]) -> list[str]:
    """Placeholder for policy-level checks (lateness windows, sequence continuity, etc.)."""

    issues: list[str] = []

    sequence_no = event.get("sequence_no")
    if isinstance(sequence_no, int) and sequence_no < 0:
        issues.append("sequence_no must be non-negative")

    return issues
