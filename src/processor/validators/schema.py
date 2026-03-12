"""Schema-level envelope validation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from src.common.schemas.event_envelope import EventEnvelope
from src.common.schemas.validation import ValidationError


@dataclass(slots=True)
class SchemaValidationResult:
    envelope: EventEnvelope | None
    errors: list[str]

    @property
    def ok(self) -> bool:
        return self.envelope is not None and not self.errors


def validate_envelope_schema(raw_event: Mapping[str, Any]) -> SchemaValidationResult:
    errors = _type_checks(raw_event)
    if errors:
        return SchemaValidationResult(envelope=None, errors=errors)

    try:
        envelope = EventEnvelope.from_dict(raw_event)
    except (ValidationError, ValueError, TypeError) as exc:
        return SchemaValidationResult(envelope=None, errors=[str(exc)])

    return SchemaValidationResult(envelope=envelope, errors=[])


def _type_checks(raw_event: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []

    required_str_fields = (
        "event_id",
        "event_type",
        "event_time",
        "ingest_time",
        "station_id",
        "connector_id",
        "operator_id",
        "schema_version",
        "producer_id",
    )
    for field in required_str_fields:
        value = raw_event.get(field)
        if not isinstance(value, str) or not value.strip():
            errors.append(f"{field} must be a non-empty string")

    session_id = raw_event.get("session_id")
    if session_id is not None and not isinstance(session_id, str):
        errors.append("session_id must be null or string")

    sequence_no = raw_event.get("sequence_no")
    if not isinstance(sequence_no, int) or isinstance(sequence_no, bool):
        errors.append("sequence_no must be an integer")
    elif sequence_no < 0:
        errors.append("sequence_no must be non-negative")

    payload = raw_event.get("payload")
    if not isinstance(payload, Mapping):
        errors.append("payload must be an object")

    location = raw_event.get("location")
    if not isinstance(location, Mapping):
        errors.append("location must be an object")
    else:
        for numeric_field in ("latitude", "longitude"):
            value = location.get(numeric_field)
            if value is not None and not isinstance(value, (int, float)):
                errors.append(f"location.{numeric_field} must be numeric when present")

    return errors
