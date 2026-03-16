"""Canonical event envelope model shared by simulator and processor."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping

from src.common.event_types import EventType
from src.common.schemas.event_payloads import PayloadModel, build_payload, payload_to_dict
from src.common.schemas.validation import REQUIRED_ENVELOPE_FIELDS, ValidationError, parse_timestamp, validate_event_type


@dataclass(slots=True)
class EventLocation:
    city: str | None = None
    country: str | None = None
    latitude: float | None = None
    longitude: float | None = None


@dataclass(slots=True)
class EventEnvelope:
    event_id: str
    event_type: EventType
    event_time: datetime
    ingest_time: datetime
    station_id: str
    connector_id: str
    operator_id: str
    session_id: str | None
    schema_version: str
    producer_id: str
    sequence_no: int
    location: EventLocation
    payload: PayloadModel

    @classmethod
    def from_dict(cls, raw_event: Mapping[str, Any]) -> "EventEnvelope":
        envelope, _ = cls.from_dict_with_payload_json(raw_event)
        return envelope

    @classmethod
    def from_dict_with_payload_json(cls, raw_event: Mapping[str, Any]) -> tuple["EventEnvelope", str]:
        required_values = _collect_required_fields(raw_event)

        event_id = _required_non_empty_str(required_values["event_id"], "event_id")
        event_type = validate_event_type(_required_non_empty_str(required_values["event_type"], "event_type"))
        event_time = parse_timestamp(_required_non_empty_str(required_values["event_time"], "event_time"), "event_time")
        ingest_time = parse_timestamp(
            _required_non_empty_str(required_values["ingest_time"], "ingest_time"),
            "ingest_time",
        )
        station_id = _required_non_empty_str(required_values["station_id"], "station_id")
        connector_id = _required_non_empty_str(required_values["connector_id"], "connector_id")
        operator_id = _required_non_empty_str(required_values["operator_id"], "operator_id")
        schema_version = _required_non_empty_str(required_values["schema_version"], "schema_version")
        producer_id = _required_non_empty_str(required_values["producer_id"], "producer_id")
        session_id = _optional_str(required_values["session_id"], "session_id")
        sequence_no = _sequence_no(required_values["sequence_no"])

        raw_payload = required_values["payload"]
        if not isinstance(raw_payload, Mapping):
            raise ValidationError("payload must be an object")
        payload = build_payload(event_type, raw_payload)
        payload_json = json.dumps(dict(raw_payload), separators=(",", ":"), ensure_ascii=True)

        raw_location = required_values["location"]
        if not isinstance(raw_location, Mapping):
            raise ValidationError("location must be an object")

        location = EventLocation(
            city=raw_location.get("city"),
            country=raw_location.get("country"),
            latitude=_optional_numeric(raw_location.get("latitude"), "location.latitude"),
            longitude=_optional_numeric(raw_location.get("longitude"), "location.longitude"),
        )

        return cls(
            event_id=event_id,
            event_type=event_type,
            event_time=event_time,
            ingest_time=ingest_time,
            station_id=station_id,
            connector_id=connector_id,
            operator_id=operator_id,
            session_id=session_id,
            schema_version=schema_version,
            producer_id=producer_id,
            sequence_no=sequence_no,
            location=location,
            payload=payload,
        ), payload_json

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type.value,
            "event_time": self.event_time.isoformat(),
            "ingest_time": self.ingest_time.isoformat(),
            "station_id": self.station_id,
            "connector_id": self.connector_id,
            "operator_id": self.operator_id,
            "session_id": self.session_id,
            "schema_version": self.schema_version,
            "producer_id": self.producer_id,
            "sequence_no": self.sequence_no,
            "location": {
                "city": self.location.city,
                "country": self.location.country,
                "latitude": self.location.latitude,
                "longitude": self.location.longitude,
            },
            "payload": payload_to_dict(self.payload),
        }


def _required_non_empty_str(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{field_name} must be a non-empty string")
    return value


def _collect_required_fields(raw_event: Mapping[str, Any]) -> dict[str, object]:
    required_values: dict[str, object] = {}
    missing_fields: list[str] = []
    for field_name in REQUIRED_ENVELOPE_FIELDS:
        if field_name not in raw_event:
            missing_fields.append(field_name)
            continue
        required_values[field_name] = raw_event[field_name]
    if missing_fields:
        raise ValidationError(f"Missing required fields: {', '.join(missing_fields)}")
    return required_values


def _optional_str(value: object, field_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValidationError(f"{field_name} must be null or string")
    return value


def _sequence_no(value: object) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValidationError("sequence_no must be an integer")
    if value < 0:
        raise ValidationError("sequence_no must be non-negative")
    return value


def _optional_numeric(value: object, field_name: str) -> float | None:
    if value is None:
        return None
    if not isinstance(value, (int, float)):
        raise ValidationError(f"{field_name} must be numeric when present")
    return float(value)
