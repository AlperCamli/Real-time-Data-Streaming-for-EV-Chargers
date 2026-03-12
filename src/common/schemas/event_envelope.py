"""Canonical event envelope model shared by simulator and processor."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping

from src.common.event_types import EventType
from src.common.schemas.event_payloads import PayloadModel, build_payload, payload_to_dict
from src.common.schemas.validation import parse_timestamp, validate_event_type, validate_required_fields


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
        validate_required_fields(raw_event)

        event_type = validate_event_type(raw_event["event_type"])
        payload = build_payload(event_type, raw_event["payload"])

        raw_location = raw_event.get("location", {})
        if not isinstance(raw_location, Mapping):
            raise ValueError("location must be a mapping")

        location = EventLocation(
            city=raw_location.get("city"),
            country=raw_location.get("country"),
            latitude=raw_location.get("latitude"),
            longitude=raw_location.get("longitude"),
        )

        return cls(
            event_id=str(raw_event["event_id"]),
            event_type=event_type,
            event_time=parse_timestamp(raw_event["event_time"], "event_time"),
            ingest_time=parse_timestamp(raw_event["ingest_time"], "ingest_time"),
            station_id=str(raw_event["station_id"]),
            connector_id=str(raw_event["connector_id"]),
            operator_id=str(raw_event["operator_id"]),
            session_id=(str(raw_event["session_id"]) if raw_event["session_id"] is not None else None),
            schema_version=str(raw_event["schema_version"]),
            producer_id=str(raw_event["producer_id"]),
            sequence_no=int(raw_event["sequence_no"]),
            location=location,
            payload=payload,
        )

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
