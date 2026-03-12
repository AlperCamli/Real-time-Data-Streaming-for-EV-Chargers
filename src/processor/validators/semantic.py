"""Business semantic validation hooks."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from src.common.event_types import EventType
from src.common.schemas.event_envelope import EventEnvelope
from src.common.schemas.event_payloads import (
    FaultAlertPayload,
    HeartbeatPayload,
    MeterUpdatePayload,
    SessionStartPayload,
    SessionStopPayload,
    StatusChangePayload,
)
from src.processor.state.session_state import SessionSnapshot


class SessionLookup(Protocol):
    def get_session(self, session_id: str) -> SessionSnapshot | None:
        """Returns active session snapshot if present."""


@dataclass(slots=True)
class SemanticValidationResult:
    errors: list[str]
    warnings: list[str]

    @property
    def ok(self) -> bool:
        return not self.errors


def validate_event_semantics(event: EventEnvelope, session_lookup: SessionLookup) -> SemanticValidationResult:
    errors: list[str] = []
    warnings: list[str] = []

    if event.event_type in {EventType.SESSION_START, EventType.METER_UPDATE, EventType.SESSION_STOP}:
        if not event.session_id:
            errors.append(f"session_id is required for {event.event_type.value}")

    if isinstance(event.payload, SessionStartPayload):
        if event.payload.initial_meter_kwh < 0:
            errors.append("SESSION_START payload.initial_meter_kwh must be non-negative")

    if isinstance(event.payload, MeterUpdatePayload):
        if event.sequence_no <= 0:
            errors.append("METER_UPDATE sequence_no must be > 0")
        if event.payload.meter_kwh < 0:
            errors.append("METER_UPDATE payload.meter_kwh must be non-negative")
        if event.payload.energy_delta_kwh < 0:
            errors.append("METER_UPDATE payload.energy_delta_kwh must be non-negative")
        if event.payload.power_kw is not None and event.payload.power_kw < 0:
            errors.append("METER_UPDATE payload.power_kw must be non-negative")

    if isinstance(event.payload, SessionStopPayload):
        if event.payload.final_meter_kwh < 0:
            errors.append("SESSION_STOP payload.final_meter_kwh must be non-negative")
        if event.payload.total_energy_kwh is not None and event.payload.total_energy_kwh < 0:
            errors.append("SESSION_STOP payload.total_energy_kwh must be non-negative")

    if isinstance(event.payload, HeartbeatPayload):
        if not event.payload.charger_status:
            errors.append("HEARTBEAT payload.charger_status must be non-empty")

    if isinstance(event.payload, StatusChangePayload):
        if not event.payload.previous_status or not event.payload.new_status:
            errors.append("STATUS_CHANGE previous_status and new_status must be non-empty")

    if isinstance(event.payload, FaultAlertPayload):
        if not event.payload.fault_code:
            errors.append("FAULT_ALERT payload.fault_code must be non-empty")
        if not event.payload.severity:
            errors.append("FAULT_ALERT payload.severity must be non-empty")

    existing_session = session_lookup.get_session(event.session_id) if event.session_id else None
    if existing_session is not None:
        if existing_session.station_id != event.station_id:
            errors.append("session identity mismatch: station_id")
        if existing_session.connector_id != event.connector_id:
            errors.append("session identity mismatch: connector_id")
        if existing_session.operator_id != event.operator_id:
            errors.append("session identity mismatch: operator_id")

    if event.event_type == EventType.SESSION_START and existing_session is not None:
        errors.append("SESSION_START received for already-active session_id")

    if event.event_type == EventType.SESSION_STOP and event.session_id and existing_session is None:
        errors.append("SESSION_STOP without active session context is invalid")

    if event.event_type == EventType.METER_UPDATE and event.session_id:
        if existing_session is None:
            warnings.append("METER_UPDATE without active session context; keep historical only")
        elif event.sequence_no <= existing_session.last_sequence_no:
            warnings.append("METER_UPDATE sequence_no is not increasing; keep historical only")

    return SemanticValidationResult(errors=errors, warnings=warnings)
