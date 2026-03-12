"""Processor-side working session state scaffold."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from threading import Lock

from src.common.event_types import EventType
from src.common.schemas.event_envelope import EventEnvelope
from src.common.schemas.event_payloads import MeterUpdatePayload, SessionStartPayload, SessionStopPayload


@dataclass(slots=True)
class SessionSnapshot:
    session_id: str
    station_id: str
    connector_id: str
    operator_id: str
    started_at: datetime
    last_event_time: datetime
    last_sequence_no: int
    meter_update_count: int
    total_energy_kwh: float
    latest_meter_kwh: float
    latest_power_kw: float | None
    finalized_reason: str | None = None


@dataclass(slots=True)
class SessionMutationResult:
    applied: bool
    reason: str
    snapshot: SessionSnapshot | None


class SessionStateStore:
    def __init__(self) -> None:
        self._sessions: dict[str, SessionSnapshot] = {}
        self._lock = Lock()

    def get_session(self, session_id: str | None) -> SessionSnapshot | None:
        if not session_id:
            return None
        with self._lock:
            snapshot = self._sessions.get(session_id)
            return replace(snapshot) if snapshot is not None else None

    def apply_event(
        self,
        event: EventEnvelope,
        *,
        finalize_on_session_stop: bool = True,
        finalize_on_fault_termination: bool = True,
    ) -> SessionMutationResult:
        if event.event_type == EventType.SESSION_START:
            return self._start_session(event)
        if event.event_type == EventType.METER_UPDATE:
            return self._update_session(event)
        if event.event_type == EventType.SESSION_STOP:
            if not finalize_on_session_stop:
                return SessionMutationResult(applied=False, reason="session_stop_finalize_disabled", snapshot=None)
            return self._close_session(event, "normal_stop")
        if event.event_type == EventType.FAULT_ALERT and event.session_id:
            if not finalize_on_fault_termination:
                return SessionMutationResult(applied=False, reason="fault_finalize_disabled", snapshot=None)
            return self._close_session(event, "fault_termination")
        return SessionMutationResult(applied=False, reason="no_session_mutation_for_event_type", snapshot=None)

    def expire_inactive(self, now: datetime, inactivity_timeout_seconds: int) -> list[SessionSnapshot]:
        threshold = now.astimezone(timezone.utc) - timedelta(seconds=max(1, inactivity_timeout_seconds))
        finalized: list[SessionSnapshot] = []

        with self._lock:
            stale_ids = [
                session_id
                for session_id, snapshot in self._sessions.items()
                if snapshot.last_event_time <= threshold
            ]
            for session_id in stale_ids:
                snapshot = self._sessions.pop(session_id)
                finalized.append(replace(snapshot, finalized_reason="inactivity_timeout"))

        return finalized

    def _start_session(self, event: EventEnvelope) -> SessionMutationResult:
        if not event.session_id:
            return SessionMutationResult(applied=False, reason="missing_session_id", snapshot=None)

        with self._lock:
            existing = self._sessions.get(event.session_id)
            if existing is not None:
                return SessionMutationResult(applied=False, reason="session_already_active", snapshot=replace(existing))

            initial_meter_kwh = 0.0
            if isinstance(event.payload, SessionStartPayload):
                initial_meter_kwh = event.payload.initial_meter_kwh

            snapshot = SessionSnapshot(
                session_id=event.session_id,
                station_id=event.station_id,
                connector_id=event.connector_id,
                operator_id=event.operator_id,
                started_at=event.event_time,
                last_event_time=event.event_time,
                last_sequence_no=event.sequence_no,
                meter_update_count=0,
                total_energy_kwh=0.0,
                latest_meter_kwh=initial_meter_kwh,
                latest_power_kw=None,
            )
            self._sessions[event.session_id] = snapshot
            return SessionMutationResult(applied=True, reason="session_started", snapshot=replace(snapshot))

    def _update_session(self, event: EventEnvelope) -> SessionMutationResult:
        if not event.session_id:
            return SessionMutationResult(applied=False, reason="missing_session_id", snapshot=None)

        with self._lock:
            snapshot = self._sessions.get(event.session_id)
            if snapshot is None:
                return SessionMutationResult(applied=False, reason="session_not_found", snapshot=None)

            if event.sequence_no <= snapshot.last_sequence_no:
                return SessionMutationResult(applied=False, reason="non_increasing_sequence", snapshot=replace(snapshot))

            if not isinstance(event.payload, MeterUpdatePayload):
                return SessionMutationResult(applied=False, reason="invalid_payload_for_meter_update", snapshot=replace(snapshot))

            updated = replace(
                snapshot,
                last_event_time=max(snapshot.last_event_time, event.event_time),
                last_sequence_no=event.sequence_no,
                meter_update_count=snapshot.meter_update_count + 1,
                total_energy_kwh=snapshot.total_energy_kwh + event.payload.energy_delta_kwh,
                latest_meter_kwh=event.payload.meter_kwh,
                latest_power_kw=event.payload.power_kw,
            )
            self._sessions[event.session_id] = updated
            return SessionMutationResult(applied=True, reason="session_meter_updated", snapshot=replace(updated))

    def _close_session(self, event: EventEnvelope, finalized_reason: str) -> SessionMutationResult:
        if not event.session_id:
            return SessionMutationResult(applied=False, reason="missing_session_id", snapshot=None)

        with self._lock:
            snapshot = self._sessions.pop(event.session_id, None)
            if snapshot is None:
                return SessionMutationResult(applied=False, reason="session_not_found", snapshot=None)

            updated_total_energy = snapshot.total_energy_kwh
            updated_latest_meter = snapshot.latest_meter_kwh
            if isinstance(event.payload, SessionStopPayload):
                updated_latest_meter = event.payload.final_meter_kwh
                if event.payload.total_energy_kwh is not None:
                    updated_total_energy = event.payload.total_energy_kwh

            finalized = replace(
                snapshot,
                last_event_time=max(snapshot.last_event_time, event.event_time),
                last_sequence_no=max(snapshot.last_sequence_no, event.sequence_no),
                total_energy_kwh=updated_total_energy,
                latest_meter_kwh=updated_latest_meter,
                finalized_reason=finalized_reason,
            )
            return SessionMutationResult(applied=True, reason=finalized_reason, snapshot=finalized)
