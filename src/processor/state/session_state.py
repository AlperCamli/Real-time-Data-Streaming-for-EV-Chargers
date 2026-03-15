"""Processor-side working session state and finalization snapshots."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from threading import Lock

from src.common.event_types import EventType
from src.common.schemas.event_envelope import EventEnvelope
from src.common.schemas.event_payloads import (
    FaultAlertPayload,
    MeterUpdatePayload,
    SessionStartPayload,
    SessionStopPayload,
    StatusChangePayload,
)


DEFAULT_TARIFF_ID = "default"
DEFAULT_TARIFF_EUR_PER_KWH = 0.35


@dataclass(slots=True)
class SessionSnapshot:
    session_id: str
    station_id: str
    connector_id: str
    operator_id: str
    location_city: str | None
    location_country: str | None
    start_event_id: str
    stop_event_id: str | None
    started_at: datetime
    ended_at: datetime | None
    last_event_time: datetime
    last_ingest_time: datetime
    last_sequence_no: int
    meter_update_count: int
    total_energy_kwh: float
    latest_meter_kwh: float
    latest_power_kw: float | None
    max_power_kw_seen: float
    power_kw_sum: float
    power_kw_samples: int
    vehicle_brand: str | None
    vehicle_model: str | None
    tariff_id: str
    tariff_eur_per_kwh: float
    stop_reason: str | None
    current_status: str
    finalized_reason: str | None = None


@dataclass(slots=True)
class SessionMutationResult:
    applied: bool
    reason: str
    snapshot: SessionSnapshot | None
    finalized: bool = False


class SessionStateStore:
    def __init__(self) -> None:
        self._sessions: dict[str, SessionSnapshot] = {}
        self._station_sessions: dict[str, set[str]] = {}
        self._lock = Lock()

    def get_session(self, session_id: str | None) -> SessionSnapshot | None:
        if not session_id:
            return None
        with self._lock:
            snapshot = self._sessions.get(session_id)
            return replace(snapshot) if snapshot is not None else None

    def active_sessions_for_station(self, station_id: str) -> int:
        with self._lock:
            return len(self._station_sessions.get(station_id, set()))

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
            stop_reason = event.payload.end_reason if isinstance(event.payload, SessionStopPayload) else "session_stop"
            return self._close_session(event, "normal_stop", stop_reason)
        if event.event_type == EventType.FAULT_ALERT and event.session_id:
            if not finalize_on_fault_termination:
                return SessionMutationResult(applied=False, reason="fault_finalize_disabled", snapshot=None)
            stop_reason = "fault"
            if isinstance(event.payload, FaultAlertPayload):
                stop_reason = f"fault:{event.payload.fault_code}"
            return self._close_session(event, "fault_termination", stop_reason)
        if event.event_type == EventType.STATUS_CHANGE and event.session_id:
            if not finalize_on_fault_termination:
                return SessionMutationResult(applied=False, reason="fault_finalize_disabled", snapshot=None)
            if isinstance(event.payload, StatusChangePayload) and event.payload.new_status.strip().lower() == "faulted":
                stop_reason = f"status_fault:{event.payload.reason or 'status_change'}"
                return self._close_session(event, "fault_termination", stop_reason)
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
                station_sessions = self._station_sessions.get(snapshot.station_id)
                if station_sessions is not None:
                    station_sessions.discard(session_id)
                    if not station_sessions:
                        del self._station_sessions[snapshot.station_id]
                finalized.append(
                    replace(
                        snapshot,
                        ended_at=snapshot.last_event_time,
                        current_status="timeout_finalized",
                        stop_reason="inactivity_timeout",
                        finalized_reason="inactivity_timeout",
                    )
                )

        return finalized

    def _start_session(self, event: EventEnvelope) -> SessionMutationResult:
        if not event.session_id:
            return SessionMutationResult(applied=False, reason="missing_session_id", snapshot=None)

        with self._lock:
            existing = self._sessions.get(event.session_id)
            if existing is not None:
                return SessionMutationResult(applied=False, reason="session_already_active", snapshot=replace(existing))

            initial_meter_kwh = 0.0
            vehicle_brand: str | None = None
            vehicle_model: str | None = None
            tariff_id = DEFAULT_TARIFF_ID
            tariff_eur_per_kwh = DEFAULT_TARIFF_EUR_PER_KWH
            if isinstance(event.payload, SessionStartPayload):
                initial_meter_kwh = event.payload.initial_meter_kwh
                vehicle_brand = event.payload.vehicle_brand
                vehicle_model = event.payload.vehicle_model
                if event.payload.tariff_id:
                    tariff_id = event.payload.tariff_id
                if event.payload.tariff_eur_per_kwh is not None and event.payload.tariff_eur_per_kwh >= 0:
                    tariff_eur_per_kwh = event.payload.tariff_eur_per_kwh

            snapshot = SessionSnapshot(
                session_id=event.session_id,
                station_id=event.station_id,
                connector_id=event.connector_id,
                operator_id=event.operator_id,
                location_city=event.location.city,
                location_country=event.location.country,
                start_event_id=event.event_id,
                stop_event_id=None,
                started_at=event.event_time,
                ended_at=None,
                last_event_time=event.event_time,
                last_ingest_time=event.ingest_time,
                last_sequence_no=event.sequence_no,
                meter_update_count=0,
                total_energy_kwh=0.0,
                latest_meter_kwh=initial_meter_kwh,
                latest_power_kw=None,
                max_power_kw_seen=0.0,
                power_kw_sum=0.0,
                power_kw_samples=0,
                vehicle_brand=vehicle_brand,
                vehicle_model=vehicle_model,
                tariff_id=tariff_id,
                tariff_eur_per_kwh=tariff_eur_per_kwh,
                stop_reason=None,
                current_status="active",
            )
            self._sessions[event.session_id] = snapshot
            if snapshot.station_id not in self._station_sessions:
                self._station_sessions[snapshot.station_id] = set()
            self._station_sessions[snapshot.station_id].add(snapshot.session_id)
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

            power_kw = event.payload.power_kw
            power_kw_sum = snapshot.power_kw_sum
            power_kw_samples = snapshot.power_kw_samples
            max_power_kw_seen = snapshot.max_power_kw_seen

            if power_kw is not None and power_kw >= 0:
                power_kw_sum += power_kw
                power_kw_samples += 1
                max_power_kw_seen = max(max_power_kw_seen, power_kw)

            updated = replace(
                snapshot,
                last_event_time=max(snapshot.last_event_time, event.event_time),
                last_ingest_time=max(snapshot.last_ingest_time, event.ingest_time),
                last_sequence_no=event.sequence_no,
                meter_update_count=snapshot.meter_update_count + 1,
                total_energy_kwh=snapshot.total_energy_kwh + event.payload.energy_delta_kwh,
                latest_meter_kwh=event.payload.meter_kwh,
                latest_power_kw=power_kw,
                max_power_kw_seen=max_power_kw_seen,
                power_kw_sum=power_kw_sum,
                power_kw_samples=power_kw_samples,
                current_status="charging",
            )
            self._sessions[event.session_id] = updated
            return SessionMutationResult(applied=True, reason="session_meter_updated", snapshot=replace(updated))

    def _close_session(self, event: EventEnvelope, finalized_reason: str, stop_reason: str) -> SessionMutationResult:
        if not event.session_id:
            return SessionMutationResult(applied=False, reason="missing_session_id", snapshot=None)

        with self._lock:
            snapshot = self._sessions.pop(event.session_id, None)
            if snapshot is None:
                return SessionMutationResult(applied=False, reason="session_not_found", snapshot=None)

            station_sessions = self._station_sessions.get(snapshot.station_id)
            if station_sessions is not None:
                station_sessions.discard(snapshot.session_id)
                if not station_sessions:
                    del self._station_sessions[snapshot.station_id]

            updated_total_energy = snapshot.total_energy_kwh
            updated_latest_meter = snapshot.latest_meter_kwh
            vehicle_brand = snapshot.vehicle_brand
            vehicle_model = snapshot.vehicle_model
            if isinstance(event.payload, SessionStopPayload):
                updated_latest_meter = event.payload.final_meter_kwh
                stop_reason = event.payload.end_reason or stop_reason
                if event.payload.total_energy_kwh is not None:
                    updated_total_energy = event.payload.total_energy_kwh
                if event.payload.vehicle_brand:
                    vehicle_brand = event.payload.vehicle_brand
                if event.payload.vehicle_model:
                    vehicle_model = event.payload.vehicle_model

            finalized = replace(
                snapshot,
                stop_event_id=event.event_id,
                ended_at=max(snapshot.last_event_time, event.event_time),
                last_event_time=max(snapshot.last_event_time, event.event_time),
                last_ingest_time=max(snapshot.last_ingest_time, event.ingest_time),
                last_sequence_no=max(snapshot.last_sequence_no, event.sequence_no),
                total_energy_kwh=updated_total_energy,
                latest_meter_kwh=updated_latest_meter,
                vehicle_brand=vehicle_brand,
                vehicle_model=vehicle_model,
                stop_reason=stop_reason,
                current_status="finalized",
                finalized_reason=finalized_reason,
            )
            return SessionMutationResult(applied=True, reason=finalized_reason, snapshot=finalized, finalized=True)
