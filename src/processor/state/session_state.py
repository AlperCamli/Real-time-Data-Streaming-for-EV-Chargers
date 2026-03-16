"""Processor-side working session state and finalization snapshots."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

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

    def export(self) -> "SessionSnapshot":
        return SessionSnapshot(
            session_id=self.session_id,
            station_id=self.station_id,
            connector_id=self.connector_id,
            operator_id=self.operator_id,
            location_city=self.location_city,
            location_country=self.location_country,
            start_event_id=self.start_event_id,
            stop_event_id=self.stop_event_id,
            started_at=self.started_at,
            ended_at=self.ended_at,
            last_event_time=self.last_event_time,
            last_ingest_time=self.last_ingest_time,
            last_sequence_no=self.last_sequence_no,
            meter_update_count=self.meter_update_count,
            total_energy_kwh=self.total_energy_kwh,
            latest_meter_kwh=self.latest_meter_kwh,
            latest_power_kw=self.latest_power_kw,
            max_power_kw_seen=self.max_power_kw_seen,
            power_kw_sum=self.power_kw_sum,
            power_kw_samples=self.power_kw_samples,
            vehicle_brand=self.vehicle_brand,
            vehicle_model=self.vehicle_model,
            tariff_id=self.tariff_id,
            tariff_eur_per_kwh=self.tariff_eur_per_kwh,
            stop_reason=self.stop_reason,
            current_status=self.current_status,
            finalized_reason=self.finalized_reason,
        )


@dataclass(slots=True)
class SessionMutationResult:
    applied: bool
    reason: str
    snapshot: SessionSnapshot | None
    active_session_count: int
    finalized: bool = False


class SessionStateStore:
    def __init__(self) -> None:
        self._sessions: dict[str, SessionSnapshot] = {}
        self._station_sessions: dict[str, set[str]] = {}

    def get_session(self, session_id: str | None) -> SessionSnapshot | None:
        snapshot = self.peek_session(session_id)
        return snapshot.export() if snapshot is not None else None

    def peek_session(self, session_id: str | None) -> SessionSnapshot | None:
        if not session_id:
            return None
        return self._sessions.get(session_id)

    def active_sessions_for_station(self, station_id: str) -> int:
        return len(self._station_sessions.get(station_id, ()))

    def apply_event(
        self,
        event: EventEnvelope,
        *,
        existing_snapshot: SessionSnapshot | None = None,
        finalize_on_session_stop: bool = True,
        finalize_on_fault_termination: bool = True,
    ) -> SessionMutationResult:
        if event.event_type == EventType.SESSION_START:
            return self._start_session(event)
        if event.event_type == EventType.METER_UPDATE:
            return self._update_session(event, existing_snapshot=existing_snapshot)
        if event.event_type == EventType.SESSION_STOP:
            if not finalize_on_session_stop:
                return SessionMutationResult(
                    applied=False,
                    reason="session_stop_finalize_disabled",
                    snapshot=existing_snapshot.export() if existing_snapshot is not None else None,
                    active_session_count=self.active_sessions_for_station(event.station_id),
                )
            stop_reason = event.payload.end_reason if isinstance(event.payload, SessionStopPayload) else "session_stop"
            return self._close_session(event, "normal_stop", stop_reason, existing_snapshot=existing_snapshot)
        if event.event_type == EventType.FAULT_ALERT and event.session_id:
            if not finalize_on_fault_termination:
                return SessionMutationResult(
                    applied=False,
                    reason="fault_finalize_disabled",
                    snapshot=existing_snapshot.export() if existing_snapshot is not None else None,
                    active_session_count=self.active_sessions_for_station(event.station_id),
                )
            stop_reason = "fault"
            if isinstance(event.payload, FaultAlertPayload):
                stop_reason = f"fault:{event.payload.fault_code}"
            return self._close_session(event, "fault_termination", stop_reason, existing_snapshot=existing_snapshot)
        if event.event_type == EventType.STATUS_CHANGE and event.session_id:
            if not finalize_on_fault_termination:
                return SessionMutationResult(
                    applied=False,
                    reason="fault_finalize_disabled",
                    snapshot=existing_snapshot.export() if existing_snapshot is not None else None,
                    active_session_count=self.active_sessions_for_station(event.station_id),
                )
            if isinstance(event.payload, StatusChangePayload) and event.payload.new_status.strip().lower() == "faulted":
                stop_reason = f"status_fault:{event.payload.reason or 'status_change'}"
                return self._close_session(event, "fault_termination", stop_reason, existing_snapshot=existing_snapshot)
        return SessionMutationResult(
            applied=False,
            reason="no_session_mutation_for_event_type",
            snapshot=existing_snapshot.export() if existing_snapshot is not None else None,
            active_session_count=self.active_sessions_for_station(event.station_id),
        )

    def expire_inactive(self, now: datetime, inactivity_timeout_seconds: int) -> list[SessionSnapshot]:
        threshold = now.astimezone(timezone.utc) - timedelta(seconds=max(1, inactivity_timeout_seconds))
        finalized: list[SessionSnapshot] = []

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
            snapshot.ended_at = snapshot.last_event_time
            snapshot.current_status = "timeout_finalized"
            snapshot.stop_reason = "inactivity_timeout"
            snapshot.finalized_reason = "inactivity_timeout"
            finalized.append(snapshot.export())

        return finalized

    def _start_session(self, event: EventEnvelope) -> SessionMutationResult:
        if not event.session_id:
            return SessionMutationResult(
                applied=False,
                reason="missing_session_id",
                snapshot=None,
                active_session_count=self.active_sessions_for_station(event.station_id),
            )

        existing = self._sessions.get(event.session_id)
        if existing is not None:
            return SessionMutationResult(
                applied=False,
                reason="session_already_active",
                snapshot=existing.export(),
                active_session_count=self.active_sessions_for_station(event.station_id),
            )

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
        station_sessions = self._station_sessions.setdefault(snapshot.station_id, set())
        station_sessions.add(snapshot.session_id)
        return SessionMutationResult(
            applied=True,
            reason="session_started",
            snapshot=snapshot.export(),
            active_session_count=len(station_sessions),
        )

    def _update_session(
        self,
        event: EventEnvelope,
        *,
        existing_snapshot: SessionSnapshot | None,
    ) -> SessionMutationResult:
        if not event.session_id:
            return SessionMutationResult(
                applied=False,
                reason="missing_session_id",
                snapshot=None,
                active_session_count=self.active_sessions_for_station(event.station_id),
            )

        snapshot = existing_snapshot or self._sessions.get(event.session_id)
        if snapshot is None:
            return SessionMutationResult(
                applied=False,
                reason="session_not_found",
                snapshot=None,
                active_session_count=self.active_sessions_for_station(event.station_id),
            )

        if event.sequence_no <= snapshot.last_sequence_no:
            return SessionMutationResult(
                applied=False,
                reason="non_increasing_sequence",
                snapshot=snapshot.export(),
                active_session_count=self.active_sessions_for_station(snapshot.station_id),
            )

        if not isinstance(event.payload, MeterUpdatePayload):
            return SessionMutationResult(
                applied=False,
                reason="invalid_payload_for_meter_update",
                snapshot=snapshot.export(),
                active_session_count=self.active_sessions_for_station(snapshot.station_id),
            )

        power_kw = event.payload.power_kw
        if power_kw is not None and power_kw >= 0:
            snapshot.power_kw_sum += power_kw
            snapshot.power_kw_samples += 1
            snapshot.max_power_kw_seen = max(snapshot.max_power_kw_seen, power_kw)

        snapshot.last_event_time = max(snapshot.last_event_time, event.event_time)
        snapshot.last_ingest_time = max(snapshot.last_ingest_time, event.ingest_time)
        snapshot.last_sequence_no = event.sequence_no
        snapshot.meter_update_count += 1
        snapshot.total_energy_kwh += event.payload.energy_delta_kwh
        snapshot.latest_meter_kwh = event.payload.meter_kwh
        snapshot.latest_power_kw = power_kw
        snapshot.current_status = "charging"
        return SessionMutationResult(
            applied=True,
            reason="session_meter_updated",
            snapshot=snapshot.export(),
            active_session_count=self.active_sessions_for_station(snapshot.station_id),
        )

    def _close_session(
        self,
        event: EventEnvelope,
        finalized_reason: str,
        stop_reason: str,
        *,
        existing_snapshot: SessionSnapshot | None,
    ) -> SessionMutationResult:
        if not event.session_id:
            return SessionMutationResult(
                applied=False,
                reason="missing_session_id",
                snapshot=None,
                active_session_count=self.active_sessions_for_station(event.station_id),
            )

        snapshot = self._sessions.pop(event.session_id, None)
        if snapshot is None:
            snapshot = existing_snapshot
            if snapshot is None:
                return SessionMutationResult(
                    applied=False,
                    reason="session_not_found",
                    snapshot=None,
                    active_session_count=self.active_sessions_for_station(event.station_id),
                )

        station_sessions = self._station_sessions.get(snapshot.station_id)
        if station_sessions is not None:
            station_sessions.discard(snapshot.session_id)
            if not station_sessions:
                del self._station_sessions[snapshot.station_id]

        if isinstance(event.payload, SessionStopPayload):
            snapshot.latest_meter_kwh = event.payload.final_meter_kwh
            stop_reason = event.payload.end_reason or stop_reason
            if event.payload.total_energy_kwh is not None:
                snapshot.total_energy_kwh = event.payload.total_energy_kwh
            if event.payload.vehicle_brand:
                snapshot.vehicle_brand = event.payload.vehicle_brand
            if event.payload.vehicle_model:
                snapshot.vehicle_model = event.payload.vehicle_model

        snapshot.stop_event_id = event.event_id
        snapshot.ended_at = max(snapshot.last_event_time, event.event_time)
        snapshot.last_event_time = max(snapshot.last_event_time, event.event_time)
        snapshot.last_ingest_time = max(snapshot.last_ingest_time, event.ingest_time)
        snapshot.last_sequence_no = max(snapshot.last_sequence_no, event.sequence_no)
        snapshot.stop_reason = stop_reason
        snapshot.current_status = "finalized"
        snapshot.finalized_reason = finalized_reason
        return SessionMutationResult(
            applied=True,
            reason=finalized_reason,
            snapshot=snapshot.export(),
            active_session_count=self.active_sessions_for_station(snapshot.station_id),
            finalized=True,
        )
