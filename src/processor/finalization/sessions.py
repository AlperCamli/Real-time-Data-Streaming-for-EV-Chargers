"""Session fact finalization helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from src.processor.state.session_state import SessionSnapshot


@dataclass(slots=True)
class FinalizedSessionFact:
    session_id: str
    operator_id: str
    station_id: str
    connector_id: str
    session_start_time: datetime
    session_end_time: datetime
    location_city: str | None
    location_country: str | None
    vehicle_brand: str | None
    vehicle_model: str | None
    tariff_id: str
    duration_seconds: int
    energy_kwh_total: float
    revenue_eur_total: float
    meter_update_count: int
    peak_power_kw: float
    avg_power_kw: float
    session_completion_status: str
    final_station_status: str
    stop_reason: str
    is_complete: int
    is_timeout_finalized: int
    peak_hour_flag: int
    revenue_per_kwh: float
    start_event_id: str
    stop_event_id: str | None
    finalized_reason: str
    finalized_at: datetime


class SessionFactFinalizer:
    def build_fact(
        self,
        snapshot: SessionSnapshot,
        *,
        finalized_at: datetime | None = None,
    ) -> FinalizedSessionFact:
        started = snapshot.started_at.astimezone(timezone.utc)
        ended = (snapshot.ended_at or snapshot.last_event_time).astimezone(timezone.utc)
        finalized_ts = (finalized_at or datetime.now(timezone.utc)).astimezone(timezone.utc)

        duration_seconds = max(0, int((ended - started).total_seconds()))
        energy_kwh_total = max(0.0, snapshot.total_energy_kwh)
        revenue_eur_total = round(energy_kwh_total * max(0.0, snapshot.tariff_eur_per_kwh), 6)

        avg_power_kw = 0.0
        if snapshot.power_kw_samples > 0:
            avg_power_kw = snapshot.power_kw_sum / snapshot.power_kw_samples
        elif duration_seconds > 0:
            avg_power_kw = energy_kwh_total / (duration_seconds / 3600.0)

        peak_power_kw = max(0.0, snapshot.max_power_kw_seen)
        revenue_per_kwh = revenue_eur_total / energy_kwh_total if energy_kwh_total > 0 else 0.0

        finalized_reason = snapshot.finalized_reason or "unknown"
        completion_status = _completion_status(finalized_reason)
        is_timeout = 1 if finalized_reason == "inactivity_timeout" else 0
        is_complete = 1 if completion_status == "completed" else 0

        final_station_status = _final_station_status(finalized_reason)
        stop_reason = snapshot.stop_reason or finalized_reason

        return FinalizedSessionFact(
            session_id=snapshot.session_id,
            operator_id=snapshot.operator_id,
            station_id=snapshot.station_id,
            connector_id=snapshot.connector_id,
            session_start_time=started,
            session_end_time=max(started, ended),
            location_city=snapshot.location_city,
            location_country=snapshot.location_country,
            vehicle_brand=snapshot.vehicle_brand,
            vehicle_model=snapshot.vehicle_model,
            tariff_id=snapshot.tariff_id,
            duration_seconds=duration_seconds,
            energy_kwh_total=energy_kwh_total,
            revenue_eur_total=revenue_eur_total,
            meter_update_count=max(0, snapshot.meter_update_count),
            peak_power_kw=peak_power_kw,
            avg_power_kw=max(0.0, avg_power_kw),
            session_completion_status=completion_status,
            final_station_status=final_station_status,
            stop_reason=stop_reason,
            is_complete=is_complete,
            is_timeout_finalized=is_timeout,
            peak_hour_flag=_is_peak_hour_flag(started),
            revenue_per_kwh=revenue_per_kwh,
            start_event_id=snapshot.start_event_id,
            stop_event_id=snapshot.stop_event_id,
            finalized_reason=finalized_reason,
            finalized_at=finalized_ts,
        )


def _completion_status(finalized_reason: str) -> str:
    if finalized_reason == "normal_stop":
        return "completed"
    if finalized_reason == "inactivity_timeout":
        return "timeout"
    if finalized_reason == "fault_termination":
        return "fault_terminated"
    return "incomplete"


def _final_station_status(finalized_reason: str) -> str:
    if finalized_reason == "fault_termination":
        return "faulted"
    if finalized_reason == "normal_stop":
        return "available"
    if finalized_reason == "inactivity_timeout":
        return "unknown"
    return "unknown"


def _is_peak_hour_flag(started_at: datetime) -> int:
    hour = started_at.astimezone(timezone.utc).hour
    return 1 if 16 <= hour <= 21 else 0
