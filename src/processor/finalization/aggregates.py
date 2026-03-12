"""In-memory aggregate accumulation and finalized window row emission."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone

from src.common.event_types import EventType
from src.common.schemas.event_envelope import EventEnvelope
from src.common.schemas.event_payloads import FaultAlertPayload, MeterUpdatePayload
from src.processor.finalization.sessions import FinalizedSessionFact
from src.processor.state.session_state import SessionSnapshot


@dataclass(slots=True)
class StationMinuteAggState:
    bucket_minute: datetime
    operator_id: str
    station_id: str
    city: str | None
    country: str | None
    meter_update_count: int = 0
    session_start_count: int = 0
    session_stop_count: int = 0
    fault_count: int = 0
    heartbeat_count: int = 0
    events_total: int = 0
    energy_kwh_sum: float = 0.0
    revenue_eur_sum: float = 0.0
    power_kw_sum: float = 0.0
    power_kw_samples: int = 0
    max_power_kw: float = 0.0
    active_connector_estimate: int = 0


@dataclass(slots=True)
class OperatorHourAggState:
    bucket_hour: datetime
    operator_id: str
    energy_kwh_sum: float = 0.0
    revenue_eur_sum: float = 0.0
    sessions_completed: int = 0
    sessions_incomplete: int = 0
    fault_count: int = 0
    session_duration_seconds_sum: int = 0
    session_duration_count: int = 0
    distinct_station_ids: set[str] = field(default_factory=set)


@dataclass(slots=True)
class CityDayFaultAggState:
    bucket_day: date
    operator_id: str
    country_code: str
    city: str
    fault_count: int = 0
    distinct_station_ids: set[str] = field(default_factory=set)
    fault_code_counts: dict[str, int] = field(default_factory=dict)


@dataclass(slots=True)
class AggregateFlushRows:
    station_minute_rows: list[tuple[object, ...]] = field(default_factory=list)
    operator_hour_rows: list[tuple[object, ...]] = field(default_factory=list)
    city_day_fault_rows: list[tuple[object, ...]] = field(default_factory=list)


class AggregateAccumulator:
    def __init__(self, default_tariff_eur_per_kwh: float = 0.35) -> None:
        self._default_tariff = max(0.0, default_tariff_eur_per_kwh)
        self._station_minute: dict[tuple[datetime, str, str], StationMinuteAggState] = {}
        self._operator_hour: dict[tuple[datetime, str], OperatorHourAggState] = {}
        self._city_day_faults: dict[tuple[date, str, str, str], CityDayFaultAggState] = {}

    def record_event(
        self,
        event: EventEnvelope,
        *,
        session_snapshot: SessionSnapshot | None,
        session_mutation_applied: bool,
        active_session_count: int,
    ) -> None:
        event_time_utc = event.event_time.astimezone(timezone.utc)
        minute_bucket = event_time_utc.replace(second=0, microsecond=0)
        station_key = (minute_bucket, event.operator_id, event.station_id)
        station_state = self._station_minute.get(station_key)
        if station_state is None:
            station_state = StationMinuteAggState(
                bucket_minute=minute_bucket,
                operator_id=event.operator_id,
                station_id=event.station_id,
                city=event.location.city,
                country=event.location.country,
            )
            self._station_minute[station_key] = station_state

        station_state.events_total += 1
        station_state.active_connector_estimate = max(station_state.active_connector_estimate, max(0, active_session_count))

        if event.event_type == EventType.METER_UPDATE:
            station_state.meter_update_count += 1
            if isinstance(event.payload, MeterUpdatePayload) and session_mutation_applied:
                energy_delta = max(0.0, event.payload.energy_delta_kwh)
                station_state.energy_kwh_sum += energy_delta
                tariff = self._default_tariff
                if session_snapshot is not None:
                    tariff = max(0.0, session_snapshot.tariff_eur_per_kwh)
                station_state.revenue_eur_sum += energy_delta * tariff
                if event.payload.power_kw is not None:
                    power = max(0.0, event.payload.power_kw)
                    station_state.power_kw_sum += power
                    station_state.power_kw_samples += 1
                    station_state.max_power_kw = max(station_state.max_power_kw, power)
        elif event.event_type == EventType.SESSION_START:
            station_state.session_start_count += 1
        elif event.event_type == EventType.SESSION_STOP:
            station_state.session_stop_count += 1
        elif event.event_type == EventType.FAULT_ALERT:
            station_state.fault_count += 1
        elif event.event_type == EventType.HEARTBEAT:
            station_state.heartbeat_count += 1

        if event.event_type == EventType.FAULT_ALERT:
            self._record_fault_event(event)

    def record_finalized_session(self, fact: FinalizedSessionFact) -> None:
        end_utc = fact.session_end_time.astimezone(timezone.utc)
        hour_bucket = end_utc.replace(minute=0, second=0, microsecond=0)
        key = (hour_bucket, fact.operator_id)
        state = self._operator_hour.get(key)
        if state is None:
            state = OperatorHourAggState(bucket_hour=hour_bucket, operator_id=fact.operator_id)
            self._operator_hour[key] = state

        state.energy_kwh_sum += fact.energy_kwh_total
        state.revenue_eur_sum += fact.revenue_eur_total
        state.session_duration_seconds_sum += max(0, fact.duration_seconds)
        state.session_duration_count += 1
        state.distinct_station_ids.add(fact.station_id)

        if fact.is_complete == 1:
            state.sessions_completed += 1
        else:
            state.sessions_incomplete += 1

        minute_bucket = end_utc.replace(second=0, microsecond=0)
        station_key = (minute_bucket, fact.operator_id, fact.station_id)
        station_state = self._station_minute.get(station_key)
        if station_state is None:
            station_state = StationMinuteAggState(
                bucket_minute=minute_bucket,
                operator_id=fact.operator_id,
                station_id=fact.station_id,
                city=fact.location_city,
                country=fact.location_country,
            )
            self._station_minute[station_key] = station_state
        if fact.finalized_reason != "normal_stop":
            station_state.session_stop_count += 1
        station_state.max_power_kw = max(station_state.max_power_kw, fact.peak_power_kw)

    def flush_ready(self, *, now: datetime, force: bool = False) -> AggregateFlushRows:
        now_utc = now.astimezone(timezone.utc)
        rows = AggregateFlushRows()

        ready_station_keys = []
        current_minute = now_utc.replace(second=0, microsecond=0)
        for key, state in self._station_minute.items():
            if force or state.bucket_minute < current_minute:
                ready_station_keys.append(key)

        for key in ready_station_keys:
            state = self._station_minute.pop(key)
            avg_power_kw = state.power_kw_sum / state.power_kw_samples if state.power_kw_samples > 0 else 0.0
            rows.station_minute_rows.append(
                (
                    state.bucket_minute,
                    state.operator_id,
                    state.station_id,
                    state.city,
                    state.country,
                    state.events_total,
                    state.meter_update_count,
                    state.session_start_count,
                    state.session_stop_count,
                    state.fault_count,
                    state.heartbeat_count,
                    round(state.energy_kwh_sum, 6),
                    round(state.revenue_eur_sum, 6),
                    round(avg_power_kw, 6),
                    round(state.max_power_kw, 6),
                    max(0, state.active_connector_estimate),
                )
            )

        ready_operator_keys = []
        current_hour = now_utc.replace(minute=0, second=0, microsecond=0)
        for key, state in self._operator_hour.items():
            if force or state.bucket_hour < current_hour:
                ready_operator_keys.append(key)

        for key in ready_operator_keys:
            state = self._operator_hour.pop(key)
            avg_duration = (
                state.session_duration_seconds_sum / state.session_duration_count if state.session_duration_count > 0 else 0.0
            )
            rows.operator_hour_rows.append(
                (
                    state.bucket_hour,
                    state.operator_id,
                    round(state.energy_kwh_sum, 6),
                    round(state.revenue_eur_sum, 6),
                    state.sessions_completed,
                    state.sessions_incomplete,
                    state.fault_count,
                    len(state.distinct_station_ids),
                    round(avg_duration, 6),
                )
            )

        ready_city_keys = []
        current_day = now_utc.date()
        for key, state in self._city_day_faults.items():
            if force or state.bucket_day < current_day:
                ready_city_keys.append(key)

        for key in ready_city_keys:
            state = self._city_day_faults.pop(key)
            most_common_fault = _most_common_fault_code(state.fault_code_counts)
            rows.city_day_fault_rows.append(
                (
                    state.bucket_day,
                    state.operator_id,
                    state.country_code,
                    state.city,
                    state.fault_count,
                    len(state.distinct_station_ids),
                    most_common_fault,
                )
            )

        return rows

    def _record_fault_event(self, event: EventEnvelope) -> None:
        bucket_day = event.event_time.astimezone(timezone.utc).date()
        city = event.location.city or "unknown"
        country = event.location.country or "unknown"
        key = (bucket_day, event.operator_id, country, city)
        state = self._city_day_faults.get(key)
        if state is None:
            state = CityDayFaultAggState(
                bucket_day=bucket_day,
                operator_id=event.operator_id,
                country_code=country,
                city=city,
            )
            self._city_day_faults[key] = state

        state.fault_count += 1
        state.distinct_station_ids.add(event.station_id)

        fault_code = "unknown"
        if isinstance(event.payload, FaultAlertPayload):
            fault_code = event.payload.fault_code or "unknown"
        state.fault_code_counts[fault_code] = state.fault_code_counts.get(fault_code, 0) + 1

        hour_bucket = event.event_time.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
        op_key = (hour_bucket, event.operator_id)
        op_state = self._operator_hour.get(op_key)
        if op_state is None:
            op_state = OperatorHourAggState(bucket_hour=hour_bucket, operator_id=event.operator_id)
            self._operator_hour[op_key] = op_state
        op_state.fault_count += 1
        op_state.distinct_station_ids.add(event.station_id)


def _most_common_fault_code(counts: dict[str, int]) -> str | None:
    if not counts:
        return None
    best = max(counts.items(), key=lambda item: (item[1], item[0]))
    return best[0]
