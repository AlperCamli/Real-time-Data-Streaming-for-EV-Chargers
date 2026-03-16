"""Session lifecycle and event generation logic."""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import uuid4

from src.simulator.config import SimulatorConfig
from src.simulator.event_factory import EventFactory
from src.simulator.models import ConnectorState, ConnectorStatus, NetworkState, SessionState, StationState
from src.simulator.network import weighted_choice


FAULT_CODES: tuple[str, ...] = (
    "OVERHEAT",
    "GROUND_FAULT",
    "COMMUNICATION_LOSS",
    "EMERGENCY_STOP",
    "POWER_MODULE_FAILURE",
)


VEHICLE_PROFILES: dict[str, tuple[tuple[float, float], tuple[float, float]]] = {
    "Tesla": ((65.0, 95.0), (80.0, 230.0)),
    "Ford": ((68.0, 98.0), (65.0, 190.0)),
    "BMW": ((58.0, 88.0), (55.0, 170.0)),
    "Hyundai": ((54.0, 82.0), (55.0, 190.0)),
    "Rivian": ((105.0, 140.0), (90.0, 220.0)),
    "Nissan": ((38.0, 70.0), (40.0, 110.0)),
}


@dataclass(frozen=True, slots=True)
class StartControlSnapshot:
    admission_scale: float
    effective_probability: float
    effective_cap: int
    blocked_overshoot: bool


class SessionEngine:
    def __init__(
        self,
        config: SimulatorConfig,
        network: NetworkState,
        event_factory: EventFactory,
        rng: random.Random,
    ) -> None:
        self._config = config
        self._network = network
        self._event_factory = event_factory
        self._rng = rng
        self._admission_scale = 1.0
        self._last_start_control = StartControlSnapshot(
            admission_scale=1.0,
            effective_probability=0.0,
            effective_cap=0,
            blocked_overshoot=False,
        )

    @property
    def last_start_control(self) -> StartControlSnapshot:
        return self._last_start_control

    def generate_events(self, now: datetime, target_eps: float, observed_eps: float, tick_seconds: float) -> list[dict[str, object]]:
        events: list[dict[str, object]] = []

        events.extend(self._recover_connectors(now))
        events.extend(self._emit_heartbeats(now))
        events.extend(self._process_active_sessions(now))
        events.extend(self._inject_faults(now, tick_seconds))

        target_events_this_tick = int(max(1.0, target_eps * tick_seconds))
        pre_start_events_count = len(events)
        demand_multiplier = self._demand_multiplier(now)
        start_events, start_control = self._start_sessions(
            now=now,
            demand_multiplier=demand_multiplier,
            observed_eps=observed_eps,
            target_eps=target_eps,
            pre_start_events_count=pre_start_events_count,
            target_events_this_tick=target_events_this_tick,
            tick_seconds=tick_seconds,
        )
        self._last_start_control = start_control
        events.extend(start_events)

        return events

    def _recover_connectors(self, now: datetime) -> list[dict[str, object]]:
        events: list[dict[str, object]] = []
        for station in self._network.stations.values():
            for connector in station.connectors.values():
                if connector.status != ConnectorStatus.FAULTED or connector.recover_at is None:
                    continue
                if now < connector.recover_at:
                    continue

                connector.status = ConnectorStatus.AVAILABLE
                connector.fault_code = None
                connector.recover_at = None
                connector.last_status_change = now
                connector.last_update_time = now

                events.append(
                    self._event_factory.status_change(
                        station=station,
                        connector_id=connector.connector_id,
                        previous_status=ConnectorStatus.FAULTED.value,
                        new_status=ConnectorStatus.AVAILABLE.value,
                        reason="auto_recovered",
                        event_time=now,
                        session_id=None,
                    )
                )
        return events

    def _emit_heartbeats(self, now: datetime) -> list[dict[str, object]]:
        events: list[dict[str, object]] = []
        for station in self._network.stations.values():
            if now < station.next_heartbeat_at:
                continue

            events.append(self._event_factory.heartbeat(station=station, event_time=now))
            station.last_heartbeat_at = now
            jitter = self._rng.uniform(-0.15, 0.15) * self._config.network.heartbeat_interval_seconds
            next_interval = max(5.0, self._config.network.heartbeat_interval_seconds + jitter)
            station.next_heartbeat_at = now + timedelta(seconds=next_interval)

        return events

    def _process_active_sessions(self, now: datetime) -> list[dict[str, object]]:
        events: list[dict[str, object]] = []

        for session_id in list(self._network.active_sessions):
            session = self._network.active_sessions.get(session_id)
            if session is None:
                continue

            station = self._network.stations[session.station_id]
            connector = station.connectors[session.connector_id]

            if now >= session.next_meter_update_time:
                events.append(self._emit_meter_update(station, connector, session, now))

            if self._should_stop_session(session, now):
                if session.sequence_no == 0:
                    events.append(self._emit_meter_update(station, connector, session, now))
                events.extend(self._close_session(station, connector, session, now, "completed"))

        return events

    def _inject_faults(self, now: datetime, tick_seconds: float) -> list[dict[str, object]]:
        events: list[dict[str, object]] = []
        per_tick_probability = min(
            0.95,
            (self._config.faults.fault_rate_per_connector_hour / 3600.0) * tick_seconds,
        )
        if per_tick_probability <= 0:
            return events

        for station in self._network.stations.values():
            for connector in station.connectors.values():
                if connector.status == ConnectorStatus.FAULTED:
                    continue
                if self._rng.random() >= per_tick_probability:
                    continue

                interrupted_session_id = connector.active_session_id
                if interrupted_session_id:
                    active_session = self._network.active_sessions.get(interrupted_session_id)
                    if active_session:
                        if active_session.sequence_no == 0:
                            events.append(self._emit_meter_update(station, connector, active_session, now))
                        events.extend(
                            self._close_session(
                                station=station,
                                connector=connector,
                                session=active_session,
                                now=now,
                                end_reason="fault_interruption",
                                emit_status_change=False,
                                force_status=ConnectorStatus.CHARGING,
                            )
                        )

                previous_status = connector.status.value
                connector.status = ConnectorStatus.FAULTED
                connector.active_session_id = None
                connector.fault_code = self._rng.choice(FAULT_CODES)
                recovery_seconds = self._rng.uniform(
                    self._config.faults.recovery_seconds_min,
                    self._config.faults.recovery_seconds_max,
                )
                connector.recover_at = now + timedelta(seconds=recovery_seconds)
                connector.last_status_change = now
                connector.last_update_time = now

                severity = weighted_choice(
                    list(self._config.faults.severity_weights.items()),
                    self._rng,
                    lambda item: item[1],
                )[0]

                events.append(
                    self._event_factory.status_change(
                        station=station,
                        connector_id=connector.connector_id,
                        previous_status=previous_status,
                        new_status=ConnectorStatus.FAULTED.value,
                        reason=connector.fault_code,
                        event_time=now,
                        session_id=interrupted_session_id,
                    )
                )
                events.append(
                    self._event_factory.fault_alert(
                        station=station,
                        connector=connector,
                        fault_code=connector.fault_code,
                        severity=severity,
                        event_time=now,
                        session_id=interrupted_session_id,
                    )
                )

        return events

    def _start_sessions(
        self,
        now: datetime,
        demand_multiplier: float,
        observed_eps: float,
        target_eps: float,
        pre_start_events_count: int,
        target_events_this_tick: int,
        tick_seconds: float,
    ) -> tuple[list[dict[str, object]], StartControlSnapshot]:
        available_connectors = self._network.available_connectors()
        self._rng.shuffle(available_connectors)

        if not available_connectors:
            return [], StartControlSnapshot(
                admission_scale=self._admission_scale,
                effective_probability=0.0,
                effective_cap=0,
                blocked_overshoot=False,
            )

        if not self._config.eps_controller.enabled:
            return self._start_sessions_legacy(
                now=now,
                demand_multiplier=demand_multiplier,
                observed_eps=observed_eps,
                target_eps=target_eps,
                target_events_this_tick=target_events_this_tick,
                pre_start_events_count=pre_start_events_count,
                tick_seconds=tick_seconds,
                available_connectors=available_connectors,
            )

        lower_bound, upper_bound = self._eps_bounds(target_eps)
        overshoot = observed_eps > upper_bound
        under_target = observed_eps < lower_bound

        if overshoot:
            self._admission_scale *= self._config.eps_controller.scale_down_step
        elif under_target:
            self._admission_scale *= self._config.eps_controller.scale_up_step
        else:
            self._admission_scale += (1.0 - self._admission_scale) * self._config.eps_controller.scale_recovery_alpha

        self._admission_scale = self._clamp(
            self._admission_scale,
            lower=self._config.eps_controller.min_admission_scale,
            upper=self._config.eps_controller.max_admission_scale,
        )

        base_probability = (
            self._config.demand.base_session_start_rate_per_idle_connector_minute
            * demand_multiplier
            * tick_seconds
            / 60.0
        )
        if self._config.benchmark.enabled and self._config.benchmark.sustained_mode:
            base_probability *= 1.2

        probability_per_connector = self._clamp(
            base_probability * self._admission_scale,
            lower=0.0001,
            upper=self._config.eps_controller.max_start_probability,
        )

        cap_by_share = int(len(available_connectors) * self._config.eps_controller.max_start_share_per_tick)
        if under_target:
            cap_by_share = max(1, cap_by_share)

        events_per_new_session = max(1, self._config.eps_controller.events_per_new_session_immediate)
        cap_by_headroom = max(0, (target_events_this_tick - pre_start_events_count) // events_per_new_session)
        if under_target and pre_start_events_count < target_events_this_tick:
            cap_by_headroom = max(1, cap_by_headroom)

        if overshoot:
            max_starts = 0
        else:
            max_starts = min(cap_by_share, cap_by_headroom)
            if under_target and pre_start_events_count < target_events_this_tick:
                max_starts = max(1, max_starts)

        events = self._start_sessions_for_connectors(
            now=now,
            available_connectors=available_connectors,
            max_starts=max_starts,
            probability_per_connector=probability_per_connector,
        )
        return events, StartControlSnapshot(
            admission_scale=self._admission_scale,
            effective_probability=probability_per_connector,
            effective_cap=max_starts,
            blocked_overshoot=overshoot and max_starts == 0,
        )

    def _start_sessions_legacy(
        self,
        *,
        now: datetime,
        demand_multiplier: float,
        observed_eps: float,
        target_eps: float,
        target_events_this_tick: int,
        pre_start_events_count: int,
        tick_seconds: float,
        available_connectors: list[ConnectorState],
    ) -> tuple[list[dict[str, object]], StartControlSnapshot]:
        deficit_ratio = 0.0
        if target_eps > 0:
            deficit_ratio = max(0.0, (target_eps - observed_eps) / target_eps)

        probability_per_connector = (
            self._config.demand.base_session_start_rate_per_idle_connector_minute
            * demand_multiplier
            * tick_seconds
            / 60.0
        )
        probability_per_connector *= 1.0 + min(2.5, deficit_ratio * 2.5)
        if self._config.benchmark.enabled and self._config.benchmark.sustained_mode:
            probability_per_connector *= 1.2
        probability_per_connector = min(self._config.eps_controller.max_start_probability, max(0.0001, probability_per_connector))

        max_starts = max(1, int(len(available_connectors) * 0.20))
        remaining_event_budget = max(0, target_events_this_tick - pre_start_events_count)
        if remaining_event_budget > 0:
            max_starts = max(max_starts, int(remaining_event_budget / 2) + 1)

        events = self._start_sessions_for_connectors(
            now=now,
            available_connectors=available_connectors,
            max_starts=max_starts,
            probability_per_connector=probability_per_connector,
        )
        return events, StartControlSnapshot(
            admission_scale=1.0,
            effective_probability=probability_per_connector,
            effective_cap=max_starts,
            blocked_overshoot=False,
        )

    def _start_sessions_for_connectors(
        self,
        *,
        now: datetime,
        available_connectors: list[ConnectorState],
        max_starts: int,
        probability_per_connector: float,
    ) -> list[dict[str, object]]:
        events: list[dict[str, object]] = []
        started = 0
        for connector in available_connectors:
            if started >= max_starts:
                break
            if self._rng.random() >= probability_per_connector:
                continue

            station = self._network.stations[connector.station_id]
            session = self._create_session(station, connector, now)

            previous_status = connector.status.value
            connector.status = ConnectorStatus.CHARGING
            connector.active_session_id = session.session_id
            connector.last_status_change = now
            connector.last_update_time = now

            self._network.active_sessions[session.session_id] = session

            events.append(
                self._event_factory.status_change(
                    station=station,
                    connector_id=connector.connector_id,
                    previous_status=previous_status,
                    new_status=ConnectorStatus.CHARGING.value,
                    reason="session_started",
                    event_time=now,
                    session_id=session.session_id,
                )
            )
            events.append(self._event_factory.session_start(station=station, connector=connector, session=session, event_time=now))

            started += 1

        return events

    def _eps_bounds(self, target_eps: float) -> tuple[float, float]:
        band_ratio = self._config.eps_controller.target_band_ratio
        return (
            max(0.0, target_eps * (1.0 - band_ratio)),
            max(0.0, target_eps * (1.0 + band_ratio)),
        )

    @staticmethod
    def _clamp(value: float, *, lower: float, upper: float) -> float:
        return min(upper, max(lower, value))

    def _emit_meter_update(
        self,
        station: StationState,
        connector: ConnectorState,
        session: SessionState,
        now: datetime,
    ) -> dict[str, object]:
        elapsed_seconds = max(1.0, (now - session.last_update_time).total_seconds())
        elapsed_hours = elapsed_seconds / 3600.0

        soc = session.current_soc_percent
        if soc < 70.0:
            taper = 1.0
        elif soc < 85.0:
            taper = 0.72
        else:
            taper = max(0.18, 1.0 - ((soc - 85.0) / 25.0))

        power_kw = max(2.5, session.max_power_kw * taper * self._rng.uniform(0.92, 1.04))
        potential_energy = power_kw * elapsed_hours * self._rng.uniform(0.96, 1.00)
        remaining_energy_capacity = max(0.0, ((100.0 - soc) / 100.0) * session.battery_capacity_kwh)

        energy_delta_kwh = min(potential_energy, remaining_energy_capacity)
        if remaining_energy_capacity > 0.002:
            energy_delta_kwh = max(0.002, energy_delta_kwh)
        else:
            energy_delta_kwh = 0.0

        session.energy_delivered_kwh += energy_delta_kwh
        session.current_meter_kwh += energy_delta_kwh

        soc_delta = (energy_delta_kwh / max(session.battery_capacity_kwh, 1.0)) * 100.0
        session.current_soc_percent = min(100.0, session.current_soc_percent + soc_delta)

        connector.current_meter_kwh = session.current_meter_kwh

        session.sequence_no += 1
        session.last_update_time = now
        connector.last_update_time = now

        next_interval = self._rng.uniform(
            self._config.session.meter_update_interval_seconds_min,
            self._config.session.meter_update_interval_seconds_max,
        )
        session.next_meter_update_time = now + timedelta(seconds=next_interval)

        voltage_v = self._rng.uniform(360.0, 430.0)
        current_a = (power_kw * 1000.0) / max(voltage_v, 1.0)

        return self._event_factory.meter_update(
            station=station,
            connector=connector,
            session=session,
            energy_delta_kwh=energy_delta_kwh,
            power_kw=power_kw,
            voltage_v=voltage_v,
            current_a=current_a,
            event_time=now,
        )

    def _should_stop_session(self, session: SessionState, now: datetime) -> bool:
        if now >= session.planned_stop_time:
            return True
        return session.current_soc_percent >= session.target_soc_percent

    def _close_session(
        self,
        station: StationState,
        connector: ConnectorState,
        session: SessionState,
        now: datetime,
        end_reason: str,
        emit_status_change: bool = True,
        force_status: ConnectorStatus | None = None,
    ) -> list[dict[str, object]]:
        events = [
            self._event_factory.session_stop(
                station=station,
                connector=connector,
                session=session,
                end_reason=end_reason,
                event_time=now,
            )
        ]

        previous_status = connector.status.value
        next_status = force_status or ConnectorStatus.AVAILABLE
        connector.status = next_status

        connector.active_session_id = None
        connector.last_update_time = now
        connector.last_status_change = now

        if emit_status_change:
            events.append(
                self._event_factory.status_change(
                    station=station,
                    connector_id=connector.connector_id,
                    previous_status=previous_status,
                    new_status=next_status.value,
                    reason=f"session_{end_reason}",
                    event_time=now,
                    session_id=session.session_id,
                )
            )

        self._network.active_sessions.pop(session.session_id, None)
        return events

    def _create_session(self, station: StationState, connector: ConnectorState, now: datetime) -> SessionState:
        brand = weighted_choice(self._config.network.vehicle_brand_distribution, self._rng, lambda item: item.weight).brand
        battery_range, power_range = VEHICLE_PROFILES.get(brand, ((45.0, 95.0), (45.0, 180.0)))

        start_soc = self._rng.uniform(10.0, 65.0)
        target_soc = self._rng.uniform(
            self._config.session.target_soc_percent_min,
            self._config.session.target_soc_percent_max,
        )
        if target_soc <= start_soc:
            target_soc = min(98.0, start_soc + self._rng.uniform(12.0, 35.0))

        duration_minutes = self._rng.triangular(
            self._config.session.min_duration_minutes,
            self._config.session.max_duration_minutes,
            self._config.session.mode_duration_minutes,
        )
        update_interval = self._rng.uniform(
            self._config.session.meter_update_interval_seconds_min,
            self._config.session.meter_update_interval_seconds_max,
        )

        return SessionState(
            session_id=f"SE-{uuid4().hex[:12]}",
            station_id=station.station_id,
            connector_id=connector.connector_id,
            operator_id=station.operator_id,
            vehicle_id=f"VEH-{uuid4().hex[:10]}",
            vehicle_brand=brand,
            auth_method=self._rng.choice(["mobile_app", "rfid", "plug_and_charge"]),
            battery_capacity_kwh=self._rng.uniform(*battery_range),
            max_power_kw=self._rng.uniform(*power_range),
            start_soc_percent=start_soc,
            target_soc_percent=target_soc,
            current_soc_percent=start_soc,
            start_time=now,
            planned_stop_time=now + timedelta(minutes=duration_minutes),
            next_meter_update_time=now + timedelta(seconds=update_interval),
            meter_update_interval_seconds=update_interval,
            start_meter_kwh=connector.current_meter_kwh,
            current_meter_kwh=connector.current_meter_kwh,
            last_update_time=now,
        )

    def _demand_multiplier(self, now: datetime) -> float:
        hour = now.hour
        multiplier = self._config.demand.hourly_weights[hour]

        if _hour_in_window(hour, self._config.demand.morning_peak.start_hour, self._config.demand.morning_peak.end_hour):
            multiplier *= self._config.demand.morning_peak.multiplier

        if _hour_in_window(hour, self._config.demand.evening_peak.start_hour, self._config.demand.evening_peak.end_hour):
            multiplier *= self._config.demand.evening_peak.multiplier

        return max(0.1, multiplier)


def _hour_in_window(hour: int, start_hour: int, end_hour: int) -> bool:
    if start_hour == end_hour:
        return True
    if start_hour < end_hour:
        return start_hour <= hour < end_hour
    return hour >= start_hour or hour < end_hour
