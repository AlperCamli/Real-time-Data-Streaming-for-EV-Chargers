"""Domain models for EV charging simulation state."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum


class ConnectorStatus(str, Enum):
    AVAILABLE = "available"
    CHARGING = "charging"
    FAULTED = "faulted"


@dataclass(frozen=True, slots=True)
class Location:
    city: str
    country: str
    latitude: float
    longitude: float


@dataclass(slots=True)
class ConnectorState:
    station_id: str
    connector_id: str
    status: ConnectorStatus
    current_meter_kwh: float
    last_update_time: datetime
    last_status_change: datetime
    active_session_id: str | None = None
    fault_code: str | None = None
    recover_at: datetime | None = None

    @property
    def is_available(self) -> bool:
        return self.status == ConnectorStatus.AVAILABLE and self.active_session_id is None


@dataclass(slots=True)
class StationState:
    station_id: str
    operator_id: str
    location: Location
    connectors: dict[str, ConnectorState]
    firmware_version: str
    last_heartbeat_at: datetime
    next_heartbeat_at: datetime

    @property
    def active_session_count(self) -> int:
        return sum(1 for connector in self.connectors.values() if connector.active_session_id is not None)

    @property
    def faulted_connector_count(self) -> int:
        return sum(1 for connector in self.connectors.values() if connector.status == ConnectorStatus.FAULTED)


@dataclass(slots=True)
class SessionState:
    session_id: str
    station_id: str
    connector_id: str
    operator_id: str
    vehicle_id: str
    vehicle_brand: str
    auth_method: str
    battery_capacity_kwh: float
    max_power_kw: float
    start_soc_percent: float
    target_soc_percent: float
    current_soc_percent: float
    start_time: datetime
    planned_stop_time: datetime
    next_meter_update_time: datetime
    meter_update_interval_seconds: float
    start_meter_kwh: float
    current_meter_kwh: float
    last_update_time: datetime
    energy_delivered_kwh: float = 0.0
    sequence_no: int = 0


@dataclass(slots=True)
class NetworkState:
    stations: dict[str, StationState]
    active_sessions: dict[str, SessionState]

    def available_connectors(self) -> list[ConnectorState]:
        connectors: list[ConnectorState] = []
        for station in self.stations.values():
            for connector in station.connectors.values():
                if connector.is_available:
                    connectors.append(connector)
        return connectors

    def connector_count(self) -> int:
        return sum(len(station.connectors) for station in self.stations.values())
