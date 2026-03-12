"""Typed payload models by event type."""

from __future__ import annotations

from dataclasses import asdict, dataclass, is_dataclass
from typing import Any, Mapping, Union

from src.common.event_types import EventType


@dataclass(slots=True)
class SessionStartPayload:
    initial_meter_kwh: float
    auth_method: str | None = None
    vehicle_id: str | None = None


@dataclass(slots=True)
class MeterUpdatePayload:
    meter_kwh: float
    energy_delta_kwh: float
    power_kw: float | None = None


@dataclass(slots=True)
class StatusChangePayload:
    previous_status: str
    new_status: str
    reason: str | None = None


@dataclass(slots=True)
class SessionStopPayload:
    final_meter_kwh: float
    end_reason: str
    duration_seconds: int | None = None


@dataclass(slots=True)
class HeartbeatPayload:
    charger_status: str
    firmware_version: str | None = None


@dataclass(slots=True)
class FaultAlertPayload:
    fault_code: str
    severity: str
    message: str | None = None


PayloadModel = Union[
    SessionStartPayload,
    MeterUpdatePayload,
    StatusChangePayload,
    SessionStopPayload,
    HeartbeatPayload,
    FaultAlertPayload,
]

PAYLOAD_MODEL_BY_EVENT_TYPE: dict[EventType, type[PayloadModel]] = {
    EventType.SESSION_START: SessionStartPayload,
    EventType.METER_UPDATE: MeterUpdatePayload,
    EventType.STATUS_CHANGE: StatusChangePayload,
    EventType.SESSION_STOP: SessionStopPayload,
    EventType.HEARTBEAT: HeartbeatPayload,
    EventType.FAULT_ALERT: FaultAlertPayload,
}


def build_payload(event_type: EventType, payload_data: Mapping[str, Any]) -> PayloadModel:
    model = PAYLOAD_MODEL_BY_EVENT_TYPE[event_type]
    try:
        return model(**dict(payload_data))
    except TypeError as exc:
        raise ValueError(f"Invalid payload for {event_type.value}: {exc}") from exc


def payload_to_dict(payload: PayloadModel | Mapping[str, Any]) -> dict[str, Any]:
    if is_dataclass(payload):
        return asdict(payload)
    return dict(payload)
