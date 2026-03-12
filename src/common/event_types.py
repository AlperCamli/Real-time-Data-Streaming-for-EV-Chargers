"""Frozen event types for EV charging events."""

from __future__ import annotations

from enum import Enum


class EventType(str, Enum):
    SESSION_START = "SESSION_START"
    METER_UPDATE = "METER_UPDATE"
    STATUS_CHANGE = "STATUS_CHANGE"
    SESSION_STOP = "SESSION_STOP"
    HEARTBEAT = "HEARTBEAT"
    FAULT_ALERT = "FAULT_ALERT"


ALL_EVENT_TYPES: tuple[str, ...] = tuple(event_type.value for event_type in EventType)


def is_valid_event_type(value: str) -> bool:
    return value in ALL_EVENT_TYPES
