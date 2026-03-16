"""Frozen Redis key patterns and helper builders."""

from __future__ import annotations

STATION_STATE_TEMPLATE = "station:{station_id}:state"
CONNECTOR_STATE_TEMPLATE = "station:{station_id}:connector:{connector_id}:state"
SESSION_STATE_TEMPLATE = "session:{session_id}:state"
DEDUP_TEMPLATE = "dedup:{event_id}"


def station_state_key(station_id: str) -> str:
    return f"station:{station_id}:state"


def connector_state_key(station_id: str, connector_id: str) -> str:
    return f"station:{station_id}:connector:{connector_id}:state"


def session_state_key(session_id: str) -> str:
    return f"session:{session_id}:state"


def dedup_key(event_id: str) -> str:
    return f"dedup:{event_id}"
