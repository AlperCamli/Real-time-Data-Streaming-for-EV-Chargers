"""Frozen Redis key patterns and helper builders."""

from __future__ import annotations

STATION_STATE_TEMPLATE = "station:{station_id}:state"
CONNECTOR_STATE_TEMPLATE = "station:{station_id}:connector:{connector_id}:state"
SESSION_STATE_TEMPLATE = "session:{session_id}:state"
DEDUP_TEMPLATE = "dedup:{event_id}"


def station_state_key(station_id: str) -> str:
    return STATION_STATE_TEMPLATE.format(station_id=station_id)


def connector_state_key(station_id: str, connector_id: str) -> str:
    return CONNECTOR_STATE_TEMPLATE.format(station_id=station_id, connector_id=connector_id)


def session_state_key(session_id: str) -> str:
    return SESSION_STATE_TEMPLATE.format(session_id=session_id)


def dedup_key(event_id: str) -> str:
    return DEDUP_TEMPLATE.format(event_id=event_id)
