"""Redis serving-state sink with timestamp guards."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from src.common.redis_keys import connector_state_key, session_state_key, station_state_key
from src.common.settings import RedisSettings
from src.common.schemas.event_envelope import EventEnvelope
from src.common.schemas.event_payloads import payload_to_dict
from src.processor.state.session_state import SessionSnapshot


@dataclass(slots=True)
class RedisApplyResult:
    applied_keys: int = 0
    stale_keys: int = 0
    error_keys: int = 0

    @property
    def is_stale_event(self) -> bool:
        return self.applied_keys == 0 and self.stale_keys > 0 and self.error_keys == 0


def build_redis_client(settings: RedisSettings, logger: logging.Logger) -> object | None:
    try:
        import redis  # type: ignore
    except ModuleNotFoundError:
        logger.warning("processor_redis_client_missing_dependency", extra={"fallback": "inmemory"})
        return None

    try:
        client = redis.Redis(host=settings.host, port=settings.port, db=settings.db, decode_responses=True)
        client.ping()
        return client
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "processor_redis_unavailable_using_inmemory_fallback",
            extra={"error": str(exc)},
        )
        return None


class RedisStateSink:
    _TIMESTAMP_GUARD_SCRIPT = """
local current = redis.call('HGET', KEYS[1], 'last_event_time_ms')
if current and tonumber(current) >= tonumber(ARGV[1]) then
  return 0
end
redis.call('HSET', KEYS[1],
  'last_event_time_ms', ARGV[1],
  'last_event_time', ARGV[2],
  'event_id', ARGV[3],
  'event_type', ARGV[4],
  'state_json', ARGV[5],
  'updated_at', ARGV[6]
)
return 1
"""

    def __init__(self, redis_client: object | None, logger: logging.Logger) -> None:
        self._redis = redis_client
        self._logger = logger
        self._inmemory_state: dict[str, tuple[int, str]] = {}

    def apply_event(self, event: EventEnvelope, session_snapshot: SessionSnapshot | None = None) -> RedisApplyResult:
        result = RedisApplyResult()

        updates = [
            (
                station_state_key(event.station_id),
                {
                    "station_id": event.station_id,
                    "operator_id": event.operator_id,
                    "connector_id": event.connector_id,
                    "session_id": event.session_id,
                    "event_type": event.event_type.value,
                    "payload": payload_to_dict(event.payload),
                },
            ),
            (
                connector_state_key(event.station_id, event.connector_id),
                {
                    "station_id": event.station_id,
                    "connector_id": event.connector_id,
                    "operator_id": event.operator_id,
                    "session_id": event.session_id,
                    "event_type": event.event_type.value,
                    "payload": payload_to_dict(event.payload),
                },
            ),
        ]

        if event.session_id:
            updates.append(
                (
                    session_state_key(event.session_id),
                    {
                        "session_id": event.session_id,
                        "station_id": event.station_id,
                        "connector_id": event.connector_id,
                        "operator_id": event.operator_id,
                        "event_type": event.event_type.value,
                        "sequence_no": event.sequence_no,
                        "payload": payload_to_dict(event.payload),
                        "session_snapshot": _snapshot_to_dict(session_snapshot),
                    },
                )
            )

        for key, state_payload in updates:
            status = self._write_if_newer(
                key=key,
                event_time=event.event_time,
                state_payload=state_payload,
                event_id=event.event_id,
                event_type=event.event_type.value,
            )
            if status == "applied":
                result.applied_keys += 1
            elif status == "stale":
                result.stale_keys += 1
            else:
                result.error_keys += 1

        return result

    def _write_if_newer(
        self,
        *,
        key: str,
        event_time: datetime,
        state_payload: dict[str, object],
        event_id: str,
        event_type: str,
    ) -> str:
        event_time_utc = event_time.astimezone(timezone.utc)
        event_time_ms = int(event_time_utc.timestamp() * 1000)
        event_time_iso = event_time_utc.isoformat()
        updated_at = datetime.now(timezone.utc).isoformat()
        state_json = json.dumps(state_payload, separators=(",", ":"), ensure_ascii=True)

        if self._redis is None:
            current = self._inmemory_state.get(key)
            if current is not None and current[0] >= event_time_ms:
                return "stale"
            self._inmemory_state[key] = (event_time_ms, state_json)
            return "applied"

        try:
            applied = int(
                self._redis.eval(
                    self._TIMESTAMP_GUARD_SCRIPT,
                    1,
                    key,
                    event_time_ms,
                    event_time_iso,
                    event_id,
                    event_type,
                    state_json,
                    updated_at,
                )
            )
            return "applied" if applied == 1 else "stale"
        except Exception as exc:  # noqa: BLE001
            self._logger.error(
                "processor_redis_write_failed",
                extra={"key": key, "error": str(exc)},
            )
            return "error"


def _snapshot_to_dict(snapshot: SessionSnapshot | None) -> dict[str, object] | None:
    if snapshot is None:
        return None
    return {
        "session_id": snapshot.session_id,
        "station_id": snapshot.station_id,
        "connector_id": snapshot.connector_id,
        "operator_id": snapshot.operator_id,
        "started_at": snapshot.started_at.isoformat(),
        "last_event_time": snapshot.last_event_time.isoformat(),
        "last_sequence_no": snapshot.last_sequence_no,
        "meter_update_count": snapshot.meter_update_count,
        "total_energy_kwh": snapshot.total_energy_kwh,
        "latest_meter_kwh": snapshot.latest_meter_kwh,
        "latest_power_kw": snapshot.latest_power_kw,
        "finalized_reason": snapshot.finalized_reason,
    }
