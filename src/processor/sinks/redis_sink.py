"""Redis serving-state sink with timestamp guards and lifecycle-aware updates."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone

from src.common.event_types import EventType
from src.common.redis_keys import connector_state_key, session_state_key, station_state_key
from src.common.settings import RedisSettings
from src.common.schemas.event_envelope import EventEnvelope
from src.common.schemas.event_payloads import FaultAlertPayload, HeartbeatPayload, MeterUpdatePayload, SessionStopPayload, StatusChangePayload
from src.processor.models import RedisBatchInput, RedisMutation
from src.processor.state.session_state import SessionSnapshot


@dataclass(slots=True)
class RedisApplyResult:
    applied_keys: int = 0
    stale_keys: int = 0
    error_keys: int = 0
    attempts: int = 0
    latency_seconds: float = 0.0

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
for i = 3, #ARGV, 2 do
  redis.call('HSET', KEYS[1], ARGV[i], ARGV[i + 1])
end
local ttl = tonumber(ARGV[2])
if ttl and ttl > 0 then
  redis.call('EXPIRE', KEYS[1], ttl)
end
return 1
"""

    def __init__(
        self,
        redis_client: object | None,
        logger: logging.Logger,
        *,
        session_state_ttl_seconds: int = 86400,
        finalized_session_ttl_seconds: int = 3600,
    ) -> None:
        self._redis = redis_client
        self._logger = logger
        self._session_state_ttl_seconds = max(60, session_state_ttl_seconds)
        self._finalized_session_ttl_seconds = max(60, finalized_session_ttl_seconds)
        self._inmemory_state: dict[str, dict[str, str]] = {}

    def build_event_mutations(
        self,
        event: EventEnvelope,
        *,
        session_snapshot: SessionSnapshot | None = None,
        session_mutation_applied: bool = False,
        active_session_count: int = 0,
    ) -> list[RedisMutation]:
        latest_by_key: dict[str, tuple[int, int, RedisMutation]] = {}
        self._merge_event_mutations(
            event=event,
            session_snapshot=session_snapshot,
            session_mutation_applied=session_mutation_applied,
            active_session_count=active_session_count,
            latest_by_key=latest_by_key,
            next_index=0,
        )
        return self._finalize_coalesced_mutations(latest_by_key)

    def build_timeout_finalization_mutations(
        self,
        snapshot: SessionSnapshot,
        *,
        active_session_count: int,
    ) -> list[RedisMutation]:
        ended_at = snapshot.ended_at or snapshot.last_event_time
        guard_time = datetime.now(timezone.utc)
        ended_at_iso = ended_at.isoformat()
        guard_time_iso = guard_time.isoformat()
        return [
            RedisMutation(
                key=station_state_key(snapshot.station_id),
                event_time=guard_time,
                fields={
                    "station_id": snapshot.station_id,
                    "last_event_time": ended_at_iso,
                    "last_ingest_time": guard_time_iso,
                    "operator_id": snapshot.operator_id,
                    "current_status": "available",
                    "active_session_count": max(0, active_session_count),
                    "fault_flag": 0,
                    "city": snapshot.location_city or "",
                    "country": snapshot.location_country or "",
                },
                ttl_seconds=None,
            ),
            RedisMutation(
                key=connector_state_key(snapshot.station_id, snapshot.connector_id),
                event_time=guard_time,
                fields={
                    "station_id": snapshot.station_id,
                    "connector_id": snapshot.connector_id,
                    "operator_id": snapshot.operator_id,
                    "last_event_time": ended_at_iso,
                    "status": "available",
                    "session_id": "",
                    "power_kw": 0.0,
                    "energy_kwh_last": max(0.0, snapshot.latest_meter_kwh),
                    "vehicle_brand": snapshot.vehicle_brand or "",
                    "fault_code": "",
                },
                ttl_seconds=None,
            ),
            RedisMutation(
                key=session_state_key(snapshot.session_id),
                event_time=guard_time,
                fields=_session_fields(snapshot),
                ttl_seconds=self._finalized_session_ttl_seconds,
            ),
        ]

    def build_mutations_for_inputs(self, inputs: list[RedisBatchInput]) -> list[RedisMutation]:
        latest_by_key: dict[str, tuple[int, int, RedisMutation]] = {}
        next_index = 0
        for item in inputs:
            next_index = self._merge_event_mutations(
                event=item.event,
                session_snapshot=item.session_snapshot,
                session_mutation_applied=item.session_mutation_applied,
                active_session_count=item.active_session_count,
                latest_by_key=latest_by_key,
                next_index=next_index,
            )
        return self._finalize_coalesced_mutations(latest_by_key)

    def build_mutations_for_batch(
        self,
        *,
        inputs: list[RedisBatchInput],
        extra_mutations: list[RedisMutation],
    ) -> list[RedisMutation]:
        latest_by_key: dict[str, tuple[int, int, RedisMutation]] = {}
        next_index = 0
        for item in inputs:
            next_index = self._merge_event_mutations(
                event=item.event,
                session_snapshot=item.session_snapshot,
                session_mutation_applied=item.session_mutation_applied,
                active_session_count=item.active_session_count,
                latest_by_key=latest_by_key,
                next_index=next_index,
            )
        for mutation in extra_mutations:
            self._merge_mutation(
                mutation=mutation,
                latest_by_key=latest_by_key,
                index=next_index,
                event_time_ms=int(mutation.event_time.timestamp() * 1000),
            )
            next_index += 1
        return self._finalize_coalesced_mutations(latest_by_key)

    def apply_mutations(self, mutations: list[RedisMutation], *, already_coalesced: bool = False) -> RedisApplyResult:
        started = time.monotonic()
        result = RedisApplyResult()
        pending = mutations if already_coalesced else self._coalesce_mutations(mutations)
        if not pending:
            return result

        if self._redis is None:
            for mutation in pending:
                status = self._write_if_newer(
                    key=mutation.key,
                    event_time=mutation.event_time,
                    fields=mutation.fields,
                    ttl_seconds=mutation.ttl_seconds,
                )
                result.attempts += 1
                if status == "applied":
                    result.applied_keys += 1
                elif status == "stale":
                    result.stale_keys += 1
                else:
                    result.error_keys += 1
        else:
            self._pipeline_writes(pending, result)

        result.latency_seconds = max(0.0, time.monotonic() - started)
        return result

    def apply_events_batch(self, inputs: list[RedisBatchInput]) -> RedisApplyResult:
        return self.apply_mutations(self.build_mutations_for_inputs(inputs), already_coalesced=True)

    def _pipeline_writes(self, pending: list[RedisMutation], result: RedisApplyResult) -> None:
        if not pending:
            return

        try:
            pipe = self._redis.pipeline(transaction=False)
            updated_at = datetime.now(timezone.utc).isoformat()
            as_redis_value = _as_redis_value

            for mutation in pending:
                event_time_ms = int(mutation.event_time.timestamp() * 1000)
                redis_fields: dict[str, str] = {
                    "last_event_time_ms": str(event_time_ms),
                    "updated_at": updated_at,
                }
                for name, value in mutation.fields.items():
                    redis_fields[name] = as_redis_value(value)
                ttl_seconds = int(mutation.ttl_seconds or 0)
                argv: list[str] = [str(event_time_ms), str(ttl_seconds)]
                for field_name, field_value in redis_fields.items():
                    argv.append(field_name)
                    argv.append(field_value)
                pipe.eval(self._TIMESTAMP_GUARD_SCRIPT, 1, mutation.key, *argv)

            eval_results = pipe.execute()

            for eval_result in eval_results:
                result.attempts += 1
                try:
                    applied = int(eval_result)
                    if applied == 1:
                        result.applied_keys += 1
                    else:
                        result.stale_keys += 1
                except Exception:  # noqa: BLE001
                    result.error_keys += 1
        except Exception as exc:  # noqa: BLE001
            self._logger.error(
                "processor_redis_batch_write_failed",
                extra={"batch_size": len(pending), "error": str(exc)},
            )
            result.error_keys += len(pending)
            result.attempts += len(pending)

    def _coalesce_mutations(self, mutations: list[RedisMutation]) -> list[RedisMutation]:
        latest_by_key: dict[str, tuple[int, int, RedisMutation]] = {}
        for index, mutation in enumerate(mutations):
            self._merge_mutation(
                mutation=mutation,
                latest_by_key=latest_by_key,
                index=index,
                event_time_ms=int(mutation.event_time.timestamp() * 1000),
            )
        return self._finalize_coalesced_mutations(latest_by_key)

    def _write_if_newer(
        self,
        *,
        key: str,
        event_time: datetime,
        fields: dict[str, object],
        ttl_seconds: int | None,
    ) -> str:
        event_time_ms = int(event_time.timestamp() * 1000)

        redis_fields: dict[str, str] = {
            "last_event_time_ms": str(event_time_ms),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        for name, value in fields.items():
            redis_fields[name] = _as_redis_value(value)

        if self._redis is None:
            current = self._inmemory_state.get(key)
            if current is not None:
                current_ms = int(current.get("last_event_time_ms", "0") or "0")
                if current_ms >= event_time_ms:
                    return "stale"
            self._inmemory_state[key] = redis_fields
            return "applied"

        argv: list[str] = [str(event_time_ms), str(int(ttl_seconds or 0))]
        for field_name, field_value in redis_fields.items():
            argv.append(field_name)
            argv.append(field_value)

        try:
            applied = int(self._redis.eval(self._TIMESTAMP_GUARD_SCRIPT, 1, key, *argv))
            return "applied" if applied == 1 else "stale"
        except Exception as exc:  # noqa: BLE001
            self._logger.error(
                "processor_redis_write_failed",
                extra={"key": key, "error": str(exc)},
            )
            return "error"

    def _merge_event_mutations(
        self,
        *,
        event: EventEnvelope,
        session_snapshot: SessionSnapshot | None,
        session_mutation_applied: bool,
        active_session_count: int,
        latest_by_key: dict[str, tuple[int, int, RedisMutation]],
        next_index: int,
    ) -> int:
        event_time_ms = int(event.event_time.timestamp() * 1000)
        event_time_iso = event.event_time.isoformat()
        ingest_time_iso = event.ingest_time.isoformat()
        station_status = _derive_station_status(event)
        connector_status = _derive_connector_status(event)
        fault_flag = 1 if _is_faulted(station_status) else 0
        latest_power_kw = _extract_power_kw(
            event,
            session_snapshot,
            allow_meter_payload=session_mutation_applied,
        )
        active_session_id = _extract_active_session_id(event)

        next_index = self._merge_mutation(
            mutation=RedisMutation(
                key=station_state_key(event.station_id),
                event_time=event.event_time,
                fields={
                    "station_id": event.station_id,
                    "last_event_time": event_time_iso,
                    "last_ingest_time": ingest_time_iso,
                    "operator_id": event.operator_id,
                    "current_status": station_status,
                    "latest_power_kw": latest_power_kw,
                    "active_session_count": max(0, active_session_count),
                    "fault_flag": fault_flag,
                    "city": event.location.city or "",
                    "country": event.location.country or "",
                    **(
                        {"last_heartbeat_time": event_time_iso}
                        if event.event_type == EventType.HEARTBEAT
                        else {}
                    ),
                },
                ttl_seconds=None,
            ),
            latest_by_key=latest_by_key,
            index=next_index,
            event_time_ms=event_time_ms,
        )

        if event.event_type == EventType.HEARTBEAT:
            return next_index

        next_index = self._merge_mutation(
            mutation=RedisMutation(
                key=connector_state_key(event.station_id, event.connector_id),
                event_time=event.event_time,
                fields={
                    "station_id": event.station_id,
                    "connector_id": event.connector_id,
                    "operator_id": event.operator_id,
                    "last_event_time": event_time_iso,
                    "status": connector_status,
                    "session_id": active_session_id,
                    "power_kw": latest_power_kw,
                    "energy_kwh_last": _extract_meter_kwh(
                        event,
                        session_snapshot,
                        allow_meter_payload=session_mutation_applied,
                    ),
                    "vehicle_brand": (session_snapshot.vehicle_brand if session_snapshot else "") or "",
                    "fault_code": _extract_fault_code(event),
                    **(
                        {"last_meter_time": event_time_iso}
                        if event.event_type == EventType.METER_UPDATE and session_mutation_applied
                        else {}
                    ),
                },
                ttl_seconds=None,
            ),
            latest_by_key=latest_by_key,
            index=next_index,
            event_time_ms=event_time_ms,
        )

        if event.session_id and session_snapshot is not None:
            if event.event_type != EventType.METER_UPDATE or session_mutation_applied:
                ttl_seconds = self._finalized_session_ttl_seconds if session_snapshot.finalized_reason else self._session_state_ttl_seconds
                next_index = self._merge_mutation(
                    mutation=RedisMutation(
                        key=session_state_key(event.session_id),
                        event_time=event.event_time,
                        fields=_session_fields(session_snapshot),
                        ttl_seconds=ttl_seconds,
                    ),
                    latest_by_key=latest_by_key,
                    index=next_index,
                    event_time_ms=event_time_ms,
                )

        return next_index

    def _merge_mutation(
        self,
        *,
        mutation: RedisMutation,
        latest_by_key: dict[str, tuple[int, int, RedisMutation]],
        index: int,
        event_time_ms: int,
    ) -> int:
        current = latest_by_key.get(mutation.key)
        if current is None or event_time_ms > current[1] or (event_time_ms == current[1] and index >= current[0]):
            latest_by_key[mutation.key] = (index, event_time_ms, mutation)
        return index + 1

    @staticmethod
    def _finalize_coalesced_mutations(
        latest_by_key: dict[str, tuple[int, int, RedisMutation]],
    ) -> list[RedisMutation]:
        coalesced = sorted(latest_by_key.values(), key=lambda item: item[0])
        return [mutation for _, _, mutation in coalesced]


def _session_fields(snapshot: SessionSnapshot) -> dict[str, object]:
    last_event_time = snapshot.last_event_time.isoformat()
    estimated_cost = max(0.0, snapshot.total_energy_kwh) * max(0.0, snapshot.tariff_eur_per_kwh)
    return {
        "session_id": snapshot.session_id,
        "station_id": snapshot.station_id,
        "connector_id": snapshot.connector_id,
        "operator_id": snapshot.operator_id,
        "start_time": snapshot.started_at.isoformat(),
        "last_event_time": last_event_time,
        "vehicle_brand": snapshot.vehicle_brand or "",
        "vehicle_model": snapshot.vehicle_model or "",
        "tariff_id": snapshot.tariff_id,
        "current_energy_kwh": max(0.0, snapshot.total_energy_kwh),
        "current_power_kw": snapshot.latest_power_kw if snapshot.latest_power_kw is not None else 0.0,
        "meter_update_count": max(0, snapshot.meter_update_count),
        "max_power_kw_seen": max(0.0, snapshot.max_power_kw_seen),
        "estimated_cost_eur": round(estimated_cost, 6),
        "status": snapshot.current_status,
    }


def _derive_station_status(event: EventEnvelope) -> str:
    if event.event_type == EventType.STATUS_CHANGE and isinstance(event.payload, StatusChangePayload):
        return event.payload.new_status
    if event.event_type == EventType.HEARTBEAT and isinstance(event.payload, HeartbeatPayload):
        return event.payload.charger_status
    if event.event_type == EventType.FAULT_ALERT:
        return "faulted"
    if event.event_type in {EventType.SESSION_START, EventType.METER_UPDATE}:
        return "charging"
    if event.event_type == EventType.SESSION_STOP:
        return "available"
    return "unknown"


def _derive_connector_status(event: EventEnvelope) -> str:
    if event.event_type == EventType.STATUS_CHANGE and isinstance(event.payload, StatusChangePayload):
        return event.payload.new_status
    if event.event_type == EventType.FAULT_ALERT:
        return "faulted"
    if event.event_type in {EventType.SESSION_START, EventType.METER_UPDATE}:
        return "charging"
    if event.event_type == EventType.SESSION_STOP:
        return "available"
    return "unknown"


def _extract_active_session_id(event: EventEnvelope) -> str:
    if event.event_type in {EventType.SESSION_STOP, EventType.FAULT_ALERT}:
        return ""
    if event.event_type == EventType.STATUS_CHANGE and isinstance(event.payload, StatusChangePayload):
        if event.payload.new_status.strip().lower() == "faulted":
            return ""
    return event.session_id or ""


def _extract_power_kw(
    event: EventEnvelope,
    session_snapshot: SessionSnapshot | None,
    *,
    allow_meter_payload: bool,
) -> float:
    if allow_meter_payload and isinstance(event.payload, MeterUpdatePayload) and event.payload.power_kw is not None:
        return max(0.0, event.payload.power_kw)
    if session_snapshot is not None and session_snapshot.latest_power_kw is not None:
        return max(0.0, session_snapshot.latest_power_kw)
    return 0.0


def _extract_meter_kwh(
    event: EventEnvelope,
    session_snapshot: SessionSnapshot | None,
    *,
    allow_meter_payload: bool,
) -> float:
    if allow_meter_payload and isinstance(event.payload, MeterUpdatePayload):
        return max(0.0, event.payload.meter_kwh)
    if isinstance(event.payload, SessionStopPayload):
        return max(0.0, event.payload.final_meter_kwh)
    if session_snapshot is not None:
        return max(0.0, session_snapshot.latest_meter_kwh)
    return 0.0


def _extract_fault_code(event: EventEnvelope) -> str:
    if isinstance(event.payload, FaultAlertPayload):
        return event.payload.fault_code
    if event.event_type == EventType.STATUS_CHANGE and isinstance(event.payload, StatusChangePayload):
        return event.payload.reason or ""
    return ""


def _is_faulted(status: str) -> bool:
    normalized = status.strip().lower()
    return normalized in {"faulted", "degraded", "error"}


def _as_redis_value(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "1" if value else "0"
    return str(value)
