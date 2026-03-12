"""ClickHouse sink with per-table buffering and batched inserts."""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable

from src.common.schemas.event_envelope import EventEnvelope
from src.common.schemas.event_payloads import payload_to_dict
from src.common.settings import ClickHouseSettings
from src.common.table_names import (
    TABLE_AGG_CITY_DAY_FAULTS,
    TABLE_AGG_OPERATOR_HOUR,
    TABLE_AGG_STATION_MINUTE,
    TABLE_DEAD_LETTER_EVENTS,
    TABLE_FACT_SESSIONS,
    TABLE_LATE_EVENTS_REJECTED,
    TABLE_RAW_EVENTS,
)
from src.processor.finalization.sessions import FinalizedSessionFact
from src.processor.models import DeadLetterRecord


@dataclass(slots=True)
class ClickHouseFlushStats:
    rows_by_table: dict[str, int] = field(default_factory=dict)
    batch_sizes: list[int] = field(default_factory=list)
    batch_latency_seconds: list[float] = field(default_factory=list)

    @property
    def total_rows(self) -> int:
        return sum(self.rows_by_table.values())

    @property
    def total_latency_seconds(self) -> float:
        return sum(self.batch_latency_seconds)


class ClickHouseSink:
    def __init__(
        self,
        *,
        settings: ClickHouseSettings,
        logger: logging.Logger,
        batch_size: int = 500,
        flush_interval_seconds: float = 1.0,
    ) -> None:
        self._logger = logger
        self._batch_size = max(1, batch_size)
        self._flush_interval_seconds = max(0.1, flush_interval_seconds)

        self._queues: dict[str, list[tuple[Any, ...]]] = {
            TABLE_RAW_EVENTS: [],
            TABLE_DEAD_LETTER_EVENTS: [],
            TABLE_LATE_EVENTS_REJECTED: [],
            TABLE_FACT_SESSIONS: [],
            TABLE_AGG_STATION_MINUTE: [],
            TABLE_AGG_OPERATOR_HOUR: [],
            TABLE_AGG_CITY_DAY_FAULTS: [],
        }
        self._last_flush_monotonic: dict[str, float] = {table: time.monotonic() for table in self._queues}

        self._table_columns: dict[str, tuple[str, ...]] = {
            TABLE_RAW_EVENTS: (
                "event_id",
                "event_type",
                "event_time",
                "ingest_time",
                "station_id",
                "connector_id",
                "operator_id",
                "session_id",
                "schema_version",
                "producer_id",
                "sequence_no",
                "location_city",
                "location_country",
                "location_latitude",
                "location_longitude",
                "payload_json",
            ),
            TABLE_DEAD_LETTER_EVENTS: (
                "event_id",
                "event_type",
                "event_time",
                "ingest_time",
                "station_id",
                "connector_id",
                "operator_id",
                "session_id",
                "schema_version",
                "producer_id",
                "sequence_no",
                "error_reason",
                "raw_payload_json",
                "failed_at",
            ),
            TABLE_LATE_EVENTS_REJECTED: (
                "event_id",
                "event_type",
                "event_time",
                "ingest_time",
                "station_id",
                "connector_id",
                "operator_id",
                "session_id",
                "schema_version",
                "producer_id",
                "sequence_no",
                "lateness_seconds",
                "payload_json",
                "rejection_reason",
                "rejected_at",
            ),
            TABLE_FACT_SESSIONS: (
                "session_id",
                "operator_id",
                "station_id",
                "connector_id",
                "session_start_time",
                "session_end_time",
                "location_city",
                "location_country",
                "vehicle_brand",
                "vehicle_model",
                "tariff_id",
                "duration_seconds",
                "energy_kwh_total",
                "revenue_eur_total",
                "meter_update_count",
                "peak_power_kw",
                "avg_power_kw",
                "session_completion_status",
                "final_station_status",
                "stop_reason",
                "is_complete",
                "is_timeout_finalized",
                "peak_hour_flag",
                "revenue_per_kwh",
                "start_event_id",
                "stop_event_id",
                "finalized_reason",
                "finalized_at",
            ),
            TABLE_AGG_STATION_MINUTE: (
                "bucket_minute",
                "operator_id",
                "station_id",
                "city",
                "country_code",
                "events_total",
                "meter_update_count",
                "session_start_count",
                "session_stop_count",
                "fault_count",
                "heartbeat_count",
                "energy_kwh_sum",
                "revenue_eur_sum",
                "avg_power_kw",
                "max_power_kw",
                "active_connector_estimate",
            ),
            TABLE_AGG_OPERATOR_HOUR: (
                "bucket_hour",
                "operator_id",
                "energy_kwh_sum",
                "revenue_eur_sum",
                "sessions_completed",
                "sessions_incomplete",
                "fault_count",
                "distinct_station_count",
                "avg_session_duration_seconds",
            ),
            TABLE_AGG_CITY_DAY_FAULTS: (
                "bucket_day",
                "operator_id",
                "country_code",
                "city",
                "fault_count",
                "distinct_station_count",
                "most_common_fault_code",
            ),
        }

        self._client = self._build_client(settings)

    def enqueue_raw_event(self, event: EventEnvelope) -> None:
        payload_json = json.dumps(payload_to_dict(event.payload), separators=(",", ":"), ensure_ascii=True)
        self._queues[TABLE_RAW_EVENTS].append(
            (
                event.event_id,
                event.event_type.value,
                event.event_time,
                event.ingest_time,
                event.station_id,
                event.connector_id,
                event.operator_id,
                event.session_id,
                event.schema_version,
                event.producer_id,
                event.sequence_no,
                event.location.city,
                event.location.country,
                event.location.latitude,
                event.location.longitude,
                payload_json,
            )
        )

    def enqueue_dead_letter(self, record: DeadLetterRecord) -> None:
        self._queues[TABLE_DEAD_LETTER_EVENTS].append(
            (
                record.event_id,
                record.event_type,
                record.event_time,
                record.ingest_time or record.failed_at,
                record.station_id,
                record.connector_id,
                record.operator_id,
                record.session_id,
                record.schema_version,
                record.producer_id,
                record.sequence_no,
                record.error_reason,
                record.raw_payload_json,
                record.failed_at,
            )
        )

    def enqueue_late_rejected(self, event: EventEnvelope, lateness_seconds: int) -> None:
        payload_json = json.dumps(payload_to_dict(event.payload), separators=(",", ":"), ensure_ascii=True)
        self._queues[TABLE_LATE_EVENTS_REJECTED].append(
            (
                event.event_id,
                event.event_type.value,
                event.event_time,
                event.ingest_time,
                event.station_id,
                event.connector_id,
                event.operator_id,
                event.session_id,
                event.schema_version,
                event.producer_id,
                event.sequence_no,
                max(0, lateness_seconds),
                payload_json,
                "too_late_rejected",
                datetime.now(timezone.utc),
            )
        )

    def enqueue_fact_session(self, fact: FinalizedSessionFact) -> None:
        self._queues[TABLE_FACT_SESSIONS].append(
            (
                fact.session_id,
                fact.operator_id,
                fact.station_id,
                fact.connector_id,
                fact.session_start_time,
                fact.session_end_time,
                fact.location_city,
                fact.location_country,
                fact.vehicle_brand,
                fact.vehicle_model,
                fact.tariff_id,
                fact.duration_seconds,
                fact.energy_kwh_total,
                fact.revenue_eur_total,
                fact.meter_update_count,
                fact.peak_power_kw,
                fact.avg_power_kw,
                fact.session_completion_status,
                fact.final_station_status,
                fact.stop_reason,
                fact.is_complete,
                fact.is_timeout_finalized,
                fact.peak_hour_flag,
                fact.revenue_per_kwh,
                fact.start_event_id,
                fact.stop_event_id,
                fact.finalized_reason,
                fact.finalized_at,
            )
        )

    def enqueue_agg_station_minute_rows(self, rows: list[tuple[object, ...]]) -> None:
        if rows:
            self._queues[TABLE_AGG_STATION_MINUTE].extend(rows)

    def enqueue_agg_operator_hour_rows(self, rows: list[tuple[object, ...]]) -> None:
        if rows:
            self._queues[TABLE_AGG_OPERATOR_HOUR].extend(rows)

    def enqueue_agg_city_day_fault_rows(self, rows: list[tuple[object, ...]]) -> None:
        if rows:
            self._queues[TABLE_AGG_CITY_DAY_FAULTS].extend(rows)

    def flush(self, force: bool = False) -> ClickHouseFlushStats:
        now_monotonic = time.monotonic()
        stats = ClickHouseFlushStats()

        for table, queue in self._queues.items():
            if not queue:
                continue

            should_flush = force
            if not should_flush and len(queue) >= self._batch_size:
                should_flush = True
            if not should_flush and (now_monotonic - self._last_flush_monotonic[table]) >= self._flush_interval_seconds:
                should_flush = True

            if not should_flush:
                continue

            rows = self._drain(queue)
            started = time.monotonic()
            self._insert_rows(table, self._table_columns[table], rows)
            latency = max(0.0, time.monotonic() - started)
            self._last_flush_monotonic[table] = now_monotonic

            stats.rows_by_table[table] = stats.rows_by_table.get(table, 0) + len(rows)
            stats.batch_sizes.append(len(rows))
            stats.batch_latency_seconds.append(latency)

        return stats

    def close(self) -> None:
        self.flush(force=True)

    def _build_client(self, settings: ClickHouseSettings) -> object | None:
        try:
            from clickhouse_driver import Client  # type: ignore
        except ModuleNotFoundError:
            self._logger.warning("processor_clickhouse_client_missing_dependency", extra={"fallback": "noop"})
            return None

        try:
            client = Client(
                host=settings.host,
                port=settings.port,
                user=settings.user,
                password=settings.password,
                database=settings.database,
            )
            client.execute("SELECT 1")
            return client
        except Exception as exc:  # noqa: BLE001
            self._logger.warning(
                "processor_clickhouse_unavailable_using_noop_sink",
                extra={"error": str(exc)},
            )
            return None

    def _insert_rows(self, table: str, columns: Iterable[str], rows: list[tuple[Any, ...]]) -> None:
        if not rows:
            return

        if self._client is None:
            self._logger.debug(
                "processor_clickhouse_noop_drop_batch",
                extra={"table": table, "rows": len(rows)},
            )
            return

        column_sql = ", ".join(columns)
        query = f"INSERT INTO {table} ({column_sql}) VALUES"

        try:
            self._client.execute(query, rows)
        except Exception as exc:  # noqa: BLE001
            self._logger.error(
                "processor_clickhouse_insert_failed",
                extra={"table": table, "rows": len(rows), "error": str(exc)},
            )
            raise

    @staticmethod
    def _drain(queue: list[tuple[Any, ...]]) -> list[tuple[Any, ...]]:
        rows = list(queue)
        queue.clear()
        return rows
