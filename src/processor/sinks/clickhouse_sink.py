"""ClickHouse sink scaffolding for processor writes."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Iterable

from src.common.schemas.event_envelope import EventEnvelope
from src.common.schemas.event_payloads import payload_to_dict
from src.common.settings import ClickHouseSettings
from src.common.table_names import TABLE_DEAD_LETTER_EVENTS, TABLE_LATE_EVENTS_REJECTED, TABLE_RAW_EVENTS
from src.processor.models import DeadLetterRecord


@dataclass(slots=True)
class ClickHouseBatchStats:
    raw_rows: int = 0
    dead_letter_rows: int = 0
    late_rows: int = 0


class ClickHouseSink:
    def __init__(
        self,
        *,
        settings: ClickHouseSettings,
        logger: logging.Logger,
        batch_size: int = 500,
    ) -> None:
        self._logger = logger
        self._batch_size = max(1, batch_size)

        self._raw_queue: list[tuple[Any, ...]] = []
        self._dead_letter_queue: list[tuple[Any, ...]] = []
        self._late_queue: list[tuple[Any, ...]] = []

        self._client = self._build_client(settings)

    def enqueue_raw_event(self, event: EventEnvelope) -> None:
        payload_json = json.dumps(payload_to_dict(event.payload), separators=(",", ":"), ensure_ascii=True)
        self._raw_queue.append(
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
        self._dead_letter_queue.append(
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
            )
        )

    def enqueue_late_rejected(self, event: EventEnvelope, lateness_seconds: int) -> None:
        payload_json = json.dumps(payload_to_dict(event.payload), separators=(",", ":"), ensure_ascii=True)
        self._late_queue.append(
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
            )
        )

    def flush(self, force: bool = False) -> ClickHouseBatchStats:
        stats = ClickHouseBatchStats()

        if self._should_flush(self._raw_queue, force):
            rows = self._drain(self._raw_queue)
            self._insert_raw_rows(rows)
            stats.raw_rows = len(rows)

        if self._should_flush(self._dead_letter_queue, force):
            rows = self._drain(self._dead_letter_queue)
            self._insert_dead_letter_rows(rows)
            stats.dead_letter_rows = len(rows)

        if self._should_flush(self._late_queue, force):
            rows = self._drain(self._late_queue)
            self._insert_late_rows(rows)
            stats.late_rows = len(rows)

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

    def _insert_raw_rows(self, rows: list[tuple[Any, ...]]) -> None:
        if not rows:
            return
        columns = (
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
        )
        self._insert_rows(TABLE_RAW_EVENTS, columns, rows)

    def _insert_dead_letter_rows(self, rows: list[tuple[Any, ...]]) -> None:
        if not rows:
            return
        columns = (
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
        )
        self._insert_rows(TABLE_DEAD_LETTER_EVENTS, columns, rows)

    def _insert_late_rows(self, rows: list[tuple[Any, ...]]) -> None:
        if not rows:
            return
        columns = (
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
        )
        self._insert_rows(TABLE_LATE_EVENTS_REJECTED, columns, rows)

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

    def _should_flush(self, queue: list[tuple[Any, ...]], force: bool) -> bool:
        return bool(queue) and (force or len(queue) >= self._batch_size)

    @staticmethod
    def _drain(queue: list[tuple[Any, ...]]) -> list[tuple[Any, ...]]:
        rows = list(queue)
        queue.clear()
        return rows
