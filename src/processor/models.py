"""Shared processor models for routing, sink payloads, and async batching."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Mapping

from src.common.schemas.event_envelope import EventEnvelope
from src.common.schemas.validation import parse_timestamp
from src.processor.state.session_state import SessionSnapshot


@dataclass(slots=True)
class DeadLetterRecord:
    error_reason: str
    raw_payload_json: str
    failed_at: datetime
    event_id: str = "unknown"
    event_type: str = "UNKNOWN"
    event_time: datetime | None = None
    ingest_time: datetime | None = None
    station_id: str | None = None
    connector_id: str | None = None
    operator_id: str | None = None
    session_id: str | None = None
    schema_version: str | None = None
    producer_id: str | None = None
    sequence_no: int | None = None
    source_topic: str | None = None
    source_partition: int | None = None
    source_offset: int | None = None


@dataclass(slots=True)
class BatchOutcome:
    processed_count: int = 0
    accepted_count: int = 0
    invalid_count: int = 0
    duplicate_count: int = 0
    late_rejected_count: int = 0
    stale_redis_count: int = 0
    accepted_late_count: int = 0
    errors: list[str] = field(default_factory=list)


@dataclass(slots=True)
class MessageContext:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    timestamp_ms: int | None = None


@dataclass(slots=True)
class KafkaTopicRecord:
    payload: dict[str, Any]
    key: str = ""


@dataclass(slots=True)
class RedisMutation:
    key: str
    event_time: datetime
    fields: dict[str, object]
    ttl_seconds: int | None = None


@dataclass(slots=True)
class AcceptedEventTelemetry:
    event_id: str
    event_type: str
    event_time: datetime
    received_at: datetime
    processing_started_monotonic: float


@dataclass(slots=True)
class RedisBatchInput:
    event: EventEnvelope
    session_snapshot: SessionSnapshot | None
    session_mutation_applied: bool
    active_session_count: int
    telemetry: AcceptedEventTelemetry


def _default_rows_by_table() -> dict[str, list[tuple[object, ...]]]:
    return {}


@dataclass(slots=True)
class SinkBatch:
    outcome: BatchOutcome = field(default_factory=BatchOutcome)
    checkpoints: dict[tuple[str, int], int] = field(default_factory=dict)
    reserved_event_ids: list[str] = field(default_factory=list)
    redis_inputs: list[RedisBatchInput] = field(default_factory=list)
    redis_mutations: list[RedisMutation] = field(default_factory=list)
    clickhouse_rows_by_table: dict[str, list[tuple[object, ...]]] = field(default_factory=_default_rows_by_table)
    kafka_dlq_records: list[KafkaTopicRecord] = field(default_factory=list)
    kafka_late_records: list[KafkaTopicRecord] = field(default_factory=list)
    metric_increments: dict[str, int] = field(default_factory=dict)
    semantic_warning_counts: dict[str, int] = field(default_factory=dict)
    session_skip_counts: dict[str, int] = field(default_factory=dict)

    def add_checkpoint(self, topic: str, partition: int, next_offset: int) -> None:
        key = (topic, partition)
        current = self.checkpoints.get(key, -1)
        if next_offset > current:
            self.checkpoints[key] = next_offset

    def add_clickhouse_row(self, table: str, row: tuple[object, ...]) -> None:
        if table not in self.clickhouse_rows_by_table:
            self.clickhouse_rows_by_table[table] = []
        self.clickhouse_rows_by_table[table].append(row)

    def inc_metric(self, name: str, amount: int = 1) -> None:
        if amount <= 0:
            return
        self.metric_increments[name] = self.metric_increments.get(name, 0) + amount

    def add_semantic_warning(self, warning: str) -> None:
        self.semantic_warning_counts[warning] = self.semantic_warning_counts.get(warning, 0) + 1

    def add_session_skip(self, reason: str) -> None:
        self.session_skip_counts[reason] = self.session_skip_counts.get(reason, 0) + 1

    def merge(self, other: "SinkBatch") -> None:
        self.outcome.processed_count += other.outcome.processed_count
        self.outcome.accepted_count += other.outcome.accepted_count
        self.outcome.invalid_count += other.outcome.invalid_count
        self.outcome.duplicate_count += other.outcome.duplicate_count
        self.outcome.late_rejected_count += other.outcome.late_rejected_count
        self.outcome.stale_redis_count += other.outcome.stale_redis_count
        self.outcome.accepted_late_count += other.outcome.accepted_late_count
        self.outcome.errors.extend(other.outcome.errors)

        for (topic, partition), next_offset in other.checkpoints.items():
            self.add_checkpoint(topic, partition, next_offset)

        self.reserved_event_ids.extend(other.reserved_event_ids)
        self.redis_inputs.extend(other.redis_inputs)
        self.redis_mutations.extend(other.redis_mutations)

        for table, rows in other.clickhouse_rows_by_table.items():
            if table not in self.clickhouse_rows_by_table:
                self.clickhouse_rows_by_table[table] = []
            self.clickhouse_rows_by_table[table].extend(rows)

        self.kafka_dlq_records.extend(other.kafka_dlq_records)
        self.kafka_late_records.extend(other.kafka_late_records)

        for name, amount in other.metric_increments.items():
            self.metric_increments[name] = self.metric_increments.get(name, 0) + amount

        for warning, count in other.semantic_warning_counts.items():
            self.semantic_warning_counts[warning] = self.semantic_warning_counts.get(warning, 0) + count

        for reason, count in other.session_skip_counts.items():
            self.session_skip_counts[reason] = self.session_skip_counts.get(reason, 0) + count

    def has_work(self) -> bool:
        return bool(
            self.checkpoints
            or self.reserved_event_ids
            or self.redis_inputs
            or self.redis_mutations
            or self.clickhouse_rows_by_table
            or self.kafka_dlq_records
            or self.kafka_late_records
            or self.metric_increments
        )


def dead_letter_from_raw(
    error_reason: str,
    raw_payload_json: str,
    failed_at: datetime,
    raw_event: Mapping[str, Any] | None,
    source_topic: str,
    source_partition: int,
    source_offset: int,
) -> DeadLetterRecord:
    raw = raw_event or {}
    return DeadLetterRecord(
        error_reason=error_reason,
        raw_payload_json=raw_payload_json,
        failed_at=failed_at,
        event_id=str(raw.get("event_id") or "unknown"),
        event_type=str(raw.get("event_type") or "UNKNOWN"),
        event_time=_as_datetime(raw.get("event_time")),
        ingest_time=_as_datetime(raw.get("ingest_time")),
        station_id=_as_optional_str(raw.get("station_id")),
        connector_id=_as_optional_str(raw.get("connector_id")),
        operator_id=_as_optional_str(raw.get("operator_id")),
        session_id=_as_optional_str(raw.get("session_id")),
        schema_version=_as_optional_str(raw.get("schema_version")),
        producer_id=_as_optional_str(raw.get("producer_id")),
        sequence_no=_as_optional_int(raw.get("sequence_no")),
        source_topic=source_topic,
        source_partition=source_partition,
        source_offset=source_offset,
    )


def _as_optional_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        parsed = int(value)
        return parsed if parsed >= 0 else None
    except (TypeError, ValueError):
        return None


def _as_optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _as_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return parse_timestamp(value, "dlq_timestamp")
        except ValueError:
            return None
    return None
