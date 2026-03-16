"""Stream processor runtime entrypoint and processing loop."""

from __future__ import annotations

import argparse
import queue
import signal
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

from src.common.logging import configure_logging
from src.common.prometheus import OptionalPrometheusRegistry
from src.common.settings import load_service_settings, load_yaml_config
from src.common.table_names import (
    TABLE_AGG_CITY_DAY_FAULTS,
    TABLE_AGG_OPERATOR_HOUR,
    TABLE_AGG_STATION_MINUTE,
    TABLE_DEAD_LETTER_EVENTS,
    TABLE_FACT_SESSIONS,
    TABLE_LATE_EVENTS_REJECTED,
    TABLE_RAW_EVENTS,
)
from src.common.topic_names import TOPIC_EVENTS_DLQ, TOPIC_EVENTS_LATE, TOPIC_EVENTS_RAW
from src.processor.config import ProcessorConfig, build_processor_config
from src.processor.consumer import ConsumedMessage, KafkaEventConsumer
from src.processor.dedup import DedupBackend, build_deduplicator
from src.processor.finalization import AggregateAccumulator, SessionFactFinalizer, SessionTimeoutSweeper
from src.processor.lateness import classify_lateness
from src.processor.metrics import (
    COUNTER_ACCEPTED_LATE,
    COUNTER_AGG_CITY_DAY_FAULTS_ROWS_WRITTEN,
    COUNTER_AGG_OPERATOR_HOUR_ROWS_WRITTEN,
    COUNTER_AGG_STATION_MINUTE_ROWS_WRITTEN,
    COUNTER_CLICKHOUSE_INSERT_FAILURES,
    COUNTER_DEAD_LETTER_ROWS_WRITTEN,
    COUNTER_DLQ_ROUTED,
    COUNTER_DUPLICATES_DETECTED,
    COUNTER_EVENTS_ACCEPTED,
    COUNTER_EVENTS_CONSUMED,
    COUNTER_FACT_ROWS_WRITTEN,
    COUNTER_FINALIZATION_FAILURES,
    COUNTER_FINALIZED_FAULT,
    COUNTER_FINALIZED_NORMAL_STOP,
    COUNTER_FINALIZED_TIMEOUT,
    COUNTER_LATE_ROWS_WRITTEN,
    COUNTER_PARSE_FAILURES,
    COUNTER_RAW_ROWS_WRITTEN,
    COUNTER_SCHEMA_VALIDATION_FAILURES,
    COUNTER_SEMANTIC_VALIDATION_FAILURES,
    COUNTER_SEMANTIC_WARNINGS,
    COUNTER_SESSION_MUTATION_SKIPS,
    COUNTER_STALE_REDIS_WRITE_SKIPS,
    COUNTER_SWEEPER_FINALIZATIONS,
    COUNTER_TOO_LATE_REJECTED,
    GAUGE_CLICKHOUSE_INSERT_ROWS_PER_SECOND,
    GAUGE_KAFKA_CONSUMER_LAG,
    HISTOGRAM_BATCH_SIZE,
    HISTOGRAM_CLICKHOUSE_BATCH_SIZE_ROWS,
    HISTOGRAM_CLICKHOUSE_INSERT_LATENCY_MS,
    HISTOGRAM_END_TO_END_LATENCY_MS,
    HISTOGRAM_INGEST_LAG_MS,
    HISTOGRAM_PROCESSOR_LATENCY_MS,
    HISTOGRAM_PROCESSOR_LOOP_DURATION_MS,
    HISTOGRAM_REDIS_WRITE_LATENCY_MS,
    ProcessorMetrics,
)
from src.processor.models import (
    AcceptedEventTelemetry,
    BatchOutcome,
    KafkaTopicRecord,
    RedisBatchInput,
    SinkBatch,
    dead_letter_from_raw,
)
from src.processor.parser import ParsedEventResult, parse_and_validate_message
from src.processor.routing import route_invalid
from src.processor.sinks import ClickHouseSink, KafkaDlqSink, KafkaJsonTopicSink, RedisApplyResult, RedisStateSink, build_redis_client
from src.processor.state import SessionSnapshot, SessionStateStore
from src.processor.validators import validate_event_semantics


DEFAULT_PROCESSOR_CONFIG_PATH = Path("config/processor.default.yaml")
_SINK_WORKER_STOP = object()
_SINK_WORKER_TIMEOUT = object()
_EXPECTED_SESSION_SKIP_REASONS = frozenset({"no_session_mutation_for_event_type"})


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ChargeSquare stream processor")
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_PROCESSOR_CONFIG_PATH,
        help="Path to processor config file",
    )
    parser.add_argument(
        "--max-loops",
        type=int,
        default=0,
        help="Optional max loop count for local smoke runs (0 means infinite)",
    )
    return parser.parse_args()


class SinkWorker:
    def __init__(
        self,
        *,
        processor: "StreamProcessor",
        clickhouse_sink: ClickHouseSink,
        redis_sink: RedisStateSink,
        dlq_sink: KafkaDlqSink,
        late_sink: KafkaJsonTopicSink | None,
        dedup_backend: DedupBackend,
        config: ProcessorConfig,
        logger,
    ) -> None:
        self._processor = processor
        self._clickhouse = clickhouse_sink
        self._redis = redis_sink
        self._dlq_sink = dlq_sink
        self._late_sink = late_sink
        self._dedup = dedup_backend.deduplicator
        self._logger = logger
        self._queue: queue.Queue[object] = queue.Queue(maxsize=max(1, config.sinks.max_pending_batches))
        self._ack_queue: queue.Queue[dict[tuple[str, int], int]] = queue.Queue()
        self._thread = threading.Thread(target=self._run, name="processor-sink-worker", daemon=True)
        self._failure_lock = threading.Lock()
        self._failure: BaseException | None = None
        self._clickhouse_batch_size = max(1, config.sinks.clickhouse_batch_size)
        self._kafka_batch_size = max(1, config.sinks.kafka_batch_size)
        self._flush_interval_seconds = max(
            0.1,
            min(config.sinks.clickhouse_flush_interval_seconds, config.sinks.flush_interval_seconds),
        )

    def start(self) -> None:
        self._thread.start()

    def enqueue_batch(self, batch: SinkBatch) -> None:
        self._queue.put_nowait(batch)

    def is_backpressured(self) -> bool:
        return self._queue.full()

    def shutdown(self) -> None:
        self._queue.put(_SINK_WORKER_STOP)

    def join(self) -> None:
        self._thread.join()

    def raise_if_failed(self) -> None:
        with self._failure_lock:
            failure = self._failure
        if failure is not None:
            raise RuntimeError("processor sink worker failed") from failure

    def drain_acknowledged_checkpoints(self) -> dict[tuple[str, int], int]:
        merged: dict[tuple[str, int], int] = {}
        while True:
            try:
                checkpoints = self._ack_queue.get_nowait()
            except queue.Empty:
                return merged

            for key, next_offset in checkpoints.items():
                current = merged.get(key, -1)
                if next_offset > current:
                    merged[key] = next_offset

    def _run(self) -> None:
        pending = SinkBatch()
        pending_started_monotonic: float | None = None
        should_exit = False
        try:
            while not should_exit:
                timeout = None
                if pending.has_work() and pending_started_monotonic is not None:
                    elapsed = max(0.0, time.monotonic() - pending_started_monotonic)
                    timeout = max(0.0, self._flush_interval_seconds - elapsed)

                try:
                    next_batch = self._queue.get(timeout=timeout)
                except queue.Empty:
                    next_batch = _SINK_WORKER_TIMEOUT

                if next_batch is _SINK_WORKER_TIMEOUT:
                    if pending.has_work():
                        self._flush_pending(pending)
                        pending = SinkBatch()
                        pending_started_monotonic = None
                    continue

                if next_batch is _SINK_WORKER_STOP:
                    if pending.has_work():
                        self._flush_pending(pending)
                    should_exit = True
                    continue

                assert isinstance(next_batch, SinkBatch)
                if pending.has_work():
                    pending.merge(next_batch)
                else:
                    pending = next_batch
                    pending_started_monotonic = time.monotonic()

                if self._should_flush_pending(pending, pending_started_monotonic):
                    self._flush_pending(pending)
                    pending = SinkBatch()
                    pending_started_monotonic = None
        except Exception:
            return
        finally:
            try:
                self._clickhouse.close()
            finally:
                self._dlq_sink.close()
                if self._late_sink is not None:
                    self._late_sink.close()

    def _should_flush_pending(self, batch: SinkBatch, pending_started_monotonic: float | None) -> bool:
        if not batch.has_work():
            return False

        if (
            not batch.reserved_event_ids
            and not batch.redis_inputs
            and not batch.redis_mutations
            and not batch.clickhouse_rows_by_table
            and not batch.kafka_dlq_records
            and not batch.kafka_late_records
        ):
            return True

        if any(len(rows) >= self._clickhouse_batch_size for rows in batch.clickhouse_rows_by_table.values()):
            return True
        if len(batch.kafka_dlq_records) >= self._kafka_batch_size:
            return True
        if len(batch.kafka_late_records) >= self._kafka_batch_size:
            return True

        if pending_started_monotonic is None:
            return False
        return (time.monotonic() - pending_started_monotonic) >= self._flush_interval_seconds

    def _flush_pending(self, batch: SinkBatch) -> None:
        try:
            redis_mutations = self._redis.build_mutations_for_batch(
                inputs=batch.redis_inputs,
                extra_mutations=batch.redis_mutations,
            )
            redis_result = self._redis.apply_mutations(redis_mutations, already_coalesced=True)
            self._clickhouse.enqueue_rows_by_table(batch.clickhouse_rows_by_table)
            clickhouse_stats = self._clickhouse.flush(force=True)

            for record in batch.kafka_dlq_records:
                self._dlq_sink.enqueue(record.payload, key=record.key)
            self._dlq_sink.flush(force=True)

            if self._late_sink is not None:
                for record in batch.kafka_late_records:
                    self._late_sink.enqueue(record.payload, key=record.key)
                self._late_sink.flush(force=True)

            self._dedup.commit_reserved(batch.reserved_event_ids)
            acknowledged_at_monotonic = time.monotonic()
            self._processor.on_sink_batch_succeeded(
                batch=batch,
                redis_result=redis_result,
                clickhouse_stats=clickhouse_stats,
                acknowledged_at_monotonic=acknowledged_at_monotonic,
            )
            self._ack_queue.put(batch.checkpoints)
        except Exception as exc:  # noqa: BLE001
            self._dedup.release_reserved(batch.reserved_event_ids)
            self._set_failure(exc, batch)
            raise

    def _set_failure(self, exc: BaseException, batch: SinkBatch) -> None:
        self._processor.record_sink_failure()
        with self._failure_lock:
            if self._failure is None:
                self._failure = exc
        self._logger.exception(
            "processor_sink_worker_failed",
            extra={
                "error": str(exc),
                "checkpoints": len(batch.checkpoints),
                "reserved_event_ids": len(batch.reserved_event_ids),
            },
        )


class StreamProcessor:
    def __init__(
        self,
        *,
        config: ProcessorConfig,
        metrics: ProcessorMetrics,
        clickhouse_sink: ClickHouseSink,
        redis_sink: RedisStateSink,
        dlq_sink: KafkaDlqSink,
        late_sink: KafkaJsonTopicSink | None,
        dedup_backend: DedupBackend,
        session_state: SessionStateStore,
        logger,
    ) -> None:
        self._config = config
        self._metrics = metrics
        self._clickhouse = clickhouse_sink
        self._redis = redis_sink
        self._dlq_sink = dlq_sink
        self._late_sink = late_sink
        self._dedup = dedup_backend.deduplicator
        self._session_state = session_state
        self._logger = logger
        self._session_finalizer = SessionFactFinalizer()
        self._aggregates = AggregateAccumulator(default_tariff_eur_per_kwh=config.session_rules.default_tariff_eur_per_kwh)

    def process_batch(self, messages: list[ConsumedMessage]) -> SinkBatch:
        batch = SinkBatch()
        reserved_event_ids: list[str] = []
        now_utc = datetime.now(timezone.utc)

        for message in messages:
            batch.add_checkpoint(message.topic, message.partition, message.offset + 1)

        try:
            parsed_events: list[tuple[ConsumedMessage, ParsedEventResult]] = []
            for message in messages:
                batch.outcome.processed_count += 1
                received_at = datetime.now(timezone.utc)
                parsed_result = parse_and_validate_message(message.value)
                if not parsed_result.ok:
                    batch.outcome.invalid_count += 1
                    if parsed_result.error_reason in {
                        "empty_message",
                        "malformed_utf8",
                        "empty_json",
                        "malformed_json",
                        "event_must_be_json_object",
                    }:
                        batch.inc_metric(COUNTER_PARSE_FAILURES)
                    else:
                        batch.inc_metric(COUNTER_SCHEMA_VALIDATION_FAILURES)
                    self._route_dead_letter(
                        reason=parsed_result.error_reason or "parse_error",
                        raw_payload_json=parsed_result.raw_payload_json,
                        raw_event=parsed_result.event_dict,
                        source_message=message,
                        failed_at=received_at,
                        batch=batch,
                    )
                    continue

                parsed_events.append((message, parsed_result))

            batch.inc_metric(COUNTER_EVENTS_CONSUMED, batch.outcome.processed_count)
            if not parsed_events:
                self._append_ready_aggregate_rows(batch, now=now_utc, force=False)
                return batch

            unique_event_ids: list[str] = []
            seen_event_ids: set[str] = set()
            for _, parsed_result in parsed_events:
                assert parsed_result.envelope is not None
                envelope = parsed_result.envelope
                if envelope.event_id in seen_event_ids:
                    continue
                seen_event_ids.add(envelope.event_id)
                unique_event_ids.append(envelope.event_id)

            dedup_duplicate_ids = self._dedup.reserve_batch(unique_event_ids)
            reserved_event_ids = [event_id for event_id in unique_event_ids if event_id not in dedup_duplicate_ids]
            batch.reserved_event_ids.extend(reserved_event_ids)

            non_duplicate_events: list[tuple[ConsumedMessage, ParsedEventResult]] = []
            seen_first_occurrence: set[str] = set()
            for message, parsed_result in parsed_events:
                assert parsed_result.envelope is not None
                envelope = parsed_result.envelope
                if envelope.event_id in seen_first_occurrence:
                    batch.outcome.duplicate_count += 1
                    batch.inc_metric(COUNTER_DUPLICATES_DETECTED)
                    continue
                seen_first_occurrence.add(envelope.event_id)

                if envelope.event_id in dedup_duplicate_ids:
                    batch.outcome.duplicate_count += 1
                    batch.inc_metric(COUNTER_DUPLICATES_DETECTED)
                    continue

                non_duplicate_events.append((message, parsed_result))

            for message, parsed_result in non_duplicate_events:
                assert parsed_result.envelope is not None
                event = parsed_result.envelope
                processing_started_monotonic = time.monotonic()
                received_at = datetime.now(timezone.utc)
                existing_session = self._session_state.peek_session(event.session_id)

                semantic = validate_event_semantics(event, existing_session)
                if not semantic.ok:
                    batch.outcome.invalid_count += 1
                    batch.inc_metric(COUNTER_SEMANTIC_VALIDATION_FAILURES)
                    self._route_dead_letter(
                        reason="; ".join(semantic.errors),
                        raw_payload_json=parsed_result.raw_payload_json,
                        raw_event=parsed_result.event_dict,
                        source_message=message,
                        failed_at=received_at,
                        batch=batch,
                    )
                    continue

                if semantic.warnings:
                    batch.inc_metric(COUNTER_SEMANTIC_WARNINGS, len(semantic.warnings))
                    for warning in semantic.warnings:
                        batch.add_semantic_warning(warning)

                lateness = classify_lateness(
                    event_time=event.event_time,
                    received_at=received_at,
                    allowed_lateness_seconds=self._config.processing.allowed_lateness_seconds,
                )

                if lateness.is_too_late:
                    batch.add_clickhouse_row(
                        TABLE_LATE_EVENTS_REJECTED,
                        self._clickhouse.build_late_rejected_row(
                            event,
                            lateness.lateness_seconds,
                            payload_json=parsed_result.payload_json,
                        ),
                    )
                    if self._late_sink is not None:
                        batch.kafka_late_records.append(
                            KafkaTopicRecord(
                                payload={
                                    "event": dict(parsed_result.event_dict) if parsed_result.event_dict is not None else event.to_dict(),
                                    "lateness_seconds": lateness.lateness_seconds,
                                    "reason": "too_late_rejected",
                                },
                                key=event.station_id,
                            )
                        )
                    batch.outcome.late_rejected_count += 1
                    batch.inc_metric(COUNTER_TOO_LATE_REJECTED)
                    continue

                session_mutation = self._session_state.apply_event(
                    event,
                    existing_snapshot=existing_session,
                    finalize_on_session_stop=self._config.session_rules.finalize_on_session_stop,
                    finalize_on_fault_termination=self._config.session_rules.finalize_on_fault_termination,
                )

                if not session_mutation.applied and _should_record_session_mutation_skip(session_mutation.reason):
                    batch.inc_metric(COUNTER_SESSION_MUTATION_SKIPS)
                    batch.add_session_skip(session_mutation.reason)

                session_snapshot = session_mutation.snapshot
                active_session_count = session_mutation.active_session_count

                batch.add_clickhouse_row(
                    TABLE_RAW_EVENTS,
                    self._clickhouse.build_raw_event_row(event, payload_json=parsed_result.payload_json),
                )
                self._aggregates.record_event(
                    event,
                    session_snapshot=session_snapshot,
                    session_mutation_applied=session_mutation.applied,
                    active_session_count=active_session_count,
                )

                if session_mutation.finalized and session_mutation.snapshot is not None:
                    try:
                        self._finalize_session_snapshot(session_mutation.snapshot, batch)
                    except Exception as exc:  # noqa: BLE001
                        self._metrics.inc(COUNTER_FINALIZATION_FAILURES)
                        self._logger.exception(
                            "processor_session_finalization_failed",
                            extra={"session_id": session_mutation.snapshot.session_id, "error": str(exc)},
                        )

                if lateness.is_accepted_late:
                    batch.outcome.accepted_late_count += 1
                    batch.inc_metric(COUNTER_ACCEPTED_LATE)

                batch.outcome.accepted_count += 1
                batch.inc_metric(COUNTER_EVENTS_ACCEPTED)
                batch.redis_inputs.append(
                    RedisBatchInput(
                        event=event,
                        session_snapshot=session_snapshot,
                        session_mutation_applied=session_mutation.applied,
                        active_session_count=active_session_count,
                        telemetry=AcceptedEventTelemetry(
                            event_id=event.event_id,
                            event_type=event.event_type.value,
                            event_time=event.event_time,
                            received_at=received_at,
                            processing_started_monotonic=processing_started_monotonic,
                        ),
                    )
                )

            self._append_ready_aggregate_rows(batch, now=now_utc, force=False)
            return batch
        except Exception:
            self._dedup.release_reserved(reserved_event_ids)
            raise

    def collect_flush_batch(self, *, force: bool = False, finalize_windows: bool = False) -> SinkBatch:
        batch = SinkBatch()
        self._append_ready_aggregate_rows(
            batch,
            now=datetime.now(timezone.utc),
            force=force or finalize_windows,
        )
        return batch

    def close(self) -> SinkBatch:
        return self.collect_flush_batch(force=True, finalize_windows=True)

    def expire_inactive_sessions(self) -> tuple[int, SinkBatch]:
        expired = self._session_state.expire_inactive(
            now=datetime.now(timezone.utc),
            inactivity_timeout_seconds=self._config.session_rules.inactivity_timeout_seconds,
        )
        batch = SinkBatch()

        for snapshot in expired:
            try:
                self._finalize_session_snapshot(snapshot, batch)
                active_count = self._session_state.active_sessions_for_station(snapshot.station_id)
                batch.redis_mutations.extend(
                    self._redis.build_timeout_finalization_mutations(snapshot, active_session_count=active_count)
                )
                batch.inc_metric(COUNTER_SWEEPER_FINALIZATIONS)
            except Exception as exc:  # noqa: BLE001
                self._metrics.inc(COUNTER_FINALIZATION_FAILURES)
                self._logger.exception(
                    "processor_timeout_finalization_failed",
                    extra={"session_id": snapshot.session_id, "error": str(exc)},
                )

        self._append_ready_aggregate_rows(batch, now=datetime.now(timezone.utc), force=False)
        return len(expired), batch

    def on_sink_batch_succeeded(
        self,
        *,
        batch: SinkBatch,
        redis_result: RedisApplyResult,
        clickhouse_stats,
        acknowledged_at_monotonic: float,
    ) -> None:
        if redis_result.stale_keys > 0:
            batch.outcome.stale_redis_count += redis_result.stale_keys
            batch.inc_metric(COUNTER_STALE_REDIS_WRITE_SKIPS, redis_result.stale_keys)

        self._record_redis_metrics(redis_result)
        self._record_clickhouse_metrics(clickhouse_stats)

        for metric_name, amount in batch.metric_increments.items():
            self._metrics.inc(metric_name, amount)

        observe_metric = self._metrics.observe
        for redis_input in batch.redis_inputs:
            ingest_lag_ms = max(
                0.0,
                (redis_input.telemetry.received_at - redis_input.telemetry.event_time).total_seconds() * 1000.0,
            )
            processor_latency_ms = max(
                0.0,
                (acknowledged_at_monotonic - redis_input.telemetry.processing_started_monotonic) * 1000.0,
            )
            end_to_end_latency_ms = ingest_lag_ms + processor_latency_ms
            observe_metric(HISTOGRAM_INGEST_LAG_MS, ingest_lag_ms)
            observe_metric(HISTOGRAM_PROCESSOR_LATENCY_MS, processor_latency_ms)
            observe_metric(HISTOGRAM_END_TO_END_LATENCY_MS, end_to_end_latency_ms)

        if batch.semantic_warning_counts:
            self._logger.debug(
                "processor_semantic_warning_summary",
                extra={"warnings": batch.semantic_warning_counts},
            )
        if batch.session_skip_counts:
            self._logger.debug(
                "processor_session_mutation_skipped_summary",
                extra={"reasons": batch.session_skip_counts},
            )
        if redis_result.error_keys > 0:
            self._logger.warning(
                "processor_redis_write_errors",
                extra={
                    "error_keys": redis_result.error_keys,
                    "attempts": redis_result.attempts,
                },
            )

    def record_sink_failure(self) -> None:
        self._metrics.inc(COUNTER_CLICKHOUSE_INSERT_FAILURES)

    def _append_ready_aggregate_rows(self, batch: SinkBatch, *, now: datetime, force: bool) -> None:
        aggregate_rows = self._aggregates.flush_ready(now=now, force=force)
        for row in aggregate_rows.station_minute_rows:
            batch.add_clickhouse_row(TABLE_AGG_STATION_MINUTE, row)
        for row in aggregate_rows.operator_hour_rows:
            batch.add_clickhouse_row(TABLE_AGG_OPERATOR_HOUR, row)
        for row in aggregate_rows.city_day_fault_rows:
            batch.add_clickhouse_row(TABLE_AGG_CITY_DAY_FAULTS, row)

    def _finalize_session_snapshot(self, snapshot: SessionSnapshot, batch: SinkBatch) -> None:
        fact = self._session_finalizer.build_fact(snapshot)
        batch.add_clickhouse_row(TABLE_FACT_SESSIONS, self._clickhouse.build_fact_session_row(fact))
        self._aggregates.record_finalized_session(fact)

        if fact.finalized_reason == "normal_stop":
            batch.inc_metric(COUNTER_FINALIZED_NORMAL_STOP)
        elif fact.finalized_reason == "fault_termination":
            batch.inc_metric(COUNTER_FINALIZED_FAULT)
        elif fact.finalized_reason == "inactivity_timeout":
            batch.inc_metric(COUNTER_FINALIZED_TIMEOUT)

    def _record_redis_metrics(self, redis_result: RedisApplyResult) -> None:
        if redis_result.attempts > 0:
            self._metrics.observe(
                HISTOGRAM_REDIS_WRITE_LATENCY_MS,
                (redis_result.latency_seconds * 1000.0) / max(1, redis_result.attempts),
            )

    def _record_clickhouse_metrics(self, ch_stats) -> None:
        rows_by_table = ch_stats.rows_by_table
        if not rows_by_table:
            return

        self._metrics.inc(COUNTER_RAW_ROWS_WRITTEN, rows_by_table.get(TABLE_RAW_EVENTS, 0))
        self._metrics.inc(COUNTER_DEAD_LETTER_ROWS_WRITTEN, rows_by_table.get(TABLE_DEAD_LETTER_EVENTS, 0))
        self._metrics.inc(COUNTER_LATE_ROWS_WRITTEN, rows_by_table.get(TABLE_LATE_EVENTS_REJECTED, 0))
        self._metrics.inc(COUNTER_FACT_ROWS_WRITTEN, rows_by_table.get(TABLE_FACT_SESSIONS, 0))
        self._metrics.inc(COUNTER_AGG_STATION_MINUTE_ROWS_WRITTEN, rows_by_table.get(TABLE_AGG_STATION_MINUTE, 0))
        self._metrics.inc(COUNTER_AGG_OPERATOR_HOUR_ROWS_WRITTEN, rows_by_table.get(TABLE_AGG_OPERATOR_HOUR, 0))
        self._metrics.inc(COUNTER_AGG_CITY_DAY_FAULTS_ROWS_WRITTEN, rows_by_table.get(TABLE_AGG_CITY_DAY_FAULTS, 0))

        for batch_size in ch_stats.batch_sizes:
            self._metrics.observe(HISTOGRAM_CLICKHOUSE_BATCH_SIZE_ROWS, float(batch_size))

        for latency in ch_stats.batch_latency_seconds:
            self._metrics.observe(HISTOGRAM_CLICKHOUSE_INSERT_LATENCY_MS, latency * 1000.0)

        if ch_stats.total_latency_seconds > 0 and ch_stats.total_rows > 0:
            rows_per_second = ch_stats.total_rows / ch_stats.total_latency_seconds
            self._metrics.set_gauge(GAUGE_CLICKHOUSE_INSERT_ROWS_PER_SECOND, rows_per_second)

    def _route_dead_letter(
        self,
        *,
        reason: str,
        raw_payload_json: str,
        raw_event: Mapping[str, object] | None,
        source_message: ConsumedMessage,
        failed_at: datetime,
        batch: SinkBatch,
    ) -> None:
        decision = route_invalid(reason)
        dead_letter = dead_letter_from_raw(
            error_reason=reason,
            raw_payload_json=raw_payload_json,
            failed_at=failed_at,
            raw_event=raw_event,
            source_topic=source_message.topic,
            source_partition=source_message.partition,
            source_offset=source_message.offset,
        )
        batch.add_clickhouse_row(TABLE_DEAD_LETTER_EVENTS, self._clickhouse.build_dead_letter_row(dead_letter))
        batch.kafka_dlq_records.append(
            KafkaTopicRecord(
                payload={
                    "error_reason": dead_letter.error_reason,
                    "failed_at": dead_letter.failed_at.isoformat(),
                    "source": {
                        "topic": dead_letter.source_topic,
                        "partition": dead_letter.source_partition,
                        "offset": dead_letter.source_offset,
                    },
                    "event": raw_event,
                    "raw_payload_json": dead_letter.raw_payload_json,
                },
                key=dead_letter.station_id or dead_letter.event_id,
            )
        )
        batch.inc_metric(COUNTER_DLQ_ROUTED)
        self._logger.debug(
            "processor_event_routed",
            extra={
                "event_id": dead_letter.event_id,
                "event_type": dead_letter.event_type,
                "disposition": decision.disposition.value,
                "reason": decision.reason,
            },
        )


def main() -> None:
    args = parse_args()
    stop_event = threading.Event()

    service_settings = load_service_settings("processor")
    raw_config = load_yaml_config(args.config)
    processor_config = build_processor_config(raw_config, service_settings)

    logger = configure_logging(
        service_name=service_settings.service_name,
        log_level=service_settings.log_level,
        json_logs=service_settings.log_json,
    )

    prometheus_registry = OptionalPrometheusRegistry(enabled=service_settings.metrics_enabled)
    metrics = ProcessorMetrics(prometheus=prometheus_registry)

    raw_topic = TOPIC_EVENTS_RAW
    dlq_topic = TOPIC_EVENTS_DLQ
    late_topic = TOPIC_EVENTS_LATE

    if service_settings.kafka.topic_raw != TOPIC_EVENTS_RAW:
        logger.warning(
            "processor_topic_override_ignored",
            extra={"configured": service_settings.kafka.topic_raw, "frozen": TOPIC_EVENTS_RAW},
        )

    redis_client = build_redis_client(service_settings.redis, logger)
    redis_sink = RedisStateSink(
        redis_client=redis_client,
        logger=logger,
        session_state_ttl_seconds=processor_config.sinks.redis_session_state_ttl_seconds,
        finalized_session_ttl_seconds=processor_config.sinks.redis_finalized_session_ttl_seconds,
    )
    dedup_backend = build_deduplicator(
        redis_client=redis_client,
        ttl_seconds=processor_config.processing.dedup_ttl_seconds,
        logger=logger,
    )

    clickhouse_sink = ClickHouseSink(
        settings=service_settings.clickhouse,
        logger=logger,
        batch_size=processor_config.sinks.clickhouse_batch_size,
        flush_interval_seconds=processor_config.sinks.clickhouse_flush_interval_seconds,
    )

    dlq_sink = KafkaDlqSink(
        bootstrap_servers=service_settings.kafka.bootstrap_servers,
        topic=dlq_topic,
        logger=logger,
        batch_size=processor_config.sinks.kafka_batch_size,
        flush_interval_seconds=processor_config.sinks.flush_interval_seconds,
    )

    late_sink: KafkaJsonTopicSink | None = None
    if processor_config.processing.late_events_enabled:
        late_sink = KafkaJsonTopicSink(
            bootstrap_servers=service_settings.kafka.bootstrap_servers,
            topic=late_topic,
            logger=logger,
            batch_size=processor_config.sinks.kafka_batch_size,
            flush_interval_seconds=processor_config.sinks.flush_interval_seconds,
        )

    session_state = SessionStateStore()
    sweeper = SessionTimeoutSweeper(run_interval_seconds=processor_config.session_rules.timeout_sweeper_interval_seconds)

    processor = StreamProcessor(
        config=processor_config,
        metrics=metrics,
        clickhouse_sink=clickhouse_sink,
        redis_sink=redis_sink,
        dlq_sink=dlq_sink,
        late_sink=late_sink,
        dedup_backend=dedup_backend,
        session_state=session_state,
        logger=logger,
    )

    consumer = KafkaEventConsumer(
        bootstrap_servers=service_settings.kafka.bootstrap_servers,
        topic=raw_topic,
        consumer_group=service_settings.kafka.consumer_group,
        poll_timeout_ms=processor_config.consumer.poll_timeout_ms,
        max_poll_records=processor_config.consumer.max_poll_records,
        auto_offset_reset=processor_config.consumer.auto_offset_reset,
        logger=logger,
    )

    sink_worker = SinkWorker(
        processor=processor,
        clickhouse_sink=clickhouse_sink,
        redis_sink=redis_sink,
        dlq_sink=dlq_sink,
        late_sink=late_sink,
        dedup_backend=dedup_backend,
        config=processor_config,
        logger=logger,
    )
    sink_worker.start()

    prometheus_registry.start_http_server(
        host=service_settings.metrics_host,
        port=service_settings.metrics_port,
        logger=logger,
    )

    def _handle_signal(sig: int, _frame: object) -> None:
        logger.info("processor_shutdown_signal", extra={"signal": sig})
        stop_event.set()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    logger.info(
        "processor_boot_complete",
        extra={
            "config_path": str(args.config),
            "consumer_group": service_settings.kafka.consumer_group,
            "raw_topic": raw_topic,
            "dlq_topic": dlq_topic,
            "late_topic": late_topic,
            "late_events_enabled": processor_config.processing.late_events_enabled,
            "dedup_backend": dedup_backend.backend_name,
            "allowed_lateness_seconds": processor_config.processing.allowed_lateness_seconds,
            "metrics_endpoint": f"http://{service_settings.metrics_host}:{service_settings.metrics_port}/metrics",
        },
    )

    loops = 0
    last_health_log = time.monotonic()

    try:
        while not stop_event.is_set():
            loop_started = time.monotonic()
            loops += 1

            _commit_worker_acks(sink_worker, consumer)
            sink_worker.raise_if_failed()

            if sweeper.should_run(loop_started):
                expired_sessions, sweeper_batch = processor.expire_inactive_sessions()
                if sweeper_batch.has_work():
                    _enqueue_with_backpressure(sweeper_batch, sink_worker, consumer, stop_event)
                if expired_sessions:
                    logger.info("processor_sessions_expired", extra={"count": expired_sessions})

            if sink_worker.is_backpressured():
                consumer.heartbeat()
            else:
                messages = consumer.poll()
                metrics.observe(HISTOGRAM_BATCH_SIZE, float(len(messages)))
                if messages:
                    try:
                        batch = processor.process_batch(messages)
                        _enqueue_with_backpressure(batch, sink_worker, consumer, stop_event)
                        logger.debug(
                            "processor_batch_processed",
                            extra={
                                "batch_size": len(messages),
                                "accepted": batch.outcome.accepted_count,
                                "invalid": batch.outcome.invalid_count,
                                "duplicates": batch.outcome.duplicate_count,
                                "late_rejected": batch.outcome.late_rejected_count,
                                "accepted_late": batch.outcome.accepted_late_count,
                            },
                        )
                    except Exception as exc:  # noqa: BLE001
                        metrics.inc(COUNTER_CLICKHOUSE_INSERT_FAILURES)
                        logger.exception("processor_batch_failed", extra={"error": str(exc), "batch_size": len(messages)})
                        stop_event.set()

            maintenance_batch = processor.collect_flush_batch(force=False, finalize_windows=False)
            if maintenance_batch.has_work():
                _enqueue_with_backpressure(maintenance_batch, sink_worker, consumer, stop_event)

            loop_duration_ms = max(0.0, (time.monotonic() - loop_started) * 1000.0)
            metrics.observe(HISTOGRAM_PROCESSOR_LOOP_DURATION_MS, loop_duration_ms)

            if args.max_loops > 0 and loops >= args.max_loops:
                logger.info("processor_runtime_loop_limit_reached", extra={"max_loops": args.max_loops})
                stop_event.set()

            now_monotonic = time.monotonic()
            if now_monotonic - last_health_log >= 10.0:
                last_health_log = now_monotonic
                consumer_lag = consumer.estimate_total_lag()
                if consumer_lag is not None:
                    metrics.set_gauge(GAUGE_KAFKA_CONSUMER_LAG, float(consumer_lag))
                snapshot = metrics.snapshot()
                logger.info(
                    "processor_runtime_health",
                    extra={
                        "loops": loops,
                        "counters": snapshot.counters,
                        "gauges": snapshot.gauges,
                        "histograms": {
                            metric_name: {
                                "count": metric_snapshot.count,
                                "sum": round(metric_snapshot.sum, 3),
                                "max": round(metric_snapshot.max, 3),
                                "p95": round(metric_snapshot.p95, 3),
                                "p99": round(metric_snapshot.p99, 3),
                            }
                            for metric_name, metric_snapshot in snapshot.histograms.items()
                        },
                    },
                )
    finally:
        final_batch = processor.close()
        if final_batch.has_work():
            _enqueue_with_backpressure(final_batch, sink_worker, consumer, stop_event)

        sink_worker.shutdown()
        sink_worker.join()
        _commit_worker_acks(sink_worker, consumer)
        consumer.close()

        snapshot = metrics.snapshot()
        logger.info(
            "processor_shutdown_complete",
            extra={
                "loops": loops,
                "counters": snapshot.counters,
                "gauges": snapshot.gauges,
            },
        )


def _commit_worker_acks(sink_worker: SinkWorker, consumer: KafkaEventConsumer) -> None:
    checkpoints = sink_worker.drain_acknowledged_checkpoints()
    if checkpoints:
        consumer.commit_offsets(checkpoints)


def _should_record_session_mutation_skip(reason: str) -> bool:
    return reason not in _EXPECTED_SESSION_SKIP_REASONS


def _enqueue_with_backpressure(
    batch: SinkBatch,
    sink_worker: SinkWorker,
    consumer: KafkaEventConsumer,
    stop_event: threading.Event,
) -> None:
    if not batch.has_work():
        return

    while True:
        _commit_worker_acks(sink_worker, consumer)
        sink_worker.raise_if_failed()
        try:
            sink_worker.enqueue_batch(batch)
            return
        except queue.Full:
            consumer.heartbeat()
            time.sleep(0.01)


if __name__ == "__main__":
    main()
