"""Stream processor runtime entrypoint and processing loop."""

from __future__ import annotations

import argparse
import signal
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

from src.common.logging import configure_logging
from src.common.settings import load_service_settings, load_yaml_config
from src.common.topic_names import TOPIC_EVENTS_DLQ, TOPIC_EVENTS_LATE, TOPIC_EVENTS_RAW
from src.processor.config import ProcessorConfig, build_processor_config
from src.processor.consumer import ConsumedMessage, KafkaEventConsumer
from src.processor.dedup import DedupBackend, build_deduplicator
from src.processor.lateness import classify_lateness
from src.processor.metrics import (
    COUNTER_ACCEPTED_LATE,
    COUNTER_DLQ_ROUTED,
    COUNTER_DUPLICATES_DETECTED,
    COUNTER_EVENTS_ACCEPTED,
    COUNTER_EVENTS_CONSUMED,
    COUNTER_PARSE_FAILURES,
    COUNTER_SCHEMA_VALIDATION_FAILURES,
    COUNTER_SEMANTIC_VALIDATION_FAILURES,
    COUNTER_STALE_REDIS_WRITE_SKIPS,
    COUNTER_TOO_LATE_REJECTED,
    HISTOGRAM_BATCH_SIZE,
    HISTOGRAM_LOOP_DURATION_SECONDS,
    HISTOGRAM_PROCESSOR_LATENCY_SECONDS,
    ProcessorMetrics,
)
from src.processor.models import BatchOutcome, dead_letter_from_raw
from src.processor.parser import ParseResult, parse_message_value
from src.processor.routing import route_accepted, route_duplicate, route_invalid, route_too_late
from src.processor.sinks import ClickHouseSink, KafkaDlqSink, KafkaJsonTopicSink, RedisStateSink, build_redis_client
from src.processor.state import SessionStateStore
from src.processor.validators import validate_envelope_schema, validate_event_semantics


DEFAULT_PROCESSOR_CONFIG_PATH = Path("config/processor.default.yaml")


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

    def process_batch(self, messages: list[ConsumedMessage]) -> BatchOutcome:
        outcome = BatchOutcome()
        for message in messages:
            outcome.processed_count += 1
            self._metrics.inc(COUNTER_EVENTS_CONSUMED)
            self._process_message(message, outcome)

        return outcome

    def flush(self, force: bool = False) -> None:
        self._clickhouse.flush(force=force)
        self._dlq_sink.flush(force=force)
        if self._late_sink is not None:
            self._late_sink.flush(force=force)

    def close(self) -> None:
        self.flush(force=True)
        self._clickhouse.close()
        self._dlq_sink.close()
        if self._late_sink is not None:
            self._late_sink.close()

    def expire_inactive_sessions(self) -> int:
        expired = self._session_state.expire_inactive(
            now=datetime.now(timezone.utc),
            inactivity_timeout_seconds=self._config.session_rules.inactivity_timeout_seconds,
        )
        return len(expired)

    def _process_message(self, message: ConsumedMessage, outcome: BatchOutcome) -> None:
        received_at = datetime.now(timezone.utc)

        parse_result = parse_message_value(message.value)
        if not parse_result.ok:
            outcome.invalid_count += 1
            self._metrics.inc(COUNTER_PARSE_FAILURES)
            self._route_dead_letter(
                reason=parse_result.error_reason or "parse_error",
                parse_result=parse_result,
                source_message=message,
                failed_at=received_at,
            )
            return

        assert parse_result.event_dict is not None
        schema_result = validate_envelope_schema(parse_result.event_dict)
        if not schema_result.ok:
            outcome.invalid_count += 1
            self._metrics.inc(COUNTER_SCHEMA_VALIDATION_FAILURES)
            self._route_dead_letter(
                reason="; ".join(schema_result.errors),
                parse_result=parse_result,
                source_message=message,
                failed_at=received_at,
            )
            return

        assert schema_result.envelope is not None
        event = schema_result.envelope

        if self._dedup.check_and_mark(event.event_id):
            decision = route_duplicate()
            outcome.duplicate_count += 1
            self._metrics.inc(COUNTER_DUPLICATES_DETECTED)
            self._logger.debug(
                "processor_event_routed",
                extra={"event_id": event.event_id, "event_type": event.event_type.value, "disposition": decision.disposition.value, "reason": decision.reason},
            )
            return

        semantic = validate_event_semantics(event, self._session_state)
        if not semantic.ok:
            outcome.invalid_count += 1
            self._metrics.inc(COUNTER_SEMANTIC_VALIDATION_FAILURES)
            self._route_dead_letter(
                reason="; ".join(semantic.errors),
                parse_result=parse_result,
                source_message=message,
                failed_at=received_at,
            )
            return

        for warning in semantic.warnings:
            self._logger.warning(
                "processor_semantic_warning",
                extra={
                    "event_id": event.event_id,
                    "event_type": event.event_type.value,
                    "warning": warning,
                },
            )

        lateness = classify_lateness(
            event_time=event.event_time,
            received_at=received_at,
            allowed_lateness_seconds=self._config.processing.allowed_lateness_seconds,
        )

        if lateness.is_too_late:
            decision = route_too_late()
            self._clickhouse.enqueue_late_rejected(event=event, lateness_seconds=lateness.lateness_seconds)
            if self._late_sink is not None:
                self._late_sink.enqueue(
                    {
                        "event": event.to_dict(),
                        "lateness_seconds": lateness.lateness_seconds,
                        "reason": "too_late_rejected",
                    },
                    key=event.station_id,
                )
            self._metrics.inc(COUNTER_TOO_LATE_REJECTED)
            outcome.late_rejected_count += 1
            self._logger.debug(
                "processor_event_routed",
                extra={
                    "event_id": event.event_id,
                    "event_type": event.event_type.value,
                    "disposition": decision.disposition.value,
                    "reason": decision.reason,
                    "lateness_seconds": lateness.lateness_seconds,
                },
            )
            return

        session_mutation = self._session_state.apply_event(
            event,
            finalize_on_session_stop=self._config.session_rules.finalize_on_session_stop,
            finalize_on_fault_termination=self._config.session_rules.finalize_on_fault_termination,
        )

        # historical acceptance remains true even when session mutation is skipped.
        if not session_mutation.applied:
            self._logger.info(
                "processor_session_mutation_skipped",
                extra={
                    "event_id": event.event_id,
                    "event_type": event.event_type.value,
                    "reason": session_mutation.reason,
                },
            )

        session_snapshot = session_mutation.snapshot
        if session_snapshot is None and event.session_id:
            session_snapshot = self._session_state.get_session(event.session_id)

        redis_result = self._redis.apply_event(event, session_snapshot=session_snapshot)
        stale_for_redis = redis_result.is_stale_event
        decision = route_accepted(lateness.classification, stale_for_redis=stale_for_redis)
        if stale_for_redis:
            outcome.stale_redis_count += 1
            self._metrics.inc(COUNTER_STALE_REDIS_WRITE_SKIPS)
        if redis_result.error_keys > 0:
            self._logger.warning(
                "processor_redis_write_errors",
                extra={
                    "event_id": event.event_id,
                    "event_type": event.event_type.value,
                    "error_keys": redis_result.error_keys,
                },
            )

        self._clickhouse.enqueue_raw_event(event)

        if lateness.is_accepted_late:
            outcome.accepted_late_count += 1
            self._metrics.inc(COUNTER_ACCEPTED_LATE)

        self._metrics.inc(COUNTER_EVENTS_ACCEPTED)
        self._metrics.observe(
            HISTOGRAM_PROCESSOR_LATENCY_SECONDS,
            max(0.0, (received_at - event.event_time.astimezone(timezone.utc)).total_seconds()),
        )
        outcome.accepted_count += 1
        self._logger.debug(
            "processor_event_routed",
            extra={
                "event_id": event.event_id,
                "event_type": event.event_type.value,
                "disposition": decision.disposition.value,
                "reason": decision.reason,
                "lateness_class": decision.lateness_class.value if decision.lateness_class else None,
                "stale_for_redis": stale_for_redis,
            },
        )

    def _route_dead_letter(
        self,
        *,
        reason: str,
        parse_result: ParseResult,
        source_message: ConsumedMessage,
        failed_at: datetime,
    ) -> None:
        decision = route_invalid(reason)
        raw_event: Mapping[str, object] | None = parse_result.event_dict
        dead_letter = dead_letter_from_raw(
            error_reason=reason,
            raw_payload_json=parse_result.raw_payload_json,
            failed_at=failed_at,
            raw_event=raw_event,
            source_topic=source_message.topic,
            source_partition=source_message.partition,
            source_offset=source_message.offset,
        )
        self._clickhouse.enqueue_dead_letter(dead_letter)
        self._dlq_sink.enqueue(
            {
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
        self._metrics.inc(COUNTER_DLQ_ROUTED)
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

    metrics = ProcessorMetrics()

    raw_topic = TOPIC_EVENTS_RAW
    dlq_topic = TOPIC_EVENTS_DLQ
    late_topic = TOPIC_EVENTS_LATE

    if service_settings.kafka.topic_raw != TOPIC_EVENTS_RAW:
        logger.warning(
            "processor_topic_override_ignored",
            extra={"configured": service_settings.kafka.topic_raw, "frozen": TOPIC_EVENTS_RAW},
        )

    redis_client = build_redis_client(service_settings.redis, logger)
    redis_sink = RedisStateSink(redis_client=redis_client, logger=logger)
    dedup_backend = build_deduplicator(
        redis_client=redis_client,
        ttl_seconds=processor_config.processing.dedup_ttl_seconds,
        logger=logger,
    )

    clickhouse_sink = ClickHouseSink(
        settings=service_settings.clickhouse,
        logger=logger,
        batch_size=processor_config.sinks.clickhouse_batch_size,
    )

    dlq_sink = KafkaDlqSink(
        bootstrap_servers=service_settings.kafka.bootstrap_servers,
        topic=dlq_topic,
        logger=logger,
        batch_size=processor_config.sinks.kafka_batch_size,
    )

    late_sink: KafkaJsonTopicSink | None = None
    if processor_config.processing.late_events_enabled:
        late_sink = KafkaJsonTopicSink(
            bootstrap_servers=service_settings.kafka.bootstrap_servers,
            topic=late_topic,
            logger=logger,
            batch_size=processor_config.sinks.kafka_batch_size,
        )

    session_state = SessionStateStore()

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
        },
    )

    loops = 0
    last_health_log = time.monotonic()

    try:
        while not stop_event.is_set():
            loop_started = time.monotonic()
            loops += 1

            messages = consumer.poll()
            metrics.observe(HISTOGRAM_BATCH_SIZE, float(len(messages)))

            expired_sessions = processor.expire_inactive_sessions()
            if expired_sessions:
                logger.info("processor_sessions_expired", extra={"count": expired_sessions})

            commit_batch = False
            if messages:
                try:
                    outcome = processor.process_batch(messages)
                    processor.flush(force=True)
                    commit_batch = True

                    logger.info(
                        "processor_batch_processed",
                        extra={
                            "batch_size": len(messages),
                            "accepted": outcome.accepted_count,
                            "invalid": outcome.invalid_count,
                            "duplicates": outcome.duplicate_count,
                            "late_rejected": outcome.late_rejected_count,
                            "accepted_late": outcome.accepted_late_count,
                            "stale_redis_events": outcome.stale_redis_count,
                        },
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.exception("processor_batch_failed", extra={"error": str(exc), "batch_size": len(messages)})
                    commit_batch = False

            if commit_batch:
                consumer.commit()

            loop_duration = time.monotonic() - loop_started
            metrics.observe(HISTOGRAM_LOOP_DURATION_SECONDS, loop_duration)

            if args.max_loops > 0 and loops >= args.max_loops:
                logger.info("processor_runtime_loop_limit_reached", extra={"max_loops": args.max_loops})
                stop_event.set()

            now_monotonic = time.monotonic()
            if now_monotonic - last_health_log >= 10.0:
                last_health_log = now_monotonic
                snapshot = metrics.snapshot()
                logger.info(
                    "processor_runtime_health",
                    extra={
                        "loops": loops,
                        "counters": snapshot.counters,
                        "histograms": {
                            metric_name: {
                                "count": metric_snapshot.count,
                                "sum": round(metric_snapshot.sum, 3),
                                "max": round(metric_snapshot.max, 3),
                            }
                            for metric_name, metric_snapshot in snapshot.histograms.items()
                        },
                    },
                )
    finally:
        try:
            processor.close()
        finally:
            consumer.close()

        snapshot = metrics.snapshot()
        logger.info(
            "processor_shutdown_complete",
            extra={
                "loops": loops,
                "counters": snapshot.counters,
                "histograms": {
                    metric_name: {
                        "count": metric_snapshot.count,
                        "sum": round(metric_snapshot.sum, 3),
                        "max": round(metric_snapshot.max, 3),
                    }
                    for metric_name, metric_snapshot in snapshot.histograms.items()
                },
            },
        )


if __name__ == "__main__":
    main()
