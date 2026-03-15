from __future__ import annotations

import logging
import time
import unittest

from src.processor.config import ConsumerConfig, ProcessingConfig, ProcessorConfig, SessionRulesConfig, SinkConfig
from src.processor.dedup import DedupBackend
from src.processor.main import SinkWorker, _commit_worker_acks
from src.processor.models import SinkBatch
from src.processor.sinks.clickhouse_sink import ClickHouseFlushStats
from src.processor.sinks.redis_sink import RedisApplyResult


class FakeProcessor:
    def __init__(self) -> None:
        self.successful_batches: list[SinkBatch] = []
        self.failures = 0

    def on_sink_batch_succeeded(self, *, batch: SinkBatch, redis_result, clickhouse_stats, acknowledged_at_monotonic: float) -> None:
        self.successful_batches.append(batch)

    def record_sink_failure(self) -> None:
        self.failures += 1


class FakeRedisSink:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.applied_batches: list[list[object]] = []

    def build_mutations_for_inputs(self, inputs: list[object]) -> list[object]:
        return []

    def apply_mutations(self, mutations: list[object]) -> RedisApplyResult:
        if self.fail:
            raise RuntimeError("redis write failed")
        self.applied_batches.append(list(mutations))
        return RedisApplyResult()


class FakeClickHouseSink:
    def __init__(self) -> None:
        self.enqueued: list[dict[str, list[tuple[object, ...]]]] = []
        self.closed = False

    def enqueue_rows_by_table(self, rows_by_table: dict[str, list[tuple[object, ...]]]) -> None:
        self.enqueued.append(rows_by_table)

    def flush(self, force: bool = False) -> ClickHouseFlushStats:
        return ClickHouseFlushStats()

    def close(self) -> None:
        self.closed = True


class FakeKafkaSink:
    def __init__(self) -> None:
        self.enqueued: list[tuple[dict[str, object], str]] = []
        self.closed = False

    def enqueue(self, payload: dict[str, object], key: str = "") -> None:
        self.enqueued.append((payload, key))

    def flush(self, force: bool = False) -> int:
        return len(self.enqueued)

    def close(self) -> None:
        self.closed = True


class FakeDeduplicator:
    def __init__(self) -> None:
        self.committed: list[list[str]] = []
        self.released: list[list[str]] = []

    def reserve_batch(self, event_ids: list[str]) -> set[str]:
        return set()

    def commit_reserved(self, event_ids: list[str]) -> None:
        self.committed.append(list(event_ids))

    def release_reserved(self, event_ids: list[str]) -> None:
        self.released.append(list(event_ids))


class FakeConsumer:
    def __init__(self) -> None:
        self.commits: list[dict[tuple[str, int], int]] = []

    def commit_offsets(self, checkpoints: dict[tuple[str, int], int]) -> None:
        self.commits.append(dict(checkpoints))


class SinkWorkerTests(unittest.TestCase):
    def test_successful_batches_ack_offsets_in_fifo_order(self) -> None:
        processor = FakeProcessor()
        deduplicator = FakeDeduplicator()
        worker = SinkWorker(
            processor=processor,
            clickhouse_sink=FakeClickHouseSink(),
            redis_sink=FakeRedisSink(),
            dlq_sink=FakeKafkaSink(),
            late_sink=None,
            dedup_backend=DedupBackend(deduplicator=deduplicator, backend_name="fake"),
            config=_test_processor_config(),
            logger=_silent_logger("sink-worker-success"),
        )
        worker.start()

        batch_one = SinkBatch()
        batch_one.checkpoints[("topic", 0)] = 5
        batch_one.reserved_event_ids.append("evt-1")
        batch_two = SinkBatch()
        batch_two.checkpoints[("topic", 0)] = 8
        batch_two.reserved_event_ids.append("evt-2")

        worker.enqueue_batch(batch_one)
        worker.enqueue_batch(batch_two)
        worker.shutdown()
        worker.join()
        worker.raise_if_failed()

        consumer = FakeConsumer()
        _commit_worker_acks(worker, consumer)

        self.assertEqual(consumer.commits, [{("topic", 0): 8}])
        self.assertEqual(deduplicator.committed, [["evt-1", "evt-2"]])
        self.assertEqual(len(processor.successful_batches), 1)

    def test_sink_failure_prevents_commit_and_releases_reservations(self) -> None:
        processor = FakeProcessor()
        deduplicator = FakeDeduplicator()
        worker = SinkWorker(
            processor=processor,
            clickhouse_sink=FakeClickHouseSink(),
            redis_sink=FakeRedisSink(fail=True),
            dlq_sink=FakeKafkaSink(),
            late_sink=None,
            dedup_backend=DedupBackend(deduplicator=deduplicator, backend_name="fake"),
            config=_test_processor_config(),
            logger=_silent_logger("sink-worker-failure"),
        )
        worker.start()

        batch = SinkBatch()
        batch.checkpoints[("topic", 0)] = 5
        batch.reserved_event_ids.append("evt-1")

        worker.enqueue_batch(batch)
        worker.shutdown()
        worker.join()

        with self.assertRaises(RuntimeError):
            worker.raise_if_failed()

        consumer = FakeConsumer()
        _commit_worker_acks(worker, consumer)

        self.assertEqual(consumer.commits, [])
        self.assertEqual(deduplicator.committed, [])
        self.assertEqual(deduplicator.released, [["evt-1"]])
        self.assertEqual(processor.failures, 1)


def _test_processor_config() -> ProcessorConfig:
    return ProcessorConfig(
        consumer=ConsumerConfig(poll_timeout_ms=10, max_poll_records=10, auto_offset_reset="latest"),
        processing=ProcessingConfig(
            event_time_field="event_time",
            dedup_key_field="event_id",
            dedup_ttl_seconds=60,
            count_duplicates_and_discard=True,
            allowed_lateness_seconds=600,
            late_events_enabled=True,
        ),
        session_rules=SessionRulesConfig(
            finalize_on_session_stop=True,
            inactivity_timeout_seconds=900,
            timeout_sweeper_interval_seconds=1.0,
            finalize_on_fault_termination=True,
            fact_sessions_append_once=True,
            retro_correction_from_ultra_late=False,
            default_tariff_eur_per_kwh=0.35,
        ),
        sinks=SinkConfig(
            clickhouse_batch_size=1000,
            clickhouse_flush_interval_seconds=0.01,
            kafka_batch_size=1000,
            flush_interval_seconds=0.01,
            max_pending_batches=4,
            redis_session_state_ttl_seconds=86400,
            redis_finalized_session_ttl_seconds=3600,
        ),
    )


def _silent_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.handlers = [logging.NullHandler()]
    logger.propagate = False
    return logger


if __name__ == "__main__":
    unittest.main()
