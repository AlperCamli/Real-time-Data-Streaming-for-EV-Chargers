from __future__ import annotations

import logging
import time
import unittest

from src.processor.sinks.kafka_dlq import KafkaJsonTopicSink


class FakeFuture:
    def get(self, timeout: float = 10.0) -> None:
        return None


class FakeProducer:
    def __init__(self) -> None:
        self.sent: list[tuple[str, str, dict[str, object]]] = []
        self.flush_calls = 0
        self.closed = False

    def send(self, topic: str, key: str, value: dict[str, object]) -> FakeFuture:
        self.sent.append((topic, key, value))
        return FakeFuture()

    def flush(self, timeout: float = 10.0) -> None:
        self.flush_calls += 1

    def close(self, timeout: float = 5.0) -> None:
        self.closed = True


class KafkaSinkFlushTests(unittest.TestCase):
    def test_low_volume_records_flush_on_interval(self) -> None:
        producer = FakeProducer()
        sink = KafkaJsonTopicSink(
            bootstrap_servers="unused:9092",
            topic="cs.ev.events.dlq",
            logger=logging.getLogger("kafka-sink-test"),
            batch_size=10,
            flush_interval_seconds=0.05,
            producer_client=producer,
        )

        sink.enqueue({"event_id": "evt-1"}, key="station-1")
        self.assertEqual(sink.flush(force=False), 0)

        time.sleep(0.12)
        self.assertEqual(sink.flush(force=False), 1)
        self.assertEqual(len(producer.sent), 1)
        self.assertEqual(producer.flush_calls, 1)


if __name__ == "__main__":
    unittest.main()
