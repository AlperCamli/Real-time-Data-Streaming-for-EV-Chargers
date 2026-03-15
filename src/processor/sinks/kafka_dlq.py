"""Kafka sink adapters for DLQ and optional late audit topic."""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Mapping


class KafkaJsonTopicSink:
    def __init__(
        self,
        *,
        bootstrap_servers: str,
        topic: str,
        logger: logging.Logger,
        batch_size: int = 200,
        flush_interval_seconds: float = 1.0,
        producer_client: object | None = None,
    ) -> None:
        self._topic = topic
        self._logger = logger
        self._batch_size = max(1, batch_size)
        self._flush_interval_seconds = max(0.1, flush_interval_seconds)
        self._queue: list[dict[str, Any]] = []
        self._last_flush_monotonic = time.monotonic()

        if producer_client is not None:
            self._producer = producer_client
        else:
            try:
                from kafka import KafkaProducer  # type: ignore
            except ModuleNotFoundError as exc:
                raise RuntimeError(
                    "kafka-python is required for processor Kafka sinks. "
                    "Install with: pip install kafka-python"
                ) from exc

            self._producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                linger_ms=25,
                batch_size=64 * 1024,
                acks="all",
                retries=3,
                value_serializer=lambda payload: json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8"),
                key_serializer=lambda key: key.encode("utf-8"),
            )

    def enqueue(self, payload: Mapping[str, Any], key: str = "") -> None:
        event = dict(payload)
        event.setdefault("routed_at", datetime.now(timezone.utc).isoformat())
        if key:
            event["_kafka_key"] = key
        self._queue.append(event)

    def flush(self, force: bool = False) -> int:
        if not self._queue:
            return 0
        if not force and len(self._queue) < self._batch_size and (time.monotonic() - self._last_flush_monotonic) < self._flush_interval_seconds:
            return 0

        queued = self._queue
        self._queue = []

        sent_count = 0
        futures = []
        for item in queued:
            key = str(item.pop("_kafka_key", ""))
            futures.append(self._producer.send(self._topic, key=key, value=item))

        for future in futures:
            future.get(timeout=10.0)
            sent_count += 1

        self._producer.flush(timeout=10.0)
        self._last_flush_monotonic = time.monotonic()
        return sent_count

    def close(self) -> None:
        try:
            self.flush(force=True)
        except Exception as exc:  # noqa: BLE001
            self._logger.warning(
                "processor_kafka_sink_flush_on_close_failed",
                extra={"topic": self._topic, "error": str(exc)},
            )
        finally:
            self._producer.close(timeout=5.0)


class KafkaDlqSink(KafkaJsonTopicSink):
    """Dedicated alias for dead-letter Kafka sink."""
