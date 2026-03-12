"""Kafka consumer adapter for processor ingestion."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class ConsumedMessage:
    topic: str
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    timestamp_ms: int | None


class KafkaEventConsumer:
    def __init__(
        self,
        *,
        bootstrap_servers: str,
        topic: str,
        consumer_group: str,
        poll_timeout_ms: int,
        max_poll_records: int,
        auto_offset_reset: str,
        logger: logging.Logger,
    ) -> None:
        self._logger = logger
        self._topic = topic
        self._max_poll_records = max_poll_records

        try:
            from kafka import KafkaConsumer  # type: ignore
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "kafka-python is required for processor consumption. "
                "Install with: pip install kafka-python"
            ) from exc

        self._consumer = KafkaConsumer(
            self._topic,
            bootstrap_servers=bootstrap_servers,
            group_id=consumer_group,
            enable_auto_commit=False,
            auto_offset_reset=auto_offset_reset,
            consumer_timeout_ms=1000,
            key_deserializer=None,
            value_deserializer=None,
            max_poll_records=max_poll_records,
        )
        self._poll_timeout_ms = poll_timeout_ms

    def poll(self) -> list[ConsumedMessage]:
        records = self._consumer.poll(
            timeout_ms=self._poll_timeout_ms,
            max_records=self._max_poll_records,
        )
        messages: list[ConsumedMessage] = []

        for topic_partition, tp_records in records.items():
            for record in tp_records:
                timestamp_ms = record.timestamp if isinstance(record.timestamp, int) else None
                messages.append(
                    ConsumedMessage(
                        topic=topic_partition.topic,
                        partition=topic_partition.partition,
                        offset=record.offset,
                        key=record.key,
                        value=record.value,
                        timestamp_ms=timestamp_ms,
                    )
                )

        messages.sort(key=lambda item: (item.topic, item.partition, item.offset))
        return messages

    def commit(self) -> None:
        self._consumer.commit()

    def close(self) -> None:
        try:
            self._consumer.commit()
        except Exception as exc:  # noqa: BLE001
            self._logger.warning("processor_consumer_commit_on_close_failed", extra={"error": str(exc)})
        finally:
            self._consumer.close()

    def __enter__(self) -> "KafkaEventConsumer":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()
