"""Kafka consumer adapter for processor ingestion."""

from __future__ import annotations

import inspect
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
        consumer_client: object | None = None,
        topic_partition_cls: object | None = None,
        offset_and_metadata_cls: object | None = None,
    ) -> None:
        self._logger = logger
        self._topic = topic
        self._max_poll_records = max_poll_records
        self._commit_constructor_mode = "unknown"
        self._topic_partition_cls = topic_partition_cls
        self._offset_and_metadata_cls = offset_and_metadata_cls

        if consumer_client is not None:
            self._consumer = consumer_client
        else:
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

        return messages

    def heartbeat(self) -> None:
        try:
            self._consumer.poll(timeout_ms=0, max_records=0)
        except Exception as exc:  # noqa: BLE001
            self._logger.debug("processor_consumer_heartbeat_failed", extra={"error": str(exc)})

    def commit_offsets(self, checkpoints: dict[tuple[str, int], int]) -> None:
        if not checkpoints:
            return
        offsets = self._build_commit_offsets(checkpoints)
        try:
            self._consumer.commit(offsets=offsets)
        except Exception:
            self._logger.exception(
                "processor_consumer_commit_failed",
                extra={
                    "checkpoint_count": len(checkpoints),
                    "constructor_mode": self._commit_constructor_mode,
                },
            )
            raise

    def estimate_total_lag(self) -> int | None:
        try:
            partitions = list(self._consumer.assignment())
            if not partitions:
                self.heartbeat()
                partitions = list(self._consumer.assignment())
            if not partitions:
                return None

            end_offsets = self._consumer.end_offsets(partitions)
            total_lag = 0
            for partition in partitions:
                latest = int(end_offsets.get(partition, 0))
                try:
                    position = int(self._consumer.position(partition))
                except Exception:  # noqa: BLE001
                    continue
                total_lag += max(0, latest - position)
            return total_lag
        except Exception as exc:  # noqa: BLE001
            self._logger.debug("processor_consumer_lag_estimation_failed", extra={"error": str(exc)})
            return None

    def close(self) -> None:
        self._consumer.close()

    def _build_commit_offsets(self, checkpoints: dict[tuple[str, int], int]) -> object:
        topic_partition_cls = self._topic_partition_cls
        offset_and_metadata_cls = self._offset_and_metadata_cls

        if topic_partition_cls is None or offset_and_metadata_cls is None:
            try:
                from kafka.structs import OffsetAndMetadata, TopicPartition  # type: ignore
            except ModuleNotFoundError:
                return {(topic, partition): next_offset for (topic, partition), next_offset in checkpoints.items()}
            topic_partition_cls = TopicPartition
            offset_and_metadata_cls = OffsetAndMetadata

        offset_factory = self._offset_and_metadata_factory(offset_and_metadata_cls)
        return {
            topic_partition_cls(topic, partition): offset_factory(next_offset)
            for (topic, partition), next_offset in checkpoints.items()
        }

    def _offset_and_metadata_factory(self, constructor: object):
        try:
            signature = inspect.signature(constructor)
        except (TypeError, ValueError):
            signature = None

        if signature is not None and "leader_epoch" in signature.parameters:
            self._commit_constructor_mode = "offset_metadata_leader_epoch"
            return lambda next_offset: constructor(next_offset, "", -1)

        def _build(next_offset: int):
            try:
                value = constructor(next_offset, "")
                self._commit_constructor_mode = "offset_metadata_legacy"
                return value
            except TypeError as exc:
                if "leader_epoch" not in str(exc):
                    raise
                self._commit_constructor_mode = "offset_metadata_leader_epoch"
                return constructor(next_offset, "", -1)

        return _build

    def __enter__(self) -> "KafkaEventConsumer":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()
