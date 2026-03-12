"""Kafka publishing for simulator events."""

from __future__ import annotations

import json
import logging
from typing import Any, Protocol

from src.simulator.config import ProducerConfig


class EventProducer(Protocol):
    def publish_batch(self, events: list[dict[str, Any]]) -> int:
        """Publishes events and returns number of publish failures."""

    def close(self) -> None:
        """Flushes and closes producer resources."""


class KafkaEventProducer:
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        config: ProducerConfig,
        logger: logging.Logger,
    ) -> None:
        self._topic = topic
        self._logger = logger
        self._request_timeout_seconds = max(config.request_timeout_ms / 1000.0, 1.0)

        try:
            from kafka import KafkaProducer  # type: ignore
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "kafka-python is required for simulator Kafka publishing. "
                "Install with: pip install kafka-python"
            ) from exc

        compression_type = config.compression_type if config.compression_type else None
        acks = _normalize_acks(config.acks)
        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            linger_ms=config.linger_ms,
            batch_size=config.batch_size,
            acks=acks,
            retries=3,
            compression_type=compression_type,
            request_timeout_ms=config.request_timeout_ms,
            value_serializer=_serialize_event,
            key_serializer=lambda value: value.encode("utf-8"),
        )

    def publish_batch(self, events: list[dict[str, Any]]) -> int:
        if not events:
            return 0

        failures = 0
        futures = []

        for event in events:
            key = str(event.get("station_id", "unknown"))
            futures.append(self._producer.send(self._topic, key=key, value=event))

        for event, future in zip(events, futures):
            try:
                future.get(timeout=self._request_timeout_seconds)
            except Exception as exc:  # noqa: BLE001
                failures += 1
                self._logger.error(
                    "simulator_publish_failed",
                    extra={
                        "event_id": event.get("event_id"),
                        "event_type": event.get("event_type"),
                        "error": str(exc),
                    },
                )

        try:
            self._producer.flush(timeout=self._request_timeout_seconds)
        except Exception as exc:  # noqa: BLE001
            failures = max(failures, len(events))
            self._logger.error(
                "simulator_publish_flush_failed",
                extra={"error": str(exc), "attempted_events": len(events)},
            )
        return failures

    def close(self) -> None:
        try:
            self._producer.flush(timeout=5.0)
        except Exception as exc:  # noqa: BLE001
            self._logger.warning("simulator_producer_flush_on_close_failed", extra={"error": str(exc)})
        finally:
            try:
                self._producer.close(timeout=5.0)
            except Exception as exc:  # noqa: BLE001
                self._logger.warning("simulator_producer_close_failed", extra={"error": str(exc)})


class DryRunEventProducer:
    def __init__(self, topic: str, logger: logging.Logger) -> None:
        self._topic = topic
        self._logger = logger

    def publish_batch(self, events: list[dict[str, Any]]) -> int:
        if events:
            self._logger.debug(
                "simulator_dry_run_publish",
                extra={
                    "topic": self._topic,
                    "event_count": len(events),
                    "first_event_type": events[0].get("event_type"),
                },
            )
        return 0

    def close(self) -> None:
        return None


def build_event_producer(
    bootstrap_servers: str,
    topic: str,
    config: ProducerConfig,
    logger: logging.Logger,
) -> EventProducer:
    if config.dry_run:
        logger.warning("simulator_producer_dry_run_enabled", extra={"topic": topic})
        return DryRunEventProducer(topic=topic, logger=logger)

    return KafkaEventProducer(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        config=config,
        logger=logger,
    )


def _serialize_event(value: dict[str, Any]) -> bytes:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def _normalize_acks(value: int | str) -> int | str:
    if isinstance(value, int):
        return value
    raw = str(value).strip().lower()
    if raw in {"all", "-1"}:
        return "all"
    try:
        return int(raw)
    except ValueError:
        return 1
