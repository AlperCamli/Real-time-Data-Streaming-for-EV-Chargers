"""Loop scheduling and data-quality injection hooks."""

from __future__ import annotations

import copy
import heapq
import random
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

from src.common.schemas.validation import parse_timestamp
from src.simulator.config import DataQualityConfig
from src.simulator.metrics import SimulatorMetrics


@dataclass(slots=True)
class DelayedEvent:
    release_monotonic: float
    sequence: int
    event: dict[str, Any]


class TickRateController:
    def __init__(self, tick_interval_seconds: float) -> None:
        self._tick_interval_seconds = max(0.05, tick_interval_seconds)

    def sleep_to_next_tick(self, tick_started_monotonic: float) -> None:
        elapsed = time.monotonic() - tick_started_monotonic
        remaining = self._tick_interval_seconds - elapsed
        if remaining > 0:
            time.sleep(remaining)


class QualityEventInjector:
    def __init__(self, config: DataQualityConfig, rng: random.Random, metrics: SimulatorMetrics) -> None:
        self._config = config
        self._rng = rng
        self._metrics = metrics
        self._delayed_events: list[tuple[float, int, dict[str, Any]]] = []
        self._delay_sequence = 0

    def apply(self, events: list[dict[str, Any]], now: datetime, now_monotonic: float) -> list[dict[str, Any]]:
        ready_events: list[dict[str, Any]] = []

        for event in events:
            if self._rng.random() < self._config.out_of_order_injection_rate:
                delay_seconds = self._rng.uniform(
                    self._config.out_of_order_delay_seconds_min,
                    self._config.out_of_order_delay_seconds_max,
                )
                self._delay_sequence += 1
                heapq.heappush(
                    self._delayed_events,
                    (now_monotonic + delay_seconds, self._delay_sequence, copy.deepcopy(event)),
                )
                self._metrics.increment_out_of_order()
            else:
                ready_events.append(event)

        ready_events.extend(self._drain_due_events(now_monotonic))

        emitted: list[dict[str, Any]] = []
        for event in ready_events:
            self._set_ingest_time(event, now)
            emitted.append(event)

            if self._rng.random() < self._config.duplicate_injection_rate:
                duplicate = copy.deepcopy(event)
                self._set_ingest_time(duplicate, now)
                emitted.append(duplicate)
                self._metrics.increment_duplicates()

            if self._rng.random() < self._config.too_late_injection_rate:
                too_late = self._to_too_late_copy(event, now)
                emitted.append(too_late)
                self._metrics.increment_too_late()

        return emitted

    def drain_all(self, now: datetime) -> list[dict[str, Any]]:
        drained: list[dict[str, Any]] = []
        while self._delayed_events:
            _, _, event = heapq.heappop(self._delayed_events)
            self._set_ingest_time(event, now)
            drained.append(event)
        return drained

    def _drain_due_events(self, now_monotonic: float) -> list[dict[str, Any]]:
        due: list[dict[str, Any]] = []
        while self._delayed_events and self._delayed_events[0][0] <= now_monotonic:
            _, _, event = heapq.heappop(self._delayed_events)
            due.append(event)
        return due

    def _to_too_late_copy(self, source_event: dict[str, Any], now: datetime) -> dict[str, Any]:
        too_late = copy.deepcopy(source_event)
        too_late["event_id"] = str(uuid4())

        event_time = parse_timestamp(too_late["event_time"], "event_time")
        lateness = self._config.too_late_threshold_seconds + self._rng.uniform(20.0, 240.0)
        too_late["event_time"] = (event_time - timedelta(seconds=lateness)).astimezone(timezone.utc).isoformat()
        too_late["ingest_time"] = now.astimezone(timezone.utc).isoformat()
        return too_late

    @staticmethod
    def _set_ingest_time(event: dict[str, Any], now: datetime) -> None:
        event["ingest_time"] = now.astimezone(timezone.utc).isoformat()
