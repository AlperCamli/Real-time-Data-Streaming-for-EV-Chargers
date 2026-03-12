"""Minimal in-memory metrics placeholders for local development scaffolding."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from threading import Lock
from typing import DefaultDict


COUNTER_DUPLICATES_DISCARDED = "duplicates_discarded_total"
COUNTER_LATE_REJECTED = "late_rejected_total"
COUNTER_EVENTS_PROCESSED = "events_processed_total"
COUNTER_EVENTS_DLQ = "events_dlq_total"


@dataclass
class MetricSnapshot:
    counters: dict[str, int]
    gauges: dict[str, float]


class InMemoryMetricRegistry:
    """Simple registry that can be replaced by Prometheus in later phases."""

    def __init__(self) -> None:
        self._counters: DefaultDict[str, int] = defaultdict(int)
        self._gauges: dict[str, float] = {}
        self._lock = Lock()

    def increment(self, metric_name: str, amount: int = 1) -> None:
        with self._lock:
            self._counters[metric_name] += amount

    def set_gauge(self, metric_name: str, value: float) -> None:
        with self._lock:
            self._gauges[metric_name] = value

    def snapshot(self) -> MetricSnapshot:
        with self._lock:
            return MetricSnapshot(counters=dict(self._counters), gauges=dict(self._gauges))


metrics = InMemoryMetricRegistry()
