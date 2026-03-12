"""Simulator-specific metric names and helpers."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Deque

from src.common.metrics import InMemoryMetricRegistry, metrics as shared_metrics


COUNTER_EVENTS_GENERATED_TOTAL = "simulator_events_generated_total"
COUNTER_EVENTS_GENERATED_BY_TYPE_PREFIX = "simulator_events_generated_type"
COUNTER_FAULT_EVENTS_GENERATED = "simulator_fault_events_generated_total"
COUNTER_DUPLICATES_INJECTED = "simulator_duplicates_injected_total"
COUNTER_OUT_OF_ORDER_INJECTED = "simulator_out_of_order_injected_total"
COUNTER_TOO_LATE_INJECTED = "simulator_too_late_injected_total"
COUNTER_PRODUCE_FAILURES = "simulator_produce_failures_total"

GAUGE_ACTIVE_SESSIONS = "simulator_active_sessions"
GAUGE_ACTUAL_EPS = "simulator_actual_eps"


@dataclass(slots=True)
class EmissionPoint:
    timestamp_monotonic: float
    count: int


class SimulatorMetrics:
    def __init__(self, registry: InMemoryMetricRegistry | None = None, eps_window_seconds: float = 30.0) -> None:
        self._registry = registry or shared_metrics
        self._eps_window_seconds = max(1.0, eps_window_seconds)
        self._emissions: Deque[EmissionPoint] = deque()

    def increment_generated(self, event_type: str, amount: int = 1) -> None:
        self._registry.increment(COUNTER_EVENTS_GENERATED_TOTAL, amount)
        metric_name = f"{COUNTER_EVENTS_GENERATED_BY_TYPE_PREFIX}_{event_type.lower()}_total"
        self._registry.increment(metric_name, amount)

    def increment_fault_events(self, amount: int = 1) -> None:
        self._registry.increment(COUNTER_FAULT_EVENTS_GENERATED, amount)

    def increment_duplicates(self, amount: int = 1) -> None:
        self._registry.increment(COUNTER_DUPLICATES_INJECTED, amount)

    def increment_out_of_order(self, amount: int = 1) -> None:
        self._registry.increment(COUNTER_OUT_OF_ORDER_INJECTED, amount)

    def increment_too_late(self, amount: int = 1) -> None:
        self._registry.increment(COUNTER_TOO_LATE_INJECTED, amount)

    def increment_produce_failures(self, amount: int = 1) -> None:
        self._registry.increment(COUNTER_PRODUCE_FAILURES, amount)

    def set_active_sessions(self, count: int) -> None:
        self._registry.set_gauge(GAUGE_ACTIVE_SESSIONS, float(max(0, count)))

    def observe_emitted(self, count: int, now_monotonic: float) -> float:
        self._emissions.append(EmissionPoint(timestamp_monotonic=now_monotonic, count=max(0, count)))
        self._trim(now_monotonic)
        eps = self.current_eps(now_monotonic)
        self._registry.set_gauge(GAUGE_ACTUAL_EPS, eps)
        return eps

    def current_eps(self, now_monotonic: float) -> float:
        self._trim(now_monotonic)
        if not self._emissions:
            return 0.0

        total_count = sum(item.count for item in self._emissions)
        elapsed = max(
            self._eps_window_seconds,
            now_monotonic - self._emissions[0].timestamp_monotonic,
        )
        return total_count / elapsed

    def snapshot(self) -> dict[str, dict[str, float | int]]:
        snap = self._registry.snapshot()
        return {
            "counters": dict(snap.counters),
            "gauges": dict(snap.gauges),
        }

    def _trim(self, now_monotonic: float) -> None:
        lower_bound = now_monotonic - self._eps_window_seconds
        while self._emissions and self._emissions[0].timestamp_monotonic < lower_bound:
            self._emissions.popleft()
