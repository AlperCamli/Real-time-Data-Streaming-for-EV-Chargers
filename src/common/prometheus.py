"""Optional Prometheus helpers for simulator/processor metrics exposure."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from threading import Lock
from typing import Any


try:
    from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, start_http_server
except ModuleNotFoundError:  # pragma: no cover - optional dependency in local runs
    CollectorRegistry = None  # type: ignore[assignment]
    Counter = None  # type: ignore[assignment]
    Gauge = None  # type: ignore[assignment]
    Histogram = None  # type: ignore[assignment]
    start_http_server = None  # type: ignore[assignment]


def prometheus_available() -> bool:
    return all(item is not None for item in (CollectorRegistry, Counter, Gauge, Histogram, start_http_server))


class OptionalPrometheusRegistry:
    """Creates Prometheus metrics only when dependency is available and enabled."""

    def __init__(self, *, enabled: bool) -> None:
        self._requested_enabled = enabled
        self._enabled = enabled and prometheus_available()
        self._lock = Lock()
        self._registry = CollectorRegistry(auto_describe=True) if self._enabled else None
        self._counters: dict[tuple[str, tuple[str, ...]], Any] = {}
        self._gauges: dict[tuple[str, tuple[str, ...]], Any] = {}
        self._histograms: dict[tuple[str, tuple[str, ...]], Any] = {}

    @property
    def enabled(self) -> bool:
        return self._enabled

    def counter(self, name: str, documentation: str, labelnames: Sequence[str] = ()) -> Any | None:
        if not self._enabled:
            return None

        key = (name, tuple(labelnames))
        with self._lock:
            metric = self._counters.get(key)
            if metric is None:
                metric = Counter(
                    name=name,
                    documentation=documentation,
                    labelnames=tuple(labelnames),
                    registry=self._registry,
                )
                self._counters[key] = metric
            return metric

    def gauge(self, name: str, documentation: str, labelnames: Sequence[str] = ()) -> Any | None:
        if not self._enabled:
            return None

        key = (name, tuple(labelnames))
        with self._lock:
            metric = self._gauges.get(key)
            if metric is None:
                metric = Gauge(
                    name=name,
                    documentation=documentation,
                    labelnames=tuple(labelnames),
                    registry=self._registry,
                )
                self._gauges[key] = metric
            return metric

    def histogram(
        self,
        name: str,
        documentation: str,
        *,
        buckets: Sequence[float] | None = None,
        labelnames: Sequence[str] = (),
    ) -> Any | None:
        if not self._enabled:
            return None

        key = (name, tuple(labelnames))
        with self._lock:
            metric = self._histograms.get(key)
            if metric is None:
                kwargs: dict[str, Any] = {
                    "name": name,
                    "documentation": documentation,
                    "labelnames": tuple(labelnames),
                    "registry": self._registry,
                }
                if buckets is not None:
                    kwargs["buckets"] = tuple(buckets)
                metric = Histogram(**kwargs)
                self._histograms[key] = metric
            return metric

    def start_http_server(self, *, host: str, port: int, logger: logging.Logger) -> bool:
        if not self._enabled:
            logger.warning(
                "prometheus_metrics_disabled",
                extra={"enabled_flag": self._requested_enabled, "dependency_available": prometheus_available()},
            )
            return False

        assert start_http_server is not None
        start_http_server(addr=host, port=port, registry=self._registry)
        logger.info("prometheus_metrics_server_started", extra={"host": host, "port": port})
        return True
