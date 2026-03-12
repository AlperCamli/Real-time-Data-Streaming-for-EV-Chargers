"""Processor metrics hooks for benchmark-friendly instrumentation."""

from __future__ import annotations

import math
from collections import defaultdict, deque
from dataclasses import dataclass
from threading import Lock
from typing import Any, DefaultDict, Deque

from src.common.prometheus import OptionalPrometheusRegistry


COUNTER_EVENTS_CONSUMED = "events_consumed_total"
COUNTER_EVENTS_ACCEPTED = "events_accepted_total"
COUNTER_PARSE_FAILURES = "parse_failures_total"
COUNTER_SCHEMA_VALIDATION_FAILURES = "schema_validation_failures_total"
COUNTER_SEMANTIC_VALIDATION_FAILURES = "semantic_validation_failures_total"
COUNTER_DUPLICATES_DETECTED = "duplicates_detected_total"
COUNTER_ACCEPTED_LATE = "accepted_late_events_total"
COUNTER_TOO_LATE_REJECTED = "too_late_rejected_total"
COUNTER_DLQ_ROUTED = "dead_letter_routed_total"
COUNTER_STALE_REDIS_WRITE_SKIPS = "stale_redis_write_skips_total"
COUNTER_RAW_ROWS_WRITTEN = "clickhouse_raw_rows_written_total"
COUNTER_DEAD_LETTER_ROWS_WRITTEN = "clickhouse_dead_letter_rows_written_total"
COUNTER_LATE_ROWS_WRITTEN = "clickhouse_late_rows_written_total"
COUNTER_FACT_ROWS_WRITTEN = "clickhouse_fact_session_rows_written_total"
COUNTER_AGG_STATION_MINUTE_ROWS_WRITTEN = "clickhouse_agg_station_minute_rows_written_total"
COUNTER_AGG_OPERATOR_HOUR_ROWS_WRITTEN = "clickhouse_agg_operator_hour_rows_written_total"
COUNTER_AGG_CITY_DAY_FAULTS_ROWS_WRITTEN = "clickhouse_agg_city_day_faults_rows_written_total"
COUNTER_FINALIZED_NORMAL_STOP = "session_finalized_normal_total"
COUNTER_FINALIZED_TIMEOUT = "session_finalized_timeout_total"
COUNTER_FINALIZED_FAULT = "session_finalized_fault_total"
COUNTER_SWEEPER_FINALIZATIONS = "session_sweeper_finalizations_total"
COUNTER_FINALIZATION_FAILURES = "session_finalization_failures_total"
COUNTER_CLICKHOUSE_INSERT_FAILURES = "clickhouse_insert_failures_total"

GAUGE_KAFKA_CONSUMER_LAG = "kafka_consumer_lag"
GAUGE_REDIS_MEMORY_USAGE_BYTES = "redis_memory_usage_bytes"
GAUGE_REDIS_KEY_COUNT_TOTAL = "redis_key_count_total"
GAUGE_REDIS_KEY_COUNT_STATION = "redis_key_count_station"
GAUGE_REDIS_KEY_COUNT_CONNECTOR = "redis_key_count_connector"
GAUGE_REDIS_KEY_COUNT_SESSION = "redis_key_count_session"
GAUGE_REDIS_OPS_PER_SECOND = "redis_ops_per_second"
GAUGE_CLICKHOUSE_INSERT_ROWS_PER_SECOND = "clickhouse_insert_rows_per_second"
GAUGE_CLICKHOUSE_ROWS_RAW_EVENTS = "clickhouse_rows_raw_events"
GAUGE_CLICKHOUSE_ROWS_FACT_SESSIONS = "clickhouse_rows_fact_sessions"
GAUGE_CLICKHOUSE_ROWS_AGG_STATION_MINUTE = "clickhouse_rows_agg_station_minute"
GAUGE_CLICKHOUSE_ROWS_AGG_OPERATOR_HOUR = "clickhouse_rows_agg_operator_hour"
GAUGE_CLICKHOUSE_ROWS_AGG_CITY_DAY_FAULTS = "clickhouse_rows_agg_city_day_faults"

HISTOGRAM_INGEST_LAG_MS = "ingest_lag_ms"
HISTOGRAM_PROCESSOR_LATENCY_MS = "processor_latency_ms"
HISTOGRAM_END_TO_END_LATENCY_MS = "end_to_end_latency_ms"
HISTOGRAM_REDIS_WRITE_LATENCY_MS = "redis_write_latency_ms"
HISTOGRAM_REDIS_READ_LATENCY_MS = "redis_read_latency_ms"
HISTOGRAM_CLICKHOUSE_INSERT_LATENCY_MS = "clickhouse_insert_latency_ms"
HISTOGRAM_CLICKHOUSE_QUERY_LATENCY_MS = "clickhouse_query_latency_ms"
HISTOGRAM_CLICKHOUSE_BATCH_SIZE_ROWS = "batch_size_clickhouse_rows"
HISTOGRAM_PROCESSOR_LOOP_DURATION_MS = "processor_loop_duration_ms"
HISTOGRAM_BATCH_SIZE = "processor_batch_size_messages"


@dataclass(slots=True)
class HistogramSnapshot:
    count: int
    sum: float
    max: float
    p50: float
    p95: float
    p99: float


@dataclass(slots=True)
class ProcessorMetricSnapshot:
    counters: dict[str, int]
    gauges: dict[str, float]
    histograms: dict[str, HistogramSnapshot]


class ProcessorMetrics:
    def __init__(
        self,
        *,
        prometheus: OptionalPrometheusRegistry | None = None,
        histogram_window: int = 20000,
        percentile_refresh_every: int = 1000,
    ) -> None:
        self._lock = Lock()
        self._counters: DefaultDict[str, int] = defaultdict(int)
        self._gauges: dict[str, float] = {}
        self._hist_counts: DefaultDict[str, int] = defaultdict(int)
        self._hist_sums: DefaultDict[str, float] = defaultdict(float)
        self._hist_max: DefaultDict[str, float] = defaultdict(float)
        self._hist_values: dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=max(100, histogram_window)))
        self._percentile_refresh_every = max(10, percentile_refresh_every)

        self._prometheus = prometheus
        self._prom_counters: dict[str, Any] = {}
        self._prom_gauges: dict[str, Any] = {}
        self._prom_histograms: dict[str, Any] = {}
        self._prom_percentile_gauges: dict[str, dict[str, Any]] = {}
        if prometheus is not None and prometheus.enabled:
            self._build_prometheus_metrics(prometheus)

    def inc(self, name: str, amount: int = 1) -> None:
        with self._lock:
            self._counters[name] += amount
        metric = self._prom_counters.get(name)
        if metric is not None:
            metric.inc(max(0, amount))

    def set_gauge(self, name: str, value: float) -> None:
        bounded = float(value)
        if math.isnan(bounded) or math.isinf(bounded):
            return
        with self._lock:
            self._gauges[name] = bounded
        metric = self._prom_gauges.get(name)
        if metric is not None:
            metric.set(bounded)

    def observe(self, name: str, value: float) -> None:
        bounded = max(0.0, float(value))
        with self._lock:
            self._hist_counts[name] += 1
            self._hist_sums[name] += bounded
            self._hist_max[name] = max(self._hist_max[name], bounded)
            samples = self._hist_values[name]
            samples.append(bounded)
            if self._hist_counts[name] % self._percentile_refresh_every == 0:
                self._refresh_percentile_gauges_locked(name)

        histogram = self._prom_histograms.get(name)
        if histogram is not None:
            histogram.observe(bounded)

    def snapshot(self) -> ProcessorMetricSnapshot:
        with self._lock:
            for name in self._hist_values:
                self._refresh_percentile_gauges_locked(name)

            histograms = {
                name: HistogramSnapshot(
                    count=self._hist_counts[name],
                    sum=self._hist_sums[name],
                    max=self._hist_max[name],
                    p50=_percentile_from_sorted(samples := sorted(self._hist_values[name]), 50.0),
                    p95=_percentile_from_sorted(samples, 95.0),
                    p99=_percentile_from_sorted(samples, 99.0),
                )
                for name in set(self._hist_counts) | set(self._hist_sums) | set(self._hist_max)
            }
            return ProcessorMetricSnapshot(
                counters=dict(self._counters),
                gauges=dict(self._gauges),
                histograms=histograms,
            )

    def _build_prometheus_metrics(self, prometheus: OptionalPrometheusRegistry) -> None:
        counter_defs = {
            COUNTER_EVENTS_CONSUMED: "Total events consumed from raw Kafka topic",
            COUNTER_EVENTS_ACCEPTED: "Total events accepted by processor",
            COUNTER_PARSE_FAILURES: "Total parse failures",
            COUNTER_SCHEMA_VALIDATION_FAILURES: "Total schema validation failures",
            COUNTER_SEMANTIC_VALIDATION_FAILURES: "Total semantic validation failures",
            COUNTER_DUPLICATES_DETECTED: "Total duplicates detected via dedup key",
            COUNTER_ACCEPTED_LATE: "Total late-but-accepted events",
            COUNTER_TOO_LATE_REJECTED: "Total too-late rejected events",
            COUNTER_DLQ_ROUTED: "Total events routed to dead letter",
            COUNTER_STALE_REDIS_WRITE_SKIPS: "Total stale Redis writes skipped by timestamp guard",
            COUNTER_RAW_ROWS_WRITTEN: "Total raw event rows written to ClickHouse",
            COUNTER_DEAD_LETTER_ROWS_WRITTEN: "Total dead-letter rows written to ClickHouse",
            COUNTER_LATE_ROWS_WRITTEN: "Total late-rejected rows written to ClickHouse",
            COUNTER_FACT_ROWS_WRITTEN: "Total fact session rows written to ClickHouse",
            COUNTER_AGG_STATION_MINUTE_ROWS_WRITTEN: "Total agg_station_minute rows written",
            COUNTER_AGG_OPERATOR_HOUR_ROWS_WRITTEN: "Total agg_operator_hour rows written",
            COUNTER_AGG_CITY_DAY_FAULTS_ROWS_WRITTEN: "Total agg_city_day_faults rows written",
            COUNTER_FINALIZED_NORMAL_STOP: "Total sessions finalized with normal stop",
            COUNTER_FINALIZED_TIMEOUT: "Total sessions finalized with inactivity timeout",
            COUNTER_FINALIZED_FAULT: "Total sessions finalized due to fault",
            COUNTER_SWEEPER_FINALIZATIONS: "Total timeout sweeper-driven session finalizations",
            COUNTER_FINALIZATION_FAILURES: "Total session finalization failures",
            COUNTER_CLICKHOUSE_INSERT_FAILURES: "Total ClickHouse insert failures",
        }
        for name, documentation in counter_defs.items():
            metric = prometheus.counter(name, documentation)
            if metric is not None:
                self._prom_counters[name] = metric

        gauge_defs = {
            GAUGE_KAFKA_CONSUMER_LAG: "Estimated Kafka consumer lag for processor group",
            GAUGE_REDIS_MEMORY_USAGE_BYTES: "Redis memory used in bytes",
            GAUGE_REDIS_KEY_COUNT_TOTAL: "Redis key count total",
            GAUGE_REDIS_KEY_COUNT_STATION: "Redis station key count",
            GAUGE_REDIS_KEY_COUNT_CONNECTOR: "Redis connector key count",
            GAUGE_REDIS_KEY_COUNT_SESSION: "Redis session key count",
            GAUGE_REDIS_OPS_PER_SECOND: "Redis instantaneous operations per second",
            GAUGE_CLICKHOUSE_INSERT_ROWS_PER_SECOND: "Observed ClickHouse insert rows per second",
            GAUGE_CLICKHOUSE_ROWS_RAW_EVENTS: "Rows in ClickHouse raw_events table",
            GAUGE_CLICKHOUSE_ROWS_FACT_SESSIONS: "Rows in ClickHouse fact_sessions table",
            GAUGE_CLICKHOUSE_ROWS_AGG_STATION_MINUTE: "Rows in ClickHouse agg_station_minute table",
            GAUGE_CLICKHOUSE_ROWS_AGG_OPERATOR_HOUR: "Rows in ClickHouse agg_operator_hour table",
            GAUGE_CLICKHOUSE_ROWS_AGG_CITY_DAY_FAULTS: "Rows in ClickHouse agg_city_day_faults table",
        }
        for name, documentation in gauge_defs.items():
            metric = prometheus.gauge(name, documentation)
            if metric is not None:
                self._prom_gauges[name] = metric

        histogram_buckets_ms = (
            0.5,
            1.0,
            2.0,
            5.0,
            10.0,
            20.0,
            50.0,
            100.0,
            250.0,
            500.0,
            1000.0,
            2000.0,
            5000.0,
            10000.0,
            30000.0,
        )
        histogram_defs = {
            HISTOGRAM_INGEST_LAG_MS: ("Observed ingest lag in milliseconds", histogram_buckets_ms),
            HISTOGRAM_PROCESSOR_LATENCY_MS: ("Processor in-loop latency in milliseconds", histogram_buckets_ms),
            HISTOGRAM_END_TO_END_LATENCY_MS: ("End-to-end event latency in milliseconds", histogram_buckets_ms),
            HISTOGRAM_REDIS_WRITE_LATENCY_MS: ("Redis write latency in milliseconds", histogram_buckets_ms),
            HISTOGRAM_REDIS_READ_LATENCY_MS: ("Redis read latency in milliseconds", histogram_buckets_ms),
            HISTOGRAM_CLICKHOUSE_INSERT_LATENCY_MS: ("ClickHouse insert latency in milliseconds", histogram_buckets_ms),
            HISTOGRAM_CLICKHOUSE_QUERY_LATENCY_MS: ("ClickHouse analytical query latency in milliseconds", histogram_buckets_ms),
            HISTOGRAM_PROCESSOR_LOOP_DURATION_MS: ("Processor loop duration in milliseconds", histogram_buckets_ms),
            HISTOGRAM_CLICKHOUSE_BATCH_SIZE_ROWS: (
                "ClickHouse insert batch size in rows",
                (1, 10, 50, 100, 250, 500, 1000, 2000, 5000, 10000),
            ),
            HISTOGRAM_BATCH_SIZE: (
                "Kafka poll batch size in messages",
                (0, 1, 10, 50, 100, 250, 500, 1000, 2000, 5000),
            ),
        }
        for name, (documentation, buckets) in histogram_defs.items():
            metric = prometheus.histogram(name, documentation, buckets=buckets)
            if metric is not None:
                self._prom_histograms[name] = metric

            p50 = prometheus.gauge(f"{name}_p50", f"Rolling p50 for {name}")
            p95 = prometheus.gauge(f"{name}_p95", f"Rolling p95 for {name}")
            p99 = prometheus.gauge(f"{name}_p99", f"Rolling p99 for {name}")
            gauge_map: dict[str, Any] = {}
            if p50 is not None:
                gauge_map["p50"] = p50
            if p95 is not None:
                gauge_map["p95"] = p95
            if p99 is not None:
                gauge_map["p99"] = p99
            if gauge_map:
                self._prom_percentile_gauges[name] = gauge_map

    def _refresh_percentile_gauges_locked(self, name: str) -> None:
        gauge_map = self._prom_percentile_gauges.get(name)
        if not gauge_map:
            return

        samples = sorted(self._hist_values[name])
        if not samples:
            return

        p50 = _percentile_from_sorted(samples, 50.0)
        p95 = _percentile_from_sorted(samples, 95.0)
        p99 = _percentile_from_sorted(samples, 99.0)

        if "p50" in gauge_map:
            gauge_map["p50"].set(p50)
        if "p95" in gauge_map:
            gauge_map["p95"].set(p95)
        if "p99" in gauge_map:
            gauge_map["p99"].set(p99)


def _percentile_from_sorted(sorted_values: list[float], percentile: float) -> float:
    if not sorted_values:
        return 0.0
    p = max(0.0, min(100.0, percentile))
    if len(sorted_values) == 1:
        return sorted_values[0]
    rank = (p / 100.0) * (len(sorted_values) - 1)
    low = int(math.floor(rank))
    high = int(math.ceil(rank))
    if low == high:
        return sorted_values[low]
    fraction = rank - low
    return sorted_values[low] + (sorted_values[high] - sorted_values[low]) * fraction
