"""Processor metrics hooks for benchmark-friendly instrumentation."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from threading import Lock
from typing import DefaultDict


COUNTER_EVENTS_CONSUMED = "events_consumed_total"
COUNTER_EVENTS_ACCEPTED = "events_accepted_total"
COUNTER_PARSE_FAILURES = "parse_failures_total"
COUNTER_SCHEMA_VALIDATION_FAILURES = "schema_validation_failures_total"
COUNTER_SEMANTIC_VALIDATION_FAILURES = "semantic_validation_failures_total"
COUNTER_DUPLICATES_DETECTED = "duplicates_detected_total"
COUNTER_ACCEPTED_LATE = "accepted_late_events_total"
COUNTER_TOO_LATE_REJECTED = "too_late_rejected_total"
COUNTER_DLQ_ROUTED = "dlq_routed_total"
COUNTER_STALE_REDIS_WRITE_SKIPS = "stale_redis_write_skips_total"
COUNTER_RAW_ROWS_WRITTEN = "clickhouse_raw_rows_written_total"
COUNTER_DEAD_LETTER_ROWS_WRITTEN = "clickhouse_dead_letter_rows_written_total"
COUNTER_LATE_ROWS_WRITTEN = "clickhouse_late_rows_written_total"
COUNTER_FACT_ROWS_WRITTEN = "clickhouse_fact_session_rows_written_total"
COUNTER_AGG_STATION_MINUTE_ROWS_WRITTEN = "clickhouse_agg_station_minute_rows_written_total"
COUNTER_AGG_OPERATOR_HOUR_ROWS_WRITTEN = "clickhouse_agg_operator_hour_rows_written_total"
COUNTER_AGG_CITY_DAY_FAULTS_ROWS_WRITTEN = "clickhouse_agg_city_day_faults_rows_written_total"
COUNTER_FINALIZED_NORMAL_STOP = "session_finalized_normal_stop_total"
COUNTER_FINALIZED_TIMEOUT = "session_finalized_timeout_total"
COUNTER_FINALIZED_FAULT = "session_finalized_fault_total"
COUNTER_SWEEPER_FINALIZATIONS = "session_sweeper_finalizations_total"
COUNTER_FINALIZATION_FAILURES = "session_finalization_failures_total"

HISTOGRAM_PROCESSOR_LATENCY_SECONDS = "processor_latency_seconds"
HISTOGRAM_LOOP_DURATION_SECONDS = "loop_duration_seconds"
HISTOGRAM_BATCH_SIZE = "batch_size"
HISTOGRAM_REDIS_WRITE_LATENCY_SECONDS = "redis_write_latency_seconds"
HISTOGRAM_CLICKHOUSE_BATCH_SIZE = "clickhouse_batch_size"
HISTOGRAM_CLICKHOUSE_BATCH_LATENCY_SECONDS = "clickhouse_batch_latency_seconds"
HISTOGRAM_CLICKHOUSE_INSERT_ROWS_PER_SECOND = "clickhouse_insert_rows_per_second"


@dataclass(slots=True)
class HistogramSnapshot:
    count: int
    sum: float
    max: float


@dataclass(slots=True)
class ProcessorMetricSnapshot:
    counters: dict[str, int]
    histograms: dict[str, HistogramSnapshot]


class ProcessorMetrics:
    def __init__(self) -> None:
        self._lock = Lock()
        self._counters: DefaultDict[str, int] = defaultdict(int)
        self._hist_counts: DefaultDict[str, int] = defaultdict(int)
        self._hist_sums: DefaultDict[str, float] = defaultdict(float)
        self._hist_max: DefaultDict[str, float] = defaultdict(float)

    def inc(self, name: str, amount: int = 1) -> None:
        with self._lock:
            self._counters[name] += amount

    def observe(self, name: str, value: float) -> None:
        bounded = max(0.0, float(value))
        with self._lock:
            self._hist_counts[name] += 1
            self._hist_sums[name] += bounded
            self._hist_max[name] = max(self._hist_max[name], bounded)

    def snapshot(self) -> ProcessorMetricSnapshot:
        with self._lock:
            histograms = {
                name: HistogramSnapshot(
                    count=self._hist_counts[name],
                    sum=self._hist_sums[name],
                    max=self._hist_max[name],
                )
                for name in set(self._hist_counts) | set(self._hist_sums) | set(self._hist_max)
            }
            return ProcessorMetricSnapshot(counters=dict(self._counters), histograms=histograms)
