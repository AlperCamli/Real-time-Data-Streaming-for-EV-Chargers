"""Benchmark tier runner for simulator + processor measurement."""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any

from src.benchmarks.clickhouse_queries import run_query_benchmark, summarize_query_latencies
from src.benchmarks.environment import capture_environment_metadata
from src.benchmarks.metrics_snapshot import PrometheusSnapshot, fetch_snapshot
from src.benchmarks.percentiles import percentile
from src.benchmarks.profiles import BenchmarkProfile, load_benchmark_profile
from src.benchmarks.redis_reads import RedisReadBenchmarkResult, benchmark_current_state_reads
from src.benchmarks.summary import persist_benchmark_result
from src.common.settings import load_service_settings, load_yaml_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ChargeSquare benchmark runner")
    parser.add_argument("--profile", type=Path, required=True, help="Path to benchmark profile yaml/json file")
    parser.add_argument("--output-dir", type=Path, default=Path("benchmark_results"), help="Result output base directory")
    parser.add_argument("--simulator-metrics-url", default="http://localhost:9200/metrics")
    parser.add_argument("--processor-metrics-url", default="http://localhost:9100/metrics")
    parser.add_argument("--launch-services", action="store_true", help="Launch simulator and processor subprocesses")
    parser.add_argument("--python-bin", default=sys.executable, help="Python executable for launched subprocesses")
    parser.add_argument("--query-iterations", type=int, default=3)
    parser.add_argument("--include-optional-query", action="store_true")
    parser.add_argument("--skip-redis-read-benchmark", action="store_true")
    parser.add_argument("--skip-query-benchmark", action="store_true")
    parser.add_argument("--mixed-load-redis-reads", action="store_true")
    parser.add_argument("--wait-metrics-seconds", type=int, default=90)
    parser.add_argument("--notes", default="", help="Optional extra notes attached to this run")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    profile = load_benchmark_profile(args.profile)
    launch_services = bool(args.launch_services or profile.launch_services)

    processor_settings = load_service_settings("processor")

    launched: list[subprocess.Popen[Any]] = []
    temp_simulator_config: Path | None = None
    try:
        if launch_services:
            temp_simulator_config = _build_temp_simulator_config(profile)
            launched = _launch_services(
                profile=profile,
                simulator_config=temp_simulator_config,
                processor_config=Path(profile.processor_config),
                python_bin=args.python_bin,
            )

        _wait_for_metrics(args.simulator_metrics_url, timeout_seconds=max(5, args.wait_metrics_seconds))
        _wait_for_metrics(args.processor_metrics_url, timeout_seconds=max(5, args.wait_metrics_seconds))

        simulator_before = _safe_fetch(args.simulator_metrics_url)
        processor_before = _safe_fetch(args.processor_metrics_url)

        if profile.warmup_seconds > 0:
            time.sleep(profile.warmup_seconds)

        measurement_started = time.monotonic()
        measurement_samples: list[dict[str, float]] = []
        while (time.monotonic() - measurement_started) < profile.duration_seconds:
            simulator_snap = _safe_fetch(args.simulator_metrics_url)
            processor_snap = _safe_fetch(args.processor_metrics_url)
            measurement_samples.append(_extract_sample(simulator_snap, processor_snap))
            time.sleep(profile.measurement_interval_seconds)

        simulator_after = _safe_fetch(args.simulator_metrics_url)
        processor_after = _safe_fetch(args.processor_metrics_url)

        if profile.cooldown_seconds > 0:
            time.sleep(profile.cooldown_seconds)

        redis_read_result: RedisReadBenchmarkResult | None = None
        redis_benchmark_error = ""
        if not args.skip_redis_read_benchmark:
            try:
                redis_read_result = benchmark_current_state_reads(
                    host=processor_settings.redis.host,
                    port=processor_settings.redis.port,
                    db=processor_settings.redis.db,
                    duration_seconds=max(5, min(30, profile.duration_seconds // 4)),
                    mixed_load=args.mixed_load_redis_reads,
                )
            except Exception as exc:  # noqa: BLE001
                redis_benchmark_error = str(exc)

        query_results: dict[str, dict[str, object]] = {}
        query_summary: dict[str, float] = {}
        clickhouse_table_counts: dict[str, int] = {}
        query_benchmark_error = ""
        if not args.skip_query_benchmark:
            try:
                query_results = run_query_benchmark(
                    host=processor_settings.clickhouse.host,
                    port=processor_settings.clickhouse.port,
                    user=processor_settings.clickhouse.user,
                    password=processor_settings.clickhouse.password,
                    database=processor_settings.clickhouse.database,
                    iterations=max(1, args.query_iterations),
                    include_optional=args.include_optional_query,
                )
                query_summary = summarize_query_latencies(query_results)
            except Exception as exc:  # noqa: BLE001
                query_benchmark_error = str(exc)
        try:
            clickhouse_table_counts = _fetch_clickhouse_table_counts(
                host=processor_settings.clickhouse.host,
                port=processor_settings.clickhouse.port,
                user=processor_settings.clickhouse.user,
                password=processor_settings.clickhouse.password,
                database=processor_settings.clickhouse.database,
            )
        except Exception as exc:  # noqa: BLE001
            query_benchmark_error = query_benchmark_error or str(exc)

        result = _build_result(
            profile=profile,
            notes=args.notes,
            simulator_before=simulator_before,
            simulator_after=simulator_after,
            processor_before=processor_before,
            processor_after=processor_after,
            measurement_samples=measurement_samples,
            redis_read_result=redis_read_result,
            query_results=query_results,
            query_summary=query_summary,
            clickhouse_table_counts=clickhouse_table_counts,
            redis_benchmark_error=redis_benchmark_error,
            query_benchmark_error=query_benchmark_error,
            environment=capture_environment_metadata(),
        )

        outputs = persist_benchmark_result(result=result, base_dir=args.output_dir)
        print(json.dumps({"result": result, "outputs": outputs}, indent=2, ensure_ascii=True))
    finally:
        _terminate_processes(launched)
        if temp_simulator_config is not None and temp_simulator_config.exists():
            temp_simulator_config.unlink()


def _build_result(
    *,
    profile: BenchmarkProfile,
    notes: str,
    simulator_before: PrometheusSnapshot,
    simulator_after: PrometheusSnapshot,
    processor_before: PrometheusSnapshot,
    processor_after: PrometheusSnapshot,
    measurement_samples: list[dict[str, float]],
    redis_read_result: RedisReadBenchmarkResult | None,
    query_results: dict[str, dict[str, object]],
    query_summary: dict[str, float],
    clickhouse_table_counts: dict[str, int],
    redis_benchmark_error: str,
    query_benchmark_error: str,
    environment: dict[str, object],
) -> dict[str, Any]:
    achieved_generated_eps = _counter_delta(simulator_before, simulator_after, "events_generated_total") / max(
        1,
        profile.duration_seconds,
    )
    achieved_accepted_eps = _counter_delta(processor_before, processor_after, "events_accepted_total") / max(
        1,
        profile.duration_seconds,
    )
    achieved_eps = achieved_accepted_eps if achieved_accepted_eps > 0 else achieved_generated_eps

    p95_ingest_lag = _last_or_percentile(measurement_samples, "ingest_lag_p95_ms", 95.0)
    p99_ingest_lag = _last_or_percentile(measurement_samples, "ingest_lag_p99_ms", 99.0)
    p95_e2e = _last_or_percentile(measurement_samples, "end_to_end_p95_ms", 95.0)
    p99_e2e = _last_or_percentile(measurement_samples, "end_to_end_p99_ms", 99.0)
    p95_redis_write = _last_or_percentile(measurement_samples, "redis_write_p95_ms", 95.0)
    p95_clickhouse_insert = _last_or_percentile(measurement_samples, "clickhouse_insert_p95_ms", 95.0)
    peak_consumer_lag = max((sample.get("consumer_lag", 0.0) for sample in measurement_samples), default=0.0)

    data_quality_counters = {
        "parse_failures_total": _counter_delta(processor_before, processor_after, "parse_failures_total"),
        "schema_validation_failures_total": _counter_delta(
            processor_before,
            processor_after,
            "schema_validation_failures_total",
        ),
        "semantic_validation_failures_total": _counter_delta(
            processor_before,
            processor_after,
            "semantic_validation_failures_total",
        ),
        "duplicates_detected_total": _counter_delta(processor_before, processor_after, "duplicates_detected_total"),
        "too_late_rejected_total": _counter_delta(processor_before, processor_after, "too_late_rejected_total"),
        "dead_letter_routed_total": _counter_delta(processor_before, processor_after, "dead_letter_routed_total"),
    }
    session_finalization = {
        "session_finalized_normal_total": _counter_delta(
            processor_before,
            processor_after,
            "session_finalized_normal_total",
        ),
        "session_finalized_timeout_total": _counter_delta(
            processor_before,
            processor_after,
            "session_finalized_timeout_total",
        ),
        "session_finalized_fault_total": _counter_delta(
            processor_before,
            processor_after,
            "session_finalized_fault_total",
        ),
    }
    aggregate_rows = {
        "agg_station_minute_rows_written_total": _counter_delta(
            processor_before,
            processor_after,
            "clickhouse_agg_station_minute_rows_written_total",
        ),
        "agg_operator_hour_rows_written_total": _counter_delta(
            processor_before,
            processor_after,
            "clickhouse_agg_operator_hour_rows_written_total",
        ),
        "agg_city_day_faults_rows_written_total": _counter_delta(
            processor_before,
            processor_after,
            "clickhouse_agg_city_day_faults_rows_written_total",
        ),
    }

    primary_bottleneck = _derive_primary_bottleneck(
        achieved_eps=achieved_eps,
        target_eps=profile.target_eps,
        p95_clickhouse_insert=p95_clickhouse_insert,
        p95_redis_write=p95_redis_write,
        peak_consumer_lag=peak_consumer_lag,
    )
    outcome = _derive_outcome(achieved_eps=achieved_eps, target_eps=profile.target_eps)

    redis_read_dict = redis_read_result.as_dict() if redis_read_result is not None else {}
    cpu_note = _build_cpu_memory_note(environment)

    return {
        "profile_name": profile.name,
        "target_eps": profile.target_eps,
        "test_type": profile.test_type,
        "duration_seconds": profile.duration_seconds,
        "warmup_seconds": profile.warmup_seconds,
        "cooldown_seconds": profile.cooldown_seconds,
        "achieved_eps": round(achieved_eps, 3),
        "achieved_generated_eps": round(achieved_generated_eps, 3),
        "achieved_accepted_eps": round(achieved_accepted_eps, 3),
        "p95_ingest_lag_ms": round(p95_ingest_lag, 3),
        "p99_ingest_lag_ms": round(p99_ingest_lag, 3),
        "p95_end_to_end_latency_ms": round(p95_e2e, 3),
        "p99_end_to_end_latency_ms": round(p99_e2e, 3),
        "p95_redis_read_latency_ms": round(float(redis_read_dict.get("latency_p95_ms", 0.0)), 3),
        "p95_redis_write_latency_ms": round(p95_redis_write, 3),
        "p95_clickhouse_insert_batch_latency_ms": round(p95_clickhouse_insert, 3),
        "analytical_query_latency_summary": query_summary,
        "query_results": query_results,
        "peak_consumer_lag": int(round(peak_consumer_lag)),
        "cpu_memory_note": cpu_note,
        "outcome": outcome,
        "primary_bottleneck": primary_bottleneck,
        "recommended_next_optimization": _recommend_optimization(primary_bottleneck),
        "environment_metadata": environment,
        "profile_runtime": asdict(profile),
        "data_quality_counters": data_quality_counters,
        "session_finalization_counts": session_finalization,
        "aggregate_row_write_counts": aggregate_rows,
        "clickhouse_insert_failures_total": _counter_delta(
            processor_before,
            processor_after,
            "clickhouse_insert_failures_total",
        ),
        "clickhouse_table_row_counts": clickhouse_table_counts,
        "redis_read_benchmark": redis_read_dict,
        "redis_benchmark_error": redis_benchmark_error,
        "query_benchmark_error": query_benchmark_error,
        "notes": notes or profile.notes,
    }


def _extract_sample(simulator_snapshot: PrometheusSnapshot, processor_snapshot: PrometheusSnapshot) -> dict[str, float]:
    return {
        "actual_generated_eps": simulator_snapshot.get("actual_generated_eps"),
        "events_generated_total": simulator_snapshot.get("events_generated_total"),
        "events_accepted_total": processor_snapshot.get("events_accepted_total"),
        "consumer_lag": processor_snapshot.get("kafka_consumer_lag"),
        "ingest_lag_p95_ms": processor_snapshot.get("ingest_lag_ms_p95"),
        "ingest_lag_p99_ms": processor_snapshot.get("ingest_lag_ms_p99"),
        "end_to_end_p95_ms": processor_snapshot.get("end_to_end_latency_ms_p95"),
        "end_to_end_p99_ms": processor_snapshot.get("end_to_end_latency_ms_p99"),
        "redis_write_p95_ms": processor_snapshot.get("redis_write_latency_ms_p95"),
        "clickhouse_insert_p95_ms": processor_snapshot.get("clickhouse_insert_latency_ms_p95"),
    }


def _safe_fetch(endpoint: str) -> PrometheusSnapshot:
    try:
        return fetch_snapshot(endpoint)
    except Exception:  # noqa: BLE001
        return PrometheusSnapshot()


def _counter_delta(before: PrometheusSnapshot, after: PrometheusSnapshot, metric: str) -> float:
    old_value = before.get(metric, default=0.0)
    new_value = after.get(metric, default=0.0)
    if new_value >= old_value:
        return new_value - old_value
    return max(0.0, new_value)


def _last_or_percentile(samples: list[dict[str, float]], key: str, p: float) -> float:
    values = [row.get(key, 0.0) for row in samples if key in row]
    if not values:
        return 0.0
    if values[-1] > 0.0:
        return values[-1]
    return percentile(values, p)


def _derive_outcome(*, achieved_eps: float, target_eps: int) -> str:
    if achieved_eps >= target_eps * 0.9:
        return "pass"
    if achieved_eps >= target_eps * 0.7:
        return "partial"
    return "fail"


def _derive_primary_bottleneck(
    *,
    achieved_eps: float,
    target_eps: int,
    p95_clickhouse_insert: float,
    p95_redis_write: float,
    peak_consumer_lag: float,
) -> str:
    if peak_consumer_lag > target_eps * 2:
        return "kafka_consumer_lag_growth"
    if p95_clickhouse_insert > 250.0:
        return "clickhouse_insert_latency"
    if p95_redis_write > 50.0:
        return "redis_write_latency"
    if achieved_eps < target_eps * 0.8:
        return "processor_throughput_limit"
    return "no_major_bottleneck_observed"


def _recommend_optimization(primary_bottleneck: str) -> str:
    mapping = {
        "kafka_consumer_lag_growth": "Increase processor parallelism or tune poll/flush batch sizing.",
        "clickhouse_insert_latency": "Increase ClickHouse batch size/flush tuning and verify storage throughput.",
        "redis_write_latency": "Reduce Redis write amplification and validate Redis host resources.",
        "processor_throughput_limit": "Profile processor hot path and reduce per-event overhead.",
        "no_major_bottleneck_observed": "Run longer sustained window to validate stability.",
    }
    return mapping.get(primary_bottleneck, "Collect longer traces and profile the slowest stage.")


def _build_cpu_memory_note(environment: dict[str, object]) -> str:
    cpu_count = environment.get("cpu_count", "unknown")
    memory_bytes = environment.get("memory_total_bytes")
    if isinstance(memory_bytes, int) and memory_bytes > 0:
        gib = round(memory_bytes / (1024**3), 2)
        return f"cpu={cpu_count}, memory_gib={gib}"
    return f"cpu={cpu_count}, memory_gib=unknown"


def _wait_for_metrics(endpoint: str, *, timeout_seconds: int) -> None:
    deadline = time.monotonic() + max(1, timeout_seconds)
    last_error = "unknown error"
    while time.monotonic() < deadline:
        try:
            fetch_snapshot(endpoint)
            return
        except Exception as exc:  # noqa: BLE001
            last_error = str(exc)
            time.sleep(1.0)
    raise RuntimeError(f"Metrics endpoint did not become ready: {endpoint}. Last error: {last_error}")


def _launch_services(
    *,
    profile: BenchmarkProfile,
    simulator_config: Path,
    processor_config: Path,
    python_bin: str,
) -> list[subprocess.Popen[Any]]:
    env = os.environ.copy()
    run_seconds = profile.warmup_seconds + profile.duration_seconds + profile.cooldown_seconds + 30

    processor_proc = subprocess.Popen(
        [python_bin, "-m", "src.processor.main", "--config", str(processor_config)],
        env=env,
    )
    simulator_proc = subprocess.Popen(
        [
            python_bin,
            "-m",
            "src.simulator.main",
            "--config",
            str(simulator_config),
            "--max-runtime-seconds",
            str(run_seconds),
        ],
        env=env,
    )
    return [processor_proc, simulator_proc]


def _terminate_processes(processes: list[subprocess.Popen[Any]]) -> None:
    for process in processes:
        if process.poll() is not None:
            continue
        process.send_signal(signal.SIGTERM)
    deadline = time.monotonic() + 10
    for process in processes:
        if process.poll() is not None:
            continue
        try:
            process.wait(timeout=max(0.1, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            process.kill()


def _build_temp_simulator_config(profile: BenchmarkProfile) -> Path:
    base = load_yaml_config(profile.simulator_config)
    network = _ensure_mapping(base, "network")
    data_quality = _ensure_mapping(base, "data_quality")
    benchmark = _ensure_mapping(base, "benchmark")

    base["mode"] = "benchmark"
    network["target_event_rate"] = profile.target_eps
    data_quality["duplicate_injection_rate"] = profile.injection.duplicate_rate
    data_quality["out_of_order_injection_rate"] = profile.injection.out_of_order_rate
    data_quality["too_late_injection_rate"] = profile.injection.too_late_rate
    data_quality["too_late_threshold_seconds"] = profile.injection.too_late_threshold_seconds
    benchmark["enabled"] = True
    benchmark["sustained_mode"] = profile.test_type == "sustained"
    benchmark["burst_mode"] = profile.test_type in {"burst", "stress"}
    benchmark["burst_multiplier"] = 1.5 if benchmark.get("burst_multiplier") is None else benchmark["burst_multiplier"]

    with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".json", delete=False) as handle:
        json.dump(base, handle, indent=2, ensure_ascii=True)
        return Path(handle.name)


def _fetch_clickhouse_table_counts(*, host: str, port: int, user: str, password: str, database: str) -> dict[str, int]:
    try:
        from clickhouse_driver import Client  # type: ignore
    except ModuleNotFoundError:
        return {}

    tables = (
        "raw_events",
        "fact_sessions",
        "agg_station_minute",
        "agg_operator_hour",
        "agg_city_day_faults",
    )
    client = Client(host=host, port=port, user=user, password=password, database=database)
    counts: dict[str, int] = {}
    for table in tables:
        try:
            response = client.execute(f"SELECT count() FROM {table}")
            if response and isinstance(response, list):
                counts[table] = int(response[0][0])
        except Exception:  # noqa: BLE001
            continue
    return counts


def _ensure_mapping(payload: dict[str, Any], key: str) -> dict[str, Any]:
    value = payload.get(key)
    if isinstance(value, dict):
        return value
    payload[key] = {}
    return payload[key]


if __name__ == "__main__":
    main()
