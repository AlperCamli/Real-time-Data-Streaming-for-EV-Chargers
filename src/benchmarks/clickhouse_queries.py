"""ClickHouse analytical query benchmark runner."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable

from src.benchmarks.percentiles import percentile_pack


@dataclass(frozen=True, slots=True)
class QueryFamily:
    name: str
    sql: str
    description: str


QUERY_FAMILIES: tuple[QueryFamily, ...] = (
    QueryFamily(
        name="hourly_energy_last_7_days",
        description="Hourly energy by operator for the last 7 days",
        sql="""
SELECT
  bucket_hour,
  operator_id,
  sum(energy_kwh_sum) AS energy_kwh
FROM agg_operator_hour
WHERE bucket_hour >= now() - INTERVAL 7 DAY
GROUP BY bucket_hour, operator_id
ORDER BY bucket_hour DESC, operator_id
LIMIT 5000
""".strip(),
    ),
    QueryFamily(
        name="uptime_downtime_by_station_operator",
        description="Approximate uptime/downtime by station and operator",
        sql="""
SELECT
  operator_id,
  station_id,
  count() AS minute_rows,
  sum(if(active_connector_estimate > 0, 1, 0)) AS charging_minutes,
  sum(if(active_connector_estimate = 0, 1, 0)) AS idle_minutes
FROM agg_station_minute
WHERE bucket_minute >= now() - INTERVAL 7 DAY
GROUP BY operator_id, station_id
ORDER BY charging_minutes DESC
LIMIT 2000
""".strip(),
    ),
    QueryFamily(
        name="avg_duration_energy_by_brand",
        description="Average charging duration and energy by vehicle brand",
        sql="""
SELECT
  coalesce(vehicle_brand, 'unknown') AS brand,
  avg(duration_seconds) AS avg_duration_seconds,
  avg(energy_kwh_total) AS avg_energy_kwh,
  count() AS session_count
FROM fact_sessions
WHERE session_start_time >= now() - INTERVAL 30 DAY
GROUP BY brand
ORDER BY session_count DESC
LIMIT 100
""".strip(),
    ),
    QueryFamily(
        name="revenue_by_tariff_time",
        description="Revenue grouped by tariff and start hour",
        sql="""
SELECT
  toStartOfHour(session_start_time) AS hour_bucket,
  tariff_id,
  sum(revenue_eur_total) AS revenue_eur,
  sum(energy_kwh_total) AS energy_kwh,
  count() AS sessions
FROM fact_sessions
WHERE session_start_time >= now() - INTERVAL 14 DAY
GROUP BY hour_bucket, tariff_id
ORDER BY hour_bucket DESC, tariff_id
LIMIT 5000
""".strip(),
    ),
    QueryFamily(
        name="fault_density_by_city",
        description="Fault density by city",
        sql="""
SELECT
  bucket_day,
  operator_id,
  city,
  country_code,
  fault_count,
  distinct_station_count
FROM agg_city_day_faults
WHERE bucket_day >= today() - 30
ORDER BY bucket_day DESC, fault_count DESC
LIMIT 5000
""".strip(),
    ),
)

OPTIONAL_QUERY_FAMILY = QueryFamily(
    name="anomaly_spike_scan_optional",
    description="Optional anomaly-style scan for station-minute event spikes",
    sql="""
SELECT
  station_id,
  max(events_total) AS max_events_minute,
  avg(events_total) AS avg_events_minute
FROM agg_station_minute
WHERE bucket_minute >= now() - INTERVAL 3 DAY
GROUP BY station_id
HAVING max_events_minute > avg_events_minute * 4
ORDER BY max_events_minute DESC
LIMIT 1000
""".strip(),
)


def run_query_benchmark(
    *,
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
    iterations: int = 3,
    include_optional: bool = False,
    observer: Callable[[str, float], None] | None = None,
) -> dict[str, dict[str, object]]:
    client = _build_clickhouse_client(host=host, port=port, user=user, password=password, database=database)
    families = list(QUERY_FAMILIES)
    if include_optional:
        families.append(OPTIONAL_QUERY_FAMILY)

    results: dict[str, dict[str, object]] = {}
    for family in families:
        timings_ms: list[float] = []
        row_counts: list[int] = []
        failures = 0
        for _ in range(max(1, iterations)):
            started = time.perf_counter()
            try:
                rows = client.execute(family.sql)
                elapsed_ms = max(0.0, (time.perf_counter() - started) * 1000.0)
                timings_ms.append(elapsed_ms)
                row_counts.append(len(rows) if isinstance(rows, list) else 0)
                if observer is not None:
                    observer(family.name, elapsed_ms)
            except Exception:  # noqa: BLE001
                failures += 1

        pack = percentile_pack(timings_ms)
        results[family.name] = {
            "description": family.description,
            "executions": max(1, iterations),
            "failures": failures,
            "row_count_max": max(row_counts) if row_counts else 0,
            "latency_p50_ms": round(pack["p50"], 3),
            "latency_p95_ms": round(pack["p95"], 3),
            "latency_p99_ms": round(pack["p99"], 3),
            "latency_avg_ms": round((sum(timings_ms) / len(timings_ms)) if timings_ms else 0.0, 3),
            "latency_max_ms": round(max(timings_ms) if timings_ms else 0.0, 3),
        }

    return results


def summarize_query_latencies(query_results: dict[str, dict[str, object]]) -> dict[str, float]:
    p95_values: list[float] = []
    p99_values: list[float] = []
    for values in query_results.values():
        p95_values.append(float(values.get("latency_p95_ms", 0.0)))
        p99_values.append(float(values.get("latency_p99_ms", 0.0)))
    pack95 = percentile_pack(p95_values)
    pack99 = percentile_pack(p99_values)
    return {
        "families": float(len(query_results)),
        "p95_of_p95_ms": round(pack95["p95"], 3),
        "p99_of_p99_ms": round(pack99["p99"], 3),
        "max_p99_ms": round(max(p99_values) if p99_values else 0.0, 3),
    }


def _build_clickhouse_client(*, host: str, port: int, user: str, password: str, database: str):
    try:
        from clickhouse_driver import Client  # type: ignore
    except ModuleNotFoundError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError("clickhouse-driver is required for query benchmarks: pip install clickhouse-driver") from exc

    client = Client(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
    )
    client.execute("SELECT 1")
    return client
