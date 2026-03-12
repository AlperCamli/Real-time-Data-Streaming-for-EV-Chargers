"""Redis current-state read benchmark routine."""

from __future__ import annotations

import random
import time
from dataclasses import dataclass

from src.benchmarks.percentiles import percentile_pack


@dataclass(slots=True)
class RedisReadBenchmarkResult:
    sampled_key_count: int
    samples_collected: int
    read_ops_per_second: float
    latency_p50_ms: float
    latency_p95_ms: float
    latency_p99_ms: float
    error_count: int
    mixed_load: bool
    redis_memory_bytes: int | None
    redis_ops_per_second: float | None
    redis_key_count_total: int | None
    key_count_station: int | None
    key_count_connector: int | None
    key_count_session: int | None

    def as_dict(self) -> dict[str, object]:
        return {
            "sampled_key_count": self.sampled_key_count,
            "samples_collected": self.samples_collected,
            "read_ops_per_second": round(self.read_ops_per_second, 3),
            "latency_p50_ms": round(self.latency_p50_ms, 3),
            "latency_p95_ms": round(self.latency_p95_ms, 3),
            "latency_p99_ms": round(self.latency_p99_ms, 3),
            "error_count": self.error_count,
            "mixed_load": self.mixed_load,
            "redis_memory_bytes": self.redis_memory_bytes,
            "redis_ops_per_second": self.redis_ops_per_second,
            "redis_key_count_total": self.redis_key_count_total,
            "key_count_station": self.key_count_station,
            "key_count_connector": self.key_count_connector,
            "key_count_session": self.key_count_session,
        }


def benchmark_current_state_reads(
    *,
    host: str,
    port: int,
    db: int,
    duration_seconds: int = 20,
    key_pattern: str = "station:*:state",
    sample_limit: int = 500,
    mixed_load: bool = False,
    rng_seed: int = 17,
) -> RedisReadBenchmarkResult:
    client = _build_redis_client(host=host, port=port, db=db)
    keys = _collect_keys(client, pattern=key_pattern, limit=sample_limit)
    if not keys:
        info = _safe_info(client)
        return RedisReadBenchmarkResult(
            sampled_key_count=0,
            samples_collected=0,
            read_ops_per_second=0.0,
            latency_p50_ms=0.0,
            latency_p95_ms=0.0,
            latency_p99_ms=0.0,
            error_count=0,
            mixed_load=mixed_load,
            redis_memory_bytes=_as_int(info.get("used_memory")),
            redis_ops_per_second=_as_float(info.get("instantaneous_ops_per_sec")),
            redis_key_count_total=_extract_db_key_count(info),
            key_count_station=0,
            key_count_connector=0,
            key_count_session=0,
        )

    random_gen = random.Random(rng_seed)
    latencies_ms: list[float] = []
    errors = 0
    started = time.monotonic()
    deadline = started + max(1, duration_seconds)
    reads = 0

    while time.monotonic() < deadline:
        key = random_gen.choice(keys)
        call_started = time.perf_counter()
        try:
            client.hgetall(key)
        except Exception:  # noqa: BLE001
            errors += 1
            continue
        latency_ms = max(0.0, (time.perf_counter() - call_started) * 1000.0)
        latencies_ms.append(latency_ms)
        reads += 1

    elapsed_seconds = max(1e-6, time.monotonic() - started)
    pack = percentile_pack(latencies_ms)
    info = _safe_info(client)

    key_counts = _count_key_families(client)
    return RedisReadBenchmarkResult(
        sampled_key_count=len(keys),
        samples_collected=len(latencies_ms),
        read_ops_per_second=reads / elapsed_seconds,
        latency_p50_ms=pack["p50"],
        latency_p95_ms=pack["p95"],
        latency_p99_ms=pack["p99"],
        error_count=errors,
        mixed_load=mixed_load,
        redis_memory_bytes=_as_int(info.get("used_memory")),
        redis_ops_per_second=_as_float(info.get("instantaneous_ops_per_sec")),
        redis_key_count_total=_extract_db_key_count(info),
        key_count_station=key_counts.get("station"),
        key_count_connector=key_counts.get("connector"),
        key_count_session=key_counts.get("session"),
    )


def _build_redis_client(*, host: str, port: int, db: int):
    try:
        import redis  # type: ignore
    except ModuleNotFoundError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError("redis package is required for Redis read benchmark: pip install redis") from exc

    client = redis.Redis(host=host, port=port, db=db, decode_responses=True)
    client.ping()
    return client


def _collect_keys(client, *, pattern: str, limit: int) -> list[str]:
    keys: list[str] = []
    try:
        for key in client.scan_iter(match=pattern, count=1000):
            keys.append(key)
            if len(keys) >= max(1, limit):
                break
    except Exception:  # noqa: BLE001
        return []
    return keys


def _count_key_families(client) -> dict[str, int]:
    counts = {"station": 0, "connector": 0, "session": 0}
    patterns = {
        "station": "station:*:state",
        "connector": "station:*:connector:*:state",
        "session": "session:*:state",
    }
    for family, pattern in patterns.items():
        seen = 0
        try:
            for _ in client.scan_iter(match=pattern, count=5000):
                seen += 1
        except Exception:  # noqa: BLE001
            continue
        counts[family] = seen
    return counts


def _safe_info(client) -> dict[str, object]:
    try:
        info = client.info()
        if isinstance(info, dict):
            return info
    except Exception:  # noqa: BLE001
        return {}
    return {}


def _extract_db_key_count(info: dict[str, object]) -> int | None:
    keyspace = info.get("keyspace")
    if isinstance(keyspace, dict):
        for db_info in keyspace.values():
            if isinstance(db_info, dict) and "keys" in db_info:
                return _as_int(db_info.get("keys"))
    db0 = info.get("db0")
    if isinstance(db0, dict):
        return _as_int(db0.get("keys"))
    return None


def _as_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
