"""Benchmark profile model and loader."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from src.common.settings import load_yaml_config


@dataclass(frozen=True, slots=True)
class InjectionConfig:
    duplicate_rate: float
    out_of_order_rate: float
    too_late_rate: float
    too_late_threshold_seconds: float


@dataclass(frozen=True, slots=True)
class BenchmarkProfile:
    name: str
    target_eps: int
    duration_seconds: int
    warmup_seconds: int
    cooldown_seconds: int
    measurement_interval_seconds: float
    test_type: str
    launch_services: bool
    simulator_config: str
    processor_config: str
    injection: InjectionConfig
    notes: str


def load_benchmark_profile(path: str | Path) -> BenchmarkProfile:
    profile_path = Path(path)
    raw = load_yaml_config(profile_path)
    return _build_profile(name=profile_path.stem, raw=raw)


def _build_profile(*, name: str, raw: Mapping[str, Any]) -> BenchmarkProfile:
    benchmark_raw = _as_mapping(raw.get("benchmark"))
    injection_raw = _as_mapping(raw.get("injection"))
    runtime_raw = _as_mapping(raw.get("runtime"))
    services_raw = _as_mapping(raw.get("services"))

    profile_name = str(benchmark_raw.get("name", name))
    target_eps = max(1, int(_as_number(benchmark_raw.get("target_eps"), 1000)))

    return BenchmarkProfile(
        name=profile_name,
        target_eps=target_eps,
        duration_seconds=max(10, int(_as_number(runtime_raw.get("duration_seconds"), 180))),
        warmup_seconds=max(0, int(_as_number(runtime_raw.get("warmup_seconds"), 30))),
        cooldown_seconds=max(0, int(_as_number(runtime_raw.get("cooldown_seconds"), 20))),
        measurement_interval_seconds=max(0.5, float(_as_number(runtime_raw.get("measurement_interval_seconds"), 2.0))),
        test_type=str(benchmark_raw.get("test_type", "sustained")).strip().lower() or "sustained",
        launch_services=_as_bool(raw.get("launch_services"), False),
        simulator_config=str(services_raw.get("simulator_config", "config/simulator.benchmark.yaml")),
        processor_config=str(services_raw.get("processor_config", "config/processor.default.yaml")),
        injection=InjectionConfig(
            duplicate_rate=max(0.0, float(_as_number(injection_raw.get("duplicate_rate"), 0.002))),
            out_of_order_rate=max(0.0, float(_as_number(injection_raw.get("out_of_order_rate"), 0.008))),
            too_late_rate=max(0.0, float(_as_number(injection_raw.get("too_late_rate"), 0.004))),
            too_late_threshold_seconds=max(1.0, float(_as_number(injection_raw.get("too_late_threshold_seconds"), 900.0))),
        ),
        notes=str(raw.get("notes", "")).strip(),
    )


def _as_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}


def _as_number(value: Any, default: float) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(value)
