"""Simulator configuration parsing and defaults."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any, Mapping


DEFAULT_HOURLY_DEMAND_WEIGHTS: tuple[float, ...] = (
    0.35,
    0.32,
    0.30,
    0.30,
    0.34,
    0.48,
    0.72,
    0.95,
    1.05,
    0.92,
    0.84,
    0.82,
    0.86,
    0.90,
    0.96,
    1.06,
    1.20,
    1.36,
    1.50,
    1.42,
    1.20,
    0.92,
    0.68,
    0.48,
)


@dataclass(frozen=True, slots=True)
class ProducerConfig:
    producer_id: str
    schema_version: str
    seed: int
    dry_run: bool
    linger_ms: int
    batch_size: int
    compression_type: str | None
    acks: str
    request_timeout_ms: int


@dataclass(frozen=True, slots=True)
class PeakWindowConfig:
    start_hour: int
    end_hour: int
    multiplier: float


@dataclass(frozen=True, slots=True)
class DemandConfig:
    base_session_start_rate_per_idle_connector_minute: float
    hourly_weights: tuple[float, ...]
    morning_peak: PeakWindowConfig
    evening_peak: PeakWindowConfig


@dataclass(frozen=True, slots=True)
class SessionConfig:
    min_duration_minutes: float
    mode_duration_minutes: float
    max_duration_minutes: float
    meter_update_interval_seconds_min: float
    meter_update_interval_seconds_max: float
    target_soc_percent_min: float
    target_soc_percent_max: float


@dataclass(frozen=True, slots=True)
class FaultConfig:
    fault_rate_per_connector_hour: float
    recovery_seconds_min: float
    recovery_seconds_max: float
    severity_weights: Mapping[str, float]


@dataclass(frozen=True, slots=True)
class DataQualityConfig:
    duplicate_injection_rate: float
    out_of_order_injection_rate: float
    too_late_injection_rate: float
    too_late_threshold_seconds: float
    out_of_order_delay_seconds_min: float
    out_of_order_delay_seconds_max: float


@dataclass(frozen=True, slots=True)
class BenchmarkConfig:
    enabled: bool
    sustained_mode: bool
    burst_mode: bool
    burst_multiplier: float
    burst_cycle_seconds: float


@dataclass(frozen=True, slots=True)
class OperatorDistributionItem:
    operator_id: str
    weight: float


@dataclass(frozen=True, slots=True)
class GeographyDistributionItem:
    city: str
    country: str
    latitude: float
    longitude: float
    weight: float


@dataclass(frozen=True, slots=True)
class VehicleBrandDistributionItem:
    brand: str
    weight: float


@dataclass(frozen=True, slots=True)
class NetworkConfig:
    station_count: int
    connector_count_distribution: Mapping[int, float]
    operator_distribution: tuple[OperatorDistributionItem, ...]
    geography_distribution: tuple[GeographyDistributionItem, ...]
    vehicle_brand_distribution: tuple[VehicleBrandDistributionItem, ...]
    heartbeat_interval_seconds: float
    target_event_rate: float
    tick_interval_seconds: float


@dataclass(frozen=True, slots=True)
class SimulatorConfig:
    mode: str
    producer: ProducerConfig
    network: NetworkConfig
    demand: DemandConfig
    session: SessionConfig
    faults: FaultConfig
    data_quality: DataQualityConfig
    benchmark: BenchmarkConfig

    def target_eps(self, monotonic_time_seconds: float) -> float:
        target = self.network.target_event_rate
        if not self.benchmark.enabled or not self.benchmark.burst_mode:
            return target

        cycle = max(self.benchmark.burst_cycle_seconds, 1.0)
        phase = monotonic_time_seconds % cycle
        if phase < cycle * 0.30:
            return target * max(self.benchmark.burst_multiplier, 1.0)
        return target


def build_simulator_config(raw: Mapping[str, Any]) -> SimulatorConfig:
    mode = str(raw.get("mode", "default"))

    producer_raw = _as_mapping(raw.get("producer"))
    simulation_raw = _as_mapping(raw.get("network") or raw.get("simulation"))
    demand_raw = _as_mapping(raw.get("demand"))
    session_raw = _as_mapping(raw.get("session"))
    faults_raw = _as_mapping(raw.get("faults"))
    quality_raw = _as_mapping(raw.get("data_quality"))
    benchmark_raw = _as_mapping(raw.get("benchmark"))
    legacy_peak_raw = _as_mapping(raw.get("peak_hour"))

    station_count = max(1, _as_int(simulation_raw.get("station_count"), 120))

    connector_distribution = _parse_connector_distribution(simulation_raw)

    operator_distribution = _parse_operator_distribution(simulation_raw)
    geography_distribution = _parse_geography_distribution(simulation_raw)
    vehicle_brand_distribution = _parse_vehicle_brand_distribution(simulation_raw)

    heartbeat_interval_seconds = _as_float(simulation_raw.get("heartbeat_interval_seconds"), 45.0)
    target_event_rate = _as_float(
        simulation_raw.get("target_event_rate", simulation_raw.get("event_rate_per_second")),
        80.0,
    )
    tick_interval_seconds = max(0.1, _as_float(simulation_raw.get("tick_interval_seconds"), 1.0))

    session_start_prob = simulation_raw.get("session_start_probability")
    base_start_rate_per_minute = _as_float(
        demand_raw.get("base_session_start_rate_per_idle_connector_minute", session_start_prob),
        0.08,
    )

    hourly_weights = _parse_hourly_weights(demand_raw.get("hourly_weights"))

    morning_peak_raw = _as_mapping(demand_raw.get("morning_peak"))
    evening_peak_raw = _as_mapping(demand_raw.get("evening_peak"))
    if not evening_peak_raw and legacy_peak_raw:
        evening_peak_raw = legacy_peak_raw

    morning_peak = PeakWindowConfig(
        start_hour=_bounded_hour(_as_int(morning_peak_raw.get("start_hour"), 7)),
        end_hour=_bounded_hour(_as_int(morning_peak_raw.get("end_hour"), 10)),
        multiplier=max(0.1, _as_float(morning_peak_raw.get("multiplier"), 1.7)),
    )
    evening_peak = PeakWindowConfig(
        start_hour=_bounded_hour(_as_int(evening_peak_raw.get("start_hour"), 17)),
        end_hour=_bounded_hour(_as_int(evening_peak_raw.get("end_hour"), 21)),
        multiplier=max(
            0.1,
            _as_float(
                evening_peak_raw.get("multiplier", evening_peak_raw.get("demand_multiplier")),
                2.0,
            ),
        ),
    )

    duration_min_raw = _as_float(session_raw.get("min_duration_minutes"), 18.0)
    duration_mode_raw = _as_float(session_raw.get("mode_duration_minutes"), 42.0)
    duration_max_raw = _as_float(session_raw.get("max_duration_minutes"), 180.0)

    duration_min = max(1.0, min(duration_min_raw, duration_max_raw))
    duration_max = max(duration_min, duration_max_raw)
    duration_mode = min(duration_max, max(duration_min, duration_mode_raw))

    session_cfg = SessionConfig(
        min_duration_minutes=duration_min,
        mode_duration_minutes=duration_mode,
        max_duration_minutes=duration_max,
        meter_update_interval_seconds_min=max(
            3.0,
            _as_float(session_raw.get("meter_update_interval_seconds_min"), 8.0),
        ),
        meter_update_interval_seconds_max=max(
            _as_float(session_raw.get("meter_update_interval_seconds_min"), 8.0),
            _as_float(session_raw.get("meter_update_interval_seconds_max"), 22.0),
        ),
        target_soc_percent_min=min(
            95.0,
            max(60.0, _as_float(session_raw.get("target_soc_percent_min"), 78.0)),
        ),
        target_soc_percent_max=min(
            99.0,
            max(65.0, _as_float(session_raw.get("target_soc_percent_max"), 94.0)),
        ),
    )

    fault_rate = _as_float(
        faults_raw.get("fault_rate_per_connector_hour", faults_raw.get("fault_rate")),
        0.003,
    )
    severity_weights = _normalize_weight_mapping(
        _as_mapping(faults_raw.get("severity_weights")),
        default={"low": 0.7, "medium": 0.2, "high": 0.1},
    )

    benchmark = BenchmarkConfig(
        enabled=_as_bool(benchmark_raw.get("enabled"), mode == "benchmark"),
        sustained_mode=_as_bool(benchmark_raw.get("sustained_mode"), mode == "benchmark"),
        burst_mode=_as_bool(benchmark_raw.get("burst_mode"), False),
        burst_multiplier=max(1.0, _as_float(benchmark_raw.get("burst_multiplier"), 1.5)),
        burst_cycle_seconds=max(10.0, _as_float(benchmark_raw.get("burst_cycle_seconds"), 90.0)),
    )

    return SimulatorConfig(
        mode=mode,
        producer=ProducerConfig(
            producer_id=str(producer_raw.get("producer_id", f"simulator-{mode}")),
            schema_version=str(producer_raw.get("schema_version", "1.0")),
            seed=_as_int(producer_raw.get("seed"), 42),
            dry_run=_as_bool(producer_raw.get("dry_run"), False),
            linger_ms=max(0, _as_int(producer_raw.get("linger_ms"), 50)),
            batch_size=max(1024, _as_int(producer_raw.get("batch_size"), 32768)),
            compression_type=_as_optional_str(producer_raw.get("compression_type"), "gzip"),
            acks=str(producer_raw.get("acks", "1")),
            request_timeout_ms=max(1000, _as_int(producer_raw.get("request_timeout_ms"), 10000)),
        ),
        network=NetworkConfig(
            station_count=station_count,
            connector_count_distribution=connector_distribution,
            operator_distribution=operator_distribution,
            geography_distribution=geography_distribution,
            vehicle_brand_distribution=vehicle_brand_distribution,
            heartbeat_interval_seconds=max(5.0, heartbeat_interval_seconds),
            target_event_rate=max(1.0, target_event_rate),
            tick_interval_seconds=tick_interval_seconds,
        ),
        demand=DemandConfig(
            base_session_start_rate_per_idle_connector_minute=max(0.001, base_start_rate_per_minute),
            hourly_weights=hourly_weights,
            morning_peak=morning_peak,
            evening_peak=evening_peak,
        ),
        session=session_cfg,
        faults=FaultConfig(
            fault_rate_per_connector_hour=max(0.0, fault_rate),
            recovery_seconds_min=max(30.0, _as_float(faults_raw.get("recovery_seconds_min"), 300.0)),
            recovery_seconds_max=max(60.0, _as_float(faults_raw.get("recovery_seconds_max"), 2400.0)),
            severity_weights=severity_weights,
        ),
        data_quality=DataQualityConfig(
            duplicate_injection_rate=max(
                0.0,
                _as_float(
                    quality_raw.get("duplicate_injection_rate"),
                    0.002,
                ),
            ),
            out_of_order_injection_rate=max(
                0.0,
                _as_float(
                    quality_raw.get("out_of_order_injection_rate"),
                    0.008,
                ),
            ),
            too_late_injection_rate=max(
                0.0,
                _as_float(
                    quality_raw.get("too_late_injection_rate", quality_raw.get("late_event_injection_rate")),
                    0.004,
                ),
            ),
            too_late_threshold_seconds=max(
                1.0,
                _as_float(quality_raw.get("too_late_threshold_seconds"), 900.0),
            ),
            out_of_order_delay_seconds_min=max(
                0.05,
                _as_float(quality_raw.get("out_of_order_delay_seconds_min"), 0.8),
            ),
            out_of_order_delay_seconds_max=max(
                _as_float(quality_raw.get("out_of_order_delay_seconds_min"), 0.8),
                _as_float(quality_raw.get("out_of_order_delay_seconds_max"), 4.0),
            ),
        ),
        benchmark=benchmark,
    )


def _parse_connector_distribution(simulation_raw: Mapping[str, Any]) -> dict[int, float]:
    configured = _as_mapping(simulation_raw.get("connector_count_distribution"))
    if configured:
        normalized: dict[int, float] = {}
        for key, value in configured.items():
            try:
                connector_count = int(key)
            except (TypeError, ValueError):
                continue
            connector_count = min(4, max(1, connector_count))
            normalized[connector_count] = normalized.get(connector_count, 0.0) + max(float(value), 0.0)
        if normalized:
            return _normalize_weight_mapping(normalized, default={2: 0.35, 3: 0.45, 4: 0.20})

    legacy_connectors = simulation_raw.get("connectors_per_station")
    if legacy_connectors is not None:
        connector_count = min(4, max(1, _as_int(legacy_connectors, 2)))
        return {connector_count: 1.0}

    return {1: 0.15, 2: 0.35, 3: 0.35, 4: 0.15}


def _parse_operator_distribution(simulation_raw: Mapping[str, Any]) -> tuple[OperatorDistributionItem, ...]:
    configured = simulation_raw.get("operator_distribution")
    items: list[OperatorDistributionItem] = []

    if isinstance(configured, list):
        for item in configured:
            item_map = _as_mapping(item)
            operator_id = str(item_map.get("operator_id") or item_map.get("id") or "").strip()
            if not operator_id:
                continue
            items.append(
                OperatorDistributionItem(
                    operator_id=operator_id,
                    weight=max(0.0, _as_float(item_map.get("weight"), 1.0)),
                )
            )
    elif isinstance(configured, Mapping):
        for operator_id, weight in configured.items():
            items.append(OperatorDistributionItem(operator_id=str(operator_id), weight=max(0.0, float(weight))))

    if not items:
        items = [
            OperatorDistributionItem(operator_id="operator-north", weight=0.42),
            OperatorDistributionItem(operator_id="operator-south", weight=0.33),
            OperatorDistributionItem(operator_id="operator-west", weight=0.25),
        ]

    normalized = _normalize_items(items)
    return tuple(normalized)


def _parse_geography_distribution(simulation_raw: Mapping[str, Any]) -> tuple[GeographyDistributionItem, ...]:
    configured = simulation_raw.get("geography_distribution")
    items: list[GeographyDistributionItem] = []

    if isinstance(configured, list):
        for item in configured:
            item_map = _as_mapping(item)
            city = str(item_map.get("city", "")).strip()
            country = str(item_map.get("country", "")).strip() or "US"
            if not city:
                continue
            items.append(
                GeographyDistributionItem(
                    city=city,
                    country=country,
                    latitude=_as_float(item_map.get("latitude"), 0.0),
                    longitude=_as_float(item_map.get("longitude"), 0.0),
                    weight=max(0.0, _as_float(item_map.get("weight"), 1.0)),
                )
            )

    if not items:
        items = [
            GeographyDistributionItem(city="Seattle", country="US", latitude=47.6062, longitude=-122.3321, weight=0.14),
            GeographyDistributionItem(city="San Francisco", country="US", latitude=37.7749, longitude=-122.4194, weight=0.17),
            GeographyDistributionItem(city="Los Angeles", country="US", latitude=34.0522, longitude=-118.2437, weight=0.18),
            GeographyDistributionItem(city="Dallas", country="US", latitude=32.7767, longitude=-96.7970, weight=0.16),
            GeographyDistributionItem(city="Chicago", country="US", latitude=41.8781, longitude=-87.6298, weight=0.17),
            GeographyDistributionItem(city="New York", country="US", latitude=40.7128, longitude=-74.0060, weight=0.18),
        ]

    normalized = _normalize_items(items)
    return tuple(normalized)


def _parse_vehicle_brand_distribution(simulation_raw: Mapping[str, Any]) -> tuple[VehicleBrandDistributionItem, ...]:
    configured = simulation_raw.get("vehicle_brand_distribution")
    items: list[VehicleBrandDistributionItem] = []

    if isinstance(configured, list):
        for item in configured:
            item_map = _as_mapping(item)
            brand = str(item_map.get("brand") or item_map.get("id") or "").strip()
            if not brand:
                continue
            items.append(
                VehicleBrandDistributionItem(
                    brand=brand,
                    weight=max(0.0, _as_float(item_map.get("weight"), 1.0)),
                )
            )
    elif isinstance(configured, Mapping):
        for brand, weight in configured.items():
            items.append(VehicleBrandDistributionItem(brand=str(brand), weight=max(0.0, float(weight))))

    if not items:
        items = [
            VehicleBrandDistributionItem(brand="Tesla", weight=0.28),
            VehicleBrandDistributionItem(brand="Ford", weight=0.16),
            VehicleBrandDistributionItem(brand="BMW", weight=0.14),
            VehicleBrandDistributionItem(brand="Hyundai", weight=0.16),
            VehicleBrandDistributionItem(brand="Rivian", weight=0.10),
            VehicleBrandDistributionItem(brand="Nissan", weight=0.16),
        ]

    normalized = _normalize_items(items)
    return tuple(normalized)


def _parse_hourly_weights(value: Any) -> tuple[float, ...]:
    if isinstance(value, list) and len(value) == 24:
        weights = tuple(max(0.1, float(item)) for item in value)
        if any(weight > 0 for weight in weights):
            return weights
    return DEFAULT_HOURLY_DEMAND_WEIGHTS


def _normalize_items(items: list[Any]) -> list[Any]:
    total = sum(item.weight for item in items)
    if total <= 0:
        uniform = 1.0 / max(len(items), 1)
        return [replace(item, weight=uniform) for item in items]
    return [replace(item, weight=item.weight / total) for item in items]


def _normalize_weight_mapping(value: Mapping[Any, float], default: Mapping[Any, float]) -> dict[Any, float]:
    cleaned = {key: max(0.0, float(weight)) for key, weight in value.items() if weight is not None}
    if not cleaned:
        cleaned = {key: float(weight) for key, weight in default.items()}

    total = sum(cleaned.values())
    if total <= 0:
        total = float(len(cleaned))
        return {key: 1.0 / total for key in cleaned}

    return {key: weight / total for key, weight in cleaned.items()}


def _bounded_hour(value: int) -> int:
    return max(0, min(23, value))


def _as_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}


def _as_float(value: Any, default: float) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_int(value: Any, default: int) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on", "y"}
    return default


def _as_optional_str(value: Any, default: str | None) -> str | None:
    if value is None:
        return default
    text = str(value).strip()
    return text if text else None
