"""Virtual charging network generation and weighted sampling helpers."""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from typing import Callable, Iterable, TypeVar

from src.simulator.config import SimulatorConfig
from src.simulator.models import ConnectorState, ConnectorStatus, Location, NetworkState, StationState


T = TypeVar("T")


def build_network(
    config: SimulatorConfig,
    rng: random.Random,
    start_time: datetime,
    *,
    shard_index: int = 0,
    shard_count: int = 1,
) -> NetworkState:
    stations: dict[str, StationState] = {}

    for idx in range(config.network.station_count):
        if idx % shard_count != shard_index:
            continue
        station_id = f"ST-{idx + 1:04d}"
        operator = weighted_choice(config.network.operator_distribution, rng, lambda item: item.weight)
        geo = weighted_choice(config.network.geography_distribution, rng, lambda item: item.weight)

        connector_count = int(weighted_choice(
            list(config.network.connector_count_distribution.items()),
            rng,
            lambda item: item[1],
        )[0])

        connectors: dict[str, ConnectorState] = {}
        for connector_idx in range(connector_count):
            connector_id = str(connector_idx + 1)
            baseline_meter = round(rng.uniform(1800.0, 9600.0), 3)
            connectors[connector_id] = ConnectorState(
                station_id=station_id,
                connector_id=connector_id,
                status=ConnectorStatus.AVAILABLE,
                current_meter_kwh=baseline_meter,
                last_update_time=start_time,
                last_status_change=start_time,
            )

        heartbeat_jitter = timedelta(seconds=rng.uniform(0, config.network.heartbeat_interval_seconds))
        station = StationState(
            station_id=station_id,
            operator_id=operator.operator_id,
            location=Location(
                city=geo.city,
                country=geo.country,
                latitude=geo.latitude,
                longitude=geo.longitude,
            ),
            connectors=connectors,
            firmware_version=rng.choice(["1.7.0", "1.8.1", "2.0.0"]),
            last_heartbeat_at=start_time - heartbeat_jitter,
            next_heartbeat_at=start_time + heartbeat_jitter,
        )
        stations[station_id] = station

    return NetworkState(stations=stations, active_sessions={})


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def weighted_choice(items: Iterable[T], rng: random.Random, weight_getter: Callable[[T], float]) -> T:
    values = list(items)
    if not values:
        raise ValueError("weighted_choice requires at least one item")

    total_weight = sum(max(0.0, float(weight_getter(item))) for item in values)
    if total_weight <= 0:
        return rng.choice(values)

    target = rng.uniform(0, total_weight)
    cumulative = 0.0
    for item in values:
        cumulative += max(0.0, float(weight_getter(item)))
        if cumulative >= target:
            return item

    return values[-1]
