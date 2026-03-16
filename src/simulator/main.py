"""Simulator service entrypoint."""

from __future__ import annotations

import argparse
import random
import signal
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

from src.common.logging import configure_logging
from src.common.prometheus import OptionalPrometheusRegistry
from src.common.settings import load_service_settings, load_yaml_config
from src.common.topic_names import TOPIC_EVENTS_RAW
from src.simulator.config import build_simulator_config
from src.simulator.event_factory import EventFactory
from src.simulator.metrics import SimulatorMetrics
from src.simulator.network import build_network
from src.simulator.producer import build_event_producer
from src.simulator.scheduler import QualityEventInjector, TickRateController
from src.simulator.session_logic import SessionEngine


DEFAULT_SIMULATOR_CONFIG_PATH = Path("config/simulator.default.yaml")


@dataclass(frozen=True, slots=True)
class SimulatorShardConfig:
    shard_index: int
    shard_count: int
    target_eps_scale: float


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ChargeSquare EV event simulator")
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_SIMULATOR_CONFIG_PATH,
        help="Path to simulator config file",
    )
    parser.add_argument(
        "--max-runtime-seconds",
        type=int,
        default=0,
        help="Optional runtime limit for local smoke runs (0 means infinite)",
    )
    parser.add_argument(
        "--shard-index",
        type=int,
        default=0,
        help="0-based shard index for deterministic station partitioning",
    )
    parser.add_argument(
        "--shard-count",
        type=int,
        default=1,
        help="Total number of simulator shards",
    )
    parser.add_argument(
        "--target-eps-scale",
        type=float,
        default=1.0,
        help="Multiplier applied to the configured simulator target EPS",
    )
    return parser.parse_args(argv)


def validate_shard_config(args: argparse.Namespace) -> SimulatorShardConfig:
    shard_count = int(args.shard_count)
    shard_index = int(args.shard_index)
    target_eps_scale = float(args.target_eps_scale)

    if shard_count < 1:
        raise ValueError("shard_count must be >= 1")
    if shard_index < 0 or shard_index >= shard_count:
        raise ValueError("shard_index must satisfy 0 <= shard_index < shard_count")
    if target_eps_scale <= 0:
        raise ValueError("target_eps_scale must be > 0")

    return SimulatorShardConfig(
        shard_index=shard_index,
        shard_count=shard_count,
        target_eps_scale=target_eps_scale,
    )


def effective_target_eps(sim_config, monotonic_time_seconds: float, shard_config: SimulatorShardConfig) -> float:
    return sim_config.target_eps(monotonic_time_seconds) * shard_config.target_eps_scale


def main() -> None:
    args = parse_args()
    shard_config = validate_shard_config(args)
    stop_event = threading.Event()

    service_settings = load_service_settings("simulator")
    raw_config = load_yaml_config(args.config)
    sim_config = build_simulator_config(raw_config)

    logger = configure_logging(
        service_name=service_settings.service_name,
        log_level=service_settings.log_level,
        json_logs=service_settings.log_json,
    )

    rng = random.Random(sim_config.producer.seed + shard_config.shard_index)
    start_time = datetime.now(timezone.utc)

    network = build_network(
        config=sim_config,
        rng=rng,
        start_time=start_time,
        shard_index=shard_config.shard_index,
        shard_count=shard_config.shard_count,
    )
    event_factory = EventFactory(
        producer_id=sim_config.producer.producer_id,
        schema_version=sim_config.producer.schema_version,
    )
    engine = SessionEngine(
        config=sim_config,
        network=network,
        event_factory=event_factory,
        rng=rng,
    )

    prometheus_registry = OptionalPrometheusRegistry(enabled=service_settings.metrics_enabled)
    simulator_metrics = SimulatorMetrics(prometheus=prometheus_registry)
    quality_injector = QualityEventInjector(sim_config.data_quality, rng, simulator_metrics)
    tick_controller = TickRateController(sim_config.network.tick_interval_seconds)

    producer = build_event_producer(
        bootstrap_servers=service_settings.kafka.bootstrap_servers,
        topic=TOPIC_EVENTS_RAW,
        config=sim_config.producer,
        logger=logger,
    )

    if service_settings.kafka.topic_raw != TOPIC_EVENTS_RAW:
        logger.warning(
            "simulator_topic_override_ignored",
            extra={"configured": service_settings.kafka.topic_raw, "frozen": TOPIC_EVENTS_RAW},
        )

    prometheus_registry.start_http_server(
        host=service_settings.metrics_host,
        port=service_settings.metrics_port,
        logger=logger,
    )

    def _handle_signal(sig: int, _frame: object) -> None:
        logger.info("simulator_shutdown_signal", extra={"signal": sig})
        stop_event.set()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    logger.info(
        "simulator_boot_complete",
        extra={
            "mode": sim_config.mode,
            "config_path": str(args.config),
            "configured_station_count": sim_config.network.station_count,
            "stations": len(network.stations),
            "connectors": network.connector_count(),
            "target_eps": sim_config.network.target_event_rate,
            "target_eps_scale": shard_config.target_eps_scale,
            "shard_index": shard_config.shard_index,
            "shard_count": shard_config.shard_count,
            "topic": TOPIC_EVENTS_RAW,
            "dry_run": sim_config.producer.dry_run,
            "metrics_endpoint": f"http://{service_settings.metrics_host}:{service_settings.metrics_port}/metrics",
        },
    )

    started_monotonic = time.monotonic()
    last_health_log = started_monotonic

    try:
        while not stop_event.is_set():
            tick_started_monotonic = time.monotonic()
            now = datetime.now(timezone.utc)

            target_eps = effective_target_eps(sim_config, tick_started_monotonic, shard_config)
            simulator_metrics.set_target_eps(target_eps)
            observed_eps = simulator_metrics.current_eps(tick_started_monotonic)
            generated_events = engine.generate_events(
                now=now,
                target_eps=target_eps,
                observed_eps=observed_eps,
                tick_seconds=sim_config.network.tick_interval_seconds,
            )
            start_control = engine.last_start_control
            simulator_metrics.set_admission_scale(start_control.admission_scale)
            simulator_metrics.set_session_start_probability_effective(start_control.effective_probability)
            simulator_metrics.set_session_start_cap_effective(start_control.effective_cap)
            if start_control.blocked_overshoot:
                simulator_metrics.increment_session_start_blocked_ticks()

            for event in generated_events:
                event_type = str(event.get("event_type", "UNKNOWN"))
                simulator_metrics.increment_generated(event_type)

            fault_count = sum(1 for event in generated_events if event.get("event_type") == "FAULT_ALERT")
            if fault_count:
                simulator_metrics.increment_fault_events(fault_count)

            outbound_events = quality_injector.apply(
                events=generated_events,
                now=now,
                now_monotonic=tick_started_monotonic,
            )

            failures = producer.publish_batch(outbound_events)
            if failures:
                simulator_metrics.increment_produce_failures(failures)

            simulator_metrics.set_active_sessions(len(network.active_sessions))
            actual_eps = simulator_metrics.observe_emitted(len(outbound_events), tick_started_monotonic)

            if tick_started_monotonic - last_health_log >= 10.0:
                last_health_log = tick_started_monotonic
                logger.info(
                    "simulator_runtime_health",
                    extra={
                        "target_eps": round(target_eps, 2),
                        "actual_eps": round(actual_eps, 2),
                        "events_generated": len(generated_events),
                        "events_published": len(outbound_events),
                        "active_sessions": len(network.active_sessions),
                    },
                )

            if args.max_runtime_seconds > 0 and (tick_started_monotonic - started_monotonic) >= args.max_runtime_seconds:
                logger.info("simulator_runtime_limit_reached", extra={"seconds": args.max_runtime_seconds})
                stop_event.set()
                continue

            tick_controller.sleep_to_next_tick(tick_started_monotonic)
    finally:
        final_now = datetime.now(timezone.utc)
        delayed_events = quality_injector.drain_all(final_now)
        if delayed_events:
            failures = producer.publish_batch(delayed_events)
            if failures:
                simulator_metrics.increment_produce_failures(failures)

        producer.close()

        metrics_snapshot = simulator_metrics.snapshot()
        logger.info(
            "simulator_shutdown_complete",
            extra={
                "active_sessions": len(network.active_sessions),
                "counters": metrics_snapshot["counters"],
                "gauges": metrics_snapshot["gauges"],
            },
        )


if __name__ == "__main__":
    main()
