from __future__ import annotations

import random
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.common.settings import load_yaml_config
from src.simulator.config import build_simulator_config
from src.simulator.event_factory import EventFactory
from src.simulator.metrics import SimulatorMetrics
from src.simulator.network import build_network
from src.simulator.scheduler import QualityEventInjector
from src.simulator.session_logic import SessionEngine


class SimulatorEpsControllerTests(unittest.TestCase):
    def setUp(self) -> None:
        raw = load_yaml_config(Path("config/simulator.default.yaml"))
        self.base_config = build_simulator_config(raw)

    def test_overshoot_reduces_scale_and_blocks_starts(self) -> None:
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        config = self._control_config(target_eps=100.0)
        network, engine = self._build_engine(config=config, now=now)

        events = engine.generate_events(
            now=now,
            target_eps=100.0,
            observed_eps=180.0,
            tick_seconds=config.network.tick_interval_seconds,
        )

        control = engine.last_start_control
        self.assertTrue(control.blocked_overshoot)
        self.assertLess(control.admission_scale, 1.0)
        self.assertEqual(control.effective_cap, 0)
        self.assertEqual(_event_type_count(events, "SESSION_START"), 0)
        self.assertEqual(len(network.active_sessions), 0)

    def test_controller_and_quality_defaults_apply_when_fields_missing(self) -> None:
        raw = load_yaml_config(Path("config/simulator.default.yaml"))
        raw.pop("eps_controller", None)
        quality_raw = raw.get("data_quality")
        if isinstance(quality_raw, dict):
            quality_raw.pop("too_late_excluded_event_types", None)

        config = build_simulator_config(raw)

        self.assertTrue(config.eps_controller.enabled)
        self.assertEqual(config.eps_controller.target_band_ratio, 0.10)
        self.assertEqual(config.eps_controller.scale_up_step, 1.08)
        self.assertEqual(config.eps_controller.scale_down_step, 0.80)
        self.assertEqual(config.data_quality.too_late_excluded_event_types, ("SESSION_START", "SESSION_STOP"))

    def test_undershoot_increases_scale_and_in_band_recovers_toward_one(self) -> None:
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        config = self._control_config(target_eps=100.0)
        _, engine = self._build_engine(config=config, now=now)

        engine.generate_events(
            now=now,
            target_eps=100.0,
            observed_eps=0.0,
            tick_seconds=config.network.tick_interval_seconds,
        )
        low_scale = engine.last_start_control.admission_scale
        self.assertGreater(low_scale, 1.0)

        engine.generate_events(
            now=now + timedelta(seconds=1),
            target_eps=100.0,
            observed_eps=100.0,
            tick_seconds=config.network.tick_interval_seconds,
        )
        recovered_scale = engine.last_start_control.admission_scale

        self.assertLess(recovered_scale, low_scale)
        self.assertGreater(recovered_scale, 1.0)

    def test_admission_scale_respects_configured_bounds(self) -> None:
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        config = self._control_config(
            target_eps=100.0,
            scale_up_step=2.0,
            scale_down_step=0.1,
            min_scale=0.5,
            max_scale=1.1,
        )
        _, engine = self._build_engine(config=config, now=now)

        for tick in range(8):
            engine.generate_events(
                now=now + timedelta(seconds=tick),
                target_eps=100.0,
                observed_eps=0.0,
                tick_seconds=config.network.tick_interval_seconds,
            )
        self.assertLessEqual(engine.last_start_control.admission_scale, 1.1)

        for tick in range(8, 16):
            engine.generate_events(
                now=now + timedelta(seconds=tick),
                target_eps=100.0,
                observed_eps=2000.0,
                tick_seconds=config.network.tick_interval_seconds,
            )
        self.assertGreaterEqual(engine.last_start_control.admission_scale, 0.5)

    def test_headroom_cap_limits_session_starts(self) -> None:
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        config = self._control_config(
            target_eps=2.0,
            max_start_share_per_tick=1.0,
            events_per_new_session_immediate=2,
            max_start_probability=1.0,
            base_start_rate=60.0,
        )
        network, engine = self._build_engine(config=config, now=now)

        events = engine.generate_events(
            now=now,
            target_eps=2.0,
            observed_eps=0.0,
            tick_seconds=config.network.tick_interval_seconds,
        )

        self.assertEqual(_event_type_count(events, "SESSION_START"), 1)
        self.assertEqual(engine.last_start_control.effective_cap, 1)
        self.assertEqual(len(network.active_sessions), 1)

    def test_share_cap_limits_session_starts(self) -> None:
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        config = self._control_config(
            target_eps=1000.0,
            max_start_share_per_tick=0.10,
            events_per_new_session_immediate=2,
            max_start_probability=1.0,
            base_start_rate=60.0,
        )
        network, engine = self._build_engine(config=config, now=now)

        available = len(network.available_connectors())
        expected_cap = int(available * 0.10)

        events = engine.generate_events(
            now=now,
            target_eps=1000.0,
            observed_eps=0.0,
            tick_seconds=config.network.tick_interval_seconds,
        )

        self.assertEqual(engine.last_start_control.effective_cap, expected_cap)
        self.assertEqual(_event_type_count(events, "SESSION_START"), expected_cap)

    def test_too_late_injection_skips_excluded_lifecycle_events(self) -> None:
        config = replace(
            self.base_config.data_quality,
            duplicate_injection_rate=0.0,
            out_of_order_injection_rate=0.0,
            too_late_injection_rate=1.0,
            too_late_threshold_seconds=60.0,
            too_late_excluded_event_types=("SESSION_START", "SESSION_STOP"),
        )
        injector = QualityEventInjector(config=config, rng=random.Random(5), metrics=SimulatorMetrics(prometheus=None))

        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        events = [
            _sample_event("evt-start", "SESSION_START", now),
            _sample_event("evt-meter", "METER_UPDATE", now),
            _sample_event("evt-stop", "SESSION_STOP", now),
        ]

        emitted = injector.apply(events=events, now=now, now_monotonic=10.0)

        session_start_emitted = [event for event in emitted if event.get("event_type") == "SESSION_START"]
        meter_emitted = [event for event in emitted if event.get("event_type") == "METER_UPDATE"]
        session_stop_emitted = [event for event in emitted if event.get("event_type") == "SESSION_STOP"]

        self.assertEqual(len(session_start_emitted), 1)
        self.assertEqual(len(session_stop_emitted), 1)
        self.assertEqual(len(meter_emitted), 2)
        self.assertEqual({event["event_id"] for event in meter_emitted}, {"evt-meter", meter_emitted[1]["event_id"]})

    def test_1k_profile_converges_into_target_band_after_warmup(self) -> None:
        raw = load_yaml_config(Path("config/simulator.loadtest.1k.yaml"))
        config = build_simulator_config(raw)
        config = replace(
            config,
            network=replace(config.network, station_count=1200),
            faults=replace(config.faults, fault_rate_per_connector_hour=0.0),
            data_quality=replace(
                config.data_quality,
                duplicate_injection_rate=0.0,
                out_of_order_injection_rate=0.0,
                too_late_injection_rate=0.0,
            ),
        )

        rng = random.Random(config.producer.seed)
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        network = build_network(config=config, rng=rng, start_time=now)
        engine = SessionEngine(
            config=config,
            network=network,
            event_factory=EventFactory(config.producer.producer_id, config.producer.schema_version),
            rng=rng,
        )
        metrics = SimulatorMetrics(prometheus=None)
        injector = QualityEventInjector(config.data_quality, rng, metrics)

        observed_eps_points: list[float] = []
        monotonic = 0.0
        for _ in range(900):
            target_eps = config.target_eps(monotonic)
            observed_eps = metrics.current_eps(monotonic)
            generated = engine.generate_events(
                now=now,
                target_eps=target_eps,
                observed_eps=observed_eps,
                tick_seconds=config.network.tick_interval_seconds,
            )
            outbound = injector.apply(generated, now=now, now_monotonic=monotonic)
            measured = metrics.observe_emitted(len(outbound), monotonic)
            observed_eps_points.append(measured)

            monotonic += config.network.tick_interval_seconds
            now += timedelta(seconds=config.network.tick_interval_seconds)

        warmup_ticks = 500
        steady_state = observed_eps_points[warmup_ticks:]
        self.assertGreater(len(steady_state), 0)

        mean_eps = sum(steady_state) / len(steady_state)
        target = config.network.target_event_rate
        lower = target * (1.0 - config.eps_controller.target_band_ratio)
        upper = target * (1.0 + config.eps_controller.target_band_ratio)

        self.assertGreaterEqual(mean_eps, lower)
        self.assertLessEqual(mean_eps, upper)

    def _control_config(
        self,
        *,
        target_eps: float,
        scale_up_step: float = 1.08,
        scale_down_step: float = 0.80,
        min_scale: float = 0.05,
        max_scale: float = 3.0,
        max_start_share_per_tick: float = 0.20,
        events_per_new_session_immediate: int = 2,
        max_start_probability: float = 0.85,
        base_start_rate: float = 30.0,
    ):
        return replace(
            self.base_config,
            network=replace(
                self.base_config.network,
                station_count=8,
                heartbeat_interval_seconds=9999.0,
                target_event_rate=target_eps,
                tick_interval_seconds=1.0,
            ),
            demand=replace(
                self.base_config.demand,
                base_session_start_rate_per_idle_connector_minute=base_start_rate,
                hourly_weights=tuple(1.0 for _ in range(24)),
            ),
            faults=replace(self.base_config.faults, fault_rate_per_connector_hour=0.0),
            data_quality=replace(
                self.base_config.data_quality,
                duplicate_injection_rate=0.0,
                out_of_order_injection_rate=0.0,
                too_late_injection_rate=0.0,
            ),
            eps_controller=replace(
                self.base_config.eps_controller,
                enabled=True,
                target_band_ratio=0.10,
                scale_up_step=scale_up_step,
                scale_down_step=scale_down_step,
                scale_recovery_alpha=0.20,
                min_admission_scale=min_scale,
                max_admission_scale=max_scale,
                max_start_share_per_tick=max_start_share_per_tick,
                events_per_new_session_immediate=events_per_new_session_immediate,
                max_start_probability=max_start_probability,
            ),
            benchmark=replace(self.base_config.benchmark, enabled=False, sustained_mode=False, burst_mode=False),
        )

    def _build_engine(self, *, config, now: datetime) -> tuple:
        rng = random.Random(config.producer.seed)
        network = build_network(config=config, rng=rng, start_time=now)
        for station in network.stations.values():
            station.next_heartbeat_at = now + timedelta(hours=24)

        engine = SessionEngine(
            config=config,
            network=network,
            event_factory=EventFactory(config.producer.producer_id, config.producer.schema_version),
            rng=rng,
        )
        return network, engine


def _event_type_count(events: list[dict[str, object]], event_type: str) -> int:
    return sum(1 for event in events if event.get("event_type") == event_type)


def _sample_event(event_id: str, event_type: str, now: datetime) -> dict[str, object]:
    return {
        "event_id": event_id,
        "event_type": event_type,
        "event_time": now.isoformat(),
        "ingest_time": now.isoformat(),
        "station_id": "ST-0001",
        "connector_id": "1",
        "operator_id": "operator-north",
        "session_id": "SE-1",
        "schema_version": "1.0",
        "producer_id": "simulator-test",
        "sequence_no": 1,
        "location": {
            "city": "Seattle",
            "country": "US",
            "latitude": 47.6,
            "longitude": -122.3,
        },
        "payload": {
            "meter_kwh": 10.0,
            "energy_delta_kwh": 0.2,
            "power_kw": 8.1,
        },
    }


if __name__ == "__main__":
    unittest.main()
