from __future__ import annotations

import random
import unittest
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from src.common.settings import load_yaml_config
from src.simulator.config import build_simulator_config
from src.simulator.main import effective_target_eps, parse_args, validate_shard_config
from src.simulator.network import build_network


class SimulatorScalingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        raw = load_yaml_config(Path("config/simulator.loadtest.10k.yaml"))
        config = build_simulator_config(raw)
        cls.config = replace(config, network=replace(config.network, station_count=12))
        cls.start_time = datetime(2026, 1, 1, tzinfo=timezone.utc)

    def test_network_shards_produce_disjoint_station_sets(self) -> None:
        shard_zero = build_network(
            self.config,
            random.Random(self.config.producer.seed),
            self.start_time,
            shard_index=0,
            shard_count=2,
        )
        shard_one = build_network(
            self.config,
            random.Random(self.config.producer.seed + 1),
            self.start_time,
            shard_index=1,
            shard_count=2,
        )

        zero_ids = set(shard_zero.stations)
        one_ids = set(shard_one.stations)

        self.assertEqual(zero_ids & one_ids, set())

    def test_combined_shards_cover_full_station_namespace(self) -> None:
        shard_zero = build_network(
            self.config,
            random.Random(self.config.producer.seed),
            self.start_time,
            shard_index=0,
            shard_count=2,
        )
        shard_one = build_network(
            self.config,
            random.Random(self.config.producer.seed + 1),
            self.start_time,
            shard_index=1,
            shard_count=2,
        )

        expected_station_ids = {f"ST-{idx + 1:04d}" for idx in range(self.config.network.station_count)}
        combined_station_ids = set(shard_zero.stations) | set(shard_one.stations)

        self.assertEqual(combined_station_ids, expected_station_ids)

    def test_target_eps_scale_halves_effective_target(self) -> None:
        shard_config = validate_shard_config(parse_args(["--target-eps-scale", "0.5"]))
        monotonic_time_seconds = 123.0

        self.assertEqual(
            effective_target_eps(self.config, monotonic_time_seconds, shard_config),
            self.config.target_eps(monotonic_time_seconds) * 0.5,
        )

    def test_invalid_shard_args_fail_fast(self) -> None:
        invalid_argvs = (
            ["--shard-count", "0"],
            ["--shard-count", "2", "--shard-index", "-1"],
            ["--shard-count", "2", "--shard-index", "2"],
            ["--target-eps-scale", "0"],
        )

        for argv in invalid_argvs:
            with self.subTest(argv=argv):
                with self.assertRaises(ValueError):
                    validate_shard_config(parse_args(argv))


if __name__ == "__main__":
    unittest.main()
