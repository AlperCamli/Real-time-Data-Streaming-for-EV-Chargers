from __future__ import annotations

import unittest
from pathlib import Path

from src.benchmarks.profiles import load_benchmark_profile
from src.common.settings import load_yaml_config
from src.processor.config import build_processor_config


class ConfigWiringTests(unittest.TestCase):
    def test_10k_processor_config_exists_and_parses(self) -> None:
        config_path = Path("config/processor.loadtest.10k.yaml")
        self.assertTrue(config_path.exists())

        raw = load_yaml_config(config_path)
        config = build_processor_config(raw, _service_settings_fixture())

        self.assertEqual(config.consumer.max_poll_records, 10000)
        self.assertEqual(config.sinks.clickhouse_batch_size, 10000)
        self.assertEqual(config.sinks.kafka_batch_size, 5000)
        self.assertEqual(config.sinks.max_pending_batches, 4)

    def test_100k_processor_config_exists_and_uses_relaxed_flush_intervals(self) -> None:
        config_path = Path("config/processor.loadtest.100k.yaml")
        self.assertTrue(config_path.exists())

        raw = load_yaml_config(config_path)
        config = build_processor_config(raw, _service_settings_fixture())

        self.assertEqual(config.consumer.max_poll_records, 10000)
        self.assertEqual(config.sinks.clickhouse_flush_interval_seconds, 0.5)
        self.assertEqual(config.sinks.flush_interval_seconds, 0.5)

    def test_10k_benchmark_profile_uses_10k_processor_config(self) -> None:
        profile = load_benchmark_profile(Path("config/benchmarks/10k.yaml"))
        self.assertEqual(profile.processor_config, "config/processor.loadtest.10k.yaml")

    def test_100k_benchmark_profile_uses_100k_loadtest_configs(self) -> None:
        profile = load_benchmark_profile(Path("config/benchmarks/100k.yaml"))
        self.assertEqual(profile.simulator_config, "config/simulator.loadtest.100k.yaml")
        self.assertEqual(profile.processor_config, "config/processor.loadtest.100k.yaml")

    def test_10k_compose_override_points_to_existing_processor_config_and_disables_redis_persistence(self) -> None:
        compose_text = Path("docker-compose.loadtest.10k.yml").read_text(encoding="utf-8")
        self.assertIn("PROCESSOR_CONFIG_PATH: config/processor.loadtest.10k.yaml", compose_text)
        self.assertIn("SIMULATOR_CONFIG_PATH: config/simulator.loadtest.10k.yaml", compose_text)
        self.assertTrue(Path("config/processor.loadtest.10k.yaml").exists())
        self.assertIn('command: ["redis-server", "--appendonly", "no", "--save", ""]', compose_text)

    def test_scaled_compose_override_adds_extra_processors_and_simulator_without_host_ports(self) -> None:
        compose_text = Path("docker-compose.loadtest.scaled.yml").read_text(encoding="utf-8")
        self.assertIn("simulator-b:", compose_text)
        self.assertIn("processor-b:", compose_text)
        self.assertIn("processor-c:", compose_text)
        self.assertIn("processor-d:", compose_text)
        self.assertIn("--shard-index 0 --shard-count 2 --target-eps-scale 0.5", compose_text)
        self.assertIn("SIMULATOR_METRICS_PORT: 9201", compose_text)
        self.assertEqual(compose_text.count("ports: []"), 4)

    def test_prometheus_scrapes_all_scaled_instances(self) -> None:
        prometheus_text = Path("config/prometheus/prometheus.yml").read_text(encoding="utf-8")
        self.assertIn("simulator:9200", prometheus_text)
        self.assertIn("simulator-b:9201", prometheus_text)
        self.assertIn("processor:9100", prometheus_text)
        self.assertIn("processor-b:9100", prometheus_text)
        self.assertIn("processor-c:9100", prometheus_text)
        self.assertIn("processor-d:9100", prometheus_text)

    def test_overview_dashboard_aggregates_multi_instance_metrics(self) -> None:
        dashboard_text = Path("dashboards/grafana/dashboards/chargesquare-overview.json").read_text(encoding="utf-8")
        self.assertIn('"expr": "sum(rate(events_generated_total[1m]))"', dashboard_text)
        self.assertIn('"expr": "max(kafka_consumer_lag)"', dashboard_text)
        self.assertIn('"expr": "sum(active_sessions)"', dashboard_text)
        self.assertIn('"expr": "max(end_to_end_latency_ms_p99)"', dashboard_text)


def _service_settings_fixture():
    from src.common.settings import ClickHouseSettings, KafkaSettings, RedisSettings, ServiceSettings

    return ServiceSettings(
        service_name="processor",
        environment="test",
        log_level="INFO",
        log_json=False,
        metrics_enabled=False,
        metrics_host="0.0.0.0",
        metrics_port=9100,
        kafka=KafkaSettings(
            bootstrap_servers="localhost:29092",
            topic_raw="cs.ev.events.raw",
            topic_dlq="cs.ev.events.dlq",
            topic_late="cs.ev.events.late",
            consumer_group="cs-processor",
        ),
        redis=RedisSettings(host="localhost", port=6379, db=0, dedup_ttl_seconds=86400),
        clickhouse=ClickHouseSettings(host="localhost", port=9000, user="default", password="password", database="default"),
    )


if __name__ == "__main__":
    unittest.main()
