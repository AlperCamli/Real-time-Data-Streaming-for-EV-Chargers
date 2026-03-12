"""Settings and config loading utilities used by simulator and processor."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap_servers: str
    topic_raw: str
    topic_dlq: str
    topic_late: str
    consumer_group: str


@dataclass(frozen=True)
class RedisSettings:
    host: str
    port: int
    db: int
    dedup_ttl_seconds: int


@dataclass(frozen=True)
class ClickHouseSettings:
    host: str
    port: int
    user: str
    password: str
    database: str


@dataclass(frozen=True)
class ServiceSettings:
    service_name: str
    environment: str
    log_level: str
    log_json: bool
    metrics_enabled: bool
    metrics_host: str
    metrics_port: int
    kafka: KafkaSettings
    redis: RedisSettings
    clickhouse: ClickHouseSettings


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value is not None else default


def _env_int(name: str, default: int) -> int:
    return int(_env(name, str(default)))


def _env_bool(name: str, default: bool) -> bool:
    raw = _env(name, "true" if default else "false").strip().lower()
    return raw in {"1", "true", "yes", "y", "on"}


def _as_int(value: str, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def load_yaml_config(path: str | Path) -> dict[str, Any]:
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file does not exist: {config_path}")

    text = config_path.read_text(encoding="utf-8")

    try:
        import yaml  # type: ignore
    except ModuleNotFoundError:
        data = json.loads(text)
    else:
        data = yaml.safe_load(text)

    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ValueError(f"Top-level config must be a mapping: {config_path}")

    return data


def load_service_settings(service_name: str) -> ServiceSettings:
    metrics_port_default = 9300
    if service_name == "processor":
        metrics_port_default = 9100
    elif service_name == "simulator":
        metrics_port_default = 9200

    metrics_port_env = os.getenv(
        f"{service_name.upper()}_METRICS_PORT",
        os.getenv("METRICS_PORT", str(metrics_port_default)),
    )
    metrics_host_env = os.getenv(
        f"{service_name.upper()}_METRICS_HOST",
        os.getenv("METRICS_HOST", "0.0.0.0"),
    )

    return ServiceSettings(
        service_name=service_name,
        environment=_env("APP_ENV", "local"),
        log_level=_env("LOG_LEVEL", "INFO"),
        log_json=_env_bool("LOG_JSON", False),
        metrics_enabled=_env_bool("METRICS_ENABLED", True),
        metrics_host=metrics_host_env,
        metrics_port=max(1, _as_int(metrics_port_env, metrics_port_default)),
        kafka=KafkaSettings(
            bootstrap_servers=_env("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
            topic_raw=_env("KAFKA_TOPIC_RAW", "cs.ev.events.raw"),
            topic_dlq=_env("KAFKA_TOPIC_DLQ", "cs.ev.events.dlq"),
            topic_late=_env("KAFKA_TOPIC_LATE", "cs.ev.events.late"),
            consumer_group=_env("KAFKA_CONSUMER_GROUP", "cs-processor"),
        ),
        redis=RedisSettings(
            host=_env("REDIS_HOST", "localhost"),
            port=_env_int("REDIS_PORT", 6379),
            db=_env_int("REDIS_DB", 0),
            dedup_ttl_seconds=_env_int("DEDUP_TTL_SECONDS", 86400),
        ),
        clickhouse=ClickHouseSettings(
            host=_env("CLICKHOUSE_HOST", "localhost"),
            port=_env_int("CLICKHOUSE_PORT", 9000),
            user=_env("CLICKHOUSE_USER", "default"),
            password=_env("CLICKHOUSE_PASSWORD", ""),
            database=_env("CLICKHOUSE_DATABASE", "default"),
        ),
    )
