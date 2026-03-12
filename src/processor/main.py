"""Stream processor entrypoint (foundation stage)."""

from __future__ import annotations

import argparse
from pathlib import Path

from src.common.logging import configure_logging
from src.common.settings import load_service_settings, load_yaml_config


DEFAULT_PROCESSOR_CONFIG_PATH = Path("config/processor.default.yaml")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ChargeSquare stream processor")
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_PROCESSOR_CONFIG_PATH,
        help="Path to processor config file",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    service_settings = load_service_settings("processor")
    config = load_yaml_config(args.config)

    logger = configure_logging(
        service_name=service_settings.service_name,
        log_level=service_settings.log_level,
        json_logs=service_settings.log_json,
    )

    logger.info("processor_boot_start")
    logger.info(
        "processor_config_loaded",
        extra={
            "config_path": str(args.config),
            "consumer_group": service_settings.kafka.consumer_group,
            "late_events_enabled": config.get("processing", {}).get("late_events_enabled"),
        },
    )
    logger.info("processor_logic_not_implemented_yet")


if __name__ == "__main__":
    main()
