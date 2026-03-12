"""Prometheus scrape snapshot helpers for benchmark runs."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Iterable
from urllib.request import urlopen


METRIC_LINE_RE = re.compile(
    r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?\s+([-+]?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?)"
)


@dataclass(slots=True)
class PrometheusSnapshot:
    values: dict[str, dict[tuple[tuple[str, str], ...], float]] = field(default_factory=dict)

    def get(self, metric_name: str, *, labels: dict[str, str] | None = None, default: float = 0.0) -> float:
        by_labels = self.values.get(metric_name)
        if not by_labels:
            return default
        if labels is None:
            return sum(by_labels.values()) if len(by_labels) > 1 else next(iter(by_labels.values()))
        label_key = tuple(sorted((k, str(v)) for k, v in labels.items()))
        return by_labels.get(label_key, default)

    def metric_names(self) -> Iterable[str]:
        return self.values.keys()


def fetch_snapshot(endpoint: str, *, timeout_seconds: float = 2.0) -> PrometheusSnapshot:
    with urlopen(endpoint, timeout=timeout_seconds) as response:  # noqa: S310
        payload = response.read().decode("utf-8")
    return parse_prometheus_text(payload)


def parse_prometheus_text(payload: str) -> PrometheusSnapshot:
    snapshot = PrometheusSnapshot()
    for raw_line in payload.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        match = METRIC_LINE_RE.match(line)
        if not match:
            continue

        metric_name = match.group(1)
        labels_blob = match.group(2)
        value = float(match.group(3))
        labels_key = _parse_label_key(labels_blob)

        metric_store = snapshot.values.setdefault(metric_name, {})
        metric_store[labels_key] = value

    return snapshot


def _parse_label_key(labels_blob: str | None) -> tuple[tuple[str, str], ...]:
    if not labels_blob:
        return ()

    labels: dict[str, str] = {}
    for part in _split_labels(labels_blob):
        if "=" not in part:
            continue
        key, raw_value = part.split("=", 1)
        labels[key.strip()] = _decode_label_value(raw_value.strip())
    return tuple(sorted(labels.items()))


def _split_labels(blob: str) -> list[str]:
    parts: list[str] = []
    current: list[str] = []
    in_quotes = False
    escape = False
    for char in blob:
        if escape:
            current.append(char)
            escape = False
            continue
        if char == "\\":
            current.append(char)
            escape = True
            continue
        if char == '"':
            in_quotes = not in_quotes
            current.append(char)
            continue
        if char == "," and not in_quotes:
            part = "".join(current).strip()
            if part:
                parts.append(part)
            current = []
            continue
        current.append(char)

    tail = "".join(current).strip()
    if tail:
        parts.append(tail)
    return parts


def _decode_label_value(raw_value: str) -> str:
    value = raw_value
    if value.startswith('"') and value.endswith('"'):
        value = value[1:-1]
    return value.replace('\\"', '"').replace("\\\\", "\\")
