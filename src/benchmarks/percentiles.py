"""Percentile helpers for benchmark result aggregation."""

from __future__ import annotations

import math
from typing import Iterable


def percentile(values: Iterable[float], p: float) -> float:
    samples = sorted(float(item) for item in values)
    if not samples:
        return 0.0

    bounded = max(0.0, min(100.0, float(p)))
    if len(samples) == 1:
        return samples[0]

    rank = (bounded / 100.0) * (len(samples) - 1)
    lower = int(math.floor(rank))
    upper = int(math.ceil(rank))
    if lower == upper:
        return samples[lower]

    fraction = rank - lower
    return samples[lower] + (samples[upper] - samples[lower]) * fraction


def percentile_pack(values: Iterable[float]) -> dict[str, float]:
    samples = list(values)
    return {
        "p50": percentile(samples, 50.0),
        "p95": percentile(samples, 95.0),
        "p99": percentile(samples, 99.0),
    }
