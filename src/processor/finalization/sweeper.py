"""Background timeout sweeper cadence helper."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass(slots=True)
class SweeperResult:
    ran: bool
    finalized_count: int


class SessionTimeoutSweeper:
    def __init__(self, run_interval_seconds: float) -> None:
        self._run_interval_seconds = max(0.5, run_interval_seconds)
        self._next_run_monotonic = 0.0

    def should_run(self, now_monotonic: float) -> bool:
        if now_monotonic < self._next_run_monotonic:
            return False
        self._next_run_monotonic = now_monotonic + self._run_interval_seconds
        return True

    @staticmethod
    def utc_now() -> datetime:
        return datetime.now(timezone.utc)
