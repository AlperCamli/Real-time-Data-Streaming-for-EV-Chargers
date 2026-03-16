"""Event-time lateness classification."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum


class LatenessClass(str, Enum):
    ON_TIME = "on_time"
    ACCEPTED_LATE = "accepted_late"
    TOO_LATE_REJECTED = "too_late_rejected"


@dataclass(slots=True)
class LatenessResult:
    classification: LatenessClass
    lateness_seconds: int

    @property
    def is_too_late(self) -> bool:
        return self.classification == LatenessClass.TOO_LATE_REJECTED

    @property
    def is_accepted_late(self) -> bool:
        return self.classification == LatenessClass.ACCEPTED_LATE


def classify_lateness(event_time: datetime, received_at: datetime, allowed_lateness_seconds: int) -> LatenessResult:
    lateness_seconds = max(0, int((received_at - event_time).total_seconds()))
    if lateness_seconds == 0:
        return LatenessResult(classification=LatenessClass.ON_TIME, lateness_seconds=lateness_seconds)
    if lateness_seconds <= max(1, allowed_lateness_seconds):
        return LatenessResult(classification=LatenessClass.ACCEPTED_LATE, lateness_seconds=lateness_seconds)
    return LatenessResult(classification=LatenessClass.TOO_LATE_REJECTED, lateness_seconds=lateness_seconds)
