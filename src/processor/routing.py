"""Routing decisions for processor outcomes."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from src.processor.lateness import LatenessClass


class Disposition(str, Enum):
    ACCEPTED = "accepted"
    INVALID = "invalid"
    DUPLICATE = "duplicate"
    TOO_LATE_REJECTED = "too_late_rejected"


@dataclass(slots=True)
class SinkActions:
    write_raw_history: bool
    write_dead_letter: bool
    write_late_audit: bool
    update_redis_state: bool
    mutate_session_state: bool
    mutate_facts_aggregates: bool


@dataclass(slots=True)
class RouteDecision:
    disposition: Disposition
    reason: str
    lateness_class: LatenessClass | None
    stale_for_redis: bool
    sink_actions: SinkActions


def route_invalid(reason: str) -> RouteDecision:
    return RouteDecision(
        disposition=Disposition.INVALID,
        reason=reason,
        lateness_class=None,
        stale_for_redis=False,
        sink_actions=SinkActions(
            write_raw_history=False,
            write_dead_letter=True,
            write_late_audit=False,
            update_redis_state=False,
            mutate_session_state=False,
            mutate_facts_aggregates=False,
        ),
    )


def route_duplicate() -> RouteDecision:
    return RouteDecision(
        disposition=Disposition.DUPLICATE,
        reason="duplicate_event_id",
        lateness_class=None,
        stale_for_redis=False,
        sink_actions=SinkActions(
            write_raw_history=False,
            write_dead_letter=False,
            write_late_audit=False,
            update_redis_state=False,
            mutate_session_state=False,
            mutate_facts_aggregates=False,
        ),
    )


def route_too_late() -> RouteDecision:
    return RouteDecision(
        disposition=Disposition.TOO_LATE_REJECTED,
        reason="too_late_rejected",
        lateness_class=LatenessClass.TOO_LATE_REJECTED,
        stale_for_redis=False,
        sink_actions=SinkActions(
            write_raw_history=False,
            write_dead_letter=False,
            write_late_audit=True,
            update_redis_state=False,
            mutate_session_state=False,
            mutate_facts_aggregates=False,
        ),
    )


def route_accepted(lateness_class: LatenessClass, stale_for_redis: bool = False) -> RouteDecision:
    return RouteDecision(
        disposition=Disposition.ACCEPTED,
        reason="accepted",
        lateness_class=lateness_class,
        stale_for_redis=stale_for_redis,
        sink_actions=SinkActions(
            write_raw_history=True,
            write_dead_letter=False,
            write_late_audit=False,
            update_redis_state=not stale_for_redis,
            mutate_session_state=True,
            mutate_facts_aggregates=True,
        ),
    )
