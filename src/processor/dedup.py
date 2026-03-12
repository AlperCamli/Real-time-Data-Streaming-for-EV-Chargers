"""Deduplication adapters (Redis-first with in-memory fallback)."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Protocol

from src.common.redis_keys import dedup_key


class Deduplicator(Protocol):
    def check_and_mark(self, event_id: str) -> bool:
        """Returns True when duplicate, False when first-seen."""


@dataclass(slots=True)
class DedupBackend:
    deduplicator: Deduplicator
    backend_name: str


class RedisDeduplicator:
    def __init__(self, redis_client: object, ttl_seconds: int, logger: logging.Logger) -> None:
        self._redis = redis_client
        self._ttl_seconds = max(1, ttl_seconds)
        self._logger = logger

    def check_and_mark(self, event_id: str) -> bool:
        key = dedup_key(event_id)
        try:
            created = self._redis.set(name=key, value="1", ex=self._ttl_seconds, nx=True)
            return not bool(created)
        except Exception as exc:  # noqa: BLE001
            self._logger.error(
                "processor_dedup_redis_failed_open",
                extra={"event_id": event_id, "error": str(exc)},
            )
            return False


class InMemoryDeduplicator:
    def __init__(self, ttl_seconds: int) -> None:
        self._ttl_seconds = max(1, ttl_seconds)
        self._expires_at: dict[str, datetime] = {}
        self._lock = Lock()

    def check_and_mark(self, event_id: str) -> bool:
        now = datetime.now(timezone.utc)
        expires = now + timedelta(seconds=self._ttl_seconds)

        with self._lock:
            self._cleanup(now)
            current = self._expires_at.get(event_id)
            if current is not None and current > now:
                return True
            self._expires_at[event_id] = expires
            return False

    def _cleanup(self, now: datetime) -> None:
        stale_keys = [key for key, expires_at in self._expires_at.items() if expires_at <= now]
        for key in stale_keys:
            self._expires_at.pop(key, None)


def build_deduplicator(
    *,
    redis_client: object | None,
    ttl_seconds: int,
    logger: logging.Logger,
) -> DedupBackend:
    if redis_client is not None:
        return DedupBackend(
            deduplicator=RedisDeduplicator(redis_client=redis_client, ttl_seconds=ttl_seconds, logger=logger),
            backend_name="redis",
        )

    logger.warning("processor_dedup_using_inmemory_fallback")
    return DedupBackend(
        deduplicator=InMemoryDeduplicator(ttl_seconds=ttl_seconds),
        backend_name="inmemory",
    )
