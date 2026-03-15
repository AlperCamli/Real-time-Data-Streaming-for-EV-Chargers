"""Deduplication adapters with staged reservation and commit semantics."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Protocol

from src.common.redis_keys import dedup_key


class Deduplicator(Protocol):
    def reserve_batch(self, event_ids: list[str]) -> set[str]:
        """Returns the set of event_ids that should be treated as duplicates."""

    def commit_reserved(self, event_ids: list[str]) -> None:
        """Marks reserved IDs as committed after all sink writes succeed."""

    def release_reserved(self, event_ids: list[str]) -> None:
        """Releases reserved IDs when a sink batch fails before commit."""


@dataclass(slots=True)
class DedupBackend:
    deduplicator: Deduplicator
    backend_name: str


class RedisDeduplicator:
    def __init__(self, redis_client: object, ttl_seconds: int, logger: logging.Logger) -> None:
        self._redis = redis_client
        self._ttl_seconds = max(1, ttl_seconds)
        self._logger = logger
        self._pending: set[str] = set()
        self._lock = Lock()

    def reserve_batch(self, event_ids: list[str]) -> set[str]:
        if not event_ids:
            return set()

        duplicates: set[str] = set()
        unique_event_ids = _unique_ids(event_ids)

        with self._lock:
            committed_duplicates = self._lookup_committed_duplicates(unique_event_ids)
            duplicates.update(committed_duplicates)

            for event_id in unique_event_ids:
                if event_id in committed_duplicates:
                    continue
                if event_id in self._pending:
                    duplicates.add(event_id)
                    continue
                self._pending.add(event_id)

        return duplicates

    def commit_reserved(self, event_ids: list[str]) -> None:
        committed_ids = _unique_ids(event_ids)
        if not committed_ids:
            return

        keys = [dedup_key(event_id) for event_id in committed_ids]
        try:
            pipe = self._redis.pipeline(transaction=False)
            for key in keys:
                pipe.set(name=key, value="1", ex=self._ttl_seconds)
            pipe.execute()
        except Exception as exc:  # noqa: BLE001
            self._logger.error(
                "processor_dedup_redis_commit_failed",
                extra={"batch_size": len(committed_ids), "error": str(exc)},
            )
            raise
        finally:
            with self._lock:
                for event_id in committed_ids:
                    self._pending.discard(event_id)

    def release_reserved(self, event_ids: list[str]) -> None:
        released_ids = _unique_ids(event_ids)
        if not released_ids:
            return
        with self._lock:
            for event_id in released_ids:
                self._pending.discard(event_id)

    def _lookup_committed_duplicates(self, event_ids: list[str]) -> set[str]:
        if not event_ids:
            return set()

        keys = [dedup_key(event_id) for event_id in event_ids]
        try:
            pipe = self._redis.pipeline(transaction=False)
            for key in keys:
                pipe.exists(key)
            results = pipe.execute()
            return {event_id for event_id, exists in zip(event_ids, results) if bool(exists)}
        except Exception as exc:  # noqa: BLE001
            self._logger.error(
                "processor_dedup_redis_reserve_failed_open",
                extra={"batch_size": len(event_ids), "error": str(exc)},
            )
            return set()


class InMemoryDeduplicator:
    def __init__(self, ttl_seconds: int) -> None:
        self._ttl_seconds = max(1, ttl_seconds)
        self._expires_at: dict[str, datetime] = {}
        self._pending: set[str] = set()
        self._lock = Lock()

    def reserve_batch(self, event_ids: list[str]) -> set[str]:
        if not event_ids:
            return set()

        now = datetime.now(timezone.utc)
        duplicates: set[str] = set()

        with self._lock:
            self._cleanup(now)
            for event_id in _unique_ids(event_ids):
                committed_until = self._expires_at.get(event_id)
                if committed_until is not None and committed_until > now:
                    duplicates.add(event_id)
                    continue

                if event_id in self._pending:
                    duplicates.add(event_id)
                    continue

                self._pending.add(event_id)

        return duplicates

    def commit_reserved(self, event_ids: list[str]) -> None:
        now = datetime.now(timezone.utc)
        expires = now + timedelta(seconds=self._ttl_seconds)
        with self._lock:
            self._cleanup(now)
            for event_id in _unique_ids(event_ids):
                self._expires_at[event_id] = expires
                self._pending.discard(event_id)

    def release_reserved(self, event_ids: list[str]) -> None:
        with self._lock:
            for event_id in _unique_ids(event_ids):
                self._pending.discard(event_id)

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


def _unique_ids(event_ids: list[str]) -> list[str]:
    unique: list[str] = []
    seen: set[str] = set()
    for event_id in event_ids:
        if event_id in seen:
            continue
        seen.add(event_id)
        unique.append(event_id)
    return unique
