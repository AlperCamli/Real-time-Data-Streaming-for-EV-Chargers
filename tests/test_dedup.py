from __future__ import annotations

import unittest

from src.processor.dedup import InMemoryDeduplicator


class StagedDedupTests(unittest.TestCase):
    def test_committed_duplicate_is_dropped(self) -> None:
        dedup = InMemoryDeduplicator(ttl_seconds=60)

        self.assertEqual(dedup.reserve_batch(["evt-1"]), set())
        dedup.commit_reserved(["evt-1"])

        self.assertEqual(dedup.reserve_batch(["evt-1"]), {"evt-1"})

    def test_reserved_unacked_duplicate_is_dropped(self) -> None:
        dedup = InMemoryDeduplicator(ttl_seconds=60)

        self.assertEqual(dedup.reserve_batch(["evt-1"]), set())
        self.assertEqual(dedup.reserve_batch(["evt-1"]), {"evt-1"})

    def test_released_reservation_can_be_retried(self) -> None:
        dedup = InMemoryDeduplicator(ttl_seconds=60)

        self.assertEqual(dedup.reserve_batch(["evt-1"]), set())
        dedup.release_reserved(["evt-1"])

        self.assertEqual(dedup.reserve_batch(["evt-1"]), set())


if __name__ == "__main__":
    unittest.main()
