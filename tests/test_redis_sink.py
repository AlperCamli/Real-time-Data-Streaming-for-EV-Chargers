from __future__ import annotations

import logging
import unittest
from datetime import datetime, timedelta, timezone

from src.processor.models import RedisMutation
from src.processor.sinks.redis_sink import RedisStateSink


class FakePipeline:
    def __init__(self, redis_client: "FakeRedis") -> None:
        self._redis = redis_client
        self._calls: list[tuple[str, int, str, tuple[str, ...]]] = []

    def eval(self, script: str, keys_count: int, key: str, *argv: str) -> None:
        self._calls.append((script, keys_count, key, argv))

    def execute(self) -> list[int]:
        self._redis.executed_batches.append(list(self._calls))
        return [1] * len(self._calls)


class FakeRedis:
    def __init__(self) -> None:
        self.executed_batches: list[list[tuple[str, int, str, tuple[str, ...]]]] = []

    def pipeline(self, transaction: bool = False) -> FakePipeline:
        return FakePipeline(self)


class RedisSinkBatchingTests(unittest.TestCase):
    def test_same_cycle_updates_coalesce_and_embed_ttl_in_guarded_write(self) -> None:
        fake_redis = FakeRedis()
        sink = RedisStateSink(
            redis_client=fake_redis,
            logger=logging.getLogger("redis-test"),
            session_state_ttl_seconds=7200,
            finalized_session_ttl_seconds=3600,
        )

        now = datetime.now(timezone.utc)
        mutations = [
            RedisMutation("station:s1:state", now, {"status": "charging"}),
            RedisMutation("station:s1:state", now + timedelta(seconds=1), {"status": "available"}),
            RedisMutation("station:s1:connector:c1:state", now, {"status": "charging"}),
            RedisMutation("station:s1:connector:c1:state", now + timedelta(seconds=1), {"status": "available"}),
            RedisMutation("session:abc:state", now, {"status": "active"}, ttl_seconds=7200),
            RedisMutation("session:abc:state", now + timedelta(seconds=1), {"status": "finalized"}, ttl_seconds=3600),
        ]

        result = sink.apply_mutations(mutations)

        self.assertEqual(result.applied_keys, 3)
        self.assertEqual(len(fake_redis.executed_batches), 1)
        executed = fake_redis.executed_batches[0]
        self.assertEqual(len(executed), 3)

        by_key = {key: argv for _, _, key, argv in executed}
        self.assertIn("station:s1:state", by_key)
        self.assertIn("station:s1:connector:c1:state", by_key)
        self.assertIn("session:abc:state", by_key)
        self.assertEqual(by_key["session:abc:state"][1], "3600")
        self.assertIn("status", by_key["session:abc:state"])
        self.assertIn("finalized", by_key["session:abc:state"])


if __name__ == "__main__":
    unittest.main()
