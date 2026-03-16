from __future__ import annotations

import logging
import unittest
from types import SimpleNamespace

from src.processor.consumer import KafkaEventConsumer
from src.processor.main import _commit_worker_acks


class FakeKafkaConsumerClient:
    def __init__(self) -> None:
        self.commits: list[object] = []

    def poll(self, timeout_ms: int = 0, max_records: int = 0):
        return {}

    def commit(self, offsets=None) -> None:
        self.commits.append(offsets)

    def assignment(self):
        return []

    def close(self) -> None:
        return None


class FakeThreeArgOffsetAndMetadata:
    def __init__(self, offset: int, metadata: str, leader_epoch: int) -> None:
        self.offset = offset
        self.metadata = metadata
        self.leader_epoch = leader_epoch


class FakeTwoArgOffsetAndMetadata:
    def __init__(self, offset: int, metadata: str) -> None:
        self.offset = offset
        self.metadata = metadata


class FakeTopicPartition:
    def __init__(self, topic: str, partition: int) -> None:
        self.topic = topic
        self.partition = partition

    def __hash__(self) -> int:
        return hash((self.topic, self.partition))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, FakeTopicPartition):
            return False
        return self.topic == other.topic and self.partition == other.partition


class FakeSinkWorker:
    def __init__(self, checkpoints: dict[tuple[str, int], int]) -> None:
        self._checkpoints = checkpoints

    def drain_acknowledged_checkpoints(self) -> dict[tuple[str, int], int]:
        checkpoints = dict(self._checkpoints)
        self._checkpoints = {}
        return checkpoints


class FakePollConsumerClient(FakeKafkaConsumerClient):
    def __init__(self, records) -> None:
        super().__init__()
        self._records = records

    def poll(self, timeout_ms: int = 0, max_records: int = 0):
        return self._records


class KafkaConsumerCommitCompatibilityTests(unittest.TestCase):
    def test_build_commit_offsets_supports_leader_epoch_constructor(self) -> None:
        consumer = _build_consumer(
            topic_partition_cls=FakeTopicPartition,
            offset_and_metadata_cls=FakeThreeArgOffsetAndMetadata,
        )
        offsets = consumer._build_commit_offsets({("cs.ev.events.raw", 2): 42})

        [(topic_partition, metadata)] = list(offsets.items())
        self.assertEqual(topic_partition.topic, "cs.ev.events.raw")
        self.assertEqual(topic_partition.partition, 2)
        self.assertEqual(metadata.offset, 42)
        self.assertEqual(metadata.metadata, "")
        self.assertEqual(metadata.leader_epoch, -1)
        self.assertEqual(consumer._commit_constructor_mode, "offset_metadata_leader_epoch")

    def test_build_commit_offsets_falls_back_to_legacy_constructor(self) -> None:
        consumer = _build_consumer(
            topic_partition_cls=FakeTopicPartition,
            offset_and_metadata_cls=FakeTwoArgOffsetAndMetadata,
        )
        offsets = consumer._build_commit_offsets({("cs.ev.events.raw", 0): 7})

        [(topic_partition, metadata)] = list(offsets.items())
        self.assertEqual(topic_partition.topic, "cs.ev.events.raw")
        self.assertEqual(topic_partition.partition, 0)
        self.assertEqual(metadata.offset, 7)
        self.assertEqual(metadata.metadata, "")
        self.assertEqual(consumer._commit_constructor_mode, "offset_metadata_legacy")

    def test_commit_worker_acks_uses_real_consumer_commit_path(self) -> None:
        client = FakeKafkaConsumerClient()
        consumer = _build_consumer(
            client=client,
            topic_partition_cls=FakeTopicPartition,
            offset_and_metadata_cls=FakeThreeArgOffsetAndMetadata,
        )
        _commit_worker_acks(FakeSinkWorker({("cs.ev.events.raw", 1): 33}), consumer)

        [offsets] = client.commits
        [(topic_partition, metadata)] = list(offsets.items())
        self.assertEqual(topic_partition.topic, "cs.ev.events.raw")
        self.assertEqual(topic_partition.partition, 1)
        self.assertEqual(metadata.offset, 33)
        self.assertEqual(metadata.leader_epoch, -1)

    def test_poll_preserves_per_partition_order_without_global_sort(self) -> None:
        partition_one = FakeTopicPartition("cs.ev.events.raw", 1)
        partition_zero = FakeTopicPartition("cs.ev.events.raw", 0)
        consumer = _build_consumer(
            client=FakePollConsumerClient(
                {
                    partition_one: [_record(3), _record(4)],
                    partition_zero: [_record(1), _record(2)],
                }
            )
        )

        messages = consumer.poll()

        self.assertEqual(
            [(message.partition, message.offset) for message in messages],
            [(1, 3), (1, 4), (0, 1), (0, 2)],
        )


def _build_consumer(
    client: FakeKafkaConsumerClient | None = None,
    topic_partition_cls: object | None = None,
    offset_and_metadata_cls: object | None = None,
) -> KafkaEventConsumer:
    return KafkaEventConsumer(
        bootstrap_servers="localhost:29092",
        topic="cs.ev.events.raw",
        consumer_group="cs-processor",
        poll_timeout_ms=10,
        max_poll_records=10,
        auto_offset_reset="latest",
        logger=logging.getLogger("consumer-test"),
        consumer_client=client or FakeKafkaConsumerClient(),
        topic_partition_cls=topic_partition_cls,
        offset_and_metadata_cls=offset_and_metadata_cls,
    )


def _record(offset: int):
    return SimpleNamespace(offset=offset, key=None, value=b"{}", timestamp=None)


if __name__ == "__main__":
    unittest.main()
