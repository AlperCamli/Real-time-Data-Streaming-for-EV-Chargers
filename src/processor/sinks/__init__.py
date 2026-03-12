"""Processor sink adapters."""

from src.processor.sinks.clickhouse_sink import ClickHouseSink
from src.processor.sinks.kafka_dlq import KafkaDlqSink, KafkaJsonTopicSink
from src.processor.sinks.redis_sink import RedisApplyResult, RedisStateSink, build_redis_client

__all__ = [
    "ClickHouseSink",
    "KafkaDlqSink",
    "KafkaJsonTopicSink",
    "RedisApplyResult",
    "RedisStateSink",
    "build_redis_client",
]
