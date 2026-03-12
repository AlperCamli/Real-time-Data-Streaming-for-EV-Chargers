"""Frozen Kafka topic names."""

TOPIC_EVENTS_RAW = "cs.ev.events.raw"
TOPIC_EVENTS_DLQ = "cs.ev.events.dlq"
TOPIC_EVENTS_LATE = "cs.ev.events.late"

ALL_TOPICS: tuple[str, ...] = (
    TOPIC_EVENTS_RAW,
    TOPIC_EVENTS_DLQ,
    TOPIC_EVENTS_LATE,
)
