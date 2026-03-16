from __future__ import annotations

import logging
import unittest
from datetime import datetime, timedelta, timezone

from src.common.event_types import EventType
from src.common.schemas.event_envelope import EventEnvelope, EventLocation
from src.common.schemas.event_payloads import MeterUpdatePayload, SessionStartPayload, StatusChangePayload
from src.processor.models import AcceptedEventTelemetry, RedisBatchInput, RedisMutation
from src.processor.parser import parse_and_validate_message
from src.processor.sinks.redis_sink import RedisStateSink
from src.processor.state.session_state import SessionStateStore


class ParsedMessageTests(unittest.TestCase):
    def test_fused_parser_reports_malformed_json(self) -> None:
        result = parse_and_validate_message(b"{bad-json")

        self.assertFalse(result.ok)
        self.assertEqual(result.error_reason, "malformed_json")
        self.assertIsNone(result.envelope)

    def test_fused_parser_reports_missing_required_field(self) -> None:
        result = parse_and_validate_message(
            b'{"event_id":"evt-1","event_type":"SESSION_START","event_time":"2026-01-01T00:00:00+00:00","ingest_time":"2026-01-01T00:00:01+00:00","connector_id":"1","operator_id":"op","session_id":"s1","schema_version":"1.0","producer_id":"sim","sequence_no":0,"location":{},"payload":{"initial_meter_kwh":1.0}}'
        )

        self.assertFalse(result.ok)
        self.assertIn("Missing required fields: station_id", result.error_reason or "")

    def test_fused_parser_keeps_compact_payload_json_for_valid_events(self) -> None:
        result = parse_and_validate_message(
            b'{"event_id":"evt-1","event_type":"SESSION_START","event_time":"2026-01-01T00:00:00+00:00","ingest_time":"2026-01-01T00:00:01+00:00","station_id":"station-1","connector_id":"1","operator_id":"op","session_id":"s1","schema_version":"1.0","producer_id":"sim","sequence_no":0,"location":{"city":"Seattle","country":"US"},"payload":{"initial_meter_kwh":1.0,"vehicle_brand":"Tesla"}}'
        )

        self.assertTrue(result.ok)
        self.assertEqual(result.payload_json, '{"initial_meter_kwh":1.0,"vehicle_brand":"Tesla"}')


class SessionSnapshotExportTests(unittest.TestCase):
    def test_exported_snapshot_does_not_track_later_in_store_mutations(self) -> None:
        store = SessionStateStore()
        start_result = store.apply_event(_session_start_event())

        assert start_result.snapshot is not None
        initial_snapshot = start_result.snapshot

        update_result = store.apply_event(
            _meter_update_event(),
            existing_snapshot=store.peek_session("session-1"),
        )

        self.assertEqual(initial_snapshot.meter_update_count, 0)
        self.assertEqual(initial_snapshot.total_energy_kwh, 0.0)
        self.assertEqual(initial_snapshot.current_status, "active")
        self.assertIsNotNone(update_result.snapshot)
        self.assertEqual(update_result.snapshot.meter_update_count, 1)
        self.assertEqual(update_result.snapshot.current_status, "charging")


class RedisDirectCoalescingTests(unittest.TestCase):
    def test_direct_builder_coalesces_inputs_and_prefers_last_on_tie(self) -> None:
        sink = RedisStateSink(redis_client=None, logger=logging.getLogger("redis-hot-path-test"))
        event_time = datetime(2026, 1, 1, tzinfo=timezone.utc)

        built = sink.build_mutations_for_inputs(
            [
                _redis_input(_status_change_event(event_time=event_time, new_status="charging")),
                _redis_input(_status_change_event(event_time=event_time, new_status="available")),
            ]
        )

        self.assertEqual(len(built), 2)
        by_key = {mutation.key: mutation for mutation in built}
        self.assertEqual(by_key["station:station-1:state"].fields["current_status"], "available")
        self.assertEqual(by_key["station:station-1:connector:1:state"].fields["status"], "available")

    def test_batch_builder_merges_extra_mutations(self) -> None:
        sink = RedisStateSink(redis_client=None, logger=logging.getLogger("redis-hot-path-test"))
        event_time = datetime(2026, 1, 1, tzinfo=timezone.utc)

        built = sink.build_mutations_for_batch(
            inputs=[_redis_input(_status_change_event(event_time=event_time, new_status="charging"))],
            extra_mutations=[
                RedisMutation(
                    key="station:station-1:state",
                    event_time=event_time + timedelta(seconds=1),
                    fields={"current_status": "faulted"},
                )
            ],
        )

        by_key = {mutation.key: mutation for mutation in built}
        self.assertEqual(by_key["station:station-1:state"].fields["current_status"], "faulted")


def _session_start_event() -> EventEnvelope:
    event_time = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return EventEnvelope(
        event_id="evt-start",
        event_type=EventType.SESSION_START,
        event_time=event_time,
        ingest_time=event_time + timedelta(seconds=1),
        station_id="station-1",
        connector_id="1",
        operator_id="op",
        session_id="session-1",
        schema_version="1.0",
        producer_id="sim",
        sequence_no=0,
        location=EventLocation(city="Seattle", country="US", latitude=47.6, longitude=-122.3),
        payload=SessionStartPayload(initial_meter_kwh=0.0, vehicle_brand="Tesla"),
    )


def _meter_update_event() -> EventEnvelope:
    event_time = datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=10)
    return EventEnvelope(
        event_id="evt-meter-1",
        event_type=EventType.METER_UPDATE,
        event_time=event_time,
        ingest_time=event_time + timedelta(seconds=1),
        station_id="station-1",
        connector_id="1",
        operator_id="op",
        session_id="session-1",
        schema_version="1.0",
        producer_id="sim",
        sequence_no=1,
        location=EventLocation(city="Seattle", country="US", latitude=47.6, longitude=-122.3),
        payload=MeterUpdatePayload(meter_kwh=1.5, energy_delta_kwh=1.5, power_kw=11.0),
    )


def _status_change_event(*, event_time: datetime, new_status: str) -> EventEnvelope:
    return EventEnvelope(
        event_id=f"evt-{new_status}",
        event_type=EventType.STATUS_CHANGE,
        event_time=event_time,
        ingest_time=event_time + timedelta(milliseconds=50),
        station_id="station-1",
        connector_id="1",
        operator_id="op",
        session_id=None,
        schema_version="1.0",
        producer_id="sim",
        sequence_no=0,
        location=EventLocation(city="Seattle", country="US", latitude=47.6, longitude=-122.3),
        payload=StatusChangePayload(previous_status="charging", new_status=new_status),
    )


def _redis_input(event: EventEnvelope) -> RedisBatchInput:
    return RedisBatchInput(
        event=event,
        session_snapshot=None,
        session_mutation_applied=False,
        active_session_count=0,
        telemetry=AcceptedEventTelemetry(
            event_id=event.event_id,
            event_type=event.event_type.value,
            event_time=event.event_time,
            received_at=event.ingest_time,
            processing_started_monotonic=0.0,
        ),
    )


if __name__ == "__main__":
    unittest.main()
