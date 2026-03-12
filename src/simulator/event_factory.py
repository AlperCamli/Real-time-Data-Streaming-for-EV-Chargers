"""Factory for canonical simulator event envelopes."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from src.common.event_types import EventType
from src.common.schemas.event_envelope import EventEnvelope, EventLocation
from src.common.schemas.event_payloads import (
    FaultAlertPayload,
    HeartbeatPayload,
    MeterUpdatePayload,
    SessionStartPayload,
    SessionStopPayload,
    StatusChangePayload,
)
from src.simulator.models import ConnectorState, SessionState, StationState


class EventFactory:
    def __init__(self, producer_id: str, schema_version: str) -> None:
        self._producer_id = producer_id
        self._schema_version = schema_version

    def session_start(self, station: StationState, connector: ConnectorState, session: SessionState, event_time: datetime) -> dict[str, Any]:
        payload = SessionStartPayload(
            initial_meter_kwh=round(session.start_meter_kwh, 3),
            auth_method=session.auth_method,
            vehicle_id=session.vehicle_id,
            vehicle_brand=session.vehicle_brand,
            soc_start_percent=round(session.start_soc_percent, 2),
            target_soc_percent=round(session.target_soc_percent, 2),
        )
        return self._build_event(
            event_type=EventType.SESSION_START,
            event_time=event_time,
            station=station,
            connector_id=connector.connector_id,
            session_id=session.session_id,
            sequence_no=0,
            payload=payload,
        )

    def meter_update(
        self,
        station: StationState,
        connector: ConnectorState,
        session: SessionState,
        energy_delta_kwh: float,
        power_kw: float,
        voltage_v: float,
        current_a: float,
        event_time: datetime,
    ) -> dict[str, Any]:
        payload = MeterUpdatePayload(
            meter_kwh=round(session.current_meter_kwh, 3),
            energy_delta_kwh=round(energy_delta_kwh, 4),
            power_kw=round(power_kw, 3),
            soc_percent=round(session.current_soc_percent, 2),
            voltage_v=round(voltage_v, 2),
            current_a=round(current_a, 2),
        )
        return self._build_event(
            event_type=EventType.METER_UPDATE,
            event_time=event_time,
            station=station,
            connector_id=connector.connector_id,
            session_id=session.session_id,
            sequence_no=session.sequence_no,
            payload=payload,
        )

    def session_stop(
        self,
        station: StationState,
        connector: ConnectorState,
        session: SessionState,
        end_reason: str,
        event_time: datetime,
    ) -> dict[str, Any]:
        duration_seconds = int(max(0, (event_time - session.start_time).total_seconds()))
        payload = SessionStopPayload(
            final_meter_kwh=round(session.current_meter_kwh, 3),
            end_reason=end_reason,
            duration_seconds=duration_seconds,
            total_energy_kwh=round(session.energy_delivered_kwh, 4),
            final_soc_percent=round(session.current_soc_percent, 2),
            vehicle_brand=session.vehicle_brand,
        )
        return self._build_event(
            event_type=EventType.SESSION_STOP,
            event_time=event_time,
            station=station,
            connector_id=connector.connector_id,
            session_id=session.session_id,
            sequence_no=session.sequence_no + 1,
            payload=payload,
        )

    def status_change(
        self,
        station: StationState,
        connector_id: str,
        previous_status: str,
        new_status: str,
        reason: str,
        event_time: datetime,
        session_id: str | None,
    ) -> dict[str, Any]:
        payload = StatusChangePayload(
            previous_status=previous_status,
            new_status=new_status,
            reason=reason,
        )
        return self._build_event(
            event_type=EventType.STATUS_CHANGE,
            event_time=event_time,
            station=station,
            connector_id=connector_id,
            session_id=session_id,
            sequence_no=0,
            payload=payload,
        )

    def heartbeat(self, station: StationState, event_time: datetime) -> dict[str, Any]:
        if station.faulted_connector_count == 0:
            charger_status = "OK"
        elif station.active_session_count > 0:
            charger_status = "DEGRADED"
        else:
            charger_status = "FAULTED"

        payload = HeartbeatPayload(
            charger_status=charger_status,
            firmware_version=station.firmware_version,
            active_session_count=station.active_session_count,
            faulted_connector_count=station.faulted_connector_count,
        )
        return self._build_event(
            event_type=EventType.HEARTBEAT,
            event_time=event_time,
            station=station,
            connector_id="0",
            session_id=None,
            sequence_no=0,
            payload=payload,
        )

    def fault_alert(
        self,
        station: StationState,
        connector: ConnectorState,
        fault_code: str,
        severity: str,
        event_time: datetime,
        session_id: str | None,
    ) -> dict[str, Any]:
        payload = FaultAlertPayload(
            fault_code=fault_code,
            severity=severity,
            message=f"{fault_code} detected on connector {connector.connector_id}",
            recoverable=True,
        )
        return self._build_event(
            event_type=EventType.FAULT_ALERT,
            event_time=event_time,
            station=station,
            connector_id=connector.connector_id,
            session_id=session_id,
            sequence_no=0,
            payload=payload,
        )

    def _build_event(
        self,
        event_type: EventType,
        event_time: datetime,
        station: StationState,
        connector_id: str,
        session_id: str | None,
        sequence_no: int,
        payload: Any,
    ) -> dict[str, Any]:
        event_time_utc = event_time.astimezone(timezone.utc)
        ingest_time_utc = datetime.now(timezone.utc)
        envelope = EventEnvelope(
            event_id=str(uuid4()),
            event_type=event_type,
            event_time=event_time_utc,
            ingest_time=ingest_time_utc,
            station_id=station.station_id,
            connector_id=connector_id,
            operator_id=station.operator_id,
            session_id=session_id,
            schema_version=self._schema_version,
            producer_id=self._producer_id,
            sequence_no=max(0, sequence_no),
            location=EventLocation(
                city=station.location.city,
                country=station.location.country,
                latitude=station.location.latitude,
                longitude=station.location.longitude,
            ),
            payload=payload,
        )
        return envelope.to_dict()
