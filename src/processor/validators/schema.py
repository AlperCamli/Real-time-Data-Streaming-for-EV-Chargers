"""Schema-level envelope validation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from src.common.schemas.event_envelope import EventEnvelope
from src.common.schemas.validation import ValidationError


@dataclass(slots=True)
class SchemaValidationResult:
    envelope: EventEnvelope | None
    errors: list[str]

    @property
    def ok(self) -> bool:
        return self.envelope is not None and not self.errors


def validate_envelope_schema(raw_event: Mapping[str, Any]) -> SchemaValidationResult:
    try:
        envelope, _ = EventEnvelope.from_dict_with_payload_json(raw_event)
    except (ValidationError, ValueError, TypeError) as exc:
        return SchemaValidationResult(envelope=None, errors=[str(exc)])

    return SchemaValidationResult(envelope=envelope, errors=[])
