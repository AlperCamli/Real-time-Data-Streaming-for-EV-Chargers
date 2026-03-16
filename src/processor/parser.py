"""Message parsing for processor ingress."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Mapping

from src.common.schemas.event_envelope import EventEnvelope
from src.common.schemas.validation import ValidationError


@dataclass(slots=True)
class ParseResult:
    raw_payload_json: str
    event_dict: Mapping[str, Any] | None
    error_reason: str | None = None

    @property
    def ok(self) -> bool:
        return self.error_reason is None and self.event_dict is not None


@dataclass(slots=True)
class ParsedEventResult:
    raw_payload_json: str
    event_dict: Mapping[str, Any] | None
    envelope: EventEnvelope | None
    payload_json: str | None = None
    errors: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        return self.envelope is not None and not self.errors

    @property
    def error_reason(self) -> str | None:
        if not self.errors:
            return None
        return "; ".join(self.errors)


def parse_message_value(value: bytes | str | None) -> ParseResult:
    if value is None:
        return ParseResult(raw_payload_json="", event_dict=None, error_reason="empty_message")

    if isinstance(value, bytes):
        try:
            raw_payload = value.decode("utf-8")
        except UnicodeDecodeError:
            return ParseResult(
                raw_payload_json=value.decode("utf-8", errors="replace"),
                event_dict=None,
                error_reason="malformed_utf8",
            )
    else:
        raw_payload = value

    text = raw_payload.strip()
    if not text:
        return ParseResult(raw_payload_json=raw_payload, event_dict=None, error_reason="empty_json")

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return ParseResult(raw_payload_json=raw_payload, event_dict=None, error_reason="malformed_json")

    if not isinstance(parsed, Mapping):
        return ParseResult(raw_payload_json=raw_payload, event_dict=None, error_reason="event_must_be_json_object")

    return ParseResult(raw_payload_json=raw_payload, event_dict=parsed)


def parse_and_validate_message(value: bytes | str | None) -> ParsedEventResult:
    parse_result = parse_message_value(value)
    if not parse_result.ok:
        assert parse_result.error_reason is not None
        return ParsedEventResult(
            raw_payload_json=parse_result.raw_payload_json,
            event_dict=parse_result.event_dict,
            envelope=None,
            errors=(parse_result.error_reason,),
        )

    assert parse_result.event_dict is not None
    try:
        envelope, payload_json = EventEnvelope.from_dict_with_payload_json(parse_result.event_dict)
    except (ValidationError, ValueError, TypeError) as exc:
        return ParsedEventResult(
            raw_payload_json=parse_result.raw_payload_json,
            event_dict=parse_result.event_dict,
            envelope=None,
            errors=(str(exc),),
        )

    return ParsedEventResult(
        raw_payload_json=parse_result.raw_payload_json,
        event_dict=parse_result.event_dict,
        envelope=envelope,
        payload_json=payload_json,
    )
