"""Message parsing for processor ingress."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(slots=True)
class ParseResult:
    raw_payload_json: str
    event_dict: Mapping[str, Any] | None
    error_reason: str | None = None

    @property
    def ok(self) -> bool:
        return self.error_reason is None and self.event_dict is not None


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
