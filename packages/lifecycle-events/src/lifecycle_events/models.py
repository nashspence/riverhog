from __future__ import annotations

import json
import uuid
from collections.abc import Mapping
from datetime import datetime
from typing import Annotated, Any

from pydantic import BaseModel, ConfigDict, Field
from time_formats import CanonicalUtcTimestamp, format_utc_timestamp, utc_now

MAX_EVENT_CONTEXT_BYTES = 4096
EventContext = Annotated[
    dict[str, Any],
    Field(
        json_schema_extra={
            "x-riverhog-extent": {
                "policy": "contract_max",
                "reason": "bounded-lifecycle-event-context",
            },
            "x-riverhog-encoded-bytes-max": MAX_EVENT_CONTEXT_BYTES,
        }
    ),
]


def event_time(value: datetime | None = None) -> str:
    return format_utc_timestamp(value or utc_now())


class LifecycleEvent(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    id: str = Field(min_length=1)
    type: str = Field(min_length=1)
    subject: str | None = Field(default=None, min_length=1)
    occurred_at: CanonicalUtcTimestamp
    payload: dict[str, Any] = Field(default_factory=dict)


class EventPage(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    events: list[LifecycleEvent]
    next_cursor: str
    has_more: bool

    def require_progress_after(self, cursor: str) -> None:
        """Reject a nonempty page that cannot advance an opaque consumer cursor."""
        if self.events and self.next_cursor == cursor:
            raise ValueError("nonempty lifecycle-event page did not advance its cursor")


def lifecycle_event(
    *,
    type: str,
    payload: Mapping[str, Any] | None = None,
    subject: str | None = None,
    occurred_at: datetime | None = None,
    event_id: str | None = None,
) -> LifecycleEvent:
    return LifecycleEvent(
        id=event_id or str(uuid.uuid4()),
        type=type,
        subject=subject,
        occurred_at=event_time(occurred_at),
        payload=dict(payload or {}),
    )


def normalize_event_context(
    value: Mapping[str, Any] | None,
    *,
    max_bytes: int = MAX_EVENT_CONTEXT_BYTES,
) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise ValueError("event_context must be a JSON object")
    try:
        encoded = json.dumps(
            dict(value),
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ValueError("event_context must contain only JSON values") from exc
    if len(encoded) > max_bytes:
        raise ValueError(f"event_context must be at most {max_bytes} UTF-8 JSON bytes")
    decoded = json.loads(encoded)
    if not isinstance(decoded, dict):  # pragma: no cover - guarded by Mapping above
        raise ValueError("event_context must be a JSON object")
    return {str(key): item for key, item in decoded.items()}


__all__ = [
    "MAX_EVENT_CONTEXT_BYTES",
    "LifecycleEvent",
    "EventContext",
    "EventPage",
    "lifecycle_event",
    "event_time",
    "normalize_event_context",
]
