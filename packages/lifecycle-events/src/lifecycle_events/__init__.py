from __future__ import annotations

from typing import TYPE_CHECKING, Any

from lifecycle_events.models import (
    MAX_EVENT_CONTEXT_BYTES,
    EventContext,
    EventPage,
    LifecycleEvent,
    lifecycle_event,
    normalize_event_context,
)
from lifecycle_events.sqlite_store import (
    SQLiteEventCursorStore,
    SQLiteLifecycleEventLog,
    create_lifecycle_event_schema,
)

if TYPE_CHECKING:
    from lifecycle_events.client import LifecycleEventClient


def __getattr__(name: str) -> Any:
    """Keep model-only imports free of the optional HTTP client runtime."""

    if name == "LifecycleEventClient":
        from lifecycle_events.client import LifecycleEventClient

        return LifecycleEventClient
    raise AttributeError(name)


__all__ = [
    "MAX_EVENT_CONTEXT_BYTES",
    "LifecycleEvent",
    "EventContext",
    "EventPage",
    "LifecycleEventClient",
    "SQLiteEventCursorStore",
    "SQLiteLifecycleEventLog",
    "lifecycle_event",
    "create_lifecycle_event_schema",
    "normalize_event_context",
]
