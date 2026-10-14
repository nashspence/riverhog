"""Local contact isolation; durable invocation state stays with its owning service."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import TypeVar

import httpx
from http_api_contracts.control import ControlBudgetExhausted, check_control_budget

T = TypeVar("T")


class ContactDeferred(TimeoutError):
    """This component is already contacted or is waiting for its retry time."""


@dataclass(slots=True)
class _Contact:
    active: bool = False
    failures: int = 0
    due: float = 0.0


class ControlContacts:
    def __init__(self, registrations: Iterable[str]) -> None:
        self._lock = threading.Lock()
        self._contacts = {key: _Contact() for key in registrations}

    def call(self, registration_id: str, contact: Callable[[], T]) -> T:
        check_control_budget()
        with self._lock:
            state = self._contacts[registration_id]
            if state.active or state.due > time.monotonic():
                raise ContactDeferred("extension contact is deferred")
            state.active = True
        unavailable = False
        try:
            return contact()
        except Exception as exc:
            unavailable = (
                isinstance(exc, (httpx.TransportError, ControlBudgetExhausted))
                or getattr(exc, "failure_kind", None) == "transport"
                or (getattr(exc, "observed_status", None) or 0) >= 500
            )
            raise
        finally:
            with self._lock:
                state.active = False
                if unavailable:
                    state.failures += 1
                    state.due = time.monotonic() + min(30, 2 ** min(state.failures, 5))
                else:
                    state.failures = 0
                    state.due = 0.0
