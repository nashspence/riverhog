"""Bound the caller's whole metadata contact, including DNS and trickled IO."""

from __future__ import annotations

import threading
from collections.abc import Callable
from concurrent.futures import Future
from concurrent.futures import TimeoutError as FutureTimeout
from contextvars import copy_context
from functools import wraps
from typing import Concatenate, Protocol

from http_api_contracts.control import ControlBudgetExhausted, control_budget, control_timeout


class MetadataClient(Protocol):
    @property
    def base_url(self) -> str: ...


class MetadataContactDeferred(ControlBudgetExhausted):
    """An actual contact still owns this origin or the finite IO capacity."""


class BoundedMetadataContacts:
    """No waiting queue and no timeout-based release of a still-running call.

    These workers perform metadata/control IO only. They never acquire execution
    resources, read artifact contents or perform a payload. A timed-out call may
    have reached the remote owner; exact idempotent invocation IDs remain required.
    Its capacity remains occupied until that contact actually returns. Responses
    are never reused as current state. Other origins retain independent capacity.
    """

    def __init__(self, maximum_contacts: int = 16) -> None:
        if type(maximum_contacts) is not int or maximum_contacts < 1:
            raise ValueError("metadata contact capacity must be positive")
        self._maximum = maximum_contacts
        self._lock = threading.Lock()
        self._active: set[str] = set()

    def call[T](self, origin: str, contact: Callable[[], T], *, maximum_seconds: float = 5.0) -> T:
        allowance = control_timeout(maximum_seconds)
        with self._lock:
            if origin in self._active or len(self._active) >= self._maximum:
                raise MetadataContactDeferred(
                    "metadata contact is deferred until actual IO capacity is free"
                )
            self._active.add(origin)
        result: Future[T] = Future()
        context = copy_context()

        def run() -> None:
            try:
                with control_budget(allowance):
                    value = contact()
                result.set_result(value)
            except BaseException as exc:
                result.set_exception(exc)
            finally:
                with self._lock:
                    self._active.remove(origin)

        thread = threading.Thread(
            target=context.run, args=(run,), daemon=True, name="http-metadata-contact"
        )
        try:
            thread.start()
        except BaseException:
            with self._lock:
                self._active.remove(origin)
            raise
        try:
            return result.result(timeout=allowance)
        except FutureTimeout as exc:
            if result.done():
                # A contact itself may raise TimeoutError; preserve that error.
                return result.result()
            raise ControlBudgetExhausted(
                "whole HTTP metadata contact allowance is exhausted"
            ) from exc


_CONTACTS = BoundedMetadataContacts()


def metadata_contact[C: MetadataClient, **P, T](
    method: Callable[Concatenate[C, P], T],
) -> Callable[Concatenate[C, P], T]:
    """Wrap one synchronous client's metadata-only transport method."""

    @wraps(method)
    def call(self: C, /, *args: P.args, **kwargs: P.kwargs) -> T:
        return _CONTACTS.call(
            self.base_url,
            lambda: method(self, *args, **kwargs),
            maximum_seconds=getattr(self, "timeout", 5.0),
        )

    return call
