"""The complete common observer interface and the bounded-source adapter port."""

from __future__ import annotations

from contextlib import AbstractContextManager
from typing import Protocol, runtime_checkable

from .model import ObservationRequest, ObservationResult, ObservationSession


@runtime_checkable
class ObservationSource(Protocol):
    def open(self, *, observer_agent_id: str) -> AbstractContextManager[ObservationSession]:
        """Open one explicitly bounded primary sequence and expose only known context.

        Implementations must not fabricate a pathname, host, native handle,
        historical state, custody chain, or payload-derived technical metadata.
        An unbounded source must be adapted to a declared segment first.
        """
        ...


@runtime_checkable
class StateObserver(Protocol):
    def observe(
        self, source: ObservationSource, request: ObservationRequest | None = None
    ) -> ObservationResult:
        """Return an attributable measurement of a bounded opaque sequence."""
        ...
