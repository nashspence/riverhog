"""Keep a finalizer's original report selection across failed receiving."""

from __future__ import annotations

from riverhog_core.catalog_db import SessionFactory
from riverhog_core.services.archive_finalizer_terminal import TerminalDrainStatus
from riverhog_core.services.immutable_archive_receiving import OriginalImmutableArchiveReport


class ArchiveFinalizerReports:
    """Called under the containing finalizer's lock and construction reservation.

    A successful bounded turn releases only this local selection. A failed turn
    retains the exact report owner even when its semantic source has no live
    provider actor. The containing owner supplies terminal validation and actual
    worker receiving; this helper provides no independent closure authority.
    """

    def __init__(self, factory: SessionFactory, collection_id: int) -> None:
        self._factory = factory
        self._collection_id = collection_id
        self._original: OriginalImmutableArchiveReport | None = None

    @property
    def has_pending(self) -> bool:
        return self._original is not None

    def advance(self) -> bool:
        if self._original is None:
            self._original = OriginalImmutableArchiveReport(self._factory, self._collection_id)
        progressed = self._original.advance()
        self._original = None
        return progressed

    def drain_terminal(self, operation_id: str) -> TerminalDrainStatus:
        if self._original is None:
            return "closed"
        return self._original.drain_terminal(operation_id)
