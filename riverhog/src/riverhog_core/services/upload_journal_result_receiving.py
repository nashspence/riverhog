"""Original application receiving through partial admission and final Native readers."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import ExitStack

from riverhog_provenance import JournalConstruction, JournalResultOpening, JournalSummary

from riverhog_core.journal_validation_workspace_lease import ValidationWorkspaceLease


class UploadJournalResultOpening:
    """Retain the actual lease and partial Native opening before their first effects."""

    def __init__(self, construction: JournalConstruction) -> None:
        self.construction = construction
        self.lease = ValidationWorkspaceLease()
        self.resources = ExitStack()
        self.native: JournalResultOpening | None = None
        self.result: JournalSummary | None = None
        self.started = False
        self.transferred = False

    def retain_native(self, opening: JournalResultOpening) -> None:
        if self.native is not None:
            raise RuntimeError("sealed result already retains its original Native opening")
        self.native = opening

    def receive_workspace(self) -> None:
        if not self.construction.dependents_received:
            raise BlockingIOError("original result construction dependents have not received")
        self.resources.close()
        # ExitStack removes a context callback before calling it. Always retry
        # this same explicit frame even after the body callback raised.
        self.lease.close()

    def close(self) -> None:
        if self.result is not None:
            self.result.close()
            if self.result.storage_received:
                self.result = None
        if self.native is not None:
            self.native.close()
            self.native = None
        if not self.construction.close_step():
            raise BlockingIOError("original result construction has not received")
        if not self.transferred:
            self.receive_workspace()
        else:
            # Final Native storage owns the body until its last reader closes.
            # An active body or original duplicate refuses premature receiving.
            self.lease.close()
        if self.result is not None:
            raise BlockingIOError("final Native readers still retain the original storage")


class UploadJournalResultResources:
    """Three retained openings beneath the original bounded catalog actor.

    Two warm journals and one opening cover a selected index step. Receiving
    precedes removal; failed construction or close keeps the same exact opening.
    """

    def __init__(self, construction_factory: Callable[[], JournalConstruction]) -> None:
        self._construction_factory = construction_factory
        self._openings: list[UploadJournalResultOpening] = []

    @property
    def pending(self) -> bool:
        return bool(self._openings)

    def retain(self) -> UploadJournalResultOpening:
        if len(self._openings) >= 3:
            raise BlockingIOError("original sealed result openings still occupy this catalog turn")
        original = UploadJournalResultOpening(self._construction_factory())
        self._openings.append(original)
        return original

    def receive(self, original: UploadJournalResultOpening) -> None:
        if original not in self._openings:
            raise ValueError("sealed result belongs to another original catalog turn")
        original.close()
        self._openings.remove(original)

    def close(self) -> None:
        while self._openings:
            original = self._openings[-1]
            original.close()
            self._openings.pop()
