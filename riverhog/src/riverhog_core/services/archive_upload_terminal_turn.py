"""One original native terminal decision/retirement execution quantum."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from threading import Lock
from typing import Literal

from riverhog_protocol.errors import Conflict
from sqlalchemy.orm import Session
from state_schema import read_snapshot

from riverhog_core.catalog_db import SessionFactory
from riverhog_core.services.archive_upload_drain import read_terminal_selection
from riverhog_core.services.archive_upload_terminal_adjudication import (
    ArchiveUploadTerminalAdjudication,
    PendingReason,
)
from riverhog_core.services.archive_upload_terminal_retirement import (
    ArchiveUploadTerminalRetirement,
)
from riverhog_core.services.physical_execution import PhysicalExecutionKey


@dataclass(frozen=True, slots=True)
class TerminalTurnProgress:
    """Internal progress, never a confirmed public cancel/discard result."""

    phase: str | None
    pending_reason: (
        PendingReason | Literal["actor-busy", "global-decision", "retained-originals"] | None
    ) = None
    retired_originals: int = 0
    originals_retired: bool = False


class ArchiveUploadTerminalTurn:
    """Retain original scope and current-authority caching across bounded turns.

    Construction reads immutable labels only. The serialized execution turn
    checks current authority through its semantic owners before adjudication
    or local release. This unit belongs inside the service's charged original
    terminal actor and shared physical owner, using the operation scope rather
    than the original construction's scope.

    Original drainage/receiving and independent cache pin release remain with
    their existing maintenance owners. The phase and local-retirement fields
    grant no new disposal permission or confirmed outcome. Provider, hidden
    registration and catalog teardown retain their own execution steps.
    """

    def __init__(
        self,
        factory: SessionFactory,
        collection_id: int,
        operation_id: str,
        *,
        guard: Callable[[Session], None],
        adjudication: ArchiveUploadTerminalAdjudication,
        retirement: ArchiveUploadTerminalRetirement,
    ) -> None:
        if not callable(guard):
            raise ValueError("terminal execution requires current authority")
        key = PhysicalExecutionKey(collection_id, operation_id)
        with read_snapshot(factory) as session:
            control, _, _ = read_terminal_selection(session, collection_id, operation_id)
            target = json.loads(control.construction_json)["archive_incarnation_id"]
            if not isinstance(target, str) or not target:
                raise Conflict("upload terminal original target is invalid")
        self._key = key
        self._target = target
        self._guard = guard
        self._adjudication = adjudication
        self._retirement = retirement
        self._lock = Lock()

    @property
    def key(self) -> PhysicalExecutionKey:
        return self._key

    @property
    def target_incarnation(self) -> str:
        return self._target

    def advance(self, *, limit: int) -> TerminalTurnProgress:
        """Execute bounded semantic work; no joins, provider IO or new scheduler."""
        if type(limit) is not int or limit < 1:
            raise ValueError("terminal execution retirement limit must be positive")
        if not self._lock.acquire(blocking=False):
            return TerminalTurnProgress(None, "actor-busy")
        try:
            key = self._key
            decision = self._adjudication.adjudicate(
                key.collection_id, key.scope_sha256, guard=self._guard
            )
            if decision.pending_reason is not None or decision.phase not in {
                "cleanup-ready",
                "cleaning",
                "refused-sealed",
                "needs-reconfirmation",
            }:
                return TerminalTurnProgress(decision.phase, decision.pending_reason)
            # The first decision is committed before local retirement. A lost
            # reply or a current-authority change cannot speculate release.
            retired = self._retirement.retire(
                key.collection_id, key.scope_sha256, guard=self._guard, limit=limit
            )
            return TerminalTurnProgress(
                retired.phase,
                retired.pending_reason,
                retired.retired,
                retired.pending_reason is None,
            )
        finally:
            self._lock.release()
