"""Release drained local originals after their committed native decision."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from riverhog_core.catalog_archive_write_models import (
    ArchiveUploadTerminalRecord,
    ArchiveWriteControlRecord,
)
from riverhog_core.catalog_db import SessionFactory, catalog_write_contention, session_scope
from riverhog_core.services.archive_finalizer_registry import ArchiveFinalizerRegistry
from riverhog_core.services.archive_upload_invocations import ArchiveUploadInvocationRegistry
from riverhog_core.services.archive_upload_terminal_adjudication import PendingReason, _pending
from riverhog_core.services.archive_upload_terminal_observation import (
    ArchiveUploadTerminalObservation,
)
from riverhog_core.services.physical_execution import PhysicalExecutionKey


@dataclass(frozen=True, slots=True)
class TerminalOriginalRetirement:
    phase: str | None
    retired: int = 0
    pending_reason: PendingReason | Literal["global-decision", "retained-originals"] | None = None
    execution_stopped: bool = False


class ArchiveUploadTerminalRetirement:
    """Compose original-local retirement without borrowing cleanup ownership.

    The committed native decision and original closed selection stay locked
    through bounded local release. Current authority and original receiving
    are checked again. An idle poll, a decision object or a phase argument
    cannot authorize release. Unknown constructors and independent cache pins
    remain with their original owners.

    This releases unused local staging after positive terminal closure, even
    when the abandoned forward step never succeeded. It leaves preparations,
    provider state, cache ownership, registration and original results intact.
    It performs no provider IO, joins, constructors or terminal confirmation.
    """

    def __init__(
        self,
        factory: SessionFactory,
        observation: ArchiveUploadTerminalObservation,
        finalizers: ArchiveFinalizerRegistry,
        invocations: ArchiveUploadInvocationRegistry,
    ) -> None:
        self._factory = factory
        self._observation = observation
        self._finalizers = finalizers
        self._invocations = invocations

    def retire(
        self,
        collection_id: int,
        operation_id: str,
        *,
        guard: Callable[[Session], None],
        limit: int,
    ) -> TerminalOriginalRetirement:
        if type(limit) is not int or limit < 1:
            raise ValueError("terminal original retirement limit must be positive")
        if not callable(guard):
            raise ValueError("terminal original retirement requires current authority")
        try:
            with session_scope(self._factory) as session:
                observed = self._observation.observe(
                    session, collection_id, operation_id, guard=guard
                )
                terminal = session.get(ArchiveUploadTerminalRecord, operation_id)
                assert terminal is not None
                if terminal.phase not in {
                    "refused-sealed",
                    "needs-reconfirmation",
                    "cleanup-ready",
                    "cleaning",
                }:
                    return TerminalOriginalRetirement(
                        terminal.phase, pending_reason="global-decision"
                    )
                reason = _pending(observed)
                if reason is not None:
                    return TerminalOriginalRetirement(terminal.phase, pending_reason=reason)
                control = session.get(ArchiveWriteControlRecord, collection_id)
                assert control is not None
                construction = control.construction_json
                key = PhysicalExecutionKey(
                    collection_id, hashlib.sha256(construction.encode()).hexdigest()
                )
                target = json.loads(construction)["archive_incarnation_id"]
                retired = int(
                    self._finalizers._retire_terminal_original(
                        key, target_incarnation=target, operation_id=operation_id
                    )
                )
                if retired < limit:
                    retired += self._invocations._retire_terminal_originals(
                        key,
                        target_incarnation=target,
                        operation_id=operation_id,
                        limit=limit - retired,
                    )
                pending = any(
                    owner.local_terminal_status(
                        key, target_incarnation=target, operation_id=operation_id
                    )
                    != "untracked"
                    for owner in (self._finalizers, self._invocations)
                )
                stopped = not pending and terminal.phase in {
                    "refused-sealed",
                    "needs-reconfirmation",
                }
                if stopped:
                    terminal.execution_next_attempt_at = None
                return TerminalOriginalRetirement(
                    terminal.phase, retired, "retained-originals" if pending else None, stopped
                )
        except OperationalError as exc:
            if catalog_write_contention(exc):
                return TerminalOriginalRetirement(None, pending_reason="catalog-busy")
            raise
