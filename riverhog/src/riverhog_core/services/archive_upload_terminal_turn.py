"""One original native terminal decision/retirement execution quantum."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass, replace
from threading import Lock
from typing import Literal

from riverhog_protocol.errors import Conflict
from sqlalchemy.orm import Session
from state_schema import read_snapshot

from riverhog_core.catalog_db import SessionFactory
from riverhog_core.incremental_emission import VolumeEmissionLimits
from riverhog_core.services.archive_upload_catalog_cleanup import ArchiveUploadCatalogCleanup
from riverhog_core.services.archive_upload_checkpoint_cleanup import ArchiveUploadCheckpointCleanup
from riverhog_core.services.archive_upload_cleanup_turn import ArchiveUploadCleanupTurn
from riverhog_core.services.archive_upload_drain import read_terminal_selection
from riverhog_core.services.archive_upload_registration_cleanup import (
    ArchiveUploadRegistrationCleanup,
)
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
        PendingReason
        | Literal[
            "actor-busy",
            "global-decision",
            "retained-originals",
            "archive-cleanup",
            "registration-cleanup",
            "catalog-cleanup",
            "checkpoint-cleanup",
        ]
        | None
    ) = None
    retired_originals: int = 0
    originals_retired: bool = False
    execution_stopped: bool = False
    archive_inventory_exhausted: bool = False
    registration_disposed: bool = False
    upload_children_exhausted: bool = False
    checkpoints_exhausted: bool = False


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
        cleanup_create: Callable[[], ArchiveUploadCleanupTurn] | None = None,
        registration_cleanup: ArchiveUploadRegistrationCleanup | None = None,
        registration_limits: VolumeEmissionLimits | None = None,
        catalog_cleanup: ArchiveUploadCatalogCleanup | None = None,
        checkpoint_cleanup: ArchiveUploadCheckpointCleanup | None = None,
    ) -> None:
        if not callable(guard):
            raise ValueError("terminal execution requires current authority")
        if cleanup_create is not None and not callable(cleanup_create):
            raise ValueError("terminal cleanup requires an original factory")
        if (registration_cleanup is None) != (registration_limits is None):
            raise ValueError("terminal registration requires its owner and bounded limits")
        if registration_cleanup is not None and cleanup_create is None:
            raise ValueError("terminal registration requires original archive cleanup")
        if catalog_cleanup is not None and registration_cleanup is None:
            raise ValueError("terminal catalog cleanup requires completed registration execution")
        if checkpoint_cleanup is not None and catalog_cleanup is None:
            raise ValueError("terminal checkpoint cleanup requires original catalog execution")
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
        self._checkpoint_cleanup = checkpoint_cleanup
        self._catalog_cleanup = catalog_cleanup
        self._registration_cleanup = registration_cleanup
        self._registration_limits = registration_limits
        self._cleanup_create = cleanup_create
        self._cleanup_entered = False
        self._cleanup_original: object | None = None
        self._cleanup: ArchiveUploadCleanupTurn | None = None
        self._lock = Lock()

    @property
    def key(self) -> PhysicalExecutionKey:
        return self._key

    @property
    def target_incarnation(self) -> str:
        return self._target

    def advance(self, *, limit: int) -> TerminalTurnProgress:
        """Execute one bounded semantic/provider quantum in the existing worker."""
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
            progress = TerminalTurnProgress(
                retired.phase,
                retired.pending_reason,
                retired.retired,
                retired.pending_reason is None,
                retired.execution_stopped,
            )
            if (
                retired.pending_reason is not None
                or retired.phase not in {"cleanup-ready", "cleaning"}
                or self._cleanup_create is None
            ):
                return progress
            if self._cleanup is None:
                if self._cleanup_entered:
                    raise Conflict("original archive cleanup constructor is unresolved")
                self._cleanup_entered = True
                self._cleanup_original = self._cleanup_create()
                cleanup = self._cleanup_original
                if (
                    not isinstance(cleanup, ArchiveUploadCleanupTurn)
                    or cleanup.key != self._key
                    or cleanup.target_incarnation != self._target
                ):
                    raise Conflict("archive cleanup constructor returned another original scope")
                self._cleanup = cleanup
            result = self._cleanup.advance()
            progress = replace(
                progress,
                pending_reason=None if result == "inventory-exhausted" else "archive-cleanup",
                archive_inventory_exhausted=result == "inventory-exhausted",
            )
            if result != "inventory-exhausted" or self._registration_cleanup is None:
                return progress
            limits = self._registration_limits
            assert limits is not None
            registration = self._registration_cleanup.advance(
                key.collection_id, key.scope_sha256, guard=self._guard, limits=limits
            )
            disposed = registration.registration_id is None or registration.phase == "disposed"
            progress = replace(
                progress,
                pending_reason=None if disposed else "registration-cleanup",
                registration_disposed=disposed,
            )
            if not disposed or self._catalog_cleanup is None:
                return progress
            catalog = self._catalog_cleanup.advance(
                key.collection_id, key.scope_sha256, guard=self._guard, limits=limits
            )
            progress = replace(
                progress,
                pending_reason=None if catalog.upload_children_exhausted else "catalog-cleanup",
                upload_children_exhausted=catalog.upload_children_exhausted,
            )
            if not catalog.upload_children_exhausted or self._checkpoint_cleanup is None:
                return progress
            checkpoints = self._checkpoint_cleanup.advance(
                key.collection_id, key.scope_sha256, guard=self._guard, limits=limits
            )
            return replace(
                progress,
                pending_reason=None if checkpoints.exhausted else "checkpoint-cleanup",
                checkpoints_exhausted=checkpoints.exhausted,
            )
        finally:
            self._lock.release()
