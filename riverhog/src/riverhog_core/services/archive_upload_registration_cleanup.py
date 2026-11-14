"""One bounded hidden-registration step under current archive terminal authority."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from sqlalchemy.orm import Session
from time_formats import utc_timestamp_now

from riverhog_core.catalog_archive_write_models import ArchiveUploadTerminalRecord
from riverhog_core.catalog_db import SessionFactory, session_scope
from riverhog_core.catalog_models import CollectionUploadRecord
from riverhog_core.incremental_emission import VolumeEmissionLimits
from riverhog_core.services.archive_upload_cleanup import ArchiveUploadCleanup
from riverhog_core.services.collection_registration_cleanup import (
    advance_registration_cleanup,
    fence_registration_disposal,
)


@dataclass(frozen=True, slots=True)
class TerminalRegistrationProgress:
    """Internal hidden-work progress, never a confirmed upload outcome."""

    registration_id: str | None
    phase: str | None


class ArchiveUploadRegistrationCleanup:
    """Use the registration owner's bounded mechanics under this terminal's guard.

    Its independent executor cannot borrow the terminal's current rights. The
    terminal maintenance owner retains scheduling; due time and registration
    state supply no new provider, global closure or result-confirmation authority.
    """

    def __init__(self, factory: SessionFactory, cleanup: ArchiveUploadCleanup) -> None:
        self._factory = factory
        self._cleanup = cleanup

    def advance(
        self,
        collection_id: int,
        operation_id: str,
        *,
        guard: Callable[[Session], None],
        limits: VolumeEmissionLimits,
    ) -> TerminalRegistrationProgress:
        with session_scope(self._factory) as session:
            self._cleanup.require_exhausted(session, collection_id, operation_id, guard=guard)
            upload = session.get(CollectionUploadRecord, collection_id)
            terminal = session.get(ArchiveUploadTerminalRecord, operation_id)
            assert upload is not None and terminal is not None
            operation = fence_registration_disposal(
                session,
                upload,
                kind="cancel" if terminal.kind == "cancel" else "orphan-discard",
                identity=operation_id,
            )
            if operation is None:
                return TerminalRegistrationProgress(None, None)
            advance_registration_cleanup(session, upload, operation, limits=limits)
            # The global terminal owns every current-authorized quantum. Its
            # frozen registration must not enter the independent due executor.
            operation.due_at = None
            operation.updated_at = utc_timestamp_now()
            return TerminalRegistrationProgress(operation.id, operation.phase)
