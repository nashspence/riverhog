"""Compose terminal resource facts inside the caller's exact catalog transaction."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

from sqlalchemy.orm import Session

from riverhog_core.catalog_archive_write_models import ArchiveWriteControlRecord
from riverhog_core.services.archive_finalizer_registry import ArchiveFinalizerRegistry
from riverhog_core.services.archive_payload_input import ArchivePayloadInputs
from riverhog_core.services.archive_upload_drain import (
    LocalTerminalStatus,
    TerminalWriteRecords,
    inspect_terminal_write_records,
)
from riverhog_core.services.archive_upload_execution import (
    ArchiveUploadExecutionKey,
    ArchiveUploadExecutionOwner,
)
from riverhog_core.services.archive_upload_invocations import ArchiveUploadInvocationRegistry


@dataclass(frozen=True, slots=True)
class TerminalResourceObservation:
    """Separate catalog, original-owner, input and actual-execution observations.

    These facts neither establish that all application writers participate nor
    adjudicate disposal rights. The caller retains its transaction and the
    responsibility for complete writer tracking and current authority.
    """

    records: TerminalWriteRecords
    finalizers: LocalTerminalStatus
    invocations: LocalTerminalStatus
    inputs: Literal["busy", "untracked", "retained"]
    execution: Literal["busy", "untracked", "running", "awaiting-join", "outcome-missing"]


class ArchiveUploadTerminalObservation:
    """Borrow the service's existing resource owners; create no scheduler or database."""

    def __init__(
        self,
        finalizers: ArchiveFinalizerRegistry,
        invocations: ArchiveUploadInvocationRegistry,
        inputs: ArchivePayloadInputs,
        execution: ArchiveUploadExecutionOwner,
    ) -> None:
        self._finalizers = finalizers
        self._invocations = invocations
        self._inputs = inputs
        self._execution = execution

    def observe(
        self,
        session: Session,
        collection_id: int,
        operation_id: str,
        *,
        guard: Callable[[Session], None] | None = None,
    ) -> TerminalResourceObservation:
        """Observe under the caller's locked original terminal selection.

        The optional current-authority guard runs after acquiring SQLite's writer
        and before any upload/terminal lock or local probe. It validates the
        caller's operation-specific rights; these resource facts grant none.
        Catalog contention propagates for the transaction owner to defer after
        rollback. Local probes are nonblocking and perform no SQL, construction,
        provider work, receiving, fencing, cleanup or resource retirement.
        """
        records = inspect_terminal_write_records(
            session, collection_id, operation_id, nowait=True, guard=guard
        )
        control = session.get(ArchiveWriteControlRecord, collection_id)
        assert control is not None
        construction = control.construction_json
        key = ArchiveUploadExecutionKey(
            collection_id, hashlib.sha256(construction.encode()).hexdigest()
        )
        target = json.loads(construction)["archive_incarnation_id"]
        return TerminalResourceObservation(
            records=records,
            finalizers=self._finalizers.local_terminal_status(
                key, target_incarnation=target, operation_id=operation_id
            ),
            invocations=self._invocations.local_terminal_status(
                key, target_incarnation=target, operation_id=operation_id
            ),
            inputs=self._inputs.status(key),
            execution=self._execution.status(key),
        )
