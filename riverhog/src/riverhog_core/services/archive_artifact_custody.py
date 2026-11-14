"""Retain one verified member-custody selection through its original execution."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from threading import Lock

from riverhog_protocol.errors import Conflict

from riverhog_core.archive_store_registry import ArchiveStoreRegistry
from riverhog_core.catalog_db import SessionFactory
from riverhog_core.runtime_config import RuntimeConfig
from riverhog_core.services.archive_artifact_custody_receipt import (
    ArchiveArtifactCustodyReceiptFinalizer,
    ArchiveArtifactCustodyReceiptSource,
)
from riverhog_core.services.archive_finalizer_terminal import (
    ArchiveFinalizerTerminal,
    TerminalDrainStatus,
)
from riverhog_core.services.archive_provenance_journal_checkpoint import ProvenanceJournalRange
from riverhog_core.services.archive_provenance_journal_writer import ArchiveProvenanceJournalWriter
from riverhog_core.services.archive_provenance_structure_writer import (
    ArchiveProvenanceStructureWriter,
)
from riverhog_core.services.archive_write_admission import Guard


@dataclass(frozen=True, slots=True)
class ArtifactCustodySelection:
    """A verified dependency or receipt; plaintext belongs to its semantic owner."""

    collection_id: int
    artifact_id: str
    structure_object_id: str | None = None
    journal_range: ProvenanceJournalRange | None = None
    receipt_source: ArchiveArtifactCustodyReceiptSource | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if (
            sum(
                value is not None
                for value in (self.structure_object_id, self.journal_range, self.receipt_source)
            )
            != 1
        ):
            raise ValueError("artifact custody selection requires exactly one verified step")
        if self.receipt_source is not None and (
            self.receipt_source.collection_id != self.collection_id
            or self.receipt_source.artifact_id != self.artifact_id
        ):
            raise ValueError("artifact custody receipt has another member identity")


class ArchiveArtifactCustodyFinalizer:
    """One application-selected dependency or receipt, never global completion.

    Selection runs before child construction and performs no provider execution.
    Once construction starts, the original receiver or its unresolved constructor
    remains owned through retries. A successful turn caches its boolean until the
    containing workflow has acknowledged it; new selection requires a new owner.
    """

    def __init__(
        self,
        factory: SessionFactory,
        *,
        collection_id: int,
        artifact_id: str,
        config: RuntimeConfig,
        archive_stores: ArchiveStoreRegistry,
        forward_guard: Guard,
        select_source: Callable[[], ArtifactCustodySelection | None],
    ) -> None:
        self._factory = factory
        self._collection_id = collection_id
        self._artifact_id = artifact_id
        self._config = config
        self._archive_stores = archive_stores
        self._forward_guard = forward_guard
        self._select_source = select_source
        self._selection: ArtifactCustodySelection | None = None
        self._original: (
            ArchiveProvenanceStructureWriter
            | ArchiveProvenanceJournalWriter
            | ArchiveArtifactCustodyReceiptFinalizer
            | None
        ) = None
        self._construction_failure = False
        self._result: bool | None = None
        self._lock = Lock()
        self._terminal = ArchiveFinalizerTerminal(factory, collection_id)

    @property
    def has_pending(self) -> bool:
        return self._construction_failure or (self._selection is not None and self._result is None)

    def advance(self) -> bool:
        with self._lock:
            self._terminal.require_forward()
            if self._result is not None:
                return self._result
            if self._construction_failure:
                raise Conflict("original artifact custody constructor remains unresolved")
            if self._selection is None:
                selected = self._select_source()
                if selected is None:
                    self._result = False
                    return False
                if (selected.collection_id, selected.artifact_id) != (
                    self._collection_id,
                    self._artifact_id,
                ):
                    raise Conflict("artifact custody selection has another member identity")
                self._selection = selected
            selected = self._selection
            if self._original is None:
                self._construction_failure = True
                if selected.structure_object_id is not None:
                    self._original = ArchiveProvenanceStructureWriter(
                        self._factory,
                        collection_id=self._collection_id,
                        config=self._config,
                        archive_stores=self._archive_stores,
                        forward_guard=self._forward_guard,
                    )
                elif selected.journal_range is not None:
                    self._original = ArchiveProvenanceJournalWriter(
                        self._factory,
                        collection_id=self._collection_id,
                        config=self._config,
                        archive_stores=self._archive_stores,
                        forward_guard=self._forward_guard,
                    )
                else:
                    receipt = selected.receipt_source
                    assert receipt is not None
                    self._original = ArchiveArtifactCustodyReceiptFinalizer(
                        self._factory,
                        collection_id=self._collection_id,
                        select_source=lambda: receipt,
                    )
                if self._original is None:
                    raise ValueError("original artifact custody constructor returned no receiver")
                self._construction_failure = False
            if isinstance(self._original, ArchiveProvenanceStructureWriter):
                result = self._original.advance(selected_object_id=selected.structure_object_id)
            elif isinstance(self._original, ArchiveProvenanceJournalWriter):
                journal = selected.journal_range
                assert journal is not None
                result = self._original.advance(
                    journal_id=journal.journal_id,
                    offset=journal.offset,
                    size=journal.size,
                    expected_range=journal,
                )
            else:
                result = self._original.advance()
            if self._original.has_pending:
                raise Conflict("original artifact custody step has not finished its bounded turn")
            self._result = result
            return result

    def drain_terminal(self, operation_id: str) -> TerminalDrainStatus:
        if not self._lock.acquire(blocking=False):
            return "busy"
        try:
            status = self._terminal.drain_original(operation_id, self._original)
            return "pending" if self._construction_failure else status
        finally:
            self._lock.release()
