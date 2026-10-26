"""Retain an exact sealed upload result beneath the current discovery slot."""

from __future__ import annotations

from contextlib import ExitStack
from pathlib import Path

from riverhog_provenance import (
    JournalResultOpening,
    JournalSummary,
    open_retained_journal_result,
)
from riverhog_provenance.replay_binding import JournalReplayBinding
from riverhog_provenance_contracts import ContractCatalog
from sqlalchemy import select
from sqlalchemy.orm import Session

from riverhog_core.canonical_discovery_archive_source import ArchiveJournalSource
from riverhog_core.canonical_discovery_archive_work import StaleArchiveTask
from riverhog_core.catalog_provenance_index_models import (
    CollectionProvenanceIndexGenerationRecord as Generation,
)
from riverhog_core.journal_validation_workspace import (
    LostJournalValidationWorkspace,
    ValidationWorkspaceCustody,
    lease_journal_validation_workspace,
)
from riverhog_core.journal_validation_workspace_reference import retain_validation_workspace
from riverhog_core.journal_validation_workspace_references import JournalWorkspaceReferences
from riverhog_core.services.discovery_journal_result_growth import (
    declare_growth_for_build_collection,
    declare_growth_for_sealed_result,
)
from riverhog_core.services.upload_journal_result import (
    SealedJournalResultAdmission,
    prepare_upload_journal_result,
)


def _build_collection_current(
    session: Session, source: ArchiveJournalSource, collection_id: int
) -> bool:
    with (
        session.no_autoflush,
        session.execute(
            select(Generation.build_id).where(
                Generation.build_id == source.metadata.job.build_id,
                Generation.collection_id == collection_id,
            )
        ) as rows,
    ):
        return rows.one_or_none() is not None


class DiscoverySealedJournalSource:
    """Original upload binding with current archive, catalog and actual lease fences."""

    def __init__(
        self,
        source: ArchiveJournalSource,
        admission: SealedJournalResultAdmission,
        custody: ValidationWorkspaceCustody,
    ) -> None:
        self.source, self.admission, self.custody = source, admission, custody
        self.identity = source.identity
        binding = admission.binding
        if (
            self.identity.journal_id != binding.journal_id
            or self.identity.source_bytes != int(binding.source_bytes)
            or self.identity.source_sha256 != binding.source_sha256
            or source.metadata.job.catalog_sha256 != binding.catalog_sha256
        ):
            raise StaleArchiveTask("sealed result differs from the full original archive source")

    @property
    def binding(self) -> JournalReplayBinding:
        return self.admission.binding

    def verify_current(self) -> bool:
        collection_id = self.admission.job.collection_id
        if collection_id is None:
            return False
        if (
            self.source.identity != self.identity
            or not self.custody.verify_current()
            or not self.source.verify_current()
        ):
            return False
        with self.source.metadata.factory() as session, session.begin():
            current = _build_collection_current(
                session, self.source, collection_id
            ) and self.admission.verify_current(session)
        return current and self.custody.verify_current() and self.source.verify_current()

    def read_window(self, offset: int, maximum_octets: int) -> bytes:
        raise RuntimeError("a sealed discovery result cannot read source payload")

    def read_exact(self, offset: int, octets: int) -> bytes:
        raise RuntimeError("a sealed discovery result cannot replay source payload")


class DiscoverySealedJournalOwner:
    """Keep actual partial Native storage before acquisition and through failed close."""

    def __init__(
        self, source: ArchiveJournalSource, admission: SealedJournalResultAdmission
    ) -> None:
        self.source, self.admission = source, admission
        self.native_source: DiscoverySealedJournalSource | None = None
        self.opening: JournalResultOpening | None = None
        self.result: JournalSummary | None = None
        self.custody: ValidationWorkspaceCustody | None = None
        self.references = JournalWorkspaceReferences()
        self.resources = ExitStack()
        self.transferred = False
        self.closing = False

    @classmethod
    def prepare(
        cls, source: ArchiveJournalSource, collection_id: int, catalog: ContractCatalog
    ) -> DiscoverySealedJournalOwner:
        if type(collection_id) is not int or collection_id < 1:
            raise ValueError("sealed discovery needs an exact collection identity")
        if not source.verify_current():
            raise StaleArchiveTask("sealed discovery source is no longer current")
        with source.metadata.factory() as session, session.begin():
            if not _build_collection_current(session, source, collection_id):
                raise StaleArchiveTask("sealed discovery source belongs to another collection")
            admission = prepare_upload_journal_result(
                session, collection_id, source.identity.journal_id, catalog=catalog
            )
        # The selected catalog transaction ends before any lease or Native I/O.
        return cls(source, admission)

    def retain_opening(self, opening: JournalResultOpening) -> None:
        if self.opening is not None:
            raise RuntimeError("sealed discovery already retains a Native opening")
        self.opening = opening

    def retain_custody(self, custody: ValidationWorkspaceCustody) -> None:
        self.custody = custody

    def open(self, root: Path, catalog: ContractCatalog) -> JournalSummary | None:
        metadata = self.source.metadata
        retain_validation_workspace(metadata.custody, on_reference=self.references.retain_parent)
        directory = self.resources.enter_context(
            lease_journal_validation_workspace(
                root,
                self.admission.job.workspace_id,
                shared=True,
                on_custody=self.retain_custody,
            )
        )
        if directory is None:
            return None
        assert self.custody is not None
        retain_validation_workspace(self.custody, on_reference=self.references.retain_child)
        self.native_source = DiscoverySealedJournalSource(self.source, self.admission, self.custody)
        self.result = open_retained_journal_result(
            directory,
            self.native_source,
            confirmed=self.admission.confirmed,
            catalog=catalog,
            certificates=self.admission.certificates,
            on_storage_open=self.retain_opening,
            on_storage_close=self.references.close,
        )
        # Retain the returned result before inspecting its terminal commitment.
        self.transferred = True
        if self.result.anchor != self.admission.anchor:
            raise LostJournalValidationWorkspace("sealed discovery result lost its original anchor")
        return self.result

    def borrow(self) -> JournalSummary:
        if (
            self.closing
            or self.result is None
            or self.native_source is None
            or not self.native_source.verify_current()
        ):
            raise StaleArchiveTask("sealed discovery result no longer has current custody")
        return self.result.borrow()

    def close(self) -> None:
        self.closing = True
        if self.result is not None:
            self.result.close()
            self.result = None
        if self.opening is not None:
            self.opening.close()
            self.opening = None
        self.resources.close()
        if not self.transferred:
            self.references.close()


declare_growth_for_sealed_result(DiscoverySealedJournalOwner, DiscoverySealedJournalSource)

declare_growth_for_build_collection(_build_collection_current)
