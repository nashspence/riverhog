"""One retained Native child beneath an application's serialized discovery slot."""

from __future__ import annotations

from contextlib import ExitStack
from pathlib import Path

from riverhog_provenance import (
    JournalGraphCertificates,
    JournalReplayBinding,
    JournalValidationSession,
)
from riverhog_provenance_contracts import ContractCatalog

from riverhog_core.canonical_discovery_archive_work import StaleArchiveTask
from riverhog_core.canonical_discovery_journal_source import DiscoveryJournalSource
from riverhog_core.canonical_discovery_metadata import DiscoveryMetadata
from riverhog_core.journal_validation_workspace import (
    ValidationWorkspaceCustody,
    lease_journal_validation_workspace,
)
from riverhog_core.journal_validation_workspace_reference import (
    retain_validation_workspace,
)
from riverhog_core.journal_validation_workspace_references import JournalWorkspaceReferences
from riverhog_core.services.discovery_journal_growth import (
    declare_growth_for_journal_custody_source,
    declare_growth_for_journal_owner,
)


class DiscoveryJournalCustodySource:
    """Original normalized source fenced by the actual held child workspace."""

    def __init__(self, source: DiscoveryJournalSource, custody: ValidationWorkspaceCustody) -> None:
        self.source, self.custody = source, custody

    @property
    def binding(self) -> JournalReplayBinding:
        return self.source.binding

    def verify_current(self) -> bool:
        return self.custody.verify_current() and self.source.verify_current()

    def _require_current(self) -> None:
        if not self.custody.verify_current():
            raise StaleArchiveTask("journal child workspace custody is no longer current")

    def read_window(self, offset: int, maximum_octets: int) -> bytes:
        self._require_current()
        result = self.source.read_window(offset, maximum_octets)
        self._require_current()
        return result

    def read_exact(self, offset: int, octets: int) -> bytes:
        self._require_current()
        result = self.source.read_exact(offset, octets)
        self._require_current()
        return result


class DiscoveryJournalOwner:
    """Retained before opening, with one closeable partial Native handle.

    Successful opening transfers the duplicated leases to Native storage through
    its final frozen borrower. Failed opening keeps them here until the actual
    partial handle and original child context confirm closure.
    """

    def __init__(self, source: DiscoveryJournalSource) -> None:
        self.source = source
        self.native_source: DiscoveryJournalCustodySource | None = None
        self.session: JournalValidationSession | None = None
        self.references = JournalWorkspaceReferences()
        self.resources = ExitStack()
        self.transferred = False
        self.closing = False
        self.directory: Path | None = None
        self.custody: ValidationWorkspaceCustody | None = None
        self.certificates: JournalGraphCertificates | None = None

    def retain_session(self, session: JournalValidationSession) -> None:
        if self.session is not None:
            raise RuntimeError("discovery child already retains Native storage")
        self.session = session

    def retain_custody(self, custody: ValidationWorkspaceCustody) -> None:
        self.custody = custody

    def open(
        self,
        metadata: DiscoveryMetadata,
        catalog: ContractCatalog,
        *,
        confirmed: bool,
        certificates: JournalGraphCertificates | None,
    ) -> JournalValidationSession | None:
        retain_validation_workspace(metadata.custody, on_reference=self.references.retain_parent)
        directory = self.resources.enter_context(
            lease_journal_validation_workspace(
                metadata.custody.directory / "journals",
                self.source.binding.directory_id,
                create=not confirmed,
                creation_current=metadata.verify_current,
                on_custody=self.retain_custody,
            )
        )
        if directory is None:
            return None
        self.directory = directory
        assert self.custody is not None
        retain_validation_workspace(self.custody, on_reference=self.references.retain_child)
        self.certificates = certificates
        self.native_source = DiscoveryJournalCustodySource(self.source, self.custody)
        session = JournalValidationSession.open_retained(
            directory,
            self.native_source,
            attempt_id=self.source.binding.attempt_id,
            worker_epoch=metadata.job.worker_epoch,
            create=not confirmed and not (directory / "snapshot.sqlite3").exists(),
            catalog=catalog,
            certificates=certificates,
            on_storage_open=self.retain_session,
            on_storage_close=self.references.close,
        )
        # The early callback retained the exact partial object before Native I/O.
        assert self.session is session
        self.transferred = True
        return session

    def close(self) -> None:
        self.closing = True
        if self.session is not None:
            self.session.close()
            self.session = None
        self.resources.close()
        if not self.transferred:
            self.references.close()


declare_growth_for_journal_owner(DiscoveryJournalOwner)

declare_growth_for_journal_custody_source(DiscoveryJournalCustodySource)
