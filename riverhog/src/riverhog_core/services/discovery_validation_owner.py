"""Serialized application custody of retained discovery metadata and Native work."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from threading import Lock
from uuid import uuid4

from riverhog_archive_contracts import HistoryJournalAnchor
from riverhog_provenance import (
    InvalidJournalWorkspace,
    JournalGraphCertificates,
    JournalSummary,
    JournalValidationBudget,
    JournalValidationProgress,
    JournalValidationSession,
    RetainedCorpusValidator,
    StaleCorpusWorkspace,
)
from riverhog_provenance_contracts import ContractCatalog

from riverhog_core import canonical_discovery_corpus_jobs as jobs
from riverhog_core import canonical_discovery_corpus_replay as replay
from riverhog_core import canonical_discovery_journal_preparation as journal_preparation
from riverhog_core import canonical_discovery_journal_registration as registration
from riverhog_core.canonical_discovery_archive_framing import ArchiveJournalFramingWork
from riverhog_core.canonical_discovery_archive_source import ArchiveJournalSource
from riverhog_core.canonical_discovery_archive_work import (
    ArchiveDiscoveryAdmission,
    StaleArchiveTask,
)
from riverhog_core.canonical_discovery_corpus_source import DiscoveryCorpusSource
from riverhog_core.canonical_discovery_journal_certificates import DiscoveryJournalGraphCertificates
from riverhog_core.canonical_discovery_journal_epochs import DiscoveryJournalEpochWork
from riverhog_core.canonical_discovery_journal_source import DiscoveryJournalSource
from riverhog_core.canonical_discovery_metadata import DiscoveryMetadata
from riverhog_core.canonical_discovery_selection import (
    CorpusSelectionWork,
    frozen_corpus_selection,
)
from riverhog_core.catalog_db import SessionFactory
from riverhog_core.journal_validation_workspace import (
    LostJournalValidationWorkspace,
    ValidationWorkspaceCustody,
    lease_journal_validation_workspace,
)
from riverhog_core.services.discovery_corpus_replay_growth import declare_corpus_replay_dispatch
from riverhog_core.services.discovery_journal_growth import declare_growth_for_journal_dispatch
from riverhog_core.services.discovery_journal_owner import DiscoveryJournalOwner
from riverhog_core.services.discovery_journal_result import DiscoverySealedJournalOwner
from riverhog_core.services.discovery_journal_result_growth import (
    declare_growth_for_sealed_dispatch,
)
from riverhog_core.services.discovery_validation_growth import declare_owner_growth


@dataclass(slots=True)
class _Slot:
    lock: Lock = field(default_factory=Lock)
    resources: ExitStack = field(default_factory=ExitStack)
    metadata: DiscoveryMetadata | None = None
    source: DiscoveryCorpusSource | None = None
    corpus: RetainedCorpusValidator | None = None
    replay: replay.CorpusReplayWork | None = None
    replay_initialized: bool = False
    replay_summary: JournalSummary | None = None
    journal: DiscoveryJournalOwner | None = None
    sealed: DiscoverySealedJournalOwner | None = None
    preparation: journal_preparation.DiscoveryJournalPreparationWork | None = None
    preparation_initialized: bool = False
    certificates: DiscoveryJournalGraphCertificates | None = None
    closing: bool = False

    def retain_corpus(self, corpus: RetainedCorpusValidator) -> None:
        if self.corpus is not None:
            raise RuntimeError("discovery slot already retains Native storage")
        self.corpus = corpus


class DiscoveryValidationWork:
    """One synchronous work quantum beneath the application's held slot mutex.

    The caller keeps all Core and Native calls inside this quantum. The owner
    alone closes the retained handles; the caller does not transfer them to
    another execution or create an independent reader lifecycle.
    """

    def __init__(self, slot: _Slot, catalog: ContractCatalog, journal_root: Path) -> None:
        self._slot, self._catalog, self._journal_root = slot, catalog, journal_root
        self._active = True

    @property
    def metadata(self) -> DiscoveryMetadata:
        if not self._active or self._slot.closing or self._slot.metadata is None:
            raise StaleArchiveTask("discovery work no longer owns an active quantum")
        return self._slot.metadata

    def journal_source(self, work: ArchiveJournalFramingWork) -> DiscoveryJournalSource:
        """Register immutable source custody before graph preparation or Native I/O."""
        metadata = self.metadata
        if work.source.metadata is not metadata:
            raise StaleArchiveTask("journal preparation belongs to another metadata owner")
        with metadata.read() as (db, key):
            selected = registration.find_discovery_journal(db, key, work)
        source = DiscoveryJournalSource(
            work, directory_id=uuid4().hex if selected is None else selected.binding.directory_id
        )
        if selected is None:
            with metadata.transaction() as (db, key):
                selected = registration.register_discovery_journal(db, key, work, source.binding)
        if selected.binding != source.binding:
            raise StaleArchiveTask("journal preparation lost its registered original source")
        return source

    def prepare_journal(
        self, work: ArchiveJournalFramingWork
    ) -> DiscoveryJournalGraphCertificates | None:
        """Publish one bounded preparation atom; return evidence only when complete."""
        metadata, slot = self.metadata, self._slot
        source = self.journal_source(work)
        selected = slot.preparation
        if selected is not None and selected.epochs.source.binding != source.binding:
            with metadata.read() as (db, key):
                current = journal_preparation.discovery_journal_preparation_progress(
                    db, key, selected
                )
            if not current.complete:
                raise StaleArchiveTask("another original journal owns unfinished preparation")
            previous = slot.journal
            if previous is not None and (
                previous.closing
                or previous.session is None
                or not previous.session.progress().complete
            ):
                raise StaleArchiveTask("another unfinished journal still owns this slot")
            selected = None
            slot.preparation = None
            slot.preparation_initialized = False
            slot.certificates = None
        if selected is None:
            original = DiscoveryJournalEpochWork(work, source)
            selected = journal_preparation.DiscoveryJournalPreparationWork(original)
            slot.preparation = selected
        if not slot.preparation_initialized:
            with metadata.transaction() as (db, key):
                journal_preparation.begin_discovery_journal_preparation(db, key, selected)
            slot.preparation_initialized = True
        with metadata.read() as (db, key):
            page = journal_preparation.prepare_discovery_journal_preparation(db, key, selected)
        if page is not None:
            with metadata.transaction() as (db, key):
                if not journal_preparation.apply_discovery_journal_preparation(
                    db, key, selected, page
                ):
                    raise StaleArchiveTask("journal preparation publication lost its current owner")
            return None
        if slot.certificates is None:
            slot.certificates = DiscoveryJournalGraphCertificates(metadata, selected.rank)
        elif not slot.certificates.verify_current():
            raise StaleArchiveTask("completed journal preparation is no longer current")
        return slot.certificates

    def advance_journal(
        self, work: ArchiveJournalFramingWork, *, budget: JournalValidationBudget
    ) -> JournalValidationProgress | None:
        """Execute one preparation atom or one Native replay quantum under this slot."""
        certificates = self.prepare_journal(work)
        if certificates is None:
            return None
        native = self.journal(work, certificates=certificates)
        return None if native is None else native.advance(budget)

    def journal(
        self,
        work: ArchiveJournalFramingWork,
        *,
        certificates: JournalGraphCertificates | None = None,
    ) -> JournalValidationSession | None:
        """Keep one selected unfinished Native atom warm across serialized quanta."""
        metadata, slot = self.metadata, self._slot
        source = self.journal_source(work)
        retained = slot.journal
        if retained is not None:
            if retained.closing or retained.session is None:
                raise StaleArchiveTask("journal child has unconfirmed cleanup")
            if retained.source.binding == source.binding:
                previous = retained.certificates
                if (
                    (None if previous is None else previous.binding)
                    != (None if certificates is None else certificates.binding)
                    or retained.native_source is None
                    or not retained.native_source.verify_current()
                ):
                    raise StaleArchiveTask("warm journal has another original input owner")
                return retained.session
            if not retained.session.progress().complete:
                raise StaleArchiveTask("another unfinished journal still owns this slot")
            retained.close()
            slot.journal = None
        # Retain the helper before reference acquisition or Native construction.
        retained = DiscoveryJournalOwner(source)
        slot.journal = retained
        with metadata.read() as (db, key):
            selected = registration.find_discovery_journal(db, key, work)
        if selected is None or selected.binding != source.binding:
            raise StaleArchiveTask("journal opening lost its exact directory admission")
        try:
            session = retained.open(
                metadata, self._catalog, confirmed=selected.confirmed, certificates=certificates
            )
        except (
            FileNotFoundError,
            InvalidJournalWorkspace,
            LostJournalValidationWorkspace,
            sqlite3.DatabaseError,
        ):
            lost = jobs.lose_discovery_corpus(
                metadata.factory,
                metadata.job,
                failure="Registered Native journal workspace is missing or incompatible",
            )
            if lost is not None:
                metadata.job = lost
            raise
        if session is None:
            retained.close()
            slot.journal = None
            return None
        with metadata.transaction() as (db, key):
            if not registration.confirm_discovery_journal(db, key, work, selected):
                raise StaleArchiveTask("Native journal opening lost its current registration")
        return session

    def sealed_journal(
        self, source: ArchiveJournalSource, *, collection_id: int
    ) -> JournalSummary | None:
        """Borrow one exact sealed result; absent or busy results remain pending."""
        metadata, slot = self.metadata, self._slot
        if source.metadata is not metadata:
            raise StaleArchiveTask("sealed source belongs to another discovery metadata owner")
        retained = slot.sealed
        if retained is not None:
            if (
                not retained.closing
                and retained.admission.job.collection_id == collection_id
                and retained.source.identity == source.identity
            ):
                return retained.borrow()
            retained.close()
            slot.sealed = None
        try:
            retained = DiscoverySealedJournalOwner.prepare(source, collection_id, self._catalog)
        except KeyError:
            return None
        # Retain before acquisition can leave partial Native cleanup uncertain.
        slot.sealed = retained
        if retained.open(self._journal_root, self._catalog) is None:
            retained.close()
            slot.sealed = None
            return None
        return retained.borrow()

    def corpus(self, selection: CorpusSelectionWork) -> RetainedCorpusValidator:
        metadata = self.metadata
        slot = self._slot
        if slot.corpus is not None:
            if (
                slot.source is None
                or selection.history.admission is not metadata.admission
                or selection.sources != slot.source.selection.sources
                or not slot.source.verify_current()
            ):
                raise StaleArchiveTask("retained Native corpus has another current input owner")
            return slot.corpus
        with metadata.read() as (db, key):
            manifest = frozen_corpus_selection(db, key, selection)
        pinned = jobs.pin_discovery_corpus(metadata.factory, metadata.job, manifest)
        if pinned is None:
            raise StaleArchiveTask("discovery input pin lost its current worker")
        metadata.job = pinned
        slot.source = DiscoveryCorpusSource(metadata, selection)
        current = jobs.discovery_worker_snapshot(metadata.factory, metadata.job)
        if current is None or current.state not in {"ready", "active"}:
            raise StaleArchiveTask("Native opening has no current pinned disposition")
        metadata.job = current
        path = metadata.custody.directory / "corpus.sqlite3"
        create = current.state == "ready" and not path.exists()
        try:
            native = RetainedCorpusValidator.open_retained(
                metadata.custody.directory,
                slot.source,
                worker_epoch=current.worker_epoch,
                create=create,
                catalog=self._catalog,
                on_storage_open=slot.retain_corpus,
            )
        except (FileNotFoundError, StaleCorpusWorkspace):
            lost = jobs.lose_discovery_corpus(
                metadata.factory,
                metadata.job,
                failure="Native discovery workspace is missing or incompatible",
            )
            if lost is not None:
                metadata.job = lost
            raise
        # The opening callback already retained this handle before initialization.
        assert slot.corpus is native
        activated = jobs.activate_discovery_corpus(metadata.factory, metadata.job)
        if activated is None:
            raise StaleArchiveTask("confirmed Native opening lost its current catalog worker")
        metadata.job = activated
        return native

    def corpus_replay(self, selection: CorpusSelectionWork) -> replay.CorpusReplayWork:
        """Retain one exact conductor beneath the current Native corpus owner."""
        metadata, slot = self.metadata, self._slot
        native = self.corpus(selection)
        with metadata.read() as (db, key):
            manifest = frozen_corpus_selection(db, key, selection)
        retained = slot.replay
        if retained is None:
            retained = replay.CorpusReplayWork(selection, manifest, native)
            slot.replay = retained
        elif retained.corpus is not native or retained.manifest != manifest:
            raise StaleArchiveTask("retained corpus replay has another original input freeze")
        if not slot.replay_initialized:
            with metadata.transaction() as (db, key):
                replay.begin_corpus_replay(db, key, retained)
            slot.replay_initialized = True
        return retained

    def next_corpus_source(self, selection: CorpusSelectionWork) -> HistoryJournalAnchor | None:
        """Request one full original source, independently of clipped selections."""
        retained = self.corpus_replay(selection)
        with self.metadata.read() as (db, key):
            return replay.next_corpus_replay_source(db, key, retained)

    def advance_corpus(
        self,
        selection: CorpusSelectionWork,
        *,
        frame_budget: int = 1,
        row_budget: int = 64,
        byte_budget: int,
    ) -> replay.CorpusReplayProgress | None:
        """Replay one quantum from the exact retained source, or remain pending.

        The caller prepares the requested journal through this slot's journal or
        sealed-result owner. This dispatch never opens a second source reader,
        revalidates a journal, or retains an all-source summary cache.
        """
        metadata, slot = self.metadata, self._slot
        retained = self.corpus_replay(selection)
        with metadata.read() as (db, key):
            target = replay.next_corpus_replay_source(db, key, retained)
        if slot.replay_summary is not None:
            raise StaleArchiveTask("corpus replay retains an unconfirmed source release")
        if target is not None:
            sealed, journal = slot.sealed, slot.journal
            if sealed is not None and sealed.source.identity.journal_id == target.journal_id:
                slot.replay_summary = sealed.borrow()
            elif journal is not None and journal.source.binding.journal_id == target.journal_id:
                if (
                    journal.closing
                    or journal.session is None
                    or journal.native_source is None
                    or not journal.native_source.verify_current()
                ):
                    raise StaleArchiveTask("corpus replay source no longer has current custody")
                if not journal.session.progress().complete:
                    return None
                slot.replay_summary = journal.session.result()
            else:
                return None
        try:
            with metadata.read() as (db, key):
                page = replay.prepare_corpus_replay(
                    db,
                    key,
                    retained,
                    summary=slot.replay_summary,
                    frame_budget=frame_budget,
                    row_budget=row_budget,
                    byte_budget=byte_budget,
                )
        finally:
            if slot.replay_summary is not None:
                slot.replay_summary.close()
                slot.replay_summary = None
        with metadata.transaction() as (db, key):
            return replay.apply_corpus_replay(db, key, retained, page)


class DiscoveryValidationOwner:
    """Keep bounded work warm and release custody only after confirmed closure.

    The pool mutex protects reservations only. Catalog, kernel custody, Core and
    Native work run under one slot mutex outside it. Cleanup failures keep their
    closing slot reachable for retry, including failed Native initialization.
    """

    def __init__(
        self,
        factory: SessionFactory,
        root: Path,
        *,
        concurrency: int = 2,
        journal_root: Path | None = None,
    ) -> None:
        if type(concurrency) is not int or concurrency < 1:
            raise ValueError("discovery concurrency must be positive")
        self.factory, self.root, self.concurrency = factory, root, concurrency
        self.journal_root = root if journal_root is None else journal_root
        self._lock = Lock()
        self._slots: dict[str, _Slot] = {}
        self._closed = False

    def _reserve(self, build_id: str) -> _Slot | None:
        with self._lock:
            if self._closed:
                raise RuntimeError("discovery validation owner is closed")
            slot = self._slots.get(build_id)
            if slot is None:
                if len(self._slots) >= self.concurrency:
                    return None
                slot = _Slot()
                self._slots[build_id] = slot
            return slot if slot.lock.acquire(blocking=False) else None

    def _drop(self, build_id: str, slot: _Slot) -> None:
        slot.closing = True
        if slot.replay_summary is not None:
            slot.replay_summary.close()
            slot.replay_summary = None
        if slot.sealed is not None:
            slot.sealed.close()
            slot.sealed = None
        if slot.journal is not None:
            slot.journal.close()
            slot.journal = None
        if slot.corpus is not None:
            slot.corpus.close()
            slot.corpus = None
        slot.replay = None
        slot.replay_initialized = False
        slot.preparation = None
        slot.preparation_initialized = False
        slot.certificates = None
        if slot.metadata is not None:
            slot.metadata.close()
            slot.metadata = None
        slot.resources.close()
        with self._lock:
            if self._slots.get(build_id) is slot:
                del self._slots[build_id]

    def _open(
        self,
        build_id: str,
        slot: _Slot,
        admission: ArchiveDiscoveryAdmission,
        catalog: ContractCatalog,
    ) -> bool:
        selected = jobs.select_discovery_job(
            self.factory, build_id=build_id, admission=admission, catalog=catalog
        )
        if selected is None:
            return False
        if selected.state == "lost":
            raise LostJournalValidationWorkspace(selected.failure or "discovery custody was lost")
        if selected.state not in {"preparing", "seeding", "ready", "active"}:
            return False
        captured: list[ValidationWorkspaceCustody] = []
        try:
            path = slot.resources.enter_context(
                lease_journal_validation_workspace(
                    self.root,
                    selected.workspace_id,
                    create=selected.state == "preparing",
                    creation_current=lambda: jobs.discovery_worker_current(self.factory, selected),
                    on_custody=captured.append,
                )
            )
        except LostJournalValidationWorkspace:
            jobs.lose_discovery_corpus(
                self.factory,
                selected,
                failure="Selected discovery workspace custody is unavailable",
            )
            raise
        if path is None:
            return False
        claimed = jobs.claim_discovery_worker(self.factory, selected)
        if claimed is None:
            return False
        # Retain this object before opening can leave a close outcome uncertain.
        slot.metadata = DiscoveryMetadata(self.factory, claimed, admission, captured[0])
        try:
            slot.metadata.open()
        except (FileNotFoundError, LostJournalValidationWorkspace):
            lost = jobs.lose_discovery_corpus(
                self.factory,
                slot.metadata.job,
                failure="Core discovery workspace is missing or incompatible",
            )
            if lost is not None:
                slot.metadata.job = lost
            raise
        return True

    @contextmanager
    def work(
        self,
        *,
        build_id: str,
        admission: ArchiveDiscoveryAdmission,
        catalog: ContractCatalog,
    ) -> Iterator[DiscoveryValidationWork | None]:
        slot = self._reserve(build_id)
        if slot is None:
            yield None
            return
        work = None
        try:
            if slot.closing:
                self._drop(build_id, slot)
                yield None
                return
            if slot.metadata is None and not self._open(build_id, slot, admission, catalog):
                self._drop(build_id, slot)
                yield None
                return
            metadata = slot.metadata
            assert metadata is not None
            if (
                metadata.admission.identity != admission.identity
                or metadata.job.catalog_sha256 != catalog.checkpoint_sha256()
                or not metadata.verify_current()
            ):
                self._drop(build_id, slot)
                yield None
                return
            work = DiscoveryValidationWork(slot, catalog, self.journal_root)
            yield work
            if not metadata.verify_current():
                raise StaleArchiveTask("discovery quantum lost its current owner")
        except BaseException:
            if not slot.closing:
                self._drop(build_id, slot)
            raise
        finally:
            if work is not None:
                work._active = False
            slot.lock.release()

    def active_build_ids(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(self._slots)

    def release_stale(self) -> None:
        with self._lock:
            slots = tuple(self._slots.items())
        errors = []
        for build_id, slot in slots:
            if not slot.lock.acquire(blocking=False):
                continue
            try:
                if slot.closing or slot.metadata is None or not slot.metadata.verify_current():
                    self._drop(build_id, slot)
            except BaseException as error:
                errors.append(error)
            finally:
                slot.lock.release()
        if errors:
            raise errors[0]

    def close(self) -> None:
        with self._lock:
            self._closed = True
            slots = tuple(self._slots.items())
        errors = []
        for build_id, slot in slots:
            try:
                with slot.lock:
                    self._drop(build_id, slot)
            except BaseException as error:
                errors.append(error)
        if errors:
            raise errors[0]


declare_owner_growth(DiscoveryValidationOwner, DiscoveryValidationWork)

declare_growth_for_journal_dispatch(DiscoveryValidationWork)

declare_growth_for_sealed_dispatch(DiscoveryValidationWork)

declare_corpus_replay_dispatch(DiscoveryValidationWork)
