"""One retained, original-bound discovery retirement quantum."""

from __future__ import annotations

import errno
import os
import re
from dataclasses import dataclass
from pathlib import Path

from riverhog_provenance import JournalValidationCapacity
from state_schema import read_snapshot

from riverhog_core import canonical_discovery_corpus_jobs as jobs
from riverhog_core.canonical_discovery_archive_work import StaleArchiveTask
from riverhog_core.canonical_discovery_child_reclamation import (
    DiscoveryChildReclamationProgress,
    reclaim_discovery_child,
)
from riverhog_core.canonical_discovery_corpus_reclamation import (
    DiscoveryCorpusReclamationProgress,
    reclaim_discovery_corpus,
)
from riverhog_core.canonical_discovery_metadata_reclamation import (
    DiscoveryMetadataReclamationProgress,
    reclaim_discovery_metadata,
)
from riverhog_core.canonical_discovery_parent_reclamation import DiscoveryParentRetirement
from riverhog_core.canonical_discovery_retired_metadata import RetiredDiscoveryMetadata
from riverhog_core.canonical_discovery_retirement_growth import declare_retirement_growth
from riverhog_core.canonical_discovery_root_reclamation import (
    DiscoveryRootReclamationProgress,
    reclaim_discovery_journals_root,
)
from riverhog_core.catalog_db import SessionFactory
from riverhog_core.journal_validation_retirement import RetiringJournalWorkspace
from riverhog_core.journal_validation_workspace import (
    LostJournalValidationWorkspace,
    ValidationWorkspaceCustody,
    _private_directory,
    _private_file,
)
from riverhog_core.retained_workspace_handles import RetainedWorkspaceHandles, _close_descriptor


@dataclass(frozen=True, slots=True)
class DiscoveryRetirementProgress:
    job: jobs.DiscoveryJob
    advanced: bool
    removed_octets: int = 0
    catalog_received: bool = False


class DiscoveryRetirementTurn:
    """Retain this one-attempt owner before calling advance.

    One phase or byte extent runs beneath the original exclusive parent lease.
    Root coordination is released before file/Core work. Every successful or busy
    return follows actual close receiving. An exception retains this object,
    including partial Core and child handles, until close positively succeeds.
    The caller retains its execution charge through that receiving and actual
    worker join. Neither a returned phase nor this local turn confers terminal
    authority or reconstructs a semantic worker.
    """

    def __init__(self, factory: SessionFactory, job: jobs.DiscoveryJob, root: Path):
        if (
            not root.is_absolute()
            or root == root.parent
            or re.fullmatch(r"[0-9a-f]{32}", job.workspace_id) is None
        ):
            raise ValueError("discovery retirement requires an exact absolute namespace")
        self.factory, self.job, self.root = factory, job, root
        self._handles = RetainedWorkspaceHandles()
        self._metadata: RetiredDiscoveryMetadata | None = None
        self._parent: DiscoveryParentRetirement | None = None
        self._custody: ValidationWorkspaceCustody | None = None
        self._attempted = False
        self._closing = False
        self._closed = False

    def close(self) -> None:
        """Receive dependent child/Core handles before releasing the parent lease."""
        self._closing = True
        if self._metadata is not None:
            self._metadata.close()
        self._handles.close()
        if self._parent is not None:
            self._parent.close()
        self._closed = True

    def _require_current(self) -> None:
        job = self.job
        if (
            job.build_id is not None
            or job.state != "retiring"
            or job.reclamation_phase
            not in {"children", "child_directory", "corpus", "journals_root", "metadata"}
        ):
            raise StaleArchiveTask("discovery turn requires its original irreversible phase")
        with read_snapshot(self.factory) as session:
            if jobs.retire_discovery_job(session, job) != job:
                raise StaleArchiveTask("discovery turn lost its exact original catalog fence")

    @staticmethod
    def _same_entry(name: str | Path, parent: int | None, descriptor: int) -> None:
        entry = os.stat(name, dir_fd=parent, follow_symlinks=False)
        held = os.fstat(descriptor)
        if (held.st_dev, held.st_ino, held.st_mode, held.st_uid, held.st_nlink) != (
            entry.st_dev,
            entry.st_ino,
            entry.st_mode,
            entry.st_uid,
            entry.st_nlink,
        ):
            raise LostJournalValidationWorkspace("original discovery turn namespace was replaced")

    def _open(self, name: str | Path, flags: int, parent: int | None = None) -> int:
        descriptor = os.open(name, flags | os.O_NOFOLLOW, dir_fd=parent)
        self._handles.callback(_close_descriptor, descriptor)
        return descriptor

    def _acquire(self) -> str:
        import fcntl

        try:
            root = self._open(self.root, os.O_RDONLY | os.O_DIRECTORY)
        except FileNotFoundError:
            return "prefix"
        _private_directory(root)
        try:
            fcntl.flock(root, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return "busy"
        coordinator = None
        coordinated = False
        try:
            try:
                coordinator = self._open(".coordinator", os.O_RDWR, root)
            except FileNotFoundError:
                return "prefix"
            _private_file(coordinator)
            try:
                fcntl.flock(coordinator, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return "busy"
            coordinated = True
            self._same_entry(self.root, None, root)
            self._same_entry(".coordinator", root, coordinator)
            self._require_current()
            try:
                directory = self._open(self.job.workspace_id, os.O_RDONLY | os.O_DIRECTORY, root)
            except FileNotFoundError:
                return "prefix"
            _private_directory(directory)
            try:
                lease = os.open(".lease", os.O_RDWR | os.O_NOFOLLOW, dir_fd=directory)
            except FileNotFoundError:
                return "prefix"

            def close_lease() -> None:
                os.close(lease)
                if self._custody is not None:
                    self._custody._held = False

            self._handles.callback(close_lease)
            _private_file(lease)
            try:
                fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return "busy"
            custody = ValidationWorkspaceCustody(
                self.root / self.job.workspace_id, root, directory, lease
            )
            self._custody = custody
            if not custody.verify_current():
                raise LostJournalValidationWorkspace("original discovery turn custody changed")
            self._require_current()
            return "held"
        finally:
            if coordinated:
                assert coordinator is not None
                fcntl.flock(coordinator, fcntl.LOCK_UN)
            fcntl.flock(root, fcntl.LOCK_UN)

    def _finish_parent(self) -> DiscoveryRetirementProgress:
        self._parent = DiscoveryParentRetirement(self.factory, self.job, self.root)
        result = self._parent.finish()
        self.job = result.job
        return DiscoveryRetirementProgress(
            self.job, result.catalog_received, catalog_received=result.catalog_received
        )

    def _advance(self, octet_budget: int) -> DiscoveryRetirementProgress:
        if self.job.state == self.job.reclamation_phase == "directory":
            return self._finish_parent()
        self._require_current()
        selection = self._acquire()
        if selection == "busy":
            return DiscoveryRetirementProgress(self.job, False)
        if selection == "prefix":
            if self.job.storage_state != "unconfirmed" or self.job.reclamation_phase != "metadata":
                raise LostJournalValidationWorkspace("confirmed discovery lost original controls")
            # Receive our partial acquisition before the parent owner reacquires
            # root coordination and positively receives the noncreating prefix.
            self._handles.close()
            return self._finish_parent()
        assert self._custody is not None
        retirement = RetiringJournalWorkspace(self._custody.directory, self._custody)
        result: (
            DiscoveryMetadataReclamationProgress
            | DiscoveryChildReclamationProgress
            | DiscoveryCorpusReclamationProgress
            | DiscoveryRootReclamationProgress
        )
        if self.job.reclamation_phase == "metadata":
            result = reclaim_discovery_metadata(
                self.factory, self.job, retirement, octet_budget=octet_budget
            )
        else:
            self._metadata = RetiredDiscoveryMetadata(self.factory, self.job, retirement)
            self._metadata.open()
            phase = self.job.reclamation_phase
            if phase in {"children", "child_directory"}:
                result = reclaim_discovery_child(self._metadata, octet_budget=octet_budget)
                if result.children_empty:
                    self._metadata._require_current()
                    with self.factory() as session, session.begin():
                        successor = jobs.advance_discovery_reclamation(session, self.job, "corpus")
                        if successor is None:
                            raise StaleArchiveTask("empty child receiving lost its original phase")
                    self.job = successor
                    return DiscoveryRetirementProgress(successor, True)
            elif phase == "corpus":
                result = reclaim_discovery_corpus(self._metadata, octet_budget=octet_budget)
            else:
                result = reclaim_discovery_journals_root(self._metadata)
        self.job = result.job
        files = getattr(result, "files", None)
        return DiscoveryRetirementProgress(
            self.job, result.advanced, 0 if files is None else files.removed_octets
        )

    def advance(self, *, octet_budget: int) -> DiscoveryRetirementProgress:
        """Run one bounded attempt; receive all actual handles before returning."""
        if type(octet_budget) is not int or octet_budget < 1:
            raise ValueError("discovery retirement requires a positive octet budget")
        if self._attempted or self._closing or self._closed:
            raise RuntimeError("discovery retirement requires a fresh retained turn")
        self._attempted = True
        try:
            result = self._advance(octet_budget)
            self.close()
            return result
        except OSError as error:
            if error.errno in {errno.ENOSPC, errno.EDQUOT}:
                raise JournalValidationCapacity("discovery retirement lacks capacity") from None
            if error.errno in {errno.ENOENT, errno.ENOTDIR, errno.ELOOP}:
                raise LostJournalValidationWorkspace(
                    "original discovery retirement custody is unavailable"
                ) from None
            raise


declare_retirement_growth(DiscoveryRetirementTurn.advance)
