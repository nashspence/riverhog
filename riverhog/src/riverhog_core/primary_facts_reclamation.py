"""Bounded physical receiving of exact retired primary preparation namespaces."""

from __future__ import annotations

import errno
import os
import re
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Protocol

from riverhog_growth_contracts import declare_growth
from riverhog_protocol.errors import Conflict
from riverhog_provenance import CorpusReclamationProgress, reclaim_retired_corpus_workspace
from workspace_reclamation import (
    FileReclamationOwner,
    FileReclamationProgress,
    reclaim_retired_files,
)

from riverhog_core.journal_validation_workspace import (
    LostJournalValidationWorkspace,
    _private_directory,
    _private_file,
)
from riverhog_core.primary_facts_workspace import (
    DATA_FILES,
    MEMO_FILES,
    identity,
    physical_declaration,
    require_empty_lease,
    require_namespace_identity,
    require_no_data,
    require_root,
)
from riverhog_core.retained_workspace_handles import (
    RetainedWorkspaceHandles,
    RetainedWorkspaceResource,
)
from riverhog_core.services.primary_facts_jobs import PrimaryFactsJob


class PrimaryFactsCleanup(Protocol):
    @property
    def root(self) -> Path: ...

    def current(self, job: PrimaryFactsJob) -> bool: ...

    def transition(self, job: PrimaryFactsJob, state: str) -> PrimaryFactsJob: ...

    def finish(self, job: PrimaryFactsJob) -> bool: ...


@dataclass(frozen=True, slots=True)
class PrimaryFactsReclamationProgress:
    job: PrimaryFactsJob
    advanced: bool
    removed_octets: int = 0
    catalog_received: bool = False


class PrimaryFactsRetirementTurn:
    """Retain this original and its charge before one bounded physical attempt.

    Exact catalog registration, the durable root/namespace pins and an exclusive
    kernel lease establish eligibility. An unpinned creation prefix can own no
    database bytes. The irreversible corpus/memo/directory phases survive every
    interruption. An exception retains all acquired originals until close; a
    successful result follows native receiving, and final catalog deletion
    follows synchronized namespace absence and every original descriptor close.
    """

    def __init__(self, cleanup: PrimaryFactsCleanup, job: PrimaryFactsJob):
        if (
            not cleanup.root.is_absolute()
            or cleanup.root == cleanup.root.parent
            or re.fullmatch(r"[0-9a-f]{32}", job.workspace_id) is None
        ):
            raise ValueError("primary retirement requires an exact absolute namespace")
        self.cleanup, self.job, self.root = cleanup, job, cleanup.root
        self._files = FileReclamationOwner()
        self._handles = RetainedWorkspaceHandles()
        self._root = RetainedWorkspaceResource("directory")
        self._coordinator = RetainedWorkspaceResource("file")
        self._directory = RetainedWorkspaceResource("directory")
        self._lease = RetainedWorkspaceResource("file")
        for original in (self._root, self._coordinator, self._directory, self._lease):
            self._handles.callback(original.close)
        self._execution = Lock()
        self._attempted = False
        self._closing = False
        self._closed = False
        self._root_locked = self._coordinator_locked = False
        self._directory_present = self._lease_present = False
        self._directory_identity: tuple[int, int] | None = None
        self._namespace_received = False

    @property
    def received(self) -> bool:
        return self._closed

    def _require_current(self) -> None:
        if not self.cleanup.current(self.job):
            raise Conflict("primary preparation retirement lost its exact catalog fence")

    def _same_entry(self, name: str, parent: int, original: RetainedWorkspaceResource) -> None:
        held = os.fstat(original.fileno())
        entry = os.stat(name, dir_fd=parent, follow_symlinks=False)
        if identity(held) != identity(entry) or held.st_nlink != entry.st_nlink:
            raise LostJournalValidationWorkspace("primary preparation namespace was replaced")

    def _verify(self) -> None:
        require_root(self.root, self._root.fileno(), self.job.root_identity)
        if self._coordinator.owner.state == "live":
            self._same_entry(".coordinator", self._root.fileno(), self._coordinator)
        if self._directory_present:
            self._same_entry(self.job.workspace_id, self._root.fileno(), self._directory)
            _private_directory(self._directory.fileno())
            if self._lease_present:
                self._same_entry(".lease", self._directory.fileno(), self._lease)
                require_empty_lease(self._lease.fileno())
            if self.job.namespace_identity is not None:
                require_namespace_identity(
                    self.job.namespace_identity,
                    self._directory.fileno(),
                    self._lease.fileno() if self._lease_present else None,
                    missing_lease=self.job.state == "directory",
                )

    def _unlock_coordination(self) -> None:
        import fcntl

        if self._coordinator_locked:
            fcntl.flock(self._coordinator.fileno(), fcntl.LOCK_UN)
            self._coordinator_locked = False
        if self._root_locked:
            fcntl.flock(self._root.fileno(), fcntl.LOCK_UN)
            self._root_locked = False

    def _acquire(self) -> str:
        import fcntl

        self._root.acquire(self.root)
        root = self._root.fileno()
        require_root(self.root, root, self.job.root_identity)
        try:
            fcntl.flock(root, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return "busy"
        self._root_locked = True
        require_root(self.root, root, self.job.root_identity)
        current = self.cleanup.current(self.job)
        if not current and self.job.state != "directory":
            raise Conflict("primary preparation retirement lost its exact catalog fence")
        try:
            self._coordinator.acquire(".coordinator", parent=self._root, writable=True)
        except FileNotFoundError:
            # A registered creator can die before root coordination exists.
            # No selected namespace may then exist, and retirement creates none.
            try:
                os.stat(self.job.workspace_id, dir_fd=root, follow_symlinks=False)
            except FileNotFoundError:
                if self.job.namespace_identity is not None and self.job.state != "directory":
                    raise LostJournalValidationWorkspace(
                        "pinned primary preparation lost root coordination"
                    ) from None
                os.fsync(root)
                return "absent"
            raise LostJournalValidationWorkspace(
                "primary preparation namespace lost root coordination"
            ) from None
        coordinator = self._coordinator.fileno()
        _private_file(coordinator)
        try:
            fcntl.flock(coordinator, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return "busy"
        self._coordinator_locked = True
        try:
            self._directory.acquire(self.job.workspace_id, parent=self._root)
        except FileNotFoundError:
            if self.job.namespace_identity is not None and self.job.state != "directory":
                raise LostJournalValidationWorkspace(
                    "pinned primary preparation directory is missing"
                ) from None
            os.fsync(root)
            return "absent"
        self._directory_present = True
        if not current:
            raise Conflict("missing primary catalog cannot dispose a surviving namespace")
        directory = self._directory.fileno()
        held_directory = _private_directory(directory)
        self._directory_identity = held_directory.st_dev, held_directory.st_ino
        try:
            self._lease.acquire(".lease", parent=self._directory, writable=True)
        except FileNotFoundError:
            if self.job.namespace_identity is not None and self.job.state != "directory":
                raise LostJournalValidationWorkspace(
                    "pinned primary preparation lost its original lease"
                ) from None
        else:
            self._lease_present = True
            lease = self._lease.fileno()
            require_empty_lease(lease)
            try:
                fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return "busy"
        self._verify()
        if self.job.namespace_identity is None:
            require_no_data(directory)
        self._require_current()
        return "held"

    def _transition(self, state: str) -> None:
        self._verify()
        self._require_current()
        self.job = self.cleanup.transition(self.job, state)

    def _directory_unit(self, selection: str) -> PrimaryFactsReclamationProgress:
        if selection == "absent":
            require_root(self.root, self._root.fileno(), self.job.root_identity)
            self._namespace_received = True
            return PrimaryFactsReclamationProgress(self.job, True)
        self._verify()
        self._require_current()
        directory = self._directory.fileno()
        for name in DATA_FILES:
            try:
                os.stat(name, dir_fd=directory, follow_symlinks=False)
            except FileNotFoundError:
                continue
            raise LostJournalValidationWorkspace("primary preparation data still require receiving")
        if self._lease_present:
            self._verify()
            os.unlink(".lease", dir_fd=directory)
            self._lease_present = False
        os.fsync(directory)
        require_root(self.root, self._root.fileno(), self.job.root_identity)
        self._same_entry(self.job.workspace_id, self._root.fileno(), self._directory)
        try:
            os.rmdir(self.job.workspace_id, dir_fd=self._root.fileno())
        except OSError as error:
            if error.errno in {errno.ENOTEMPTY, errno.EEXIST}:
                raise LostJournalValidationWorkspace(
                    "unclassified entries prevent primary namespace receiving"
                ) from None
            raise
        self._directory_present = False
        os.fsync(self._root.fileno())
        require_root(self.root, self._root.fileno(), self.job.root_identity)
        self._namespace_received = True
        return PrimaryFactsReclamationProgress(self.job, True)

    def _advance(self, octet_budget: int) -> PrimaryFactsReclamationProgress:
        if self.job.state not in {"allocated", "pinned", "corpus", "memo", "directory"}:
            raise Conflict("primary preparation has an invalid retirement phase")
        if self.job.state != "directory":
            self._require_current()
        selection = self._acquire()
        if selection == "busy":
            return PrimaryFactsReclamationProgress(self.job, False)
        if self.job.state == "directory":
            return self._directory_unit(selection)
        # An actual lease excludes every creator after coordination releases.
        # A pre-lease prefix instead keeps coordination through its irreversible
        # phase CAS, so a checked creator cannot race the retirement decision.
        if self._lease_present:
            self._unlock_coordination()
        if self.job.state in {"allocated", "pinned"}:
            self._transition("corpus")
            return PrimaryFactsReclamationProgress(self.job, True)
        successor = "memo" if self.job.state == "corpus" else "directory"
        if selection == "absent" or self.job.namespace_identity is None:
            if self._directory_present:
                require_no_data(self._directory.fileno())
                os.fsync(self._directory.fileno())
            self._transition(successor)
            return PrimaryFactsReclamationProgress(self.job, True)
        self._verify()
        self._require_current()
        path = self.root / self.job.workspace_id
        files: CorpusReclamationProgress | FileReclamationProgress
        if self.job.state == "corpus":
            files = reclaim_retired_corpus_workspace(
                path,
                receiver=self._files,
                octet_budget=octet_budget,
                expected_directory_identity=self._directory_identity,
            )
        else:
            files = reclaim_retired_files(
                path,
                receiver=self._files,
                names=MEMO_FILES,
                octet_budget=octet_budget,
                expected_directory_identity=self._directory_identity,
            )
        self._verify()
        changed = files.removed_octets > 0 or files.removed_files > 0
        if files.files_reclaimed:
            os.fsync(self._directory.fileno())
            self._transition(successor)
            changed = True
        return PrimaryFactsReclamationProgress(self.job, changed, files.removed_octets)

    def _receive(self) -> None:
        self._closing = True
        self._files.close()
        self._handles.close()
        self._closed = True

    def close(self) -> None:
        if not self._execution.acquire(blocking=False):
            raise BlockingIOError("primary retirement executor still uses its original borrows")
        try:
            self._receive()
        finally:
            self._execution.release()

    def advance(self, *, octet_budget: int) -> PrimaryFactsReclamationProgress:
        """Dispose one extent or commit one phase, then receive the original."""
        if type(octet_budget) is not int or octet_budget < 1:
            raise ValueError("primary retirement requires a positive octet budget")
        if not self._execution.acquire(blocking=False):
            raise BlockingIOError("primary retirement executor still uses its original borrows")
        try:
            if self._attempted or self._closing:
                raise RuntimeError("primary retirement requires a fresh retained turn")
            self._attempted = True
            result = self._advance(octet_budget)
            self._receive()
            if self._namespace_received:
                if not self.cleanup.finish(self.job):
                    raise Conflict("primary final catalog receiving lost its exact original")
                return PrimaryFactsReclamationProgress(
                    self.job, True, result.removed_octets, catalog_received=True
                )
            return result
        finally:
            self._execution.release()


declare_growth(
    physical_declaration(
        "retirement-turn",
        __name__ + ".PrimaryFactsRetirementTurn.advance",
        {
            "open_calls": 6,
            "close_calls": 6,
            "dup_calls": 0,
            "stat_calls": 80,
            "uid_calls": 32,
            "flock_calls": 5,
            "fsync_calls": 3,
            "truncate_calls": 1,
            "unlink_calls": 1,
            "rmdir_calls": 1,
            "mkdir_calls": 0,
            "directory_enumerations": 0,
            "index_sqlite_connections": 0,
            "current_calls": 5,
            "transition_calls": 1,
            "finish_calls": 1,
        },
        extent=True,
    )
)(PrimaryFactsRetirementTurn.advance)
declare_growth(
    physical_declaration(
        "retirement-close",
        __name__ + ".PrimaryFactsRetirementTurn.close",
        {
            "open_calls": 0,
            "close_calls": 6,
            "dup_calls": 0,
            "stat_calls": 0,
            "uid_calls": 0,
            "flock_calls": 0,
            "fsync_calls": 0,
            "truncate_calls": 0,
            "unlink_calls": 0,
            "rmdir_calls": 0,
            "mkdir_calls": 0,
            "directory_enumerations": 0,
            "index_sqlite_connections": 0,
            "current_calls": 0,
            "transition_calls": 0,
            "finish_calls": 0,
        },
        receiving=True,
    )
)(PrimaryFactsRetirementTurn.close)
