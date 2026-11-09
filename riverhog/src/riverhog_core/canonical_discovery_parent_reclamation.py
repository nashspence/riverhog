"""Original discovery parent receiving after irreversible file and handle disposal."""

from __future__ import annotations

import errno
import os
import re
import stat
from dataclasses import dataclass
from pathlib import Path

from riverhog_provenance import JournalValidationCapacity
from sqlalchemy import select
from state_schema import read_snapshot

from riverhog_core import canonical_discovery_corpus_jobs as jobs
from riverhog_core.canonical_discovery_archive_work import StaleArchiveTask
from riverhog_core.canonical_discovery_parent_reclamation_growth import (
    declare_parent_close_growth,
    declare_parent_reclamation_growth,
)
from riverhog_core.catalog_db import SessionFactory
from riverhog_core.catalog_discovery_validation_models import DiscoveryValidationJobRecord as Row
from riverhog_core.journal_validation_workspace import (
    LostJournalValidationWorkspace,
    _private_directory,
    _private_file,
)

_DATA_NAMES = (
    *(
        database + suffix
        for database in ("snapshot.sqlite3", "corpus.sqlite3", "discovery.sqlite3")
        for suffix in ("-journal", "-wal", "-shm", "")
    ),
    "journals",
)


@dataclass(frozen=True, slots=True)
class DiscoveryParentReceivingProgress:
    job: jobs.DiscoveryJob
    namespace_received: bool
    catalog_received: bool = False


class DiscoveryParentRetirement:
    """Retain this owner before opening any original namespace descriptors.

    An unconfirmed empty creation prefix can advance to the directory phase.
    Confirmed storage must already have that durable phase, which its file and
    handle owners establish. A missing lease never grants byte-disposal authority.

    Every creator selects under the actual global-root inode mutex before its
    catalog callback. The receiver participates before consulting an optional
    coordinator. A root not yet created cannot contain an admitted creator;
    later creation must observe irreversible retirement at that callback.

    No root, coordinator, lease or payload is created or repaired. Exact rmdir
    receives emptiness without enumerating or removing unknown entries. The
    shared root and coordinator remain. A successful result follows actual
    descriptor closure and synchronized selected absence. Reclaim preserves the
    catalog row; finish subsequently receives its exact original disposition.

    A failed close retains its descriptor and the remaining custody. The caller
    must retain this object until close is positively received. After any
    attempt, use a new owner with the current exact job for cold receiving.
    """

    def __init__(self, factory: SessionFactory, job: jobs.DiscoveryJob, root: Path):
        if (
            not root.is_absolute()
            or root == root.parent
            or root.name in {"", ".", ".."}
            or re.fullmatch(r"[0-9a-f]{32}", job.workspace_id) is None
        ):
            raise ValueError("discovery parent receiving requires an exact absolute namespace")
        self.factory = factory
        self.job = job
        self.root = root
        self._anchor: int | None = None
        self._root: int | None = None
        self._coordinator: int | None = None
        self._directory: int | None = None
        self._lease: int | None = None
        self._attempted = False
        self._closed = False
        self._catalog_absent = False

    def close(self) -> None:
        for slot in ("_lease", "_directory", "_coordinator", "_root", "_anchor"):
            descriptor = getattr(self, slot)
            if descriptor is not None:
                os.close(descriptor)
                setattr(self, slot, None)
        self._closed = True

    def _require_current(self) -> None:
        job = self.job
        if job.build_id is not None or not (
            (job.state == job.reclamation_phase == "directory")
            or (
                job.state == "retiring"
                and job.reclamation_phase == "metadata"
                and job.storage_state == "unconfirmed"
            )
        ):
            raise StaleArchiveTask("parent receiving requires its original irreversible phase")
        with read_snapshot(self.factory) as session:
            row = session.execute(
                select(Row).where(Row.workspace_id == job.workspace_id)
            ).scalar_one_or_none()
            if row is None and job.state == "directory":
                self._catalog_absent = True
                return
            if row is None or jobs.DiscoveryJob.read(row) != job:
                raise StaleArchiveTask("parent receiving lost its exact original catalog fence")
            self._catalog_absent = False

    @staticmethod
    def _same_entry(name: str | Path, parent: int | None, descriptor: int) -> None:
        entry = os.stat(name, dir_fd=parent, follow_symlinks=False)
        held = os.fstat(descriptor)
        if (entry.st_dev, entry.st_ino, entry.st_mode, entry.st_uid, entry.st_nlink) != (
            held.st_dev,
            held.st_ino,
            held.st_mode,
            held.st_uid,
            held.st_nlink,
        ):
            raise LostJournalValidationWorkspace("original discovery parent namespace was replaced")

    def _verify_anchor(self) -> None:
        assert self._anchor is not None
        held = os.fstat(self._anchor)
        if not stat.S_ISDIR(held.st_mode) or held.st_uid != os.getuid() or held.st_mode & 0o022:
            raise LostJournalValidationWorkspace("discovery root parent requires owner custody")
        self._same_entry(self.root.parent, None, self._anchor)

    def _verify_root(self) -> None:
        self._verify_anchor()
        assert self._anchor is not None and self._root is not None
        _private_directory(self._root)
        self._same_entry(self.root.name, self._anchor, self._root)
        if self._coordinator is not None:
            _private_file(self._coordinator)
            self._same_entry(".coordinator", self._root, self._coordinator)

    def _verify_directory(self) -> None:
        self._verify_root()
        assert self._root is not None and self._directory is not None
        _private_directory(self._directory)
        self._same_entry(self.job.workspace_id, self._root, self._directory)
        if self._lease is not None:
            _private_file(self._lease)
            self._same_entry(".lease", self._directory, self._lease)

    def _advance(self) -> None:
        if self.job.state == "directory":
            return
        with self.factory() as session, session.begin():
            successor = jobs.advance_discovery_reclamation(session, self.job, "directory")
            if successor is None:
                raise StaleArchiveTask("empty prefix receiving lost its original catalog phase")
        self.job = successor

    def _receive(self) -> bool:
        import fcntl

        self._anchor = os.open(self.root.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        self._verify_anchor()
        try:
            self._root = os.open(
                self.root.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=self._anchor
            )
        except FileNotFoundError:
            self._require_current()
            self._verify_anchor()
            os.fsync(self._anchor)
            self._advance()
            return True
        _private_directory(self._root)
        try:
            fcntl.flock(self._root, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        try:
            self._coordinator = os.open(
                ".coordinator", os.O_RDWR | os.O_NOFOLLOW, dir_fd=self._root
            )
        except FileNotFoundError:
            pass
        else:
            _private_file(self._coordinator)
            try:
                fcntl.flock(self._coordinator, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return False
        self._verify_root()
        self._require_current()
        try:
            self._directory = os.open(
                self.job.workspace_id,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                dir_fd=self._root,
            )
        except FileNotFoundError:
            os.fsync(self._root)
            self._advance()
            return True
        if self._catalog_absent:
            raise StaleArchiveTask("missing discovery catalog cannot dispose a surviving parent")
        _private_directory(self._directory)
        if self._coordinator is None:
            raise LostJournalValidationWorkspace("selected parent has lost its root coordinator")
        try:
            self._lease = os.open(".lease", os.O_RDWR | os.O_NOFOLLOW, dir_fd=self._directory)
        except FileNotFoundError:
            # Unconfirmed creation can stop between mkdir and lease creation.
            # Directory-phase retries can stop after their exact lease unlink.
            pass
        else:
            _private_file(self._lease)
            try:
                fcntl.flock(self._lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return False
        self._verify_directory()
        for name in _DATA_NAMES:
            try:
                os.stat(name, dir_fd=self._directory, follow_symlinks=False)
            except FileNotFoundError:
                continue
            raise LostJournalValidationWorkspace("original discovery data still require receiving")
        # All data names are absent under actual creator and optional lease
        # custody. Commit the absorbing phase before any final control unlink.
        if self.job.state != "directory":
            os.fsync(self._directory)
        self._advance()
        self._verify_directory()
        if self._lease is not None:
            os.unlink(".lease", dir_fd=self._directory)
        os.fsync(self._directory)
        self._verify_root()
        self._same_entry(self.job.workspace_id, self._root, self._directory)
        try:
            os.rmdir(self.job.workspace_id, dir_fd=self._root)
        except OSError as error:
            if error.errno in {errno.ENOTEMPTY, errno.EEXIST}:
                raise LostJournalValidationWorkspace(
                    "unclassified entries prevent discovery parent receiving"
                ) from None
            raise
        os.fsync(self._root)
        return True

    def reclaim(self) -> DiscoveryParentReceivingProgress:
        """Receive one exact parent attempt, preserving catalog and unrelated owners."""
        if self._attempted or self._closed:
            raise RuntimeError("parent receiving requires a fresh retained owner")
        self._attempted = True
        try:
            self._require_current()
            received = self._receive()
            if received:
                if self._root is None:
                    self._verify_anchor()
                else:
                    self._verify_root()
                self._require_current()
        except OSError as error:
            if error.errno in {errno.ENOSPC, errno.EDQUOT}:
                raise JournalValidationCapacity(
                    "discovery parent receiving lacks capacity"
                ) from None
            if error.errno in {errno.ENOENT, errno.ENOTDIR, errno.ELOOP}:
                raise LostJournalValidationWorkspace(
                    "original discovery parent custody is unavailable"
                ) from None
            raise
        finally:
            self.close()
        return DiscoveryParentReceivingProgress(self.job, received)

    def _delete_catalog(self) -> None:
        with self.factory() as session, session.begin():
            if not jobs.delete_retired_discovery_job(session, self.job):
                raise StaleArchiveTask("final discovery catalog receiving lost its original fence")

    def finish(self) -> DiscoveryParentReceivingProgress:
        """Receive the actual original namespace, closure and exact catalog row.

        Retain this owner before calling. Busy custody leaves the catalog intact.
        A close or commit exception never reports completion. A fresh cold
        retry keeps the original directory-phase job and positively receives
        actual absence again; a missing catalog row never authorizes removing
        a surviving namespace. Application terminal authority remains separate.
        """
        result = self.reclaim()
        if not result.namespace_received:
            return result
        self._delete_catalog()
        return DiscoveryParentReceivingProgress(self.job, True, catalog_received=True)


declare_parent_reclamation_growth(DiscoveryParentRetirement.reclaim)


declare_parent_close_growth(DiscoveryParentRetirement.close)


declare_parent_reclamation_growth(DiscoveryParentRetirement.finish, catalog=True)
