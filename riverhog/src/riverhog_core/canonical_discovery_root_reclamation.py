"""Receive the original journals namespace and Core close before metadata disposal."""

from __future__ import annotations

import errno
import os
from dataclasses import dataclass

from riverhog_provenance import JournalValidationCapacity

from riverhog_core import canonical_discovery_corpus_jobs as jobs
from riverhog_core.canonical_discovery_archive_work import StaleArchiveTask
from riverhog_core.canonical_discovery_retired_metadata import RetiredDiscoveryMetadata
from riverhog_core.canonical_discovery_root_reclamation_growth import (
    declare_root_reclamation_growth,
)
from riverhog_core.journal_validation_workspace import (
    LostJournalValidationWorkspace,
    _private_directory,
    _private_file,
)

_CORPUS_FILES = tuple("corpus.sqlite3" + suffix for suffix in ("-journal", "-wal", "-shm", ""))


@dataclass(frozen=True, slots=True)
class DiscoveryRootReclamationProgress:
    job: jobs.DiscoveryJob
    advanced: bool
    metadata_closed: bool = False


def _same_entry(name: str, parent: int, descriptor: int) -> None:
    entry = os.stat(name, dir_fd=parent, follow_symlinks=False)
    held = os.fstat(descriptor)
    if (entry.st_dev, entry.st_ino, entry.st_mode) != (held.st_dev, held.st_ino, held.st_mode):
        raise LostJournalValidationWorkspace("original discovery journals namespace was replaced")


def _receive_namespace(metadata: RetiredDiscoveryMetadata) -> bool:
    import fcntl

    if metadata._child_root is not None or metadata._root_coordinator is not None:
        raise LostJournalValidationWorkspace("discovery journals namespace closure is unconfirmed")
    parent = metadata._custody._owner
    try:
        try:
            metadata._child_root = os.open(
                "journals", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent
            )
        except FileNotFoundError:
            os.fsync(parent)
            return True
        root = metadata._child_root
        _private_directory(root)
        try:
            fcntl.flock(root, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        try:
            metadata._root_coordinator = os.open(
                ".coordinator", os.O_RDWR | os.O_NOFOLLOW, dir_fd=root
            )
        except FileNotFoundError:
            # Actual creation can stop before this control exists; irreversible
            # root removal can also lose its reply after unlinking it. Neither
            # prefix permits control repair or an unclassified child deletion.
            pass
        else:
            coordinator = metadata._root_coordinator
            _private_file(coordinator)
            try:
                fcntl.flock(coordinator, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return False
            _same_entry(".coordinator", root, coordinator)
        _same_entry("journals", parent, root)
        metadata._require_current()
        if metadata._root_coordinator is not None:
            os.unlink(".coordinator", dir_fd=root)
            os.fsync(root)
        _same_entry("journals", parent, root)
        try:
            os.rmdir("journals", dir_fd=parent)
        except OSError as error:
            if error.errno in {errno.ENOTEMPTY, errno.EEXIST}:
                raise LostJournalValidationWorkspace(
                    "unclassified entries prevent discovery journals-root retirement"
                ) from None
            raise
        os.fsync(parent)
        return True
    finally:
        metadata._close_namespace_handles()


def _advance(metadata: RetiredDiscoveryMetadata) -> jobs.DiscoveryJob:
    # Core was verified under exclusive parent custody immediately before its
    # actual close. No metadata extent can start while that close is uncertain.
    if (
        not metadata._closed
        or metadata._db is not None
        or metadata._descriptor is not None
        or metadata.job != metadata._original
        or not metadata._custody.verify_current()
    ):
        raise StaleArchiveTask("discovery metadata disposal lacks actual original Core closure")
    with metadata.factory() as session, session.begin():
        successor = jobs.advance_discovery_reclamation(session, metadata.job, "metadata")
        if successor is None:
            raise StaleArchiveTask("journals-root receiving lost its exact original catalog phase")
    return successor


def reclaim_discovery_journals_root(
    metadata: RetiredDiscoveryMetadata,
) -> DiscoveryRootReclamationProgress:
    """Receive exact root absence, actual Core close and one durable phase.

    The caller retains the original exclusive parent lease and this metadata
    object through actual close/worker receiving. Registered children must be
    empty and the separate corpus phase must already be complete. No root or
    coordination marker is created, repaired or inferred from a missing row.

    Exact rmdir receives emptiness without enumerating or deleting unknown
    entries. A busy root returns promptly with Core still open. Completed root
    receiving closes every acquired namespace and Core handle before the exact
    metadata phase CAS. Lost sync, close or commit replies preserve the same
    original identity for cold receiving; no metadata bytes are disposed here.
    """
    metadata._require_current()
    job = metadata.job
    if job.reclamation_phase != "journals_root":
        raise StaleArchiveTask("journals-root receiving has another irreversible phase")
    if metadata.child() is not None:
        raise LostJournalValidationWorkspace("registered children still require physical receiving")
    for name in _CORPUS_FILES:
        try:
            os.stat(name, dir_fd=metadata._custody._owner, follow_symlinks=False)
        except FileNotFoundError:
            continue
        raise LostJournalValidationWorkspace("corpus files still require physical receiving")
    try:
        if not _receive_namespace(metadata):
            return DiscoveryRootReclamationProgress(job, False)
        metadata._require_current()
        metadata.close()
        return DiscoveryRootReclamationProgress(_advance(metadata), True, metadata_closed=True)
    except OSError as error:
        if error.errno in {errno.ENOSPC, errno.EDQUOT}:
            raise JournalValidationCapacity(
                "discovery journals-root retirement lacks capacity"
            ) from None
        if error.errno in {errno.ENOENT, errno.ENOTDIR, errno.ELOOP}:
            raise LostJournalValidationWorkspace(
                "original discovery journals custody is unavailable"
            ) from None
        raise


declare_root_reclamation_growth(reclaim_discovery_journals_root)
