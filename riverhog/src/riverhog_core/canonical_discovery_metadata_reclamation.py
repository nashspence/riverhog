"""One admitted Core metadata extent after durable original handle receiving."""

from __future__ import annotations

import errno
import os
from dataclasses import dataclass

from riverhog_provenance import JournalValidationCapacity
from state_schema import read_snapshot
from workspace_reclamation import (
    FileReclamationProgress,
    InvalidWorkspaceFiles,
    WorkspaceReclamationCapacity,
    reclaim_retired_files,
)

from riverhog_core import canonical_discovery_corpus_jobs as jobs
from riverhog_core.canonical_discovery_archive_work import StaleArchiveTask
from riverhog_core.canonical_discovery_metadata_reclamation_growth import (
    declare_metadata_reclamation_growth,
)
from riverhog_core.catalog_db import SessionFactory
from riverhog_core.journal_validation_retirement import RetiringJournalWorkspace
from riverhog_core.journal_validation_workspace import LostJournalValidationWorkspace

_FILES = tuple("discovery.sqlite3" + suffix for suffix in ("-journal", "-wal", "-shm", ""))
_PRIOR_NAMES = (
    *("corpus.sqlite3" + suffix for suffix in ("-journal", "-wal", "-shm", "")),
    "journals",
)


@dataclass(frozen=True, slots=True)
class DiscoveryMetadataReclamationProgress:
    job: jobs.DiscoveryJob
    advanced: bool
    files: FileReclamationProgress


def _require_current(
    factory: SessionFactory, job: jobs.DiscoveryJob, retirement: RetiringJournalWorkspace
) -> None:
    custody = retirement.custody
    if (
        job.build_id is not None
        or job.state != "retiring"
        or job.reclamation_phase != "metadata"
        or retirement.directory is None
        or retirement.directory.name != job.workspace_id
        or custody is None
        or custody.directory != retirement.directory
        or not custody.verify_current()
    ):
        raise StaleArchiveTask(
            "metadata disposal requires exact original exclusive retired custody"
        )
    with read_snapshot(factory) as session:
        if jobs.retire_discovery_job(session, job) != job:
            raise StaleArchiveTask(
                "metadata disposal lost its exact original retired catalog phase"
            )


def _advance(
    factory: SessionFactory, job: jobs.DiscoveryJob, retirement: RetiringJournalWorkspace
) -> jobs.DiscoveryJob:
    _require_current(factory, job, retirement)
    with factory() as session, session.begin():
        successor = jobs.advance_discovery_reclamation(session, job, "directory")
        if successor is None:
            raise StaleArchiveTask("metadata receiving lost its original catalog phase")
    return successor


def reclaim_discovery_metadata(
    factory: SessionFactory,
    job: jobs.DiscoveryJob,
    retirement: RetiringJournalWorkspace,
    *,
    octet_budget: int,
) -> DiscoveryMetadataReclamationProgress:
    """Dispose one original metadata extent without reopening its database.

    The caller retains the original exclusive parent lease through every actual
    Core/Native handle and worker receiving outcome. Confirmed storage reaches
    this durable phase only after child/corpus/root receiving and actual Core
    close. An unconfirmed Core creation prefix has no Native ownership.

    Current exact catalog state and original namespace custody remain available
    after partial truncation; the retired database is never reopened, parsed or
    repaired. All prior Native/root names must remain absent. The shared file
    owner receives only four Core metadata filenames and a positive byte
    admission. Synchronized final absence precedes the exact directory phase.
    Neither that phase nor a missing file reports parent/catalog completion.
    """
    if type(octet_budget) is not int or octet_budget < 1:
        raise ValueError("metadata reclamation requires a positive octet budget")
    _require_current(factory, job, retirement)
    custody = retirement.custody
    assert custody is not None and retirement.directory is not None
    for name in _PRIOR_NAMES:
        try:
            os.stat(name, dir_fd=custody._owner, follow_symlinks=False)
        except FileNotFoundError:
            continue
        raise LostJournalValidationWorkspace(
            "prior discovery files or root still require receiving"
        )
    try:
        files = reclaim_retired_files(retirement.directory, names=_FILES, octet_budget=octet_budget)
    except InvalidWorkspaceFiles as error:
        raise LostJournalValidationWorkspace(str(error)) from error
    except WorkspaceReclamationCapacity as error:
        raise JournalValidationCapacity(str(error)) from None
    _require_current(factory, job, retirement)
    successor = job
    if files.files_reclaimed:
        try:
            os.fsync(custody._owner)
        except OSError as error:
            if error.errno in {errno.ENOSPC, errno.EDQUOT}:
                raise JournalValidationCapacity(
                    "metadata absence receiving lacks capacity"
                ) from None
            raise
        successor = _advance(factory, job, retirement)
    return DiscoveryMetadataReclamationProgress(
        successor,
        successor != job or files.removed_octets > 0 or files.removed_files > 0,
        files,
    )


declare_metadata_reclamation_growth(reclaim_discovery_metadata, reclaim_retired_files)
