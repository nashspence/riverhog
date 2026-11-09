"""Byte-admitted disposal of a retired owner's retained journal projection."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from riverhog_growth_contracts import declare_growth
from workspace_reclamation import (
    InvalidWorkspaceFiles,
    WorkspaceReclamationCapacity,
    reclaim_retired_files,
)

from ._reclamation_growth import reclamation_growth
from .errors import JournalValidationCapacity
from .journal_store import InvalidJournalWorkspace

_FILES = tuple("snapshot.sqlite3" + suffix for suffix in ("-journal", "-wal", "-shm", ""))


@dataclass(frozen=True, slots=True)
class JournalReclamationProgress:
    removed_octets: int
    removed_files: int
    remaining_octets: int
    remaining_files: int

    @property
    def files_reclaimed(self) -> bool:
        return self.remaining_files == 0


@declare_growth(
    reclamation_growth(
        identity="riverhog-provenance.posix-workspace-reclamation.growth/v1",
        subject="riverhog_provenance.journal_reclamation.reclaim_retired_journal_workspace",
        names=_FILES,
    ),
    delegates=(("files", reclaim_retired_files),),
)
def reclaim_retired_journal_workspace(
    directory: Path,
    *,
    octet_budget: int = 4 * 1024 * 1024,
) -> JournalReclamationProgress:
    """Dispose at most one admitted file extent after exclusive owner retirement.

    The owning application must durably retire this exact job before calling and
    hold its kernel lease, including exclusion of every independent result reader.
    The workspace is no longer a resumable validator once retirement begins.
    No call here authorizes retirement or authenticates an imported database.

    Each unit removes at most octet_budget bytes from one known SQLite file.
    A large file loses only its admitted tail, followed by fsync. An admitted
    small file is unlinked and the directory synchronized. A crashed unit is
    reconciled from the current owned file extent; already retired bytes need
    neither reconstruction nor a scan. Even damaged indexes can be disposed.

    This never opens SQLite, walks directory contents, or removes the directory
    or the application's lease. files_reclaimed reports only these owned files;
    the application completes its separate directory/catalog retirement phase.
    POSIX private-directory/file semantics are required by this backend.
    """
    if os.name != "posix":
        raise RuntimeError("retained journal reclamation requires the POSIX owner backend")
    if type(octet_budget) is not int or octet_budget < 1:
        raise ValueError("journal reclamation requires a positive octet budget")
    try:
        progress = reclaim_retired_files(directory, names=_FILES, octet_budget=octet_budget)
    except InvalidWorkspaceFiles as error:
        raise InvalidJournalWorkspace(
            str(error).replace("retired workspace", "retired journal")
        ) from None
    except WorkspaceReclamationCapacity:
        raise JournalValidationCapacity("retired journal reclamation lacks capacity") from None
    return JournalReclamationProgress(
        progress.removed_octets,
        progress.removed_files,
        progress.remaining_octets,
        progress.remaining_files,
    )
