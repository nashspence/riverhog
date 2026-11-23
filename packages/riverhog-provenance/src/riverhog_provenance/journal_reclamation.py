"""Byte-admitted disposal of a retired owner's retained journal and generation files."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from riverhog_growth_contracts import declare_growth
from workspace_reclamation import (
    FileReclamationOwner,
    InvalidWorkspaceFiles,
    WorkspaceReclamationCapacity,
    reclaim_retired_files,
)

from ._reclamation_growth import reclamation_growth
from .errors import JournalValidationCapacity
from .journal_store import InvalidJournalWorkspace

_GENERATION_FILES = tuple(
    "generations.sqlite3" + suffix for suffix in ("-journal", "-wal", "-shm", "")
)

_FILES = tuple(
    database + suffix
    for database in ("snapshot.sqlite3", "generations.sqlite3")
    for suffix in ("-journal", "-wal", "-shm", "")
)


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
    receiver: FileReclamationOwner,
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
    return _reclaim(directory, receiver=receiver, names=_FILES, octet_budget=octet_budget)


@declare_growth(
    reclamation_growth(
        identity="riverhog-provenance.posix-generation-reclamation.growth/v1",
        subject="riverhog_provenance.journal_reclamation.reclaim_retired_generation_storage",
        names=_GENERATION_FILES,
    ),
    delegates=(("files", reclaim_retired_files),),
)
def reclaim_retired_generation_storage(
    directory: Path,
    *,
    receiver: FileReclamationOwner,
    expected_directory_identity: tuple[int, int],
    octet_budget: int,
) -> JournalReclamationProgress:
    """Dispose one admitted extent of an exclusively retired generation store.

    The caller must first establish authoritative nonpublication of the exact
    original store and receive its constructor, capture, restore and every
    independent reader. A rejected candidate inside a store retaining a selected
    predecessor does not authorize disposal of the store.

    The caller retains its live workspace lease and supplies that original
    directory's device/inode identity. Only the four Native generation filenames
    are admitted. Working semantic databases, source files, the lease and the
    directory remain untouched; this operation does not receive lifecycle costs.

    Retain the passive receiver before calling. An uncertain mutation or close
    keeps that same receiver and charge reachable. Close retries only receive
    originals; the next admitted extent requires a new, separately retained
    receiver after the previous one positively receives.
    """
    if expected_directory_identity is None:
        raise ValueError("generation reclamation requires its original directory identity")
    return _reclaim(
        directory,
        receiver=receiver,
        names=_GENERATION_FILES,
        octet_budget=octet_budget,
        expected_directory_identity=expected_directory_identity,
    )


def _reclaim(
    directory: Path,
    *,
    receiver: FileReclamationOwner,
    names: tuple[str, ...],
    octet_budget: int,
    expected_directory_identity: tuple[int, int] | None = None,
) -> JournalReclamationProgress:
    if os.name != "posix":
        raise RuntimeError("retained journal reclamation requires the POSIX owner backend")
    if type(octet_budget) is not int or octet_budget < 1:
        raise ValueError("journal reclamation requires a positive octet budget")
    try:
        progress = reclaim_retired_files(
            directory,
            receiver=receiver,
            names=names,
            octet_budget=octet_budget,
            expected_directory_identity=expected_directory_identity,
        )
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
