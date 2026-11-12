"""Byte-admitted disposal of a retired owner's retained corpus projection."""

from __future__ import annotations

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
from .corpus_workspace import StaleCorpusWorkspace
from .errors import JournalValidationCapacity

_FILES = tuple("corpus.sqlite3" + suffix for suffix in ("-journal", "-wal", "-shm", ""))


@dataclass(frozen=True, slots=True)
class CorpusReclamationProgress:
    removed_octets: int
    removed_files: int
    remaining_octets: int
    remaining_files: int

    @property
    def files_reclaimed(self) -> bool:
        return self.remaining_files == 0


@declare_growth(
    reclamation_growth(
        identity="riverhog-provenance.posix-corpus-reclamation.growth/v1",
        subject="riverhog_provenance.corpus_reclamation.reclaim_retired_corpus_workspace",
        names=_FILES,
    ),
    delegates=(("files", reclaim_retired_files),),
)
def reclaim_retired_corpus_workspace(
    directory: Path,
    *,
    receiver: FileReclamationOwner,
    octet_budget: int = 4 * 1024 * 1024,
) -> CorpusReclamationProgress:
    """Dispose one admitted extent of an exclusively retired Native corpus.

    The caller owns durable irreversible retirement and the original exclusive
    workspace lease, including every writer and reader. Only corpus.sqlite3 and
    its SQLite sidecars belong to this operation. Core routing metadata, journals,
    leases, unrelated files and final namespace/catalog removal remain with their
    respective owners. No file-format parser or directory enumeration runs.
    """
    try:
        progress = reclaim_retired_files(
            directory, receiver=receiver, names=_FILES, octet_budget=octet_budget
        )
    except InvalidWorkspaceFiles as error:
        raise StaleCorpusWorkspace(str(error)) from None
    except WorkspaceReclamationCapacity as error:
        raise JournalValidationCapacity(str(error)) from None
    return CorpusReclamationProgress(
        progress.removed_octets,
        progress.removed_files,
        progress.remaining_octets,
        progress.remaining_files,
    )
