"""One original-bound Native corpus extent beneath retained retired Core custody."""

from __future__ import annotations

import os
from dataclasses import dataclass

from riverhog_provenance import CorpusReclamationProgress, reclaim_retired_corpus_workspace

from riverhog_core import canonical_discovery_corpus_jobs as jobs
from riverhog_core.canonical_discovery_archive_work import StaleArchiveTask
from riverhog_core.canonical_discovery_corpus_reclamation_growth import (
    declare_corpus_reclamation_growth,
)
from riverhog_core.canonical_discovery_retired_metadata import RetiredDiscoveryMetadata
from riverhog_core.journal_validation_workspace import LostJournalValidationWorkspace

_FILES = tuple("corpus.sqlite3" + suffix for suffix in ("-journal", "-wal", "-shm", ""))


@dataclass(frozen=True, slots=True)
class DiscoveryCorpusReclamationProgress:
    job: jobs.DiscoveryJob
    advanced: bool
    files: CorpusReclamationProgress | None = None


def _advance(metadata: RetiredDiscoveryMetadata) -> jobs.DiscoveryJob:
    metadata._require_current()
    with metadata.factory() as session, session.begin():
        successor = jobs.advance_discovery_reclamation(session, metadata.job, "journals_root")
        if successor is None:
            raise StaleArchiveTask("corpus reclamation lost its exact retired phase")
    return successor


def reclaim_discovery_corpus(
    metadata: RetiredDiscoveryMetadata, *, octet_budget: int
) -> DiscoveryCorpusReclamationProgress:
    """Dispose one admitted original corpus extent, preserving other owners.

    Core's exact pinned source/selection manifests and counts supply the original
    public Native binding. Current retired Core/header/file custody and the
    exclusive parent lease supply disposal authority; neither Core storage
    confirmation nor pathname labels can substitute for the original binding.

    An actual pre-pin prefix has no Native ownership. All four corpus names must
    instead be positively absent and synchronized before its phase can advance.
    No Native storage is reopened or semantic worker reconstructed. Completion
    here concerns only corpus files; root, Core metadata and parent receiving
    remain separate owners.
    """
    if type(octet_budget) is not int or octet_budget < 1:
        raise ValueError("corpus reclamation requires a positive octet budget")
    metadata._require_current()
    job = metadata.job
    if job.reclamation_phase != "corpus":
        raise StaleArchiveTask("corpus reclamation has another irreversible phase")
    if metadata.child() is not None:
        raise LostJournalValidationWorkspace("registered children still require physical receiving")
    original = (
        job.source_manifest_sha256,
        job.source_count,
        job.selection_manifest_sha256,
        job.selection_count,
    )
    if all(value is None for value in original):
        for name in _FILES:
            try:
                os.stat(name, dir_fd=metadata._custody._owner, follow_symlinks=False)
            except FileNotFoundError:
                continue
            raise LostJournalValidationWorkspace("unpinned discovery has Native corpus files")
        os.fsync(metadata._custody._owner)
        return DiscoveryCorpusReclamationProgress(_advance(metadata), True)
    try:
        binding = job.binding
    except (TypeError, ValueError) as error:
        raise StaleArchiveTask("retired corpus original Native binding is invalid") from error
    if (
        binding.attempt_id != job.attempt_id
        or binding.directory_id != metadata._custody.directory.name
        or binding.policy_sha256 != job.policy_sha256
        or binding.catalog_sha256 != job.catalog_sha256
    ):
        raise StaleArchiveTask("retired corpus has another original Native binding")
    progress = reclaim_retired_corpus_workspace(
        metadata._custody.directory, receiver=metadata._file_receiver(), octet_budget=octet_budget
    )
    metadata._require_current()
    successor = job
    if progress.files_reclaimed:
        # Receive absence after an uncertain unlink before the durable phase,
        # even when this retry's Native unit has no remaining data filename.
        os.fsync(metadata._custody._owner)
        successor = _advance(metadata)
    return DiscoveryCorpusReclamationProgress(
        successor,
        successor != job or progress.removed_octets > 0 or progress.removed_files > 0,
        files=progress,
    )


declare_corpus_reclamation_growth(reclaim_discovery_corpus, reclaim_retired_corpus_workspace)
