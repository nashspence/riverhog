"""One bounded physical unit beneath original retired discovery custody."""

from __future__ import annotations

import os
from dataclasses import dataclass

from riverhog_provenance import JournalReclamationProgress, reclaim_retired_journal_workspace

from riverhog_core import canonical_discovery_corpus_jobs as jobs
from riverhog_core.canonical_discovery_archive_work import StaleArchiveTask
from riverhog_core.canonical_discovery_child_reclamation_growth import (
    declare_child_reclamation_growth,
)
from riverhog_core.canonical_discovery_journal_registration import DiscoveryJournalRegistration
from riverhog_core.canonical_discovery_retired_metadata import RetiredDiscoveryMetadata
from riverhog_core.journal_validation_retirement import (
    _DATA_FILES,
    RetiringJournalWorkspace,
    lease_retiring_journal_workspace,
    remove_retired_journal_workspace_directory,
)
from riverhog_core.journal_validation_workspace import (
    LostJournalValidationWorkspace,
    _private_directory,
)
from riverhog_core.retained_workspace_handles import _close_descriptor


@dataclass(frozen=True, slots=True)
class DiscoveryChildReclamationProgress:
    job: jobs.DiscoveryJob
    advanced: bool
    files: JournalReclamationProgress | None = None
    children_empty: bool = False


def _same_entry(name: str, parent: int, descriptor: int) -> None:
    entry = os.stat(name, dir_fd=parent, follow_symlinks=False)
    identity = os.fstat(descriptor)
    if (entry.st_dev, entry.st_ino, entry.st_mode) != (
        identity.st_dev,
        identity.st_ino,
        identity.st_mode,
    ):
        raise LostJournalValidationWorkspace("original child namespace was replaced")


def _creation_prefix(
    metadata: RetiredDiscoveryMetadata, selected: DiscoveryJournalRegistration
) -> str:
    """Receive exact absence or an unopened private prefix; never repair controls."""
    parent = metadata._custody._owner
    with metadata._child_handles as resources:
        try:
            root_fd = os.open(
                "journals", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent
            )
        except FileNotFoundError:
            os.fsync(parent)
            return "absent"
        resources.callback(_close_descriptor, root_fd)
        _private_directory(root_fd)
        try:
            child = os.open(
                selected.binding.directory_id,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                dir_fd=root_fd,
            )
        except FileNotFoundError:
            os.fsync(root_fd)
            _same_entry("journals", parent, root_fd)
            return "absent"
        resources.callback(_close_descriptor, child)
        _private_directory(child)
        try:
            os.stat(".lease", dir_fd=child, follow_symlinks=False)
        except FileNotFoundError:
            if selected.confirmed:
                raise LostJournalValidationWorkspace(
                    "confirmed retired child lost its lease"
                ) from None
            for name in _DATA_FILES:
                try:
                    os.stat(name, dir_fd=child, follow_symlinks=False)
                except FileNotFoundError:
                    continue
                raise LostJournalValidationWorkspace(
                    "unopened child prefix contains Native data"
                ) from None
            os.fsync(child)
            os.fsync(root_fd)
            _same_entry(selected.binding.directory_id, root_fd, child)
            _same_entry("journals", parent, root_fd)
            return "unopened"
        _same_entry(selected.binding.directory_id, root_fd, child)
        _same_entry("journals", parent, root_fd)
    return "present"


def _directory_missing(metadata: RetiredDiscoveryMetadata) -> bool:
    parent = metadata._custody._owner
    with metadata._child_handles as resources:
        try:
            root = os.open("journals", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
        except FileNotFoundError:
            return True
        resources.callback(_close_descriptor, root)
        _private_directory(root)
        assert metadata.job.reclamation_directory_id is not None
        try:
            os.stat(metadata.job.reclamation_directory_id, dir_fd=root, follow_symlinks=False)
        except FileNotFoundError:
            missing = True
        else:
            missing = False
        _same_entry("journals", parent, root)
        return missing


def _selected_current(
    metadata: RetiredDiscoveryMetadata,
    selected: DiscoveryJournalRegistration,
    child: RetiringJournalWorkspace,
) -> None:
    custody = child.custody
    if (
        custody is None
        or child.directory
        != metadata._custody.directory / "journals" / selected.binding.directory_id
        or custody.directory != child.directory
        or not custody.verify_current()
        or metadata.child() != selected
    ):
        raise StaleArchiveTask("registered child lost its exact original exclusive custody")


def _advance(
    metadata: RetiredDiscoveryMetadata,
    phase: str,
    selected: DiscoveryJournalRegistration | None = None,
) -> jobs.DiscoveryJob:
    metadata._require_current()
    with metadata.factory() as session, session.begin():
        successor = jobs.advance_discovery_reclamation(
            session,
            metadata.job,
            phase,
            journal_id=None if selected is None else selected.binding.journal_id,
            directory_id=None if selected is None else selected.binding.directory_id,
        )
        if successor is None:
            raise StaleArchiveTask("child reclamation lost its exact retired catalog phase")
    return successor


def reclaim_discovery_child(
    metadata: RetiredDiscoveryMetadata, *, octet_budget: int
) -> DiscoveryChildReclamationProgress:
    """Receive one actual file or selected-directory unit before returning progress.

    The caller retains metadata before opening and keeps its original exclusive
    parent lease through actual metadata/worker close receiving. This synchronous
    unit holds the original exclusive child lease around Native extent disposal;
    a successful return receives every context it acquired. An exception never
    reports completion or releases the caller's parent/resource ownership.

    Child-directory state is durable only after positive synchronized file
    completion, including actual unopened creation prefixes. It survives
    interruption between lease unlink, rmdir, routing deletion and catalog CAS.
    Unknown entries are preserved. Empty children do not prove parent, corpus,
    root, metadata or catalog completion.
    """
    if type(octet_budget) is not int or octet_budget < 1:
        raise ValueError("child reclamation requires a positive octet budget")
    metadata._require_current()
    job = metadata.job
    selected = metadata.child()
    root = metadata._custody.directory / "journals"
    if job.reclamation_phase == "child_directory":
        if selected is not None:
            # The selected routing may already be deleted after a lost reply.
            # The durable catalog still retains its exact namespace identity.
            if selected.binding.directory_id != job.reclamation_directory_id:
                raise StaleArchiveTask("directory unit has another registered child")
        assert job.reclamation_directory_id is not None
        if not _directory_missing(metadata):
            if not remove_retired_journal_workspace_directory(
                root, job.reclamation_directory_id, handles=metadata._child_handles
            ):
                return DiscoveryChildReclamationProgress(job, False)
        metadata.retire_child()
        return DiscoveryChildReclamationProgress(_advance(metadata, "children"), True)
    if job.reclamation_phase != "children":
        raise StaleArchiveTask("child reclamation has another irreversible phase")
    if selected is None:
        return DiscoveryChildReclamationProgress(job, False, children_empty=True)
    prefix = _creation_prefix(metadata, selected)
    if metadata.child() != selected:
        raise StaleArchiveTask("child creation prefix lost its original registration")
    if prefix != "present":
        return DiscoveryChildReclamationProgress(
            _advance(metadata, "child_directory", selected), True
        )
    with lease_retiring_journal_workspace(
        root, selected.binding.directory_id, handles=metadata._child_handles
    ) as child:
        if child is None:
            return DiscoveryChildReclamationProgress(job, False)
        if child.directory is None:
            # The actual coordinated missing-directory receiver, rather than a
            # missing registration or a pathname label, supplies this proof.
            return DiscoveryChildReclamationProgress(
                _advance(metadata, "child_directory", selected), True
            )
        _selected_current(metadata, selected, child)
        progress = reclaim_retired_journal_workspace(child.directory, octet_budget=octet_budget)
        _selected_current(metadata, selected, child)
        successor = job
        if progress.files_reclaimed:
            assert child.custody is not None
            # A retry after an uncertain unlink may observe all names absent.
            # Receive that namespace durability before committing the phase.
            os.fsync(child.custody._owner)
            _selected_current(metadata, selected, child)
            successor = _advance(metadata, "child_directory", selected)
        return DiscoveryChildReclamationProgress(
            successor,
            successor != job or progress.removed_octets > 0 or progress.removed_files > 0,
            files=progress,
        )


declare_child_reclamation_growth(reclaim_discovery_child, reclaim_retired_journal_workspace)
