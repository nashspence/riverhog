"""Byte-admitted disposal of a retired owner's retained SQLite projection."""

from __future__ import annotations

import errno
import os
import stat
from dataclasses import dataclass
from pathlib import Path

from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)

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


def _private_file(identity: os.stat_result) -> None:
    if (
        not stat.S_ISREG(identity.st_mode)
        or identity.st_uid != os.getuid()
        or stat.S_IMODE(identity.st_mode) != 0o600
        or identity.st_nlink != 1
    ):
        raise InvalidJournalWorkspace("reclamation requires private owned regular files")


def _files(directory: int) -> list[tuple[str, os.stat_result]]:
    result = []
    for name in _FILES:
        try:
            identity = os.stat(name, dir_fd=directory, follow_symlinks=False)
        except FileNotFoundError:
            continue
        _private_file(identity)
        result.append((name, identity))
    return result


@declare_growth(
    Declaration(
        id="riverhog-provenance.posix-workspace-reclamation.growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance.journal_reclamation.reclaim_retired_journal_workspace",),
        scope="native-posix-retired-sqlite-workspace-file-extent-disposal",
        starts_at="receive one durably retired owned workspace under its exclusive kernel lease",
        ends_at="synchronize one admitted extent removal and report remaining owned files",
        dimensions=(
            Dimension(
                "retained_octets", OCTETS, "Logical extents of the remaining owned data files."
            ),
            Dimension("retained_files", COUNT, "Existing files among the four SQLite data names."),
            Dimension("octet_budget", OCTETS, "Explicit maximum logical extent removed this unit."),
        ),
        bounds=(
            Bound(
                "reclaimed_extent_octets",
                OCTETS,
                "upper",
                Expression("dimension", name="octet_budget"),
                "The actual truncated tail or admitted unlinked file extent.",
            ),
            *(
                Bound(
                    metric, COUNT, "upper", Expression("constant", value=limit, unit=COUNT), meaning
                )
                for metric, limit, meaning in (
                    ("metadata_calls", 11, "Actual os.stat and os.fstat invocations."),
                    ("owner_identity_calls", 10, "Actual os.getuid ownership probes."),
                    ("open_calls", 2, "One directory and at most one owned data handle."),
                    ("close_calls", 2, "Every opened directory or data handle closes."),
                    ("truncate_calls", 1, "At most one byte-admitted tail truncation."),
                    ("unlink_calls", 1, "At most one admitted small file unlink."),
                    ("sync_calls", 1, "The affected file or directory is synchronized."),
                )
            ),
            Bound(
                "payload_read_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "File contents are never loaded.",
            ),
            *(
                Bound(metric, COUNT, "equal", Expression("constant", value=0, unit=COUNT), meaning)
                for metric, meaning in (
                    ("enumeration_calls", "No directory enumeration occurs."),
                    ("sqlite_connections", "No database is opened or interpreted."),
                )
            ),
        ),
        conditions=(
            "The caller durably retired this exact job and excludes its writer and every reader.",
            "Private POSIX directory/file custody and the four current SQLite names are preserved.",
        ),
        invariants=(
            "Archive and retained source authority remain outside this disposable projection.",
            "Each confirmed unit removes at most its admitted extent without reconstructing bytes.",
            "The lease, directory and unrelated files remain with the application owner.",
        ),
        limitations=(
            "Syscall counts exclude kernel index work, physical I/O, wait time and CPU.",
            "Extent octets describe logical file ranges rather than physically allocated blocks.",
            "Retirement, kernel leases and final directory/catalog removal are separate.",
        ),
    )
)
def reclaim_retired_journal_workspace(
    directory: Path, *, octet_budget: int = 4 * 1024 * 1024
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
    descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        identity = os.fstat(descriptor)
        if (
            not stat.S_ISDIR(identity.st_mode)
            or identity.st_uid != os.getuid()
            or stat.S_IMODE(identity.st_mode) != 0o700
        ):
            raise InvalidJournalWorkspace("reclamation requires a private owned directory")
        before = _files(descriptor)
        removed_octets = removed_files = 0
        if before:
            name, selected = before[0]
            handle = os.open(name, os.O_RDWR | os.O_NOFOLLOW, dir_fd=descriptor)
            try:
                current = os.fstat(handle)
                _private_file(current)
                if (current.st_dev, current.st_ino, current.st_size) != (
                    selected.st_dev,
                    selected.st_ino,
                    selected.st_size,
                ):
                    raise InvalidJournalWorkspace("retired journal file was replaced")
                removed_octets = min(current.st_size, octet_budget)
                if current.st_size > octet_budget:
                    os.ftruncate(handle, current.st_size - octet_budget)
                    os.fsync(handle)
                else:
                    check = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
                    if (check.st_dev, check.st_ino) != (current.st_dev, current.st_ino):
                        raise InvalidJournalWorkspace("retired journal file was replaced")
                    os.unlink(name, dir_fd=descriptor)
                    removed_files = 1
                    os.fsync(descriptor)
            finally:
                os.close(handle)
        after = _files(descriptor)
        return JournalReclamationProgress(
            removed_octets, removed_files, sum(item.st_size for _, item in after), len(after)
        )
    except OSError as error:
        if error.errno in {errno.ENOSPC, errno.EDQUOT}:
            raise JournalValidationCapacity("retired journal reclamation lacks capacity") from None
        raise
    finally:
        os.close(descriptor)
