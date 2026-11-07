"""Exclusive byte disposal and final directory retirement for exact owned jobs."""

from __future__ import annotations

import errno
import os
import re
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
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
from riverhog_provenance import JournalValidationCapacity

from .journal_validation_workspace import (
    LostJournalValidationWorkspace,
    _private_directory,
    _private_file,
)


def _retirement_custody_declaration(mode: str) -> Declaration:
    lease = mode == "lease"

    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def scaled(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=value), term))

    operation = (
        "lease_retiring_journal_workspace"
        if lease
        else "remove_retired_journal_workspace_directory"
    )
    return Declaration(
        id="riverhog-server.validation-retirement-" + mode + ".growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.journal_validation_retirement." + operation,),
        scope="exact POSIX retired-workspace custody and "
        + ("complete lease context" if lease else "directory removal"),
        starts_at="receive an exact retired job identity and its owner-controlled root",
        ends_at=(
            "close every acquired descriptor after success, busy custody, "
            "reconciliation or rejection"
        ),
        dimensions=(
            Dimension("root_path_octets", OCTETS, "Complete native encoded input root path width."),
            Dimension(
                "workspace_identity_octets",
                OCTETS,
                ("Complete encoded input identity width, including rejected strings."),
            ),
        ),
        bounds=(
            *(
                Bound(metric, COUNT, "upper" if limit else "equal", count(limit), meaning)
                for metric, limit, meaning in (
                    (
                        "open_calls",
                        4,
                        ("Root, coordinator, selected directory and original lease attempts."),
                    ),
                    ("close_calls", 4, "Every acquired descriptor closes on completed exits."),
                    (
                        "stat_calls",
                        4 if lease else 12,
                        (
                            "Exact descriptor custody, four "
                            "data-name probes and original "
                            "lease/directory identity checks."
                        ),
                    ),
                    ("uid_calls", 4, "Each acquired custody object retains its owning-user check."),
                    (
                        "flock_calls",
                        3,
                        ("Nonblocking root and selected lease acquisition, then root unlock."),
                    ),
                    (
                        "fsync_calls",
                        1 if lease else 2,
                        (
                            "Missing-directory reconciliation or"
                            " exact "
                            "lease-unlink/directory-removal "
                            "durability."
                        ),
                    ),
                    (
                        "unlink_calls",
                        0 if lease else 1,
                        (
                            "Only the original lease can be "
                            "unlinked after all data names are "
                            "absent."
                        ),
                    ),
                    (
                        "rmdir_calls",
                        0 if lease else 1,
                        ("Only the identity-checked selected empty directory can be removed."),
                    ),
                    ("mkdir_calls", 0, "Retirement never creates or repairs storage."),
                    (
                        "truncate_calls",
                        0,
                        ("Byte-extent reclamation belongs to the separate file owner."),
                    ),
                    (
                        "directory_enumerations",
                        0,
                        ("No sibling or child directory enumeration occurs."),
                    ),
                    (
                        "sqlite_connections",
                        0,
                        ("Custody and directory retirement never interpret SQLite."),
                    ),
                )
            ),
            Bound(
                "path_octets",
                OCTETS,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        Expression("dimension", name="root_path_octets"),
                        scaled(
                            1 if lease else 3,
                            Expression("dimension", name="workspace_identity_octets"),
                        ),
                        Expression("constant", value=32 if lease else 128, unit=OCTETS),
                    ),
                ),
                (
                    "Actual encoded syscall names include the root, selected identity "
                    "and fixed relative controls/data names."
                ),
            ),
            Bound(
                "payload_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                ("No source, archive, database or unrelated file payload is read or rewritten."),
            ),
        ),
        conditions=(
            (
                "The application has durably selected this exact disposable job "
                "for irreversible retirement."
            ),
            (
                "The maintained private POSIX directory/file modes, no-follow "
                "paths and nonblocking flock backend apply."
            ),
            (
                "The lease context observes its own enter/exit work; the caller "
                "body retains its separate cost and authority."
            ),
            (
                "Directory removal requires the durable directory phase after all "
                "four owned data files have been reclaimed."
            ),
        ),
        invariants=(
            "Live sources and independently borrowed readers remain outside retirement authority.",
            (
                "Root coordination is released before the lease body while the "
                "exact original selected lease remains exclusive."
            ),
            (
                "Missing selected directories are reconciled without recreation; "
                "missing custody controls remain explicit faults."
            ),
            (
                "The root coordinator and unrelated files remain intact; "
                "unclassified children prevent directory completion."
            ),
            (
                "All acquired descriptors close on completed success, busy or "
                "rejected calls; no payload or directory scan substitutes for "
                "custody checks."
            ),
        ),
        limitations=(
            (
                "Logical syscall/name-width counts exclude kernel pathname/index "
                "work, allocation, physical I/O, lock/fsync latency and "
                "interrupted-process cleanup."
            ),
            (
                "Catalog selection/transitions, file-extent disposal and "
                "application scheduling retain their own composed scopes."
            ),
        ),
    )


_DATA_FILES = tuple("snapshot.sqlite3" + suffix for suffix in ("-journal", "-wal", "-shm", ""))


@dataclass(frozen=True, slots=True)
class RetiringJournalWorkspace:
    directory: Path | None


def _identity(root: Path, workspace_id: str) -> None:
    if not root.is_absolute() or re.fullmatch(r"[0-9a-f]{32}", workspace_id) is None:
        raise ValueError("retirement requires an absolute root and exact owned workspace identity")


@contextmanager
def lease_retiring_journal_workspace(
    root: Path, workspace_id: str
) -> Iterator[RetiringJournalWorkspace | None]:
    """Exclude writers and borrowed readers after durable irreversible retirement.

    No file or directory is created. Missing storage roots or coordination files
    remain explicit storage faults. An absent exact job directory is reconciled
    under root coordination; a present directory must retain its original lease.
    Busy ownership returns None without waiting or holding root coordination.
    """
    import fcntl

    _identity(root, workspace_id)
    exposed = False
    try:
        with ExitStack() as resources:
            parent = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            resources.callback(os.close, parent)
            _private_directory(parent)
            coordinator = os.open(".coordinator", os.O_RDWR | os.O_NOFOLLOW, dir_fd=parent)
            resources.callback(os.close, coordinator)
            _private_file(coordinator)
            try:
                fcntl.flock(coordinator, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                exposed = True
                yield None
                return
            selected: RetiringJournalWorkspace | None = None
            try:
                try:
                    directory = os.open(
                        workspace_id, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent
                    )
                except FileNotFoundError:
                    os.fsync(parent)
                    selected = RetiringJournalWorkspace(None)
                else:
                    resources.callback(os.close, directory)
                    _private_directory(directory)
                    lease = os.open(".lease", os.O_RDWR | os.O_NOFOLLOW, dir_fd=directory)
                    resources.callback(os.close, lease)
                    _private_file(lease)
                    try:
                        fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    except BlockingIOError:
                        pass
                    else:
                        selected = RetiringJournalWorkspace(root / workspace_id)
            finally:
                fcntl.flock(coordinator, fcntl.LOCK_UN)
            exposed = True
            yield selected
    except OSError as error:
        if exposed:
            raise
        if error.errno in {errno.ENOSPC, errno.EDQUOT}:
            raise JournalValidationCapacity("retired workspace storage lacks capacity") from None
        if error.errno in {errno.ENOENT, errno.ENOTDIR, errno.ELOOP}:
            raise LostJournalValidationWorkspace(
                "retired workspace custody is unavailable"
            ) from None
        raise


def remove_retired_journal_workspace_directory(root: Path, workspace_id: str) -> bool:
    """Finish only a catalog-confirmed directory phase, retaining its root marker.

    The application has already confirmed that all four data files were reclaimed
    under the exclusive original lease. Its durable directory phase forbids any
    future native open or directory creation. Root coordination serializes lease
    unlink and rmdir, including a retry after process exit between those calls.
    Unclassified files are preserved and prevent directory completion.
    """
    import fcntl

    _identity(root, workspace_id)
    try:
        with ExitStack() as resources:
            parent = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            resources.callback(os.close, parent)
            _private_directory(parent)
            coordinator = os.open(".coordinator", os.O_RDWR | os.O_NOFOLLOW, dir_fd=parent)
            resources.callback(os.close, coordinator)
            _private_file(coordinator)
            try:
                fcntl.flock(coordinator, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return False
            try:
                try:
                    directory = os.open(
                        workspace_id, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent
                    )
                except FileNotFoundError:
                    os.fsync(parent)
                    return True
                resources.callback(os.close, directory)
                _private_directory(directory)
                try:
                    lease = os.open(".lease", os.O_RDWR | os.O_NOFOLLOW, dir_fd=directory)
                except FileNotFoundError:
                    lease = None
                else:
                    assert lease is not None
                    resources.callback(os.close, lease)
                    _private_file(lease)
                    try:
                        fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    except BlockingIOError:
                        return False
                for name in _DATA_FILES:
                    try:
                        os.stat(name, dir_fd=directory, follow_symlinks=False)
                    except FileNotFoundError:
                        continue
                    raise LostJournalValidationWorkspace(
                        "directory retirement still has data files"
                    )
                if lease is not None:
                    current = os.stat(".lease", dir_fd=directory, follow_symlinks=False)
                    opened = os.fstat(lease)
                    if (current.st_dev, current.st_ino) != (opened.st_dev, opened.st_ino):
                        raise LostJournalValidationWorkspace("retirement lease was replaced")
                    os.unlink(".lease", dir_fd=directory)
                    os.fsync(directory)
                current = os.stat(workspace_id, dir_fd=parent, follow_symlinks=False)
                opened = os.fstat(directory)
                if (current.st_dev, current.st_ino) != (opened.st_dev, opened.st_ino):
                    raise LostJournalValidationWorkspace("retirement directory was replaced")
                try:
                    os.rmdir(workspace_id, dir_fd=parent)
                except OSError as error:
                    if error.errno in {errno.ENOTEMPTY, errno.EEXIST}:
                        raise LostJournalValidationWorkspace(
                            "unclassified files prevent directory retirement"
                        ) from None
                    raise
                os.fsync(parent)
                return True
            finally:
                fcntl.flock(coordinator, fcntl.LOCK_UN)
    except OSError as error:
        if error.errno in {errno.ENOSPC, errno.EDQUOT}:
            raise JournalValidationCapacity("directory retirement lacks storage capacity") from None
        if error.errno in {errno.ENOENT, errno.ENOTDIR, errno.ELOOP}:
            raise LostJournalValidationWorkspace(
                "retirement directory custody is unavailable"
            ) from None
        raise


declare_growth(_retirement_custody_declaration("lease"))(lease_retiring_journal_workspace)
declare_growth(_retirement_custody_declaration("directory"))(
    remove_retired_journal_workspace_directory
)
