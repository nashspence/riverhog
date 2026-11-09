"""Byte-admitted extent disposal inside an exactly retired private POSIX workspace."""

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

__all__ = [
    "FileReclamationProgress",
    "InvalidWorkspaceFiles",
    "WorkspaceReclamationCapacity",
    "reclaim_retired_files",
]


class InvalidWorkspaceFiles(RuntimeError):
    """The selected namespace does not retain private original file custody."""


class WorkspaceReclamationCapacity(RuntimeError):
    """An admitted physical disposal could not complete its storage synchronization."""


@dataclass(frozen=True, slots=True)
class FileReclamationProgress:
    removed_octets: int
    removed_files: int
    remaining_octets: int
    remaining_files: int

    @property
    def files_reclaimed(self) -> bool:
        """Only the caller's exact named set is exhausted."""
        return self.remaining_files == 0


def _compare_names(left: bytes, right: bytes, *, equal: bool = False) -> bool:
    return left == right if equal else left <= right


def _validate_names(names: tuple[str, ...]) -> None:
    if not isinstance(names, tuple):
        raise TypeError("workspace file names require an explicit tuple")
    encoded = []
    for name in names:
        if not isinstance(name, str) or name in {"", ".", ".."} or "/" in name or "\x00" in name:
            raise ValueError("workspace file names must be exact single path components")
        encoded.append(os.fsencode(name))
    # A bounded comparison sort validates uniqueness without relying on hash
    # collision behavior and preserves the caller's independent disposal order.
    count = len(encoded)
    ordered, scratch = encoded, [b""] * count
    width = 1
    while width < count:
        for start in range(0, count, 2 * width):
            middle, end = min(start + width, count), min(start + 2 * width, count)
            left, right = start, middle
            for target in range(start, end):
                if right == end or (
                    left < middle and _compare_names(ordered[left], ordered[right])
                ):
                    scratch[target] = ordered[left]
                    left += 1
                else:
                    scratch[target] = ordered[right]
                    right += 1
        ordered, scratch = scratch, ordered
        width *= 2
    for index in range(1, count):
        if _compare_names(ordered[index - 1], ordered[index], equal=True):
            raise ValueError("workspace file names must be distinct")


def _private_file(identity: os.stat_result) -> None:
    if (
        not stat.S_ISREG(identity.st_mode)
        or identity.st_uid != os.getuid()
        or stat.S_IMODE(identity.st_mode) != 0o600
        or identity.st_nlink != 1
    ):
        raise InvalidWorkspaceFiles("reclamation requires private owned regular files")


def _files(directory: int, names: tuple[str, ...]) -> list[tuple[str, os.stat_result]]:
    selected = []
    for name in names:
        try:
            identity = os.stat(name, dir_fd=directory, follow_symlinks=False)
        except FileNotFoundError:
            continue
        _private_file(identity)
        selected.append((name, identity))
    return selected


def _declaration() -> Declaration:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def dimension(name: str) -> Expression:
        return Expression("dimension", name=name)

    def add(*values: Expression) -> Expression:
        return Expression("sum", terms=values)

    def scale(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=value), term))

    candidates = dimension("candidate_files")
    comparisons = scale(
        2,
        Expression(
            "product",
            terms=(
                candidates,
                Expression("log2_ceil", terms=(add(count(2), candidates),)),
            ),
        ),
    )
    comparison_octets = Expression(
        "product",
        terms=(
            Expression("constant", value=2, unit=(("count", -1),)),
            comparisons,
            dimension("maximum_name_octets"),
        ),
    )
    return Declaration(
        id="workspace-reclamation.private-file-extents.growth/v1",
        authority="workspace-reclamation",
        subjects=("workspace_reclamation.reclaim_retired_files",),
        scope="one admitted extent from the exact caller-owned private file set",
        starts_at="receive a durably retired namespace under exclusive writer/reader custody",
        ends_at="synchronize the admitted removal and report the remaining selected files",
        dimensions=(
            Dimension("candidate_files", COUNT, "Complete exact proposed filename cardinality."),
            Dimension(
                "name_octets", OCTETS, "Complete filesystem-encoded proposed filename bytes."
            ),
            Dimension(
                "maximum_name_octets", OCTETS, "Largest filesystem-encoded proposed filename."
            ),
            Dimension("retained_files", COUNT, "Existing files in the exact selected set."),
            Dimension("retained_octets", OCTETS, "Current logical extents in that selected set."),
            Dimension("octet_budget", OCTETS, "Explicit maximum logical extent removed this call."),
        ),
        bounds=(
            Bound(
                "encoded_name_octets",
                OCTETS,
                "upper",
                dimension("name_octets"),
                "Actual encoding of each proposed name at most once.",
            ),
            Bound(
                "name_order_comparisons",
                COUNT,
                "upper",
                comparisons,
                "Bottom-up merge comparisons plus one adjacent uniqueness pass.",
            ),
            Bound(
                "name_comparison_octets",
                OCTETS,
                "upper",
                comparison_octets,
                "Complete operand widths of the actual name comparisons.",
            ),
            Bound(
                "reclaimed_extent_octets",
                OCTETS,
                "upper",
                dimension("octet_budget"),
                "Actual truncated tail or admitted unlinked file extent.",
            ),
            Bound(
                "metadata_calls",
                COUNT,
                "upper",
                add(scale(2, candidates), count(3)),
                "Both exact filename passes, directory/file fstat and unlink identity.",
            ),
            Bound(
                "owner_identity_calls",
                COUNT,
                "upper",
                add(scale(2, candidates), count(2)),
                "Actual ownership probes for directory and selected regular files.",
            ),
            *(
                Bound(metric, COUNT, "upper", count(limit), meaning)
                for metric, limit, meaning in (
                    ("open_calls", 2, "One directory and at most one owned data handle."),
                    ("close_calls", 2, "Every opened directory/data handle closes."),
                    ("truncate_calls", 1, "At most one admitted tail truncation."),
                    ("unlink_calls", 1, "At most one admitted small file unlink."),
                    ("sync_calls", 1, "The affected file or directory is synchronized."),
                )
            ),
            Bound(
                "payload_read_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "No file content is read.",
            ),
            *(
                Bound(metric, COUNT, "equal", count(0), meaning)
                for metric, meaning in (
                    ("enumeration_calls", "Only the exact supplied filenames are probed."),
                    ("sqlite_connections", "No database is opened or interpreted."),
                )
            ),
        ),
        conditions=(
            "The caller has durably retired this exact namespace and excludes every writer/reader.",
            "Private POSIX directory/file custody and the exact caller-owned names are preserved.",
            "The octet budget is a positive integer; names are distinct single path components.",
        ),
        invariants=(
            "No archive authority, receiving receipt, lease or unclassified file is selected.",
            "Caller order is retained; uniqueness validation uses a separate comparison sort.",
            "A crashed or uncertain mutation reconciles from its actual remaining file extent.",
        ),
        limitations=(
            "Syscall counts exclude kernel path/index work, physical IO, wait time and CPU.",
            "Logical file extents are not physically allocated block counts.",
            "The caller retains irreversible retirement, identity, leases and final "
            "namespace removal.",
        ),
    )


@declare_growth(_declaration())
def reclaim_retired_files(
    directory: Path,
    *,
    names: tuple[str, ...],
    octet_budget: int,
) -> FileReclamationProgress:
    """Remove one byte-admitted extent from an exactly retired owned file set.

    The caller supplies positive original namespace ownership, durable irreversible
    retirement, and exclusive custody over every writer and reader. This function
    validates private POSIX files, not application identities or disposal rights.
    Only exact supplied names are inspected. The caller's lease, directory and
    every unclassified entry remain intact. Missing named data is reconciled;
    missing directory custody remains a fault. No SQLite parser or enumeration
    runs, and a damaged retired data file needs no reconstruction.
    """
    if os.name != "posix":
        raise RuntimeError("workspace reclamation requires the POSIX owner backend")
    if type(octet_budget) is not int or octet_budget < 1:
        raise ValueError("workspace reclamation requires a positive octet budget")
    _validate_names(names)
    descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        identity = os.fstat(descriptor)
        if (
            not stat.S_ISDIR(identity.st_mode)
            or identity.st_uid != os.getuid()
            or stat.S_IMODE(identity.st_mode) != 0o700
        ):
            raise InvalidWorkspaceFiles("reclamation requires a private owned directory")
        before = _files(descriptor, names)
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
                    raise InvalidWorkspaceFiles("retired workspace file was replaced")
                removed_octets = min(current.st_size, octet_budget)
                if current.st_size > octet_budget:
                    os.ftruncate(handle, current.st_size - octet_budget)
                    os.fsync(handle)
                else:
                    check = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
                    if (check.st_dev, check.st_ino) != (current.st_dev, current.st_ino):
                        raise InvalidWorkspaceFiles("retired workspace file was replaced")
                    os.unlink(name, dir_fd=descriptor)
                    removed_files = 1
                    os.fsync(descriptor)
            finally:
                os.close(handle)
        after = _files(descriptor, names)
        return FileReclamationProgress(
            removed_octets,
            removed_files,
            sum(item.st_size for _, item in after),
            len(after),
        )
    except OSError as error:
        if error.errno in {errno.ENOSPC, errno.EDQUOT}:
            raise WorkspaceReclamationCapacity(
                "retired workspace reclamation lacks capacity"
            ) from None
        raise
    finally:
        os.close(descriptor)
