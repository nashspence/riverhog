"Job-owned validation directories and nonblocking kernel custody leases."

from __future__ import annotations

import errno
import os
import re
import stat
from collections.abc import Callable, Iterator
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


def _lease_declaration() -> Declaration:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def dimension(name: str) -> Expression:
        return Expression("dimension", name=name)

    def scale(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=value), term))

    return Declaration(
        id="riverhog-server.validation-workspace-lease.growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.journal_validation_workspace.lease_journal_validation_workspace",),
        scope=("POSIX owner-private workspace lease acquisition and complete context release"),
        starts_at=(
            "enter the context with the exact absolute root, workspace identity and custody mode"
        ),
        ends_at=(
            "release all owned descriptors after normal exit, busy/stale selection or rejection"
        ),
        dimensions=(
            Dimension(
                "root_path_octets", OCTETS, "Complete native encoded absolute root path width."
            ),
            Dimension(
                "workspace_identity_octets",
                OCTETS,
                "Complete input workspace identity width, including rejected inputs.",
            ),
        ),
        bounds=(
            Bound(
                "open_calls",
                COUNT,
                "upper",
                count(6),
                (
                    "Root, optional parent, coordinator, owner directory and at "
                    "most two lease-open attempts."
                ),
            ),
            Bound(
                "close_calls",
                COUNT,
                "upper",
                count(5),
                ("The optional parent and four retained descriptors close on all completed exits."),
            ),
            Bound(
                "stat_calls",
                COUNT,
                "upper",
                count(7),
                (
                    "Four exact descriptor checks and at most three "
                    "Core/Native database-existence checks; no payload read."
                ),
            ),
            Bound(
                "uid_calls",
                COUNT,
                "upper",
                count(4),
                (
                    "Every acquired root, coordinator, owner and lease retains "
                    "its owning identity check."
                ),
            ),
            Bound(
                "mkdir_calls",
                COUNT,
                "upper",
                count(2),
                ("Only the selected root and workspace can be created, with private modes."),
            ),
            Bound(
                "fsync_calls",
                COUNT,
                "upper",
                count(5),
                (
                    "Optional parent/root durability plus the exact coordinator, "
                    "lease and owner directory."
                ),
            ),
            Bound(
                "flock_calls",
                COUNT,
                "upper",
                count(3),
                (
                    "Nonblocking coordinator and selected lease acquisitions, "
                    "plus coordinator unlock."
                ),
            ),
            Bound(
                "path_octets",
                OCTETS,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        scale(3, dimension("root_path_octets")),
                        scale(2, dimension("workspace_identity_octets")),
                        Expression("constant", value=80, unit=OCTETS),
                    ),
                ),
                (
                    "At most three root/parent path arguments and the fixed exact"
                    " relative names; no sibling enumeration."
                ),
            ),
            Bound(
                "creation_callbacks",
                COUNT,
                "upper",
                count(1),
                (
                    "Only an admitted exclusive creation attempt invokes its "
                    "caller-owned currentness callback."
                ),
            ),
            Bound(
                "custody_callbacks",
                COUNT,
                "upper",
                count(1),
                "A successful selected lease exposes its exact descriptor custody at most once.",
            ),
            Bound(
                "payload_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                (
                    "Custody checks do not read, interpret or rewrite source or "
                    "retained database payload."
                ),
            ),
            Bound(
                "directory_enumerations",
                COUNT,
                "equal",
                count(0),
                ("The exact selected identity is opened without listing siblings or children."),
            ),
        ),
        conditions=(
            (
                "The maintained POSIX backend exposes descriptor-relative "
                "paths, no-follow flags, fsync and advisory flock."
            ),
            (
                "The selected input root and workspace are owner-controlled; "
                "real kernel path/storage capacity and permission errors "
                "remain explicit failures."
            ),
            (
                "Primitive observations cover the context manager's own "
                "enter/exit work. The caller's context body and creation "
                "callback internals retain their own composed costs."
            ),
            (
                "A busy lease returns promptly without waiting; an occupied "
                "sibling is outside the selected identity's custody."
            ),
        ),
        invariants=(
            (
                "Shared readers admit only existing storage and exclude "
                "writers until every independent reader closes."
            ),
            (
                "Existing directories, databases, coordinator and leases are "
                "never silently replaced or repaired."
            ),
            (
                "Creation requires the current owner under root coordination;"
                " stale selection cannot recreate a retired identity."
            ),
            (
                "The exact selected files remain after release, with no "
                "payload interpretation, directory traversal or cleanup "
                "authority borrowed from the caller."
            ),
        ),
        limitations=(
            (
                "Logical syscall and encoded path counts exclude "
                "kernel-internal pathname traversal, filesystem lookup "
                "comparisons, physical I/O and lock latency."
            ),
            (
                "Callback internals, native validation, catalog publication, "
                "service coordination and reclamation remain separately owned"
                " work."
            ),
        ),
    )


class _StaleWorkspaceCreation(Exception):
    pass


class LostJournalValidationWorkspace(RuntimeError):
    "The retained job directory or lease is missing, replaced or incompatible."


def _private_directory(descriptor: int) -> os.stat_result:
    identity = os.fstat(descriptor)
    if (
        not stat.S_ISDIR(identity.st_mode)
        or identity.st_uid != os.getuid()
        or stat.S_IMODE(identity.st_mode) != 0o700
    ):
        raise LostJournalValidationWorkspace("validation directories require private owner custody")
    return identity


def _private_file(descriptor: int) -> os.stat_result:
    identity = os.fstat(descriptor)
    if (
        not stat.S_ISREG(identity.st_mode)
        or identity.st_uid != os.getuid()
        or stat.S_IMODE(identity.st_mode) != 0o600
        or identity.st_nlink != 1
    ):
        raise LostJournalValidationWorkspace("validation leases require private owner custody")
    return identity


@dataclass(slots=True)
class ValidationWorkspaceCustody:
    """Exact live descriptors supplied only by the held workspace lease."""

    directory: Path
    _root: int
    _owner: int
    _lease: int
    _held: bool = True

    def verify_current(self) -> bool:
        if not self._held:
            return False
        try:
            descriptors = (
                _private_directory(self._root),
                _private_directory(self._owner),
                _private_file(self._lease),
            )
            entries = (
                os.stat(self.directory.parent, follow_symlinks=False),
                os.stat(self.directory.name, dir_fd=self._root, follow_symlinks=False),
                os.stat(".lease", dir_fd=self._owner, follow_symlinks=False),
            )
            return all(
                (descriptor.st_dev, descriptor.st_ino, descriptor.st_mode)
                == (entry.st_dev, entry.st_ino, entry.st_mode)
                for descriptor, entry in zip(descriptors, entries, strict=True)
            )
        except (OSError, LostJournalValidationWorkspace):
            return False


@contextmanager
def lease_journal_validation_workspace(
    root: Path,
    workspace_id: str,
    *,
    create: bool = False,
    shared: bool = False,
    creation_current: Callable[[], bool] | None = None,
    on_custody: Callable[[ValidationWorkspaceCustody], None] | None = None,
) -> Iterator[Path | None]:
    """Lease one exact catalog-selected directory, preserving its files on release.

    All workers resuming this job use the same retained root. The catalog owns
    attempt selection, worker epochs, terminal disposition and bounded cleanup.
    A busy lease returns None without waiting; unrelated job leases remain free.
    Existing directories, databases or leases are never replaced or repaired.
    The owner creation check runs under root coordination before directory creation;
    a stale selection returns None so a retired identity cannot reappear.
    Shared leases admit only existing owner-confirmed frozen readers; all
    writers and reclamation require an exclusive lease until every reader closes.
    """
    import fcntl

    if not root.is_absolute() or re.fullmatch(r"[0-9a-f]{32}", workspace_id) is None:
        raise ValueError("validation workspace requires an absolute root and exact owned identity")
    if type(create) is not bool or type(shared) is not bool:
        raise TypeError("validation workspace creation and reader mode must be explicit")
    if create and shared:
        raise ValueError("shared journal readers cannot create a workspace")

    exposed = False
    custody: ValidationWorkspaceCustody | None = None
    try:
        with ExitStack() as resources:
            root_created = False
            if create:
                try:
                    root.mkdir(mode=0o700)
                except FileExistsError:
                    pass
                else:
                    root_created = True
            root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            resources.callback(os.close, root_fd)
            _private_directory(root_fd)
            if root_created:
                parent = os.open(root.parent, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(parent)
                finally:
                    os.close(parent)
            coordinator = os.open(
                ".coordinator",
                os.O_RDWR | (os.O_CREAT if create else 0) | os.O_NOFOLLOW,
                0o600,
                dir_fd=root_fd,
            )
            resources.callback(os.close, coordinator)
            _private_file(coordinator)
            os.fsync(coordinator)
            try:
                fcntl.flock(coordinator, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                exposed = True
                yield None
                return
            try:
                if create and creation_current is not None and creation_current() is not True:
                    raise _StaleWorkspaceCreation
                created = False
                if create:
                    try:
                        os.mkdir(workspace_id, mode=0o700, dir_fd=root_fd)
                    except FileExistsError:
                        pass
                    else:
                        created = True
                owner = os.open(
                    workspace_id, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=root_fd
                )
                resources.callback(os.close, owner)
                _private_directory(owner)
                try:
                    lease = os.open(".lease", os.O_RDWR | os.O_NOFOLLOW, dir_fd=owner)
                except FileNotFoundError:
                    if not create:
                        raise
                    # Only interrupted initial creation can lack a lease.
                    # Any retained Core or Native database establishes existing owned state.
                    for state_name in ("snapshot.sqlite3", "corpus.sqlite3", "discovery.sqlite3"):
                        try:
                            os.stat(state_name, dir_fd=owner, follow_symlinks=False)
                        except FileNotFoundError:
                            continue
                        raise LostJournalValidationWorkspace(
                            "retained validation lease was lost"
                        ) from None
                    lease = os.open(
                        ".lease",
                        os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                        0o600,
                        dir_fd=owner,
                    )
                    resources.callback(os.close, lease)
                else:
                    resources.callback(os.close, lease)
                _private_file(lease)
                busy = False
                try:
                    fcntl.flock(lease, (fcntl.LOCK_SH if shared else fcntl.LOCK_EX) | fcntl.LOCK_NB)
                except BlockingIOError:
                    busy = True
                else:
                    os.fsync(lease)
                    os.fsync(owner)
                    if created:
                        os.fsync(root_fd)
            finally:
                fcntl.flock(coordinator, fcntl.LOCK_UN)
            exposed = True
            if busy:
                yield None
                return
            custody = ValidationWorkspaceCustody(root / workspace_id, root_fd, owner, lease)
            resources.callback(setattr, custody, "_held", False)
            if on_custody is not None:
                on_custody(custody)
            yield custody.directory
    except _StaleWorkspaceCreation:
        yield None
    except OSError as error:
        if exposed:
            raise
        if error.errno in {errno.ENOSPC, errno.EDQUOT}:
            raise JournalValidationCapacity(
                "journal validation storage capacity exhausted"
            ) from None
        if error.errno in {errno.ENOENT, errno.ENOTDIR, errno.ELOOP}:
            raise LostJournalValidationWorkspace(
                "retained validation directory or lease is unavailable"
            ) from None
        raise
    finally:
        if custody is not None:
            custody._held = False


def _custody_declaration() -> Declaration:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    return Declaration(
        id="riverhog-server.validation-workspace-current.growth/v1",
        authority="riverhog-server",
        subjects=(
            "riverhog_core.journal_validation_workspace.ValidationWorkspaceCustody.verify_current",
        ),
        scope="one exact held root, directory and kernel lease identity check",
        starts_at="receive descriptor custody from the successful selected lease context",
        ends_at="return current or reject closed, missing, replaced or incompatible custody",
        dimensions=(
            Dimension("root_path_octets", OCTETS, "Complete native encoded absolute root path."),
            Dimension("workspace_identity_octets", OCTETS, "Exact selected directory identity."),
        ),
        bounds=(
            Bound("stat_calls", COUNT, "upper", count(6), "Three descriptors and exact entries."),
            Bound(
                "uid_calls", COUNT, "upper", count(3), "Each descriptor retains its owner check."
            ),
            Bound(
                "path_octets",
                OCTETS,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        Expression("dimension", name="root_path_octets"),
                        Expression("dimension", name="workspace_identity_octets"),
                        Expression("constant", value=6, unit=OCTETS),
                    ),
                ),
                "Root path, selected relative directory and exact lease filename only.",
            ),
            *(
                Bound(
                    metric,
                    COUNT,
                    "equal",
                    count(0),
                    "Verification neither changes nor traverses storage.",
                )
                for metric in (
                    "open_calls",
                    "close_calls",
                    "mkdir_calls",
                    "fsync_calls",
                    "flock_calls",
                    "creation_callbacks",
                    "custody_callbacks",
                    "directory_enumerations",
                )
            ),
            Bound(
                "payload_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "No source or retained database bytes are read.",
            ),
        ),
        conditions=("The caller retains the original lease context and descriptor custody.",),
        invariants=(
            "Closed custody never becomes current when the same descriptor numbers are reused.",
            "Root, selected directory and lease entries must still name the held descriptors.",
            "Missing or incompatible entries never authorize recreation or repair.",
        ),
        limitations=("Kernel path lookup and physical I/O latency retain filesystem ownership.",),
    )


declare_growth(_custody_declaration())(ValidationWorkspaceCustody.verify_current)


declare_growth(_lease_declaration())(lease_journal_validation_workspace)
