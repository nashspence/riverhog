"Job-owned validation directories and nonblocking kernel custody leases."

from __future__ import annotations

import errno
import os
import re
import stat
from collections.abc import Callable, Iterator
from contextlib import contextmanager
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

from riverhog_core.journal_validation_workspace_lease import ValidationWorkspaceLease
from riverhog_core.retained_workspace_handles import RetainedWorkspaceResource


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
        scope=("POSIX owner-private original workspace lease acquisition and body receiving"),
        starts_at=(
            "enter the context with the exact absolute root, workspace identity and custody mode"
        ),
        ends_at=(
            "receive the original lease after body exit; retain parents of independent references"
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
                count(21),
                (
                    "Four private descriptor checks, twelve known sidecar probes "
                    "and at most five actual native kind checks."
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
                count(5),
                (
                    "Nonblocking root-inode, coordinator and selected lease "
                    "acquisitions, plus both root coordination unlocks."
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
                        Expression("constant", value=272, unit=OCTETS),
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
            Bound("dup_calls", COUNT, "equal", count(0), "Lease selection duplicates no resource."),
            Bound(
                "native_control_octets",
                OCTETS,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        scale(2, dimension("root_path_octets")),
                        scale(4, dimension("workspace_identity_octets")),
                        Expression("constant", value=108, unit=OCTETS),
                    ),
                ),
                "Two absolute opens and the native four-scan relative name validation.",
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
                "Creation requires the current owner under root-inode and marker coordination;"
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


_RETAINED_STATE_NAMES = tuple(
    database + suffix
    for database in ("snapshot.sqlite3", "corpus.sqlite3", "discovery.sqlite3")
    for suffix in ("-journal", "-wal", "-shm", "")
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
    _resources: tuple[
        RetainedWorkspaceResource, RetainedWorkspaceResource, RetainedWorkspaceResource
    ]
    _held: bool = True

    def verify_current(self) -> bool:
        if not self._held:
            return False
        try:
            if not all(
                original.owner.state == "live"
                and original.borrow.active
                and original.fileno() == descriptor
                for original, descriptor in zip(
                    self._resources, (self._root, self._owner, self._lease), strict=True
                )
            ):
                return False
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


def _select_workspace(
    root_owner: RetainedWorkspaceResource,
    receiver: ValidationWorkspaceLease,
    *,
    workspace_id: str,
    create: bool,
    shared: bool,
    creation_current: Callable[[], bool] | None,
) -> tuple[RetainedWorkspaceResource, RetainedWorkspaceResource] | None:
    """Select under actual root-inode custody before any control can exist."""
    import fcntl

    root_fd = root_owner.fileno()
    coordinator_owner = receiver.coordinator
    coordinator_owner.acquire(".coordinator", parent=root_owner, writable=True, create=create)
    coordinator = coordinator_owner.fileno()
    _private_file(coordinator)
    os.fsync(coordinator)
    try:
        fcntl.flock(coordinator, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return None
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
        directory_owner = receiver.directory
        directory_owner.acquire(workspace_id, parent=root_owner)
        owner = directory_owner.fileno()
        _private_directory(owner)
        lease_owner = receiver.existing_lease
        try:
            lease_owner.acquire(".lease", parent=directory_owner, writable=True)
        except FileNotFoundError:
            if not create:
                raise
            for state_name in _RETAINED_STATE_NAMES:
                try:
                    os.stat(state_name, dir_fd=owner, follow_symlinks=False)
                except FileNotFoundError:
                    continue
                raise LostJournalValidationWorkspace("retained validation lease was lost") from None
            lease_owner = receiver.select_created_lease()
            lease_owner.acquire(
                ".lease", parent=directory_owner, writable=True, create=True, exclusive=True
            )
        lease = lease_owner.fileno()
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
    return None if busy else (directory_owner, lease_owner)


@contextmanager
def lease_journal_validation_workspace(
    root: Path,
    workspace_id: str,
    *,
    receiver: ValidationWorkspaceLease,
    create: bool = False,
    shared: bool = False,
    creation_current: Callable[[], bool] | None = None,
    on_custody: Callable[[ValidationWorkspaceCustody], None] | None = None,
) -> Iterator[Path | None]:
    """Lease an exact selected directory through its original retained native frame.

    The caller retains receiver and its original admission before entering.
    Body exit invalidates custody and receives the original lease. Independent
    references retain their original native parents; the caller must receive
    the complete frame before releasing its original reservation.
    """
    import fcntl

    if not root.is_absolute() or re.fullmatch(r"[0-9a-f]{32}", workspace_id) is None:
        raise ValueError("validation workspace requires an absolute root and exact owned identity")
    if not isinstance(receiver, ValidationWorkspaceLease):
        raise TypeError("validation workspace requires its retained original native lease frame")
    if type(create) is not bool or type(shared) is not bool:
        raise TypeError("validation workspace creation and reader mode must be explicit")
    if create and shared:
        raise ValueError("shared journal readers cannot create a workspace")

    exposed = False
    custody: ValidationWorkspaceCustody | None = None
    receiver.begin()
    try:
        root_created = False
        if create:
            try:
                root.mkdir(mode=0o700)
            except FileExistsError:
                pass
            else:
                root_created = True
        root_owner = receiver.root
        root_owner.acquire(root)
        root_fd = root_owner.fileno()
        _private_directory(root_fd)
        if root_created:
            receiver.parent_sync.acquire(root.parent)
            os.fsync(receiver.parent_sync.fileno())
            receiver.parent_sync.close()
        try:
            fcntl.flock(root_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            exposed = True
            yield None
            return
        try:
            selected = _select_workspace(
                root_owner,
                receiver,
                workspace_id=workspace_id,
                create=create,
                shared=shared,
                creation_current=creation_current,
            )
        finally:
            fcntl.flock(root_fd, fcntl.LOCK_UN)
        exposed = True
        if selected is None:
            yield None
            return
        directory_owner, lease_owner = selected
        custody = ValidationWorkspaceCustody(
            root / workspace_id,
            root_fd,
            directory_owner.fileno(),
            lease_owner.fileno(),
            (root_owner, directory_owner, lease_owner),
        )
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
        receiver.finish()


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
