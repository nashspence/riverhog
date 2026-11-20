"""Exact application-owned namespaces for disposable primary preparation facts."""

from __future__ import annotations

import json
import os
import re
import stat
from collections.abc import Callable
from pathlib import Path
from threading import Lock

from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)
from riverhog_protocol.errors import Conflict

from riverhog_core.journal_validation_workspace import (
    LostJournalValidationWorkspace,
    ValidationWorkspaceCustody,
    _private_directory,
    _private_file,
)
from riverhog_core.journal_validation_workspace_lease import ValidationWorkspaceLease
from riverhog_core.services.primary_facts_jobs import PrimaryFactsJob

CORPUS_FILES = tuple("corpus.sqlite3" + suffix for suffix in ("-journal", "-wal", "-shm", ""))
MEMO_FILES = tuple("checks.sqlite3" + suffix for suffix in ("-journal", "-wal", "-shm", ""))
DATA_FILES = CORPUS_FILES + MEMO_FILES


def identity(value: os.stat_result) -> tuple[str, str, str, str]:
    return str(value.st_dev), str(value.st_ino), str(value.st_mode), str(value.st_uid)


def encode_identity(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def namespace_identity(directory: int, lease: int) -> str:
    return encode_identity(
        {"directory": identity(os.fstat(directory)), "lease": identity(os.fstat(lease))}
    )


def require_namespace_identity(
    expected: str, directory: int, lease: int | None, *, missing_lease: bool = False
) -> None:
    """Compare the durable original pin, including a final unlinked lease retry."""
    try:
        pin = json.loads(expected)
        valid = (
            isinstance(pin, dict)
            and set(pin) == {"directory", "lease"}
            and all(
                isinstance(pin[key], list)
                and len(pin[key]) == 4
                and all(isinstance(item, str) and item.isdecimal() for item in pin[key])
                for key in pin
            )
            and encode_identity(pin) == expected
        )
    except (ValueError, TypeError):
        valid = False
    if not valid:
        raise LostJournalValidationWorkspace("primary preparation namespace pin is invalid")
    if tuple(pin["directory"]) != identity(os.fstat(directory)):
        raise LostJournalValidationWorkspace("primary preparation directory was replaced")
    if lease is None:
        if not missing_lease:
            raise LostJournalValidationWorkspace("primary preparation lost its original lease")
    elif tuple(pin["lease"]) != identity(os.fstat(lease)):
        raise LostJournalValidationWorkspace("primary preparation lease was replaced")


def require_root(root: Path, descriptor: int, expected: str) -> None:
    held = _private_directory(descriptor)
    entry = os.stat(root, follow_symlinks=False)
    if identity(entry) != identity(held) or encode_identity(identity(held)) != expected:
        raise LostJournalValidationWorkspace("primary preparation root was replaced")


def require_no_data(directory: int) -> None:
    for name in DATA_FILES:
        try:
            os.stat(name, dir_fd=directory, follow_symlinks=False)
        except FileNotFoundError:
            continue
        raise LostJournalValidationWorkspace("unconfirmed primary namespace contains index data")


def require_empty_lease(descriptor: int) -> None:
    if _private_file(descriptor).st_size:
        raise LostJournalValidationWorkspace("primary preparation lease has unexpected content")


def capture_root_identity(root: Path) -> str:
    """Read the already provisioned private root; never create or repair it."""
    if not root.is_absolute() or root == root.parent:
        raise ValueError("primary preparation requires an absolute owned root")
    value = os.stat(root, follow_symlinks=False)
    if (
        not stat.S_ISDIR(value.st_mode)
        or value.st_uid != os.getuid()
        or stat.S_IMODE(value.st_mode) != 0o700
    ):
        raise LostJournalValidationWorkspace("primary preparation root requires private custody")
    return encode_identity(identity(value))


class PrimaryFactsWorkspace:
    """Retain one exclusive original lease across live preparation turns.

    The allocated catalog identity commits before open. The namespace pin must
    separately commit before either index constructor is invoked. Unknown
    acquisition and close outcomes retain this frame; retries only receive it.
    """

    def __init__(self, root: Path, job: PrimaryFactsJob):
        if (
            not root.is_absolute()
            or root == root.parent
            or re.fullmatch(r"[0-9a-f]{32}", job.workspace_id) is None
        ):
            raise ValueError("primary preparation requires an exact absolute namespace")
        self.root, self.job = root, job
        self.path = root / job.workspace_id
        self.lease = ValidationWorkspaceLease()
        self.custody: ValidationWorkspaceCustody | None = None
        self._execution = Lock()
        self._attempted = False
        self._body = False
        self._closing = False
        self._deferred = False

    @property
    def received(self) -> bool:
        return self.lease.received

    @property
    def namespace_identity(self) -> str:
        custody = self.custody
        if custody is None or not custody.verify_current():
            raise LostJournalValidationWorkspace("primary preparation has no live original custody")
        return namespace_identity(custody._owner, custody._lease)

    def _same_scope(self, job: PrimaryFactsJob) -> bool:
        return all(
            getattr(job, name) == getattr(self.job, name)
            for name in (
                "workspace_id",
                "collection_id",
                "construction",
                "epoch",
                "owner_id",
                "root_identity",
            )
        )

    def require_current(self, job: PrimaryFactsJob) -> None:
        custody = self.custody
        if (
            self._closing
            or job.state != "pinned"
            or job.namespace_identity is None
            or not self._same_scope(job)
            or custody is None
            or not custody.verify_current()
        ):
            raise Conflict("primary preparation lost its exact pinned live namespace")
        require_root(self.root, custody._root, job.root_identity)
        require_namespace_identity(job.namespace_identity, custody._owner, custody._lease)

    def open(self, creation_current: Callable[[], bool]) -> bool:
        """Acquire once; a false result follows positive receiving of busy custody."""
        import fcntl

        if not self._execution.acquire(blocking=False):
            raise BlockingIOError("primary workspace acquisition is already executing")
        try:
            if self._closing:
                raise RuntimeError("primary workspace receiving has started")
            if self.custody is not None:
                if not self.custody.verify_current():
                    raise LostJournalValidationWorkspace("primary preparation custody changed")
                return True
            if self._attempted:
                raise RuntimeError("primary workspace acquisition has an unconfirmed outcome")
            if self.job.state not in {"allocated", "pinned"}:
                raise Conflict("retired primary preparation cannot create or reopen storage")
            self._attempted = True
            self.lease.begin()
            self._body = True
            held_root = held_coordinator = False
            receiver = self.lease
            receiver.root.acquire(self.root)
            root = receiver.root.fileno()
            require_root(self.root, root, self.job.root_identity)
            try:
                try:
                    fcntl.flock(root, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    return self._busy()
                held_root = True
                require_root(self.root, root, self.job.root_identity)
                if creation_current() is not True:
                    return self._busy()
                creating = self.job.state == "allocated"
                receiver.coordinator.acquire(
                    ".coordinator", parent=receiver.root, writable=True, create=creating
                )
                coordinator = receiver.coordinator.fileno()
                _private_file(coordinator)
                try:
                    fcntl.flock(coordinator, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    return self._busy()
                held_coordinator = True
                if creating:
                    os.fsync(coordinator)
                    os.fsync(root)
                    try:
                        os.mkdir(self.job.workspace_id, mode=0o700, dir_fd=root)
                    except FileExistsError:
                        pass
                receiver.directory.acquire(self.job.workspace_id, parent=receiver.root)
                directory = receiver.directory.fileno()
                _private_directory(directory)
                if creating:
                    require_no_data(directory)
                try:
                    receiver.existing_lease.acquire(
                        ".lease", parent=receiver.directory, writable=True
                    )
                except FileNotFoundError:
                    if not creating:
                        raise LostJournalValidationWorkspace(
                            "pinned primary preparation lost its lease"
                        ) from None
                    receiver.select_created_lease().acquire(
                        ".lease",
                        parent=receiver.directory,
                        writable=True,
                        create=True,
                        exclusive=True,
                    )
                lease = receiver.lease.fileno()
                require_empty_lease(lease)
                try:
                    fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    return self._busy()
                if self.job.namespace_identity is not None:
                    require_namespace_identity(self.job.namespace_identity, directory, lease)
                if creating:
                    os.fsync(lease)
                    os.fsync(directory)
                    os.fsync(root)
                custody = ValidationWorkspaceCustody(
                    self.path,
                    root,
                    directory,
                    lease,
                    (receiver.root, receiver.directory, receiver.lease),
                )
                if not custody.verify_current():
                    raise LostJournalValidationWorkspace("primary preparation custody changed")
                self.custody = custody
                return True
            finally:
                # Busy paths receive only after releasing coordination; close
                # can retain originals whose actual native outcome is unknown.
                if held_coordinator:
                    fcntl.flock(receiver.coordinator.fileno(), fcntl.LOCK_UN)
                if held_root:
                    fcntl.flock(root, fcntl.LOCK_UN)
        finally:
            try:
                if self._deferred:
                    self._close_originals()
            finally:
                self._execution.release()

    def _busy(self) -> bool:
        # Open's outer finally receives after releasing root coordination.
        self._deferred = True
        return False

    def _close_originals(self) -> None:
        self._closing = True
        if self._body:
            self._body = False
            if self.custody is not None:
                self.custody._held = False
            self.lease.finish()
        else:
            self.lease.close()

    def close(self) -> None:
        """Receive the original after both database owners and actual I/O exit."""
        if not self._execution.acquire(blocking=False):
            raise BlockingIOError("primary workspace execution still uses its borrows")
        try:
            self._close_originals()
        finally:
            self._execution.release()


def physical_declaration(
    name: str,
    subject: str,
    limits: dict[str, int],
    *,
    extent: bool = False,
    receiving: bool = False,
) -> Declaration:
    """Inclusive fixed-profile physical bounds; catalog callbacks own their SQL."""

    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def dimension(value: str) -> Expression:
        return Expression("dimension", name=value)

    def scale(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=value), term))

    width = (
        Expression("constant", value=0, unit=OCTETS)
        if receiving
        else Expression(
            "sum",
            terms=(
                scale(16, dimension("root_path_octets")),
                scale(32, dimension("control_octets")),
                Expression("constant", value=8192, unit=OCTETS),
            ),
        )
    )
    return Declaration(
        id="riverhog-server.primary-facts-" + name + ".growth/v1",
        authority="riverhog-server",
        subjects=(subject,),
        scope="one original exact primary workspace physical operation and native receiving",
        starts_at="receive the exact registered job, private root and explicit physical admission",
        ends_at="return only after receiving, or retain the same uncertain original on exception",
        dimensions=(
            Dimension("root_path_octets", OCTETS, "Complete encoded configured root path."),
            Dimension(
                "control_octets", OCTETS, "Complete original job and proposed control width."
            ),
            *(
                (Dimension("octet_budget", OCTETS, "Admitted data extent for this turn."),)
                if extent
                else ()
            ),
        ),
        bounds=(
            *(
                Bound(
                    metric,
                    COUNT,
                    "upper" if value else "equal",
                    count(value),
                    "Actual direct and delegated fixed-profile physical primitives or callbacks.",
                )
                for metric, value in limits.items()
            ),
            *(
                Bound(
                    metric,
                    OCTETS,
                    "upper",
                    width,
                    "Complete physical path/control operands; includes native owner validation.",
                )
                for metric in (
                    "path_octets",
                    "native_control_octets",
                    "json_encoded_octets",
                    "json_decoded_octets",
                )
            ),
            *(
                Bound(
                    metric,
                    OCTETS,
                    "equal",
                    Expression("constant", value=0, unit=OCTETS),
                    "No source reconstruction, database payload interpretation or replay.",
                )
                for metric in ("source_payload_octets", "payload_read_octets")
            ),
            *(
                (
                    Bound(
                        "reclaimed_extent_octets",
                        OCTETS,
                        "upper",
                        dimension("octet_budget"),
                        "Only one selected database extent; the checked lease control is empty.",
                    ),
                )
                if extent
                else ()
            ),
        ),
        conditions=(
            "The eight exact index/SQLite-sidecar names and an empty lease define the profile.",
            "Private POSIX directories, no-follow handles, fsync and nonblocking flock apply.",
            "Catalog callback internals retain separately declared SQL bounds.",
            "The caller retains the original receiver and its charge through actual executor join.",
        ),
        invariants=(
            "Durable root/namespace identity and the exact current catalog fence precede disposal.",
            "Unknown native outcomes retain original dependencies without raw descriptor retries.",
            "No enumeration, recursive deletion, index reopening or semantic replay occurs.",
        ),
        limitations=(
            "Logical syscall counts exclude kernel index traversal and physical I/O latency.",
            "Catalog SQL, scheduling, registration and index construction are separate owners.",
        ),
    )


_ZERO = {
    "dup_calls": 0,
    "directory_enumerations": 0,
    "index_sqlite_connections": 0,
    "truncate_calls": 0,
    "unlink_calls": 0,
    "rmdir_calls": 0,
}
declare_growth(
    physical_declaration(
        "root-identity",
        __name__ + ".capture_root_identity",
        {
            **_ZERO,
            "open_calls": 0,
            "close_calls": 0,
            "stat_calls": 1,
            "uid_calls": 1,
            "flock_calls": 0,
            "fsync_calls": 0,
            "mkdir_calls": 0,
        },
    )
)(capture_root_identity)
declare_growth(
    physical_declaration(
        "workspace-open",
        __name__ + ".PrimaryFactsWorkspace.open",
        {
            **_ZERO,
            "open_calls": 5,
            "close_calls": 4,
            "stat_calls": 26,
            "uid_calls": 8,
            "flock_calls": 5,
            "fsync_calls": 5,
            "mkdir_calls": 1,
            "current_calls": 1,
        },
    )
)(PrimaryFactsWorkspace.open)
declare_growth(
    physical_declaration(
        "workspace-current",
        __name__ + ".PrimaryFactsWorkspace.require_current",
        {
            **_ZERO,
            "open_calls": 0,
            "close_calls": 0,
            "stat_calls": 10,
            "uid_calls": 4,
            "flock_calls": 0,
            "fsync_calls": 0,
            "mkdir_calls": 0,
        },
    )
)(PrimaryFactsWorkspace.require_current)
declare_growth(
    physical_declaration(
        "workspace-close",
        __name__ + ".PrimaryFactsWorkspace.close",
        {
            **_ZERO,
            "open_calls": 0,
            "close_calls": 4,
            "stat_calls": 0,
            "uid_calls": 0,
            "flock_calls": 0,
            "fsync_calls": 0,
            "mkdir_calls": 0,
        },
        receiving=True,
    )
)(PrimaryFactsWorkspace.close)
