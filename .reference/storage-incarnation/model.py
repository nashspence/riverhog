"""Executable, non-authoritative #908 reference; not imported by Riverhog.

The filesystem witness is real. SQLite models catalog invariants, not Riverhog's
PostgreSQL implementation. Copy recording assumes an already verified receipt.
"""

from __future__ import annotations

import fcntl
import os
import re
import sqlite3
import stat
from collections.abc import Iterator
from contextlib import closing, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol
from uuid import UUID, uuid4

MARKER = ".riverhog-incarnation"
LOCK = ".riverhog-incarnation.lock"
MAGIC = b"riverhog-storage-incarnation/v1\n"


class IdentityError(RuntimeError):
    """No safe binding may be inferred from this failure."""


class IdentityUnavailable(IdentityError):
    pass


class IdentityMismatch(IdentityError):
    pass


class BindingUnavailable(IdentityError):
    pass


def incarnation(value: str) -> str:
    """Accept one canonical UUID4 spelling; never coerce names or URLs."""
    if not isinstance(value, str):
        raise IdentityError("incarnation must be a canonical UUID4")
    try:
        parsed = UUID(value)
    except ValueError as exc:
        raise IdentityError("incarnation must be a canonical UUID4") from exc
    if str(parsed) != value or parsed.version != 4:
        raise IdentityError("incarnation must be a canonical UUID4")
    return value


@contextmanager
def _root(path: Path) -> Iterator[int]:
    # Opening once pins this operation to a single root, even if its path moves.
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        yield fd
    finally:
        os.close(fd)


def _read_identity(root_fd: int) -> str:
    fd = os.open(
        MARKER, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC,
        dir_fd=root_fd,
    )
    with os.fdopen(fd, "rb") as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise IdentityError("identity marker must be a regular file")
        raw = stream.read(128)
    if len(raw) != len(MAGIC) + 37 or not raw.startswith(MAGIC) or raw[-1:] != b"\n":
        raise IdentityError("invalid identity marker; never repair automatically")
    try:
        return incarnation(raw[len(MAGIC):-1].decode("ascii"))
    except UnicodeError as exc:
        raise IdentityError("invalid identity marker encoding") from exc


def provision(root: Path) -> str:
    """Explicit enrollment preparation, never called by descriptor/read/startup.

    The operator must establish the intended mounted, exclusively owned root.
    Existing valid markers are preserved; unmarked nonempty roots are refused.
    This Linux reference serializes provisioners and publishes a fsynced marker
    without replacement. Crash debris is deliberately not silently adopted.
    """
    with _root(root) as fd:
        lock_fd = os.open(
            LOCK, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600,
            dir_fd=fd,
        )
        with os.fdopen(lock_fd, "rb") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            try:
                existing = _read_identity(fd)
            except FileNotFoundError:
                pass
            else:
                # Also settle a prior publish whose directory fsync was interrupted.
                os.fsync(fd)
                return existing
            if set(os.listdir(fd)) - {LOCK}:
                raise IdentityError("refuse to initialize an unmarked nonempty root")
            candidate = incarnation(str(uuid4()))
            temporary = f"{MARKER}.{uuid4().hex}.tmp"
            temp_fd = os.open(
                temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC,
                0o600, dir_fd=fd,
            )
            with os.fdopen(temp_fd, "wb") as stream:
                stream.write(MAGIC + candidate.encode("ascii") + b"\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.link(temporary, MARKER, src_dir_fd=fd, dst_dir_fd=fd, follow_symlinks=False)
            os.fsync(fd)
            os.unlink(temporary, dir_fd=fd)
            os.fsync(fd)
            return _read_identity(fd)


@dataclass(frozen=True)
class Evidence:
    incarnation_id: str
    readable: bool = True
    writable: bool = True

    def __post_init__(self) -> None:
        incarnation(self.incarnation_id)
        if type(self.readable) is not bool or type(self.writable) is not bool:
            raise IdentityError("capabilities must be booleans")


class Adapter(Protocol):
    def describe(self) -> Evidence: ...

    def read(self, expected_incarnation: str, object_key: str) -> bytes:
        """Verify expected identity at the backend effect boundary, not just HTTP routing."""
        ...


class FilesystemWitness:
    def __init__(self, root: Path) -> None:
        self.root = root

    def describe(self) -> Evidence:
        try:
            with _root(self.root) as fd:
                return Evidence(_read_identity(fd))
        except OSError as exc:
            raise IdentityUnavailable("backend identity cannot be read") from exc

    def read(self, expected_incarnation: str, object_key: str) -> bytes:
        expected = incarnation(expected_incarnation)
        # Flat fixture keys only: this is not the production object-path parser.
        if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", object_key) is None:
            raise ValueError("reference object key must be a safe flat filename")
        try:
            with _root(self.root) as fd:
                if _read_identity(fd) != expected:
                    raise IdentityMismatch("backend incarnation changed before object I/O")
                object_fd = os.open(
                    object_key, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC,
                    dir_fd=fd,
                )
                with os.fdopen(object_fd, "rb") as stream:
                    if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                        raise IdentityError("reference object must be a regular file")
                    return stream.read()
        except OSError as exc:
            raise IdentityUnavailable("backend object cannot be read") from exc


_SCHEMA = """
CREATE TABLE incarnations (id TEXT PRIMARY KEY NOT NULL);
CREATE TABLE bindings (
    name TEXT PRIMARY KEY NOT NULL,
    owner TEXT NOT NULL REFERENCES incarnations(id),
    endpoint TEXT NOT NULL,
    configured INTEGER NOT NULL CHECK(configured IN (0,1)),
    generation INTEGER NOT NULL CHECK(generation > 0)
);
CREATE UNIQUE INDEX one_configured_binding_per_owner
    ON bindings(owner) WHERE configured = 1;
CREATE TABLE archive_copies (
    id TEXT PRIMARY KEY NOT NULL,
    owner TEXT NOT NULL REFERENCES incarnations(id),
    object_key TEXT NOT NULL
);
CREATE TABLE pending_work (
    id TEXT PRIMARY KEY NOT NULL,
    owner TEXT NOT NULL REFERENCES incarnations(id),
    object_key TEXT NOT NULL
);
CREATE TRIGGER immutable_archive_owner BEFORE UPDATE OF owner ON archive_copies
WHEN NEW.owner != OLD.owner BEGIN SELECT RAISE(ABORT, 'immutable archive owner'); END;
CREATE TRIGGER immutable_work_owner BEFORE UPDATE OF owner ON pending_work
WHEN NEW.owner != OLD.owner BEGIN SELECT RAISE(ABORT, 'immutable work owner'); END;
"""


class Catalog:
    """Reference registration/resolution service with durable identity FKs.

    Call initialize once for a new reference database; opening never migrates it.
    Live verified clients are deliberately not restored from database rows.
    """

    @staticmethod
    def initialize(path: Path) -> None:
        with closing(sqlite3.connect(path)) as db:
            db.executescript("BEGIN IMMEDIATE;" + _SCHEMA + "COMMIT;")

    def __init__(self, path: Path) -> None:
        self._db = sqlite3.connect(f"{path.resolve().as_uri()}?mode=rw", uri=True,
                                   isolation_level=None)
        self._db.execute("PRAGMA foreign_keys = ON")
        self._live: dict[str, tuple[int, Adapter]] = {}
        self.observations: dict[str, str] = {}

    def close(self) -> None:
        self._live.clear()
        self._db.close()

    @contextmanager
    def _transaction(self) -> Iterator[None]:
        self._db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self._db.commit()
        except BaseException:
            self._db.rollback()
            raise

    def _binding(self, name: str) -> tuple[str, str, int, int] | None:
        return self._db.execute(
            "SELECT owner, endpoint, configured, generation FROM bindings WHERE name=?",
            (name,),
        ).fetchone()

    def bind(self, name: str, endpoint: str, expected: str, adapter: Adapter,
             *, replacing: str | None = None) -> None:
        """Verify an explicit expected ID; replacement additionally needs the old ID.

        Neither name nor endpoint is an identity witness. The replacement path
        changes only the current alias: no historical FK is ever rewritten.
        """
        incarnation(expected)
        if not name or not endpoint:
            raise ValueError("name and endpoint are required")
        before = self._binding(name)
        self._live.pop(name, None)  # A failed rebind cannot retain old live admission.
        self.observations[name] = "unverified"
        try:
            evidence = adapter.describe()
            if evidence.incarnation_id != expected:
                raise IdentityMismatch("adapter did not prove the expected incarnation")
            with self._transaction():
                if self._binding(name) != before:
                    raise BindingUnavailable("binding changed during verification")
                if before and before[0] != expected and replacing != before[0]:
                    raise IdentityMismatch("name replacement requires explicit prior owner")
                if replacing is not None and (before is None or replacing != before[0]):
                    raise IdentityMismatch("replacement compare-and-swap failed")
                duplicate = self._db.execute(
                    "SELECT name FROM bindings WHERE owner=? AND configured=1 AND name!=?",
                    (expected, name),
                ).fetchone()
                if duplicate:
                    raise IdentityMismatch("incarnation already has a configured binding")
                self._db.execute("INSERT OR IGNORE INTO incarnations VALUES (?)", (expected,))
                generation = before[3] + 1 if before else 1
                self._db.execute(
                    "INSERT INTO bindings VALUES (?,?,?,1,?) ON CONFLICT(name) DO UPDATE SET "
                    "owner=excluded.owner, endpoint=excluded.endpoint, configured=1, "
                    "generation=excluded.generation", (name, expected, endpoint, generation),
                )
            self._live[name] = (generation, adapter)
            self.observations[name] = "verified"
        except IdentityError as exc:
            self.observations[name] = (
                "mismatch" if isinstance(exc, IdentityMismatch) else "unavailable"
            )
            raise

    def remove(self, name: str) -> None:
        with self._transaction():
            self._db.execute(
                "UPDATE bindings SET configured=0, generation=generation+1 WHERE name=?",
                (name,),
            )
        self._live.pop(name, None)
        self.observations.pop(name, None)

    def administrative_state(self, owner: str) -> str:
        if not self._db.execute("SELECT 1 FROM incarnations WHERE id=?", (owner,)).fetchone():
            raise KeyError(owner)
        bound = self._db.execute(
            "SELECT 1 FROM bindings WHERE owner=? AND configured=1", (owner,),
        ).fetchone()
        return "bound" if bound else "disabled"

    def _resolve(self, owner: str) -> tuple[str, int, Adapter, Evidence]:
        row = self._db.execute(
            "SELECT name, generation FROM bindings WHERE owner=? AND configured=1", (owner,),
        ).fetchone()
        if row is None or row[0] not in self._live or self._live[row[0]][0] != row[1]:
            raise BindingUnavailable("historical owner has no current verified binding")
        name, generation = row
        adapter = self._live[name][1]
        try:
            evidence = adapter.describe()
            if evidence.incarnation_id != owner:
                raise IdentityMismatch("binding no longer denotes the historical owner")
        except IdentityError as exc:
            self._live.pop(name, None)
            self.observations[name] = (
                "mismatch" if isinstance(exc, IdentityMismatch) else "unavailable"
            )
            raise
        return name, generation, adapter, evidence

    def _record(self, table: str, name: str, record_id: str, object_key: str,
                expected_incarnation: str) -> None:
        # Only the two fixed call sites below supply table names.
        owner = incarnation(expected_incarnation)
        row = self._binding(name)
        if row is None:
            raise BindingUnavailable("store is not enrolled")
        if row[0] != owner:
            raise IdentityMismatch("receipt or admitted work belongs to a different incarnation")
        resolved_name, generation, _, evidence = self._resolve(owner)
        if resolved_name != name or not evidence.writable:
            raise BindingUnavailable("store is not admitted for new archive work")
        with self._transaction():
            if self._binding(name) != (owner, row[1], 1, generation):
                raise BindingUnavailable("binding changed before catalog recording")
            self._db.execute(f"INSERT INTO {table} VALUES (?,?,?)", (record_id, owner, object_key))

    def record_copy(self, name: str, copy_id: str, object_key: str,
                    *, receipt_incarnation: str) -> None:
        """Model recording a receipt already validated against the admitted owner.

        Never derive the receipt's owner from whichever backend now has its name.
        This reference defers late completion when the owner is no longer bound.
        """
        self._record("archive_copies", name, copy_id, object_key, receipt_incarnation)

    def record_work(self, name: str, work_id: str, object_key: str,
                    *, expected_incarnation: str) -> None:
        self._record("pending_work", name, work_id, object_key, expected_incarnation)

    def holdings(self) -> tuple[tuple[str, str, str], ...]:
        return tuple(self._db.execute("SELECT id,owner,object_key FROM archive_copies ORDER BY id"))

    def work(self) -> tuple[tuple[str, str, str], ...]:
        return tuple(self._db.execute("SELECT id,owner,object_key FROM pending_work ORDER BY id"))

    def read_copy(self, copy_id: str) -> bytes:
        row = self._db.execute(
            "SELECT owner,object_key FROM archive_copies WHERE id=?", (copy_id,),
        ).fetchone()
        if row is None:
            raise KeyError(copy_id)
        owner, object_key = row
        name, _, adapter, evidence = self._resolve(owner)
        if not evidence.readable:
            raise BindingUnavailable("verified owner is not currently readable")
        try:
            return adapter.read(owner, object_key)
        except IdentityError as exc:
            self._live.pop(name, None)
            self.observations[name] = (
                "mismatch" if isinstance(exc, IdentityMismatch) else "unavailable"
            )
            raise
