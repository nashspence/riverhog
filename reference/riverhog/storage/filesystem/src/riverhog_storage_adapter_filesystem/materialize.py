"""Read-only materialization of one exact committed filesystem projection.

The filesystem adapter's on-disk layout is private to this distribution.  This
module is therefore the only supported bridge from that layout to an ordinary
logical object tree suitable for independent Riverhog recovery.  It never
starts the adapter, reconciles its state, or mutates the source root.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import shutil
import sqlite3
import stat
import struct
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from riverhog_storage_adapter_protocol import DeletePrefixRequest, ObjectLocator

from riverhog_storage_adapter_filesystem.adapter import _ObjectRecord

_STATE_SCHEMA = "riverhog-filesystem-materialization-state/v1"
_COMMITMENT_DOMAIN = b"riverhog-filesystem-materialization-source/v1\0"
_HEX = frozenset("0123456789abcdef")
_COPY_CHUNK_BYTES = 8 * 1024 * 1024
_PRIVATE_DIRECTORY_MODE = 0o700
_PRIVATE_FILE_MODE = 0o600


class MaterializationError(RuntimeError):
    """The selected committed projection could not be materialized exactly."""


class MaterializationInterrupted(MaterializationError):
    """A test-controlled interruption left durable restart state."""


@dataclass(frozen=True, slots=True)
class MaterializationSelection:
    """Exact logical object paths and prefixes selected for one operation."""

    paths: tuple[str, ...] = ()
    prefixes: tuple[str, ...] = ()
    all_objects: bool = False

    def __post_init__(self) -> None:
        paths = tuple(sorted(set(self.paths)))
        prefixes = tuple(sorted(set(self.prefixes)))
        if paths != self.paths or prefixes != self.prefixes:
            raise ValueError("materialization selectors must be unique and canonically ordered")
        if self.all_objects == bool(paths or prefixes):
            raise ValueError("select either all objects or at least one exact path/prefix")
        for path in paths:
            ObjectLocator(object_path=path)
        for prefix in prefixes:
            DeletePrefixRequest(object_prefix=prefix)

    def contains(self, object_path: str) -> bool:
        return (
            self.all_objects
            or object_path in self.paths
            or any(object_path.startswith(prefix) for prefix in self.prefixes)
        )

    def canonical_json(self) -> str:
        return json.dumps(
            {
                "all_objects": self.all_objects,
                "paths": list(self.paths),
                "prefixes": list(self.prefixes),
            },
            separators=(",", ":"),
            sort_keys=True,
        )


@dataclass(frozen=True, slots=True)
class MaterializationSummary:
    """Bounded operation evidence; object membership remains in the output tree."""

    destination: Path
    selected_objects: int
    selected_bytes: int
    source_metadata_bytes: int
    destination_verified_objects: int
    destination_verified_bytes: int
    copied_objects: int
    copied_bytes: int

    def as_json(self) -> dict[str, object]:
        return {
            "format": "riverhog-filesystem-materialization-result/v1",
            "destination": str(self.destination),
            "selected_objects": self.selected_objects,
            "selected_bytes": self.selected_bytes,
            "source_metadata_bytes": self.source_metadata_bytes,
            "destination_verified_objects": self.destination_verified_objects,
            "destination_verified_bytes": self.destination_verified_bytes,
            "copied_objects": self.copied_objects,
            "copied_bytes": self.copied_bytes,
        }


@dataclass(frozen=True, slots=True)
class _Projection:
    sha256: str
    objects: int
    bytes: int
    metadata_bytes: int


def materialize_committed_objects(
    *,
    source: Path,
    destination: Path,
    selection: MaterializationSelection,
    _interrupt_after_objects: int | None = None,
) -> MaterializationSummary:
    """Materialize one immutable selected projection with durable exact restart.

    Every invocation reopens the source under its exclusive instance lock and
    recomputes the selected metadata commitment before it trusts prior progress.
    Payload already published at the destination is independently revalidated.
    The private checkpoint is removed only after the exact projection completes.
    """

    source = _canonical_source(source)
    destination = _canonical_destination(destination)
    if destination == source or destination.is_relative_to(source):
        raise MaterializationError("destination must be outside the adapter root")
    checkpoint = destination.parent / f".{destination.name}.riverhog-materialization"
    if checkpoint == source or checkpoint.is_relative_to(source):
        raise MaterializationError("materialization checkpoint must be outside the adapter root")

    new_checkpoint = not checkpoint.exists()
    if new_checkpoint:
        if destination.exists() or destination.is_symlink():
            raise MaterializationError(
                "destination already exists without a resumable materialization checkpoint"
            )
        checkpoint.mkdir(mode=_PRIVATE_DIRECTORY_MODE)
    _require_directory(checkpoint, "materialization checkpoint")
    os.chmod(checkpoint, _PRIVATE_DIRECTORY_MODE)

    completed = False
    database = checkpoint / "state.sqlite3"
    if not new_checkpoint:
        _require_regular_file(database, "materialization checkpoint database")
    try:
        with _source_lock(source):
            state = sqlite3.connect(database)
            try:
                _initialize_state(state)
                projection = _scan_projection(
                    state,
                    source=source,
                    selection=selection,
                )
                _bind_projection(
                    state,
                    source=source,
                    selection=selection,
                    projection=projection,
                    destination=destination,
                )
                destination.mkdir(mode=_PRIVATE_DIRECTORY_MODE, exist_ok=True)
                _require_directory(destination, "materialization destination")
                verified_objects, verified_bytes = _verify_published_progress(
                    state,
                    destination=destination,
                )
                (
                    late_verified_objects,
                    late_verified_bytes,
                    copied_objects,
                    copied_bytes,
                ) = _copy_remaining(
                    state,
                    source=source,
                    destination=destination,
                    interrupt_after_objects=_interrupt_after_objects,
                )
                _verify_destination_namespace(state, destination)
                _fsync_directory(destination)
                completed = True
                return MaterializationSummary(
                    destination=destination,
                    selected_objects=projection.objects,
                    selected_bytes=projection.bytes,
                    source_metadata_bytes=projection.metadata_bytes,
                    destination_verified_objects=(verified_objects + late_verified_objects),
                    destination_verified_bytes=verified_bytes + late_verified_bytes,
                    copied_objects=copied_objects,
                    copied_bytes=copied_bytes,
                )
            finally:
                state.close()
    except MaterializationError:
        raise
    except (OSError, sqlite3.Error, UnicodeError, ValueError, json.JSONDecodeError) as exc:
        raise MaterializationError(str(exc)) from exc
    finally:
        if completed:
            shutil.rmtree(checkpoint)
            _fsync_directory(checkpoint.parent)


def _canonical_source(path: Path) -> Path:
    candidate = path.expanduser()
    if not candidate.is_absolute():
        candidate = Path.cwd() / candidate
    if candidate.is_symlink():
        raise MaterializationError("adapter root must not be a symbolic link")
    try:
        resolved = candidate.resolve(strict=True)
    except OSError as exc:
        raise MaterializationError(f"adapter root is unavailable: {exc}") from exc
    _require_directory(resolved, "adapter root")
    if resolved == Path("/"):
        raise MaterializationError("adapter root must not be the filesystem root")
    return resolved


def _canonical_destination(path: Path) -> Path:
    candidate = path.expanduser()
    if not candidate.is_absolute():
        candidate = Path.cwd() / candidate
    if candidate.is_symlink():
        raise MaterializationError("destination must not be a symbolic link")
    parent = candidate.parent.resolve(strict=True)
    return parent / candidate.name


@contextmanager
def _source_lock(source: Path) -> Iterator[None]:
    lock_path = source / "instance.lock"
    _require_regular_file(lock_path, "adapter instance lock")
    fd = os.open(lock_path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise MaterializationError(
                "adapter root is active; quiesce it for the complete materialization attempt"
            ) from exc
        for name in ("objects", "writes", "staging"):
            _require_directory(source / name, f"adapter {name} tree")
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def _initialize_state(state: sqlite3.Connection) -> None:
    state.executescript(
        """
        PRAGMA journal_mode = DELETE;
        PRAGMA synchronous = FULL;
        PRAGMA foreign_keys = ON;
        CREATE TABLE IF NOT EXISTS authority (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS projection (
            object_key TEXT PRIMARY KEY,
            object_path TEXT NOT NULL UNIQUE,
            metadata_json TEXT NOT NULL,
            metadata_bytes INTEGER NOT NULL,
            stored_bytes INTEGER NOT NULL
        );
        """
    )
    state.commit()


def _scan_projection(
    state: sqlite3.Connection,
    *,
    source: Path,
    selection: MaterializationSelection,
) -> _Projection:
    state.execute("DELETE FROM projection")
    source_metadata_bytes = 0
    matched_paths: set[str] = set()
    matched_prefixes: set[str] = set()
    for object_key, object_dir in _iter_object_directories(source / "objects"):
        path_file = object_dir / "path"
        _require_regular_file(path_file, "filesystem object identity")
        source_metadata_bytes += path_file.stat(follow_symlinks=False).st_size
        object_path = _read_text(path_file, maximum_bytes=4096 * 4)
        try:
            ObjectLocator(object_path=object_path)
        except ValueError as exc:
            raise MaterializationError("filesystem object identity is not canonical") from exc
        if hashlib.sha256(object_path.encode("utf-8")).hexdigest() != object_key:
            raise MaterializationError("filesystem object identity differs from its object key")
        revisions = object_dir / "revisions"
        _require_directory(revisions, "filesystem object revisions")
        current_path = object_dir / "current"
        if not current_path.exists():
            if current_path.is_symlink():
                raise MaterializationError("filesystem current pointer is a symbolic link")
            continue
        _require_regular_file(current_path, "filesystem current pointer")
        source_metadata_bytes += current_path.stat(follow_symlinks=False).st_size
        revision = _read_text(current_path, maximum_bytes=2000 * 4)
        if (
            not revision
            or len(revision) > 2000
            or any(item in revision for item in ("/", "\\", "\x00"))
            or revision in {".", ".."}
        ):
            raise MaterializationError("filesystem current revision pointer is invalid")
        if object_path in selection.paths:
            matched_paths.add(object_path)
        matched_prefixes.update(
            prefix for prefix in selection.prefixes if object_path.startswith(prefix)
        )
        if not selection.contains(object_path):
            continue
        revision_dir = revisions / revision
        _require_directory(revision_dir, "filesystem current revision")
        metadata_path = revision_dir / "metadata.json"
        _require_regular_file(metadata_path, "filesystem object metadata")
        metadata_bytes = metadata_path.read_bytes()
        source_metadata_bytes += len(metadata_bytes)
        try:
            raw = json.loads(metadata_bytes)
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise MaterializationError("filesystem object metadata is unreadable") from exc
        if not isinstance(raw, dict):
            raise MaterializationError("filesystem object metadata root is invalid")
        try:
            record = _ObjectRecord.from_json(cast(dict[str, Any], raw))
        except RuntimeError as exc:
            raise MaterializationError(str(exc)) from exc
        if record.object_path != object_path or record.revision != revision:
            raise MaterializationError("filesystem object metadata differs from its current path")
        payload = revision_dir / "payload.data"
        _require_regular_file(payload, "filesystem object payload")
        if payload.stat(follow_symlinks=False).st_size != record.stored_bytes:
            raise MaterializationError("filesystem object payload differs from its metadata")
        canonical = json.dumps(
            record.as_json(),
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        try:
            state.execute(
                "INSERT INTO projection(object_key, object_path, metadata_json, "
                "metadata_bytes, stored_bytes) VALUES (?, ?, ?, ?, ?)",
                (object_key, object_path, canonical, len(metadata_bytes), record.stored_bytes),
            )
        except sqlite3.IntegrityError as exc:
            raise MaterializationError("filesystem current projection is ambiguous") from exc
    missing_paths = sorted(set(selection.paths) - matched_paths)
    missing_prefixes = sorted(set(selection.prefixes) - matched_prefixes)
    if missing_paths or missing_prefixes:
        raise MaterializationError(
            "materialization selector has no current committed object: "
            + ", ".join([*missing_paths, *missing_prefixes])
        )
    state.commit()

    digest = hashlib.sha256()
    digest.update(_COMMITMENT_DOMAIN)
    selection_bytes = selection.canonical_json().encode("utf-8")
    _commit_field(digest, selection_bytes)
    objects = 0
    stored_bytes = 0
    cursor = state.execute(
        "SELECT object_key, object_path, metadata_json, metadata_bytes, stored_bytes "
        "FROM projection ORDER BY object_key"
    )
    while row := cursor.fetchone():
        entry = json.dumps(
            {
                "metadata": json.loads(str(row[2])),
                "object_key": str(row[0]),
                "object_path": str(row[1]),
            },
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        _commit_field(digest, entry)
        objects += 1
        stored_bytes += int(row[4])
    _commit_field(digest, b"terminal")
    return _Projection(
        sha256=digest.hexdigest(),
        objects=objects,
        bytes=stored_bytes,
        metadata_bytes=source_metadata_bytes,
    )


def _commit_field(digest: Any, value: bytes) -> None:
    digest.update(struct.pack(">Q", len(value)))
    digest.update(value)


def _bind_projection(
    state: sqlite3.Connection,
    *,
    source: Path,
    selection: MaterializationSelection,
    projection: _Projection,
    destination: Path,
) -> None:
    root = source.stat(follow_symlinks=False)
    expected = {
        "schema": _STATE_SCHEMA,
        "source": str(source),
        "source_device": str(root.st_dev),
        "source_inode": str(root.st_ino),
        "destination": str(destination),
        "selection": selection.canonical_json(),
        "source_projection_sha256": projection.sha256,
    }
    persisted = dict(state.execute("SELECT key, value FROM authority").fetchall())
    if not persisted:
        if destination.exists():
            raise MaterializationError("materialization checkpoint has no trustworthy authority")
        state.executemany(
            "INSERT INTO authority(key, value) VALUES (?, ?)",
            [*expected.items(), ("last_object_key", "")],
        )
        state.commit()
        return
    for key, value in expected.items():
        if persisted.get(key) != value:
            raise MaterializationError(
                "selected source projection changed since materialization began"
            )
    if "last_object_key" not in persisted:
        raise MaterializationError("materialization checkpoint is incomplete")


def _verify_published_progress(
    state: sqlite3.Connection,
    *,
    destination: Path,
) -> tuple[int, int]:
    row = state.execute("SELECT value FROM authority WHERE key = 'last_object_key'").fetchone()
    if row is None:
        raise MaterializationError("materialization checkpoint has no progress authority")
    last_key = str(row[0])
    if not last_key:
        return 0, 0
    found = False
    objects = 0
    total = 0
    cursor = state.execute(
        "SELECT object_key, object_path, metadata_json FROM projection "
        "WHERE object_key <= ? ORDER BY object_key",
        (last_key,),
    )
    while current := cursor.fetchone():
        found = found or str(current[0]) == last_key
        record = _record(str(current[2]))
        _verify_destination_object(destination, str(current[1]), record)
        objects += 1
        total += record.stored_bytes
    if not found:
        raise MaterializationError("materialization progress is absent from current projection")
    return objects, total


def _copy_remaining(
    state: sqlite3.Connection,
    *,
    source: Path,
    destination: Path,
    interrupt_after_objects: int | None,
) -> tuple[int, int, int, int]:
    last_row = state.execute("SELECT value FROM authority WHERE key = 'last_object_key'").fetchone()
    last_key = str(last_row[0]) if last_row is not None else ""
    verified_objects = 0
    verified_bytes = 0
    copied_objects = 0
    copied_bytes = 0
    cursor = state.execute(
        "SELECT object_key, object_path, metadata_json FROM projection "
        "WHERE object_key > ? ORDER BY object_key",
        (last_key,),
    )
    while row := cursor.fetchone():
        object_key = str(row[0])
        object_path = str(row[1])
        record = _record(str(row[2]))
        target = _destination_path(destination, object_path, create=True)
        if target.exists() or target.is_symlink():
            _verify_destination_object(destination, object_path, record)
            verified_objects += 1
            verified_bytes += record.stored_bytes
        else:
            _copy_object(source, object_key, record, target)
            copied_objects += 1
            copied_bytes += record.stored_bytes
        state.execute(
            "UPDATE authority SET value = ? WHERE key = 'last_object_key'",
            (object_key,),
        )
        state.commit()
        completed_objects = verified_objects + copied_objects
        if interrupt_after_objects is not None and completed_objects >= interrupt_after_objects:
            raise MaterializationInterrupted("test-controlled materialization interruption")
    return verified_objects, verified_bytes, copied_objects, copied_bytes


def _copy_object(source: Path, object_key: str, record: _ObjectRecord, target: Path) -> None:
    source_path = (
        source
        / "objects"
        / object_key[:2]
        / object_key[2:4]
        / object_key
        / "revisions"
        / record.revision
        / "payload.data"
    )
    _require_regular_file(source_path, "filesystem object payload")
    temporary = target.parent / f".{target.name}.{uuid.uuid4().hex}.tmp"
    source_fd = os.open(source_path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
    target_fd = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
        _PRIVATE_FILE_MODE,
    )
    try:
        observed = _copy_and_verify_parts(source_fd, target_fd, record)
        if observed != record.stored_bytes:
            raise MaterializationError("filesystem object copy differs from its byte count")
        os.fsync(target_fd)
    except BaseException:
        os.close(target_fd)
        os.close(source_fd)
        temporary.unlink(missing_ok=True)
        raise
    os.close(target_fd)
    os.close(source_fd)
    try:
        os.replace(temporary, target)
        _fsync_directory(target.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _copy_and_verify_parts(source_fd: int, target_fd: int, record: _ObjectRecord) -> int:
    whole = hashlib.sha256()
    written = 0
    for part in record.parts:
        part_digest = hashlib.sha256()
        remaining = part.stored_bytes
        source_offset = part.offset
        while remaining:
            chunk = os.pread(source_fd, min(remaining, _COPY_CHUNK_BYTES), source_offset)
            if not chunk:
                raise MaterializationError("filesystem object ended before its metadata")
            _write_all(target_fd, chunk)
            part_digest.update(chunk)
            whole.update(chunk)
            remaining -= len(chunk)
            source_offset += len(chunk)
            written += len(chunk)
        if part_digest.hexdigest() != part.stored_sha256:
            raise MaterializationError("filesystem object part failed SHA-256 validation")
    if record.stored_sha256 is not None and whole.hexdigest() != record.stored_sha256:
        raise MaterializationError("filesystem object failed SHA-256 validation")
    return written


def _verify_destination_object(
    destination: Path,
    object_path: str,
    record: _ObjectRecord,
) -> None:
    target = _destination_path(destination, object_path, create=False)
    _require_regular_file(target, "materialized object")
    details = target.stat(follow_symlinks=False)
    if details.st_size != record.stored_bytes:
        raise MaterializationError("materialized object conflicts with selected source identity")
    fd = os.open(target, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
    try:
        whole = hashlib.sha256()
        offset = 0
        for part in record.parts:
            part_digest = hashlib.sha256()
            remaining = part.stored_bytes
            while remaining:
                chunk = os.pread(fd, min(remaining, _COPY_CHUNK_BYTES), offset)
                if not chunk:
                    raise MaterializationError("materialized object ended before its identity")
                part_digest.update(chunk)
                whole.update(chunk)
                remaining -= len(chunk)
                offset += len(chunk)
            if part_digest.hexdigest() != part.stored_sha256:
                raise MaterializationError(
                    "materialized object conflicts with selected source identity"
                )
        if record.stored_sha256 is not None and whole.hexdigest() != record.stored_sha256:
            raise MaterializationError(
                "materialized object conflicts with selected source identity"
            )
    finally:
        os.close(fd)


def _destination_path(root: Path, object_path: str, *, create: bool) -> Path:
    ObjectLocator(object_path=object_path)
    current = root
    parts = object_path.split("/")
    for part in parts[:-1]:
        current = current / part
        if current.exists():
            _require_directory(current, "materialization destination parent")
        elif current.is_symlink():
            raise MaterializationError("materialization destination contains a symbolic link")
        elif create:
            current.mkdir(mode=_PRIVATE_DIRECTORY_MODE)
        else:
            raise MaterializationError("materialized object is missing")
    return current / parts[-1]


def _verify_destination_namespace(state: sqlite3.Connection, destination: Path) -> None:
    pending = [destination]
    while pending:
        current = pending.pop()
        with os.scandir(current) as entries:
            for entry in entries:
                path = Path(entry.path)
                if entry.is_symlink():
                    raise MaterializationError(
                        "materialization destination contains a symbolic link"
                    )
                if entry.is_dir(follow_symlinks=False):
                    pending.append(path)
                    continue
                if not entry.is_file(follow_symlinks=False):
                    raise MaterializationError(
                        "materialization destination contains a special file"
                    )
                logical = path.relative_to(destination).as_posix()
                if (
                    state.execute(
                        "SELECT 1 FROM projection WHERE object_path = ?", (logical,)
                    ).fetchone()
                    is None
                ):
                    raise MaterializationError(
                        "materialization destination contains an unselected object"
                    )


def _iter_object_directories(objects: Path) -> Iterator[tuple[str, Path]]:
    _require_directory(objects, "filesystem objects tree")
    with os.scandir(objects) as first_entries:
        for first in first_entries:
            if not first.is_dir(follow_symlinks=False) or not _is_hex(first.name, 2):
                raise MaterializationError("filesystem objects tree has an invalid first shard")
            with os.scandir(first.path) as second_entries:
                for second in second_entries:
                    if not second.is_dir(follow_symlinks=False) or not _is_hex(second.name, 2):
                        raise MaterializationError(
                            "filesystem objects tree has an invalid second shard"
                        )
                    with os.scandir(second.path) as object_entries:
                        for entry in object_entries:
                            if (
                                not entry.is_dir(follow_symlinks=False)
                                or not _is_hex(entry.name, 64)
                                or entry.name[:2] != first.name
                                or entry.name[2:4] != second.name
                            ):
                                raise MaterializationError(
                                    "filesystem objects tree has an invalid object key"
                                )
                            yield entry.name, Path(entry.path)


def _is_hex(value: str, length: int) -> bool:
    return len(value) == length and all(character in _HEX for character in value)


def _record(payload: str) -> _ObjectRecord:
    raw = json.loads(payload)
    if not isinstance(raw, dict):
        raise MaterializationError("checkpoint object metadata is invalid")
    try:
        return _ObjectRecord.from_json(cast(dict[str, Any], raw))
    except RuntimeError as exc:
        raise MaterializationError(str(exc)) from exc


def _read_text(path: Path, *, maximum_bytes: int) -> str:
    details = path.stat(follow_symlinks=False)
    if details.st_size > maximum_bytes:
        raise MaterializationError("filesystem adapter identity field exceeds its contract")
    return path.read_text(encoding="utf-8")


def _require_directory(path: Path, label: str) -> None:
    try:
        details = path.stat(follow_symlinks=False)
    except FileNotFoundError as exc:
        raise MaterializationError(f"{label} is missing") from exc
    if not stat.S_ISDIR(details.st_mode):
        raise MaterializationError(f"{label} is not a real directory")


def _require_regular_file(path: Path, label: str) -> None:
    try:
        details = path.stat(follow_symlinks=False)
    except FileNotFoundError as exc:
        raise MaterializationError(f"{label} is missing") from exc
    if not stat.S_ISREG(details.st_mode):
        raise MaterializationError(f"{label} is not a regular file")


def _write_all(fd: int, payload: bytes) -> None:
    offset = 0
    while offset < len(payload):
        written = os.write(fd, payload[offset:])
        if written <= 0:
            raise MaterializationError("materialization write made no progress")
        offset += written


def _fsync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


__all__ = [
    "MaterializationError",
    "MaterializationInterrupted",
    "MaterializationSelection",
    "MaterializationSummary",
    "materialize_committed_objects",
]
