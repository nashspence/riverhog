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
from typing import Any

import ijson  # type: ignore[import-untyped]
from riverhog_storage_adapter_protocol import (
    DeletePrefixRequest,
    ObjectLocator,
    ObjectMetadataReceipt,
)

from riverhog_storage_adapter_filesystem.adapter import _PartRecord

_STATE_SCHEMA = "riverhog-filesystem-materialization-state/v1"
_COMMITMENT_DOMAIN = b"riverhog-filesystem-materialization-source/v1\0"
_OPERATION_COMMITMENT_DOMAIN = b"riverhog-filesystem-materialization-operation/v1\0"
_HEX = frozenset("0123456789abcdef")
_COPY_CHUNK_BYTES = 8 * 1024 * 1024
_PRIVATE_DIRECTORY_MODE = 0o700
_PRIVATE_FILE_MODE = 0o600
_PROJECTION_BATCH_ROWS = 128
_CLEANUP_BATCH_ROWS = 512
_OBJECT_SCHEMA = "riverhog-filesystem-object/v1"
_BOOTSTRAP_SCHEMA = "riverhog-filesystem-materialization-bootstrap/v1"
_COMPLETION_SCHEMA = "riverhog-filesystem-materialization-completion/v1"
_IDENTITY_RECORD_BYTES_MAX = 8192


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


@dataclass(frozen=True, slots=True)
class MaterializationSummary:
    """Bounded operation evidence; object membership remains in the output tree."""

    destination: Path
    selected_objects: int
    selected_bytes: int
    source_metadata_bytes: int
    destination_verified_objects: int
    destination_verified_bytes: int
    staging_verified_bytes: int
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
            "staging_verified_bytes": self.staging_verified_bytes,
            "copied_objects": self.copied_objects,
            "copied_bytes": self.copied_bytes,
        }


@dataclass(frozen=True, slots=True)
class _Projection:
    generation: str
    sha256: str
    objects: int
    bytes: int
    metadata_bytes: int


@dataclass(frozen=True, slots=True)
class _ProjectedObject:
    object_path: str
    revision: str
    stored_bytes: int
    stored_sha256: str | None


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
    A bounded private completion receipt makes a lost success response
    replayable. Working state is removed only after that receipt is durable.
    """

    source = _canonical_source(source)
    destination = _canonical_destination(destination)
    if destination == source or destination.is_relative_to(source):
        raise MaterializationError("destination must be outside the adapter root")
    checkpoint = destination.parent / f".{destination.name}.riverhog-materialization"
    initializing = destination.parent / f".{destination.name}.riverhog-materialization-init"
    bootstrap = destination.parent / f".{destination.name}.riverhog-materialization-bootstrap.json"
    completion = destination.parent / f".{destination.name}.riverhog-materialization-complete.json"
    if checkpoint == source or checkpoint.is_relative_to(source):
        raise MaterializationError("materialization checkpoint must be outside the adapter root")

    database = checkpoint / "state.sqlite3"
    try:
        with _source_lock(source):
            completion_record = _read_completion_record(
                completion,
                source=source,
                destination=destination,
                selection=selection,
            )
            new_checkpoint = _prepare_checkpoint(
                checkpoint=checkpoint,
                initializing=initializing,
                bootstrap=bootstrap,
                source=source,
                destination=destination,
                selection=selection,
                destination_may_exist=completion_record is not None,
            )
            _require_directory(checkpoint, "materialization checkpoint")
            os.chmod(checkpoint, _PRIVATE_DIRECTORY_MODE)
            staging = checkpoint / "staging"
            if not staging.exists():
                staging.mkdir(mode=_PRIVATE_DIRECTORY_MODE)
                _fsync_directory(checkpoint)
            _require_directory(staging, "materialization staging")
            if not new_checkpoint:
                _require_regular_file(database, "materialization checkpoint database")
            state = sqlite3.connect(database)
            try:
                _initialize_state(state)
                projection = _scan_projection(
                    state,
                    source=source,
                    selection=selection,
                )
                if completion_record is not None:
                    _validate_completed_projection(completion_record, projection)
                _bind_projection(
                    state,
                    source=source,
                    selection=selection,
                    projection=projection,
                    destination=destination,
                    allow_existing_destination=completion_record is not None,
                )
                _cleanup_inactive_generations(state)
                _verify_staging_namespace(state, staging)
                destination.mkdir(mode=_PRIVATE_DIRECTORY_MODE, exist_ok=True)
                _require_directory(destination, "materialization destination")
                verified_objects, verified_bytes = _verify_published_progress(
                    state,
                    destination=destination,
                )
                (
                    late_verified_objects,
                    late_verified_bytes,
                    staging_verified_bytes,
                    copied_objects,
                    copied_bytes,
                ) = _copy_remaining(
                    state,
                    source=source,
                    destination=destination,
                    staging=staging,
                    interrupt_after_objects=_interrupt_after_objects,
                )
                _verify_staging_namespace(state, staging)
                _verify_destination_namespace(state, destination)
                _fsync_directory(destination)
                summary = MaterializationSummary(
                    destination=destination,
                    selected_objects=projection.objects,
                    selected_bytes=projection.bytes,
                    source_metadata_bytes=projection.metadata_bytes,
                    destination_verified_objects=(verified_objects + late_verified_objects),
                    destination_verified_bytes=verified_bytes + late_verified_bytes,
                    staging_verified_bytes=staging_verified_bytes,
                    copied_objects=copied_objects,
                    copied_bytes=copied_bytes,
                )
                _write_completion_record(
                    completion,
                    source=source,
                    destination=destination,
                    selection=selection,
                    projection=projection,
                )
            finally:
                state.close()
            _retire_checkpoint(checkpoint)
            return summary
    except MaterializationError:
        raise
    except (OSError, sqlite3.Error, UnicodeError, ValueError, json.JSONDecodeError) as exc:
        raise MaterializationError(str(exc)) from exc


def _operation_binding(
    *, source: Path, destination: Path, selection: MaterializationSelection
) -> dict[str, str]:
    root = source.stat(follow_symlinks=False)
    digest = hashlib.sha256()
    digest.update(_OPERATION_COMMITMENT_DOMAIN)
    for label, value in (
        (b"source", str(source).encode("utf-8")),
        (b"source-device", str(root.st_dev).encode("ascii")),
        (b"source-inode", str(root.st_ino).encode("ascii")),
        (b"destination", str(destination).encode("utf-8")),
    ):
        _commit_field(digest, label)
        _commit_field(digest, value)
    _commit_selection(digest, selection)
    _commit_field(digest, b"operation-terminal")
    return {"operation_sha256": digest.hexdigest()}


def _prepare_checkpoint(
    *,
    checkpoint: Path,
    initializing: Path,
    bootstrap: Path,
    source: Path,
    destination: Path,
    selection: MaterializationSelection,
    destination_may_exist: bool,
) -> bool:
    """Publish a complete working checkpoint or resume an exact one."""

    expected: dict[str, object] = {
        "schema": _BOOTSTRAP_SCHEMA,
        **_operation_binding(source=source, destination=destination, selection=selection),
    }
    if destination_may_exist:
        if bootstrap.exists() or bootstrap.is_symlink():
            _require_exact_json(bootstrap, expected, "materialization bootstrap")
        for path, label in (
            (checkpoint, "materialization checkpoint"),
            (initializing, "materialization initializing checkpoint"),
        ):
            if path.exists() or path.is_symlink():
                _require_directory(path, label)
                shutil.rmtree(path)
                _fsync_directory(path.parent)
        if bootstrap.exists():
            bootstrap.unlink()
            _fsync_directory(bootstrap.parent)
    if checkpoint.exists() or checkpoint.is_symlink():
        _require_directory(checkpoint, "materialization checkpoint")
        if bootstrap.exists() or bootstrap.is_symlink():
            _require_exact_json(bootstrap, expected, "materialization bootstrap")
            bootstrap.unlink()
            _fsync_directory(bootstrap.parent)
        if initializing.exists() or initializing.is_symlink():
            raise MaterializationError("materialization bootstrap state is ambiguous")
        return False
    if destination.exists() or destination.is_symlink():
        if not destination_may_exist:
            raise MaterializationError(
                "destination already exists without a completed materialization receipt"
            )
        _require_directory(destination, "materialization destination")
    if bootstrap.exists() or bootstrap.is_symlink():
        _require_exact_json(bootstrap, expected, "materialization bootstrap")
        if initializing.exists() or initializing.is_symlink():
            _require_directory(initializing, "materialization initializing checkpoint")
            shutil.rmtree(initializing)
            _fsync_directory(initializing.parent)
    else:
        if initializing.exists() or initializing.is_symlink():
            raise MaterializationError("unowned materialization bootstrap state exists")
        _write_json_exclusive(bootstrap, expected)
    initializing.mkdir(mode=_PRIVATE_DIRECTORY_MODE)
    staging = initializing / "staging"
    staging.mkdir(mode=_PRIVATE_DIRECTORY_MODE)
    database = initializing / "state.sqlite3"
    state = sqlite3.connect(database)
    try:
        _initialize_state(state)
    finally:
        state.close()
    _fsync_directory(initializing)
    os.replace(initializing, checkpoint)
    _fsync_directory(checkpoint.parent)
    bootstrap.unlink()
    _fsync_directory(bootstrap.parent)
    return True


def _completion_payload(
    *,
    source: Path,
    destination: Path,
    selection: MaterializationSelection,
    projection: _Projection,
) -> dict[str, object]:
    return {
        "format": _COMPLETION_SCHEMA,
        **_operation_binding(source=source, destination=destination, selection=selection),
        "source_projection_sha256": projection.sha256,
        "selected_objects": projection.objects,
        "selected_bytes": projection.bytes,
        "source_metadata_bytes": projection.metadata_bytes,
    }


def _read_completion_record(
    path: Path,
    *,
    source: Path,
    destination: Path,
    selection: MaterializationSelection,
) -> dict[str, object] | None:
    if not path.exists() and not path.is_symlink():
        return None
    _require_regular_file(path, "materialization completion receipt")
    try:
        raw = json.loads(_read_text(path, maximum_bytes=_IDENTITY_RECORD_BYTES_MAX))
    except json.JSONDecodeError as exc:
        raise MaterializationError("materialization completion receipt is invalid") from exc
    expected_binding = _operation_binding(
        source=source, destination=destination, selection=selection
    )
    expected_keys = {
        "format",
        *expected_binding,
        "source_projection_sha256",
        "selected_objects",
        "selected_bytes",
        "source_metadata_bytes",
    }
    if (
        not isinstance(raw, dict)
        or set(raw) != expected_keys
        or raw.get("format") != _COMPLETION_SCHEMA
        or any(raw.get(key) != value for key, value in expected_binding.items())
        or not isinstance(raw.get("source_projection_sha256"), str)
        or not _is_hex(str(raw["source_projection_sha256"]), 64)
        or any(
            type(raw.get(key)) is not int or int(raw[key]) < 0
            for key in ("selected_objects", "selected_bytes", "source_metadata_bytes")
        )
    ):
        raise MaterializationError("materialization completion receipt conflicts with invocation")
    return raw


def _validate_completed_projection(record: dict[str, object], projection: _Projection) -> None:
    expected = {
        "source_projection_sha256": projection.sha256,
        "selected_objects": projection.objects,
        "selected_bytes": projection.bytes,
        "source_metadata_bytes": projection.metadata_bytes,
    }
    if any(record.get(key) != value for key, value in expected.items()):
        raise MaterializationError("selected source projection changed after materialization")


def _write_completion_record(
    path: Path,
    *,
    source: Path,
    destination: Path,
    selection: MaterializationSelection,
    projection: _Projection,
) -> None:
    payload = _completion_payload(
        source=source,
        destination=destination,
        selection=selection,
        projection=projection,
    )
    if path.exists() or path.is_symlink():
        _require_exact_json(path, payload, "materialization completion receipt")
        return
    _write_json_exclusive(path, payload)


def _retire_checkpoint(checkpoint: Path) -> None:
    shutil.rmtree(checkpoint)
    _fsync_directory(checkpoint.parent)


def _require_exact_json(path: Path, expected: dict[str, object], label: str) -> None:
    _require_regular_file(path, label)
    try:
        observed = json.loads(_read_text(path, maximum_bytes=_IDENTITY_RECORD_BYTES_MAX))
    except json.JSONDecodeError as exc:
        raise MaterializationError(f"{label} is invalid") from exc
    if observed != expected:
        raise MaterializationError(f"{label} conflicts with invocation")


def _write_json_exclusive(path: Path, payload: dict[str, object]) -> None:
    encoded = json.dumps(
        payload,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    if len(encoded) > _IDENTITY_RECORD_BYTES_MAX:
        raise MaterializationError("filesystem adapter identity field exceeds its contract")
    temporary = path.parent / f".{path.name}.{uuid.uuid4().hex}.partial"
    fd = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
        _PRIVATE_FILE_MODE,
    )
    try:
        offset = 0
        while offset < len(encoded):
            written = os.write(fd, encoded[offset:])
            if written <= 0:
                raise OSError("materialization metadata write made no progress")
            offset += written
        os.fsync(fd)
    finally:
        os.close(fd)
    try:
        os.link(temporary, path, follow_symlinks=False)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)
        _fsync_directory(path.parent)


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
        CREATE TABLE IF NOT EXISTS projection_generation (
            generation TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            source_projection_sha256 TEXT,
            object_count INTEGER,
            stored_bytes INTEGER,
            metadata_bytes INTEGER
        );
        CREATE TABLE IF NOT EXISTS projection (
            generation TEXT NOT NULL,
            object_key TEXT NOT NULL,
            object_path TEXT NOT NULL,
            header_json TEXT NOT NULL,
            metadata_bytes INTEGER NOT NULL,
            stored_bytes INTEGER NOT NULL,
            PRIMARY KEY (generation, object_key),
            UNIQUE (generation, object_path)
        );
        CREATE TABLE IF NOT EXISTS projection_part (
            generation TEXT NOT NULL,
            object_key TEXT NOT NULL,
            number INTEGER NOT NULL,
            offset INTEGER NOT NULL,
            stored_bytes INTEGER NOT NULL,
            stored_sha256 TEXT NOT NULL,
            PRIMARY KEY (generation, object_key, number)
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
    _cleanup_inactive_generations(state)
    generation = uuid.uuid4().hex
    state.execute(
        "INSERT INTO projection_generation(generation, status) VALUES (?, 'building')",
        (generation,),
    )
    state.commit()
    source_metadata_bytes = 0
    matched_paths: set[str] = set()
    matched_prefixes: set[str] = set()
    inserted_since_commit = 0
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
        metadata_size = metadata_path.stat(follow_symlinks=False).st_size
        source_metadata_bytes += metadata_size

        def accept_part(part: _PartRecord, object_key: str = object_key) -> None:
            nonlocal inserted_since_commit
            state.execute(
                "INSERT INTO projection_part(generation, object_key, number, offset, "
                "stored_bytes, stored_sha256) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    generation,
                    object_key,
                    part.number,
                    part.offset,
                    part.stored_bytes,
                    part.stored_sha256,
                ),
            )
            inserted_since_commit += 1
            if inserted_since_commit >= _PROJECTION_BATCH_ROWS:
                state.commit()
                inserted_since_commit = 0

        record, header_json = _stream_object_metadata(metadata_path, accept_part=accept_part)
        if record.object_path != object_path or record.revision != revision:
            raise MaterializationError("filesystem object metadata differs from its current path")
        payload = revision_dir / "payload.data"
        _require_regular_file(payload, "filesystem object payload")
        if payload.stat(follow_symlinks=False).st_size != record.stored_bytes:
            raise MaterializationError("filesystem object payload differs from its metadata")
        try:
            state.execute(
                "INSERT INTO projection(generation, object_key, object_path, header_json, "
                "metadata_bytes, stored_bytes) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    generation,
                    object_key,
                    object_path,
                    header_json,
                    metadata_size,
                    record.stored_bytes,
                ),
            )
        except sqlite3.IntegrityError as exc:
            raise MaterializationError("filesystem current projection is ambiguous") from exc
        inserted_since_commit += 1
        if inserted_since_commit >= _PROJECTION_BATCH_ROWS:
            state.commit()
            inserted_since_commit = 0
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
    _commit_selection(digest, selection)
    objects = 0
    stored_bytes = 0
    cursor = state.execute(
        "SELECT object_key, object_path, header_json, metadata_bytes, stored_bytes "
        "FROM projection WHERE generation = ? ORDER BY object_key",
        (generation,),
    )
    while row := cursor.fetchone():
        _commit_field(digest, b"object")
        _commit_field(digest, str(row[0]).encode("ascii"))
        _commit_field(digest, str(row[1]).encode("utf-8"))
        _commit_field(digest, str(row[2]).encode("utf-8"))
        part_cursor = state.execute(
            "SELECT number, offset, stored_bytes, stored_sha256 FROM projection_part "
            "WHERE generation = ? AND object_key = ? ORDER BY number",
            (generation, str(row[0])),
        )
        while part_row := part_cursor.fetchone():
            _commit_field(digest, b"part")
            _commit_field(digest, str(int(part_row[0])).encode("ascii"))
            _commit_field(digest, str(int(part_row[1])).encode("ascii"))
            _commit_field(digest, str(int(part_row[2])).encode("ascii"))
            _commit_field(digest, str(part_row[3]).encode("ascii"))
        _commit_field(digest, b"object-terminal")
        objects += 1
        stored_bytes += int(row[4])
    _commit_field(digest, b"terminal")
    commitment = digest.hexdigest()
    state.execute(
        "UPDATE projection_generation SET status = 'complete', "
        "source_projection_sha256 = ?, object_count = ?, stored_bytes = ?, "
        "metadata_bytes = ? WHERE generation = ? AND status = 'building'",
        (commitment, objects, stored_bytes, source_metadata_bytes, generation),
    )
    state.commit()
    return _Projection(
        generation=generation,
        sha256=commitment,
        objects=objects,
        bytes=stored_bytes,
        metadata_bytes=source_metadata_bytes,
    )


def _cleanup_inactive_generations(state: sqlite3.Connection) -> None:
    active_row = state.execute(
        "SELECT value FROM authority WHERE key = 'active_generation'"
    ).fetchone()
    active = str(active_row[0]) if active_row is not None else None
    while True:
        rows = state.execute(
            "SELECT rowid FROM projection_part WHERE generation != ? LIMIT ?",
            (active or "", _CLEANUP_BATCH_ROWS),
        ).fetchall()
        if not rows:
            break
        state.executemany("DELETE FROM projection_part WHERE rowid = ?", rows)
        state.commit()
    while True:
        rows = state.execute(
            "SELECT rowid FROM projection WHERE generation != ? LIMIT ?",
            (active or "", _CLEANUP_BATCH_ROWS),
        ).fetchall()
        if not rows:
            break
        state.executemany("DELETE FROM projection WHERE rowid = ?", rows)
        state.commit()
    state.execute("DELETE FROM projection_generation WHERE generation != ?", (active or "",))
    state.commit()


def _stream_object_metadata(
    path: Path,
    *,
    accept_part: Any,
) -> tuple[_ProjectedObject, str]:
    expected = {
        "schema",
        "object_path",
        "revision",
        "entity_token",
        "stored_bytes",
        "stored_sha256",
        "content_type",
        "required_identity_assertions",
        "placement",
        "completed_at",
        "parts",
    }
    scalars: dict[str, object] = {}
    assertions: dict[str, str] = {}
    top_keys: set[str] = set()
    assertion_key: str | None = None
    part: dict[str, object] | None = None
    part_key: str | None = None
    next_part = 1
    next_offset = 0
    part_bytes = 0
    try:
        with path.open("rb") as stream:
            for prefix, event, value in ijson.parse(stream):
                if prefix == "" and event == "map_key":
                    if value in top_keys:
                        raise MaterializationError(
                            "filesystem object metadata contains a duplicate field"
                        )
                    top_keys.add(str(value))
                    continue
                if prefix in expected - {"required_identity_assertions", "parts"} and event in {
                    "string",
                    "number",
                    "null",
                }:
                    if prefix in scalars:
                        raise MaterializationError(
                            "filesystem object metadata contains a duplicate value"
                        )
                    scalars[prefix] = value
                    continue
                if prefix == "required_identity_assertions" and event == "map_key":
                    assertion_key = str(value)
                    if assertion_key in assertions:
                        raise MaterializationError(
                            "filesystem object assertions contain a duplicate field"
                        )
                    continue
                if prefix.startswith("required_identity_assertions.") and event == "string":
                    if assertion_key is None:
                        raise MaterializationError("filesystem object assertions are invalid")
                    assertions[assertion_key] = str(value)
                    assertion_key = None
                    continue
                if prefix == "parts.item" and event == "start_map":
                    if part is not None:
                        raise MaterializationError("filesystem object parts are invalid")
                    part = {}
                    continue
                if prefix == "parts.item" and event == "map_key":
                    if part is None or str(value) in part:
                        raise MaterializationError("filesystem object part is invalid")
                    part_key = str(value)
                    continue
                if prefix.startswith("parts.item.") and event in {"string", "number", "null"}:
                    if part is None or part_key is None:
                        raise MaterializationError("filesystem object part is invalid")
                    part[part_key] = value
                    part_key = None
                    continue
                if prefix == "parts.item" and event == "end_map":
                    if part is None:
                        raise MaterializationError("filesystem object part is invalid")
                    try:
                        parsed_part = _PartRecord.from_json(part)
                    except RuntimeError as exc:
                        raise MaterializationError(str(exc)) from exc
                    if parsed_part.number != next_part or parsed_part.offset != next_offset:
                        raise MaterializationError("filesystem object parts are not contiguous")
                    accept_part(parsed_part)
                    next_part += 1
                    next_offset += parsed_part.stored_bytes
                    part_bytes += parsed_part.stored_bytes
                    part = None
                    part_key = None
    except (ijson.JSONError, UnicodeError) as exc:
        raise MaterializationError("filesystem object metadata is unreadable") from exc
    if top_keys != expected or part is not None or assertion_key is not None or next_part == 1:
        raise MaterializationError("filesystem object metadata has an invalid shape")
    if scalars.get("schema") != _OBJECT_SCHEMA:
        raise MaterializationError("filesystem object metadata has an invalid shape")
    try:
        receipt = ObjectMetadataReceipt.model_validate(
            {
                "object_path": scalars["object_path"],
                "revision": scalars["revision"],
                "entity_token": scalars["entity_token"],
                "stored_bytes": scalars["stored_bytes"],
                "stored_sha256": scalars["stored_sha256"],
                "content_type": scalars["content_type"],
                "observed_identity_assertions": assertions,
                "verified_placement": scalars["placement"],
                "completed_at": scalars["completed_at"],
            }
        )
    except (KeyError, ValueError) as exc:
        raise MaterializationError("filesystem object metadata is invalid") from exc
    if part_bytes != receipt.stored_bytes:
        raise MaterializationError("filesystem object parts differ from its byte count")
    header = {
        "completed_at": receipt.completed_at,
        "content_type": receipt.content_type,
        "entity_token": receipt.entity_token,
        "object_path": receipt.object_path,
        "placement": receipt.verified_placement,
        "required_identity_assertions": receipt.observed_identity_assertions,
        "revision": receipt.revision,
        "schema": _OBJECT_SCHEMA,
        "stored_bytes": receipt.stored_bytes,
        "stored_sha256": receipt.stored_sha256,
    }
    return (
        _ProjectedObject(
            object_path=receipt.object_path,
            revision=receipt.revision or "",
            stored_bytes=receipt.stored_bytes,
            stored_sha256=receipt.stored_sha256,
        ),
        json.dumps(
            header,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ),
    )


def _commit_field(digest: Any, value: bytes) -> None:
    digest.update(struct.pack(">Q", len(value)))
    digest.update(value)


def _commit_selection(digest: Any, selection: MaterializationSelection) -> None:
    _commit_field(digest, b"selection")
    _commit_field(digest, b"all" if selection.all_objects else b"selected")
    for path in selection.paths:
        _commit_field(digest, b"path")
        _commit_field(digest, path.encode("utf-8"))
    _commit_field(digest, b"paths-terminal")
    for prefix in selection.prefixes:
        _commit_field(digest, b"prefix")
        _commit_field(digest, prefix.encode("utf-8"))
    _commit_field(digest, b"prefixes-terminal")


def _bind_projection(
    state: sqlite3.Connection,
    *,
    source: Path,
    selection: MaterializationSelection,
    projection: _Projection,
    destination: Path,
    allow_existing_destination: bool = False,
) -> None:
    expected = {
        "schema": _STATE_SCHEMA,
        **_operation_binding(source=source, destination=destination, selection=selection),
        "source_projection_sha256": projection.sha256,
    }
    persisted = dict(state.execute("SELECT key, value FROM authority").fetchall())
    if not persisted:
        if destination.exists() and not allow_existing_destination:
            raise MaterializationError("materialization checkpoint has no trustworthy authority")
        state.executemany(
            "INSERT INTO authority(key, value) VALUES (?, ?)",
            [
                *expected.items(),
                ("active_generation", projection.generation),
                ("last_object_key", ""),
            ],
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
    state.execute(
        "UPDATE authority SET value = ? WHERE key = 'active_generation'",
        (projection.generation,),
    )
    state.commit()


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
    generation = _active_generation(state)
    cursor = state.execute(
        "SELECT object_key, object_path, header_json FROM projection "
        "WHERE generation = ? AND object_key <= ? ORDER BY object_key",
        (generation, last_key),
    )
    while current := cursor.fetchone():
        found = found or str(current[0]) == last_key
        record = _record(str(current[2]))
        _verify_destination_object(
            state, generation, str(current[0]), destination, str(current[1]), record
        )
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
    staging: Path,
    interrupt_after_objects: int | None,
) -> tuple[int, int, int, int, int]:
    last_row = state.execute("SELECT value FROM authority WHERE key = 'last_object_key'").fetchone()
    last_key = str(last_row[0]) if last_row is not None else ""
    verified_objects = 0
    verified_bytes = 0
    copied_objects = 0
    copied_bytes = 0
    staging_verified_bytes = 0
    generation = _active_generation(state)
    cursor = state.execute(
        "SELECT object_key, object_path, header_json FROM projection "
        "WHERE generation = ? AND object_key > ? ORDER BY object_key",
        (generation, last_key),
    )
    while row := cursor.fetchone():
        object_key = str(row[0])
        object_path = str(row[1])
        record = _record(str(row[2]))
        target = _destination_path(destination, object_path, create=True)
        if target.exists() or target.is_symlink():
            _verify_destination_object(
                state, generation, object_key, destination, object_path, record
            )
            verified_objects += 1
            verified_bytes += record.stored_bytes
        else:
            verified_staging, copied = _copy_object(
                state,
                generation,
                source,
                object_key,
                record,
                staging / f"{object_key}.partial",
                target,
            )
            copied_objects += 1
            staging_verified_bytes += verified_staging
            copied_bytes += copied
        state.execute(
            "UPDATE authority SET value = ? WHERE key = 'last_object_key'",
            (object_key,),
        )
        state.commit()
        completed_objects = verified_objects + copied_objects
        if interrupt_after_objects is not None and completed_objects >= interrupt_after_objects:
            raise MaterializationInterrupted("test-controlled materialization interruption")
    return (
        verified_objects,
        verified_bytes,
        staging_verified_bytes,
        copied_objects,
        copied_bytes,
    )


def _copy_object(
    state: sqlite3.Connection,
    generation: str,
    source: Path,
    object_key: str,
    record: _ProjectedObject,
    staged: Path,
    target: Path,
) -> tuple[int, int]:
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
    source_fd = os.open(source_path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
    if staged.exists() or staged.is_symlink():
        _require_regular_file(staged, "materialization staged object")
        staged_bytes = staged.stat(follow_symlinks=False).st_size
        if staged_bytes > record.stored_bytes:
            os.close(source_fd)
            raise MaterializationError("materialization staged object exceeds its identity")
        target_fd = os.open(staged, os.O_RDWR | os.O_CLOEXEC | os.O_NOFOLLOW)
    else:
        staged_bytes = 0
        target_fd = os.open(
            staged,
            os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
            _PRIVATE_FILE_MODE,
        )
        _fsync_directory(staged.parent)
    try:
        observed = _copy_and_verify_parts(
            state,
            generation,
            object_key,
            source_fd,
            target_fd,
            staged_bytes=staged_bytes,
            record=record,
        )
        if observed != record.stored_bytes:
            raise MaterializationError("filesystem object copy differs from its byte count")
        os.fsync(target_fd)
    except BaseException:
        os.close(target_fd)
        os.close(source_fd)
        raise
    os.close(target_fd)
    os.close(source_fd)
    os.replace(staged, target)
    _fsync_directory(target.parent)
    _fsync_directory(staged.parent)
    return staged_bytes, record.stored_bytes - staged_bytes


def _copy_and_verify_parts(
    state: sqlite3.Connection,
    generation: str,
    object_key: str,
    source_fd: int,
    target_fd: int,
    *,
    staged_bytes: int,
    record: _ProjectedObject,
) -> int:
    whole = hashlib.sha256()
    written = 0
    for part in _iter_parts(state, generation, object_key):
        part_digest = hashlib.sha256()
        remaining = part.stored_bytes
        source_offset = part.offset
        while remaining:
            chunk = os.pread(source_fd, min(remaining, _COPY_CHUNK_BYTES), source_offset)
            if not chunk:
                raise MaterializationError("filesystem object ended before its metadata")
            if source_offset < staged_bytes:
                compare_bytes = min(len(chunk), staged_bytes - source_offset)
                staged_chunk = os.pread(target_fd, compare_bytes, source_offset)
                if staged_chunk != chunk[:compare_bytes]:
                    raise MaterializationError(
                        "materialization staged object conflicts with selected source identity"
                    )
                if compare_bytes < len(chunk):
                    os.lseek(target_fd, source_offset + compare_bytes, os.SEEK_SET)
                    _write_all(target_fd, chunk[compare_bytes:])
            else:
                os.lseek(target_fd, source_offset, os.SEEK_SET)
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
    state: sqlite3.Connection,
    generation: str,
    object_key: str,
    destination: Path,
    object_path: str,
    record: _ProjectedObject,
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
        for part in _iter_parts(state, generation, object_key):
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
    generation = _active_generation(state)
    stack: list[Any] = [os.scandir(destination)]
    try:
        while stack:
            try:
                entry = next(stack[-1])
            except StopIteration:
                stack.pop().close()
                continue
            path = Path(entry.path)
            if entry.is_symlink():
                raise MaterializationError("materialization destination contains a symbolic link")
            if entry.is_dir(follow_symlinks=False):
                stack.append(os.scandir(path))
                continue
            if not entry.is_file(follow_symlinks=False):
                raise MaterializationError("materialization destination contains a special file")
            logical = path.relative_to(destination).as_posix()
            if (
                state.execute(
                    "SELECT 1 FROM projection WHERE generation = ? AND object_path = ?",
                    (generation, logical),
                ).fetchone()
                is None
            ):
                raise MaterializationError(
                    "materialization destination contains an unselected object"
                )
    finally:
        for iterator in stack:
            iterator.close()


def _verify_staging_namespace(state: sqlite3.Connection, staging: Path) -> None:
    generation = _active_generation(state)
    with os.scandir(staging) as entries:
        for entry in entries:
            if entry.is_symlink() or not entry.is_file(follow_symlinks=False):
                raise MaterializationError("materialization staging contains an unknown entry")
            if not entry.name.endswith(".partial"):
                raise MaterializationError("materialization staging contains an unknown entry")
            object_key = entry.name.removesuffix(".partial")
            if not _is_hex(object_key, 64):
                raise MaterializationError("materialization staging contains an unknown entry")
            if (
                state.execute(
                    "SELECT 1 FROM projection WHERE generation = ? AND object_key = ?",
                    (generation, object_key),
                ).fetchone()
                is None
            ):
                raise MaterializationError("materialization staging contains an unowned object")


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


def _record(payload: str) -> _ProjectedObject:
    raw = json.loads(payload)
    expected = {
        "completed_at",
        "content_type",
        "entity_token",
        "object_path",
        "placement",
        "required_identity_assertions",
        "revision",
        "schema",
        "stored_bytes",
        "stored_sha256",
    }
    if not isinstance(raw, dict) or set(raw) != expected or raw.get("schema") != _OBJECT_SCHEMA:
        raise MaterializationError("checkpoint object metadata is invalid")
    try:
        receipt = ObjectMetadataReceipt(
            object_path=raw["object_path"],
            revision=raw["revision"],
            entity_token=raw["entity_token"],
            stored_bytes=raw["stored_bytes"],
            stored_sha256=raw["stored_sha256"],
            content_type=raw["content_type"],
            observed_identity_assertions=raw["required_identity_assertions"],
            verified_placement=raw["placement"],
            completed_at=raw["completed_at"],
        )
    except ValueError as exc:
        raise MaterializationError("checkpoint object metadata is invalid") from exc
    return _ProjectedObject(
        object_path=receipt.object_path,
        revision=receipt.revision or "",
        stored_bytes=receipt.stored_bytes,
        stored_sha256=receipt.stored_sha256,
    )


def _active_generation(state: sqlite3.Connection) -> str:
    row = state.execute("SELECT value FROM authority WHERE key = 'active_generation'").fetchone()
    if row is None:
        raise MaterializationError("materialization checkpoint has no active projection")
    generation = str(row[0])
    status = state.execute(
        "SELECT status FROM projection_generation WHERE generation = ?", (generation,)
    ).fetchone()
    if status is None or str(status[0]) != "complete":
        raise MaterializationError("materialization checkpoint projection is incomplete")
    return generation


def _iter_parts(
    state: sqlite3.Connection,
    generation: str,
    object_key: str,
) -> Iterator[_PartRecord]:
    expected_number = 1
    expected_offset = 0
    cursor = state.execute(
        "SELECT number, offset, stored_bytes, stored_sha256 FROM projection_part "
        "WHERE generation = ? AND object_key = ? ORDER BY number",
        (generation, object_key),
    )
    while row := cursor.fetchone():
        raw = {
            "number": int(row[0]),
            "offset": int(row[1]),
            "stored_bytes": int(row[2]),
            "stored_sha256": str(row[3]),
        }
        try:
            part = _PartRecord.from_json(raw)
        except RuntimeError as exc:
            raise MaterializationError("checkpoint object part is invalid") from exc
        if part.number != expected_number or part.offset != expected_offset:
            raise MaterializationError("checkpoint object parts are not contiguous")
        yield part
        expected_number += 1
        expected_offset += part.stored_bytes


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
