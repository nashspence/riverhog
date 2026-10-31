"""Durably identify local upload sources before any Riverhog registration."""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import stat
import tempfile
import uuid
from collections.abc import Generator, Iterator
from contextlib import ExitStack, closing, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from riverhog_canonical_json import canonical_json_bytes
from riverhog_client import PrimaryStatePolicy, PrimaryStore, ProducerFile
from riverhog_protocol.artifact_identity import ArtifactId
from riverhog_protocol.provenance_transport import MaterializationHintDocument

if TYPE_CHECKING:
    from a_riverhog_cli.upload_windows import WindowsUploadState

_FORMAT = "a-riverhog-cli-directory-upload/v1"


@dataclass(frozen=True, slots=True)
class LocalSource:
    root: Path
    relative_parts: tuple[str, ...]
    artifact_id: str
    bytes: int
    sha256: str

    @property
    def path(self) -> Path:
        """Materialize the pathname when a source is consumed."""
        return self.root.joinpath(*self.relative_parts)

    def producer_file(self, *, observation: Any = None) -> ProducerFile:
        try:
            MaterializationHintDocument(components=list(self.relative_parts))
        except ValueError:
            return ProducerFile(
                source=self.path,
                artifact_id=ArtifactId(self.artifact_id),
                allow_missing_materialization_hint=True,
                observation=observation,
            )
        return ProducerFile(
            source=self.path,
            artifact_id=ArtifactId(self.artifact_id),
            materialization_hint=self.relative_parts,
            observation=observation,
        )


@dataclass(frozen=True, slots=True)
class LocalUpload:
    sources: tuple[LocalSource, ...]
    source_naming_view_id: str


@dataclass(frozen=True, slots=True)
class UploadPreview:
    artifact_count: int
    total_bytes: int
    sources: tuple[dict[str, object], ...]


def summarize_upload(root: Path) -> UploadPreview:
    """Read every source while retaining only the first five canonical examples.

    Enumeration holds one iterator per directory depth. Hashing keeps its
    bounded byte window; counts include zero-byte and equal-byte members.
    No allocation state or remote connection is opened for this preview.
    """
    root = root.expanduser().resolve()
    if not root.is_dir():
        raise ValueError("collection source must be a directory")
    artifact_count = total_bytes = 0
    sample: list[tuple[str, dict[str, object]]] = []
    with closing(_walk_source_paths(root)) as paths:
        for path in paths:
            fixity = _source_fixity(path)
            artifact_count += 1
            total_bytes += cast(int, fixity["bytes"])
            key = path.relative_to(root).as_posix()
            item = {"relative_components": list(path.relative_to(root).parts), **fixity}
            if len(sample) < 5:
                sample.append((key, item))
            else:
                largest = max(range(len(sample)), key=lambda index: sample[index][0])
                if key < sample[largest][0]:
                    sample[largest] = key, item
    if artifact_count == 0:
        raise ValueError("collection source has no regular files")
    return UploadPreview(
        artifact_count,
        total_bytes,
        tuple(item for _key, item in sorted(sample, key=lambda pair: pair[0])),
    )


def preview_upload(root: Path) -> tuple[dict[str, object], ...]:
    """Inspect local bytes and proposed advice without allocating member IDs."""

    root = root.expanduser().resolve()
    if not root.is_dir():
        raise ValueError("collection source must be a directory")
    paths = _source_paths(root)
    if not paths:
        raise ValueError("collection source has no regular files")
    return tuple(
        {
            "relative_components": list(path.relative_to(root).parts),
            **_source_fixity(path),
        }
        for path in paths
    )


def _walk_source_paths(root: Path) -> Generator[Path, None, None]:
    opened = [os.scandir(root)]
    try:
        while opened:
            try:
                entry = next(opened[-1])
            except StopIteration:
                opened[-1].close()
                opened.pop()
                continue
            path = Path(entry.path)
            relative = path.relative_to(root)
            if entry.is_symlink():
                raise ValueError(f"upload source is a symbolic link: {relative}")
            if entry.is_dir(follow_symlinks=False):
                opened.append(os.scandir(path))
            elif entry.is_file(follow_symlinks=False):
                yield path
            else:
                raise ValueError(f"upload source is not a regular file: {relative}")
    finally:
        with ExitStack() as cleanup:
            for iterator in opened:
                cleanup.callback(iterator.close)


def _source_paths(root: Path) -> tuple[Path, ...]:
    with closing(_walk_source_paths(root)) as paths:
        return tuple(sorted(paths, key=lambda path: path.relative_to(root).as_posix()))


def _source_stamp(value: os.stat_result) -> tuple[int, int, int, int, int]:
    return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns


def _require_source_stamp(
    expected: tuple[int, ...], current: tuple[int, ...], *, phase: str
) -> None:
    names = ("device", "inode", "bytes", "mtime", "ctime")[: len(expected)]
    changed = [
        name for name, left, right in zip(names, expected, current, strict=True) if left != right
    ]
    if changed:
        raise ValueError(f"upload source changed while hashing ({phase}: {','.join(changed)})")


def _source_fixity(path: Path) -> dict[str, object]:
    """Hash one observed regular file without chasing a growing EOF.

    The byte count comes from the same descriptor as the hash. Metadata checks
    reject observed replacement or mutation; they do not freeze a mutable
    filesystem. Publication still verifies the original source commitment.
    """
    observed = path.lstat()
    if not stat.S_ISREG(observed.st_mode):
        raise ValueError("upload source is not a regular file")
    flags = (
        os.O_RDONLY
        | getattr(os, "O_BINARY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    observed_stamp = _source_stamp(observed)
    descriptor = os.open(path, flags)
    try:
        before = os.fstat(descriptor)
        stamp = _source_stamp(before)
        if not stat.S_ISREG(before.st_mode):
            raise ValueError("upload source changed while hashing (opened nonregular file)")
        # Windows path ctime can describe creation while descriptor ctime
        # describes metadata change. Check it within each API, preserving both
        # observations; device/inode/size/mtime still bind the opened source.
        _require_source_stamp(observed_stamp[:4], stamp[:4], phase="open")
        _require_source_stamp(observed_stamp, _source_stamp(path.lstat()), phase="path")
        digest = hashlib.sha256()
        count = 0
        while chunk := os.read(descriptor, min(8 * 1024 * 1024, before.st_size - count + 1)):
            count += len(chunk)
            if count > before.st_size:
                raise ValueError("upload source changed while hashing")
            digest.update(chunk)
        if count != before.st_size:
            raise ValueError("upload source changed while hashing (read byte count)")
        _require_source_stamp(stamp, _source_stamp(os.fstat(descriptor)), phase="descriptor")
        _require_source_stamp(observed_stamp, _source_stamp(path.lstat()), phase="path")
        return {"bytes": count, "sha256": digest.hexdigest()}
    finally:
        os.close(descriptor)


def _allocation_id(root: Path, key: str) -> str:
    return hashlib.sha256(canonical_json_bytes([str(root), key])).hexdigest()


def _state_path(root: Path, key: str, *, create_parent: bool = True) -> Path:
    state_root = Path(os.environ.get("XDG_STATE_HOME") or Path.home() / ".local/state")
    directory = state_root / "a-riverhog-cli" / "uploads"
    if create_parent:
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    return directory / f"{_allocation_id(root, key)}.json"


def _validated_upload_root(root: Path, idempotency_key: str) -> Path:
    root = root.expanduser().resolve()
    if not root.is_dir():
        raise ValueError("collection source must be a directory")
    if not idempotency_key or idempotency_key != idempotency_key.strip():
        raise ValueError("upload idempotency key must be nonempty and canonical")
    return root


def _verified_upload_sources(
    root: Path, idempotency_key: str
) -> tuple[Path, list[dict[str, object]]]:
    root = _validated_upload_root(root, idempotency_key)
    current = [
        {
            "relative_parts": item["relative_components"],
            "bytes": item["bytes"],
            "sha256": item["sha256"],
        }
        for item in preview_upload(root)
    ]
    return root, current


def prepare_upload(root: Path, idempotency_key: str) -> LocalUpload:
    """Reuse an exact persisted allocation or publish one before opening a session."""
    root, current = _verified_upload_sources(root, idempotency_key)
    return _prepare_verified_upload(root, idempotency_key, current)


def _prepare_verified_upload(
    root: Path,
    idempotency_key: str,
    current: list[dict[str, object]],
    *,
    initialize: bool = True,
) -> LocalUpload:
    destination = _state_path(root, idempotency_key, create_parent=initialize)
    if not initialize and not destination.is_file():
        raise FileNotFoundError(
            "retained upload identity is missing; resume cannot allocate replacements"
        )
    if initialize and not destination.exists():
        document = _new_upload_identity(root, idempotency_key, current)
        temporary: str | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb", dir=destination.parent, prefix=".upload-", delete=False
            ) as handle:
                temporary = handle.name
                for chunk in _upload_identity_chunks(document):
                    handle.write(chunk)
                handle.flush()
                os.fsync(handle.fileno())
            try:
                os.link(temporary, destination)
            except FileExistsError:
                pass
            directory_fd = os.open(destination.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            if temporary is not None:
                Path(temporary).unlink(missing_ok=True)
        # Drop serialization input before decoding the selected original.
        del document
    return _load_upload_identity(
        json.loads(destination.read_bytes()), root, idempotency_key, current
    )


def _new_upload_identity(
    root: Path, idempotency_key: str, current: list[dict[str, object]]
) -> dict[str, object]:
    return {
        "format": _FORMAT,
        "root": str(root),
        "idempotency_key": idempotency_key,
        "source_naming_view_id": "urn:uuid:" + str(uuid.uuid4()),
        "sources": [{**item, "artifact_id": secrets.token_hex(32)} for item in current],
    }


def _upload_identity_chunks(document: dict[str, object]) -> Iterator[bytes]:
    """Encode the original allocation with one record-sized serialization buffer.

    This application's fixed header sorts before its sources array. Each value
    still uses the shared canonical codec; the stored format and original IDs
    are unchanged. Enumeration and decoded replay have their own lifetimes.
    """
    if set(document) != {"format", "idempotency_key", "root", "source_naming_view_id", "sources"}:
        raise ValueError("upload identity encoding requires the complete known document")
    header = {key: value for key, value in document.items() if key != "sources"}
    yield canonical_json_bytes(header)[:-1] + b',"sources":['
    for index, row in enumerate(cast(list[dict[str, object]], document["sources"])):
        if index:
            yield b","
        yield canonical_json_bytes(row)
    yield b"]}"


def _load_upload_identity(
    loaded: object, root: Path, idempotency_key: str, current: list[dict[str, object]]
) -> LocalUpload:
    if (
        not isinstance(loaded, dict)
        or loaded.get("format") != _FORMAT
        or loaded.get("root") != str(root)
        or loaded.get("idempotency_key") != idempotency_key
        or not isinstance(loaded.get("sources"), list)
        or len(loaded["sources"]) != len(current)
        or any(
            not isinstance(item, dict)
            or {key: item.get(key) for key in ("relative_parts", "bytes", "sha256")} != expected
            for item, expected in zip(loaded["sources"], current, strict=True)
        )
    ):
        raise ValueError("saved upload identity differs from the current local sources")
    view_id = loaded.get("source_naming_view_id")
    if not isinstance(view_id, str) or not view_id.startswith("urn:uuid:"):
        raise ValueError("saved upload source view identity is invalid")
    ids: set[str] = set()
    for item in loaded["sources"]:
        value = item.get("artifact_id")
        if (
            not isinstance(value, str)
            or len(value) != 64
            or any(character not in "0123456789abcdef" for character in value)
            or value in ids
        ):
            raise ValueError("saved upload artifact identities are invalid")
        ids.add(value)
    # The original IDs are validated before materialization, without retaining
    # another collection of them while the source descriptors are constructed.
    del ids
    sources = tuple(
        LocalSource(
            root=root,
            relative_parts=tuple(item["relative_parts"]),
            artifact_id=item["artifact_id"],
            bytes=item["bytes"],
            sha256=item["sha256"],
        )
        for item in loaded["sources"]
    )
    return LocalUpload(sources=sources, source_naming_view_id=view_id)


@contextmanager
def prepare_primary_upload(
    root: Path,
    idempotency_key: str,
    policy: PrimaryStatePolicy,
    *,
    initialize: bool,
    windows: WindowsUploadState | None = None,
) -> Iterator[tuple[LocalUpload, PrimaryStore]]:
    """Own an explicitly fresh or resumed allocation before touching Riverhog."""
    if type(initialize) is not bool:
        raise ValueError("upload state initialization requires explicit boolean authority")
    root = _validated_upload_root(root, idempotency_key)
    if windows is not None:
        from a_riverhog_cli.upload_windows import prepare_windows_upload

        with prepare_windows_upload(
            root, idempotency_key, policy, windows, initialize=initialize
        ) as prepared:
            yield prepared
        return
    destination = _state_path(root, idempotency_key, create_parent=False)
    if initialize and destination.exists():
        raise FileExistsError("fresh upload initialization conflicts with retained identity")
    if not initialize and not destination.is_file():
        raise FileNotFoundError(
            "retained upload identity is missing; resume cannot allocate replacements"
        )
    root, current = _verified_upload_sources(root, idempotency_key)
    if initialize:
        destination = _state_path(root, idempotency_key)
    with PrimaryStore(
        destination.with_suffix(".primary"),
        allocation_id=destination.stem,
        scope=policy.scope,
        limits=policy.limits(),
        initialize=initialize,
    ) as store:
        upload = _prepare_verified_upload(root, idempotency_key, current, initialize=initialize)
        # Verification input is no longer needed during remote publication.
        del current
        yield upload, store
