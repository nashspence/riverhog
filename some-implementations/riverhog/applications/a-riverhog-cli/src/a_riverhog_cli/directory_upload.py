"""Durably identify local upload sources before any Riverhog registration."""

from __future__ import annotations

import hashlib
import os
import secrets
import stat
import tempfile
import uuid
from collections.abc import Callable, Generator, Iterable, Iterator
from contextlib import ExitStack, closing, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, BinaryIO, Protocol, cast

from riverhog_canonical_json import canonical_json_bytes
from riverhog_client import (
    PreparedPrimaryInventory,
    PrimaryOrdering,
    PrimaryStatePolicy,
    PrimaryStorage,
    PrimaryStore,
    ProducerArtifactIdentity,
    posix_primary_files,
    prepare_primary_inventory,
)

from a_riverhog_cli.upload_identity_records import UploadIdentityRecords
from a_riverhog_cli.upload_sources import LocalUpload, SourceReaderFactory

if TYPE_CHECKING:
    from a_riverhog_cli.upload_windows import WindowsUploadState

_FORMAT = "a-riverhog-cli-directory-upload/v1"


class _VerifiedSources(Protocol):
    def __len__(self) -> int: ...

    def __iter__(self) -> Iterator[dict[str, object]]: ...


@dataclass(frozen=True, slots=True, weakref_slot=True)
class _OrderedSources:
    ordering: PrimaryOrdering

    def __len__(self) -> int:
        return self.ordering.count

    def __iter__(self) -> Iterator[dict[str, object]]:
        for key, byte_count, digest in self.ordering.rows():
            yield {"relative_parts": key.split("/"), "bytes": byte_count, "sha256": digest}


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


@contextmanager
def _verified_upload_sources(
    root: Path, idempotency_key: str, *, byte_limit: int
) -> Iterator[tuple[Path, _OrderedSources]]:
    """Spool verified POSIX metadata before creating persistent allocation state.

    The temporary directory follows the process temporary-storage placement.
    Its fixed scratch names contain only rebuildable ordering, charged under
    the caller's ordering admission. This owner closes before publication;
    original source IDs and identity bytes never live in this scratch.
    """
    root = _validated_upload_root(root, idempotency_key)
    with tempfile.TemporaryDirectory(prefix="a-riverhog-cli-source-order-") as temporary:
        with posix_primary_files(
            Path(temporary).resolve() / "sources",
            allocation_descriptor=b"temporary-source-ordering",
            initialize=True,
        ) as storage:
            with _verified_ordered_upload_sources(
                root,
                idempotency_key,
                storage,
                byte_limit=byte_limit,
                prepare_payload=lambda: None,
            ) as selected:
                yield selected


@contextmanager
def _verified_ordered_upload_sources(
    root: Path,
    idempotency_key: str,
    storage: PrimaryStorage,
    *,
    byte_limit: int,
    prepare_payload: Callable[[], None],
) -> Iterator[tuple[Path, _OrderedSources]]:
    """Verify all observed sources into one admitted derived ordering view."""
    root = _validated_upload_root(root, idempotency_key)

    def rows() -> Iterator[tuple[str, int, str]]:
        with closing(_walk_source_paths(root)) as paths:
            for path in paths:
                fixity = _source_fixity(path)
                prepare_payload()
                yield (
                    path.relative_to(root).as_posix(),
                    cast(int, fixity["bytes"]),
                    cast(str, fixity["sha256"]),
                )

    prepare_payload()
    with storage.ordered_rows(rows(), byte_limit=byte_limit) as ordered:
        if ordered.count == 0:
            raise ValueError("collection source has no regular files")
        yield root, _OrderedSources(ordered)


@contextmanager
def _open_validated_upload_identity(
    root: Path,
    idempotency_key: str,
    current: _VerifiedSources,
    *,
    initialize: bool = True,
) -> Iterator[tuple[BinaryIO, _ValidatedUploadIdentity, SourceReaderFactory]]:
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
    with destination.open("rb") as stream:
        validated = _validate_upload_identity(
            UploadIdentityRecords(stream), root, idempotency_key, current
        )
        yield stream, validated, lambda: destination.open("rb")


def _new_upload_identity(
    root: Path, idempotency_key: str, current: Iterable[dict[str, object]]
) -> dict[str, object]:
    """Allocate the view now and each member ID only when its record is consumed."""
    return {
        "format": _FORMAT,
        "root": str(root),
        "idempotency_key": idempotency_key,
        "source_naming_view_id": "urn:uuid:" + str(uuid.uuid4()),
        "sources": ({**item, "artifact_id": secrets.token_hex(32)} for item in current),
    }


def _upload_identity_chunks(document: dict[str, object]) -> Iterator[bytes]:
    """Encode the original allocation with one record-sized serialization buffer.

    This application's fixed header sorts before its sources array. Each value
    still uses the shared canonical codec; the stored format and original IDs
    are unchanged. Source rows are consumed once; enumeration and replay own their lifetimes.
    """
    if set(document) != {"format", "idempotency_key", "root", "source_naming_view_id", "sources"}:
        raise ValueError("upload identity encoding requires the complete known document")
    header = {key: value for key, value in document.items() if key != "sources"}
    yield canonical_json_bytes(header)[:-1] + b',"sources":['
    for index, row in enumerate(cast(Iterable[dict[str, object]], document["sources"])):
        if index:
            yield b","
        yield canonical_json_bytes(row)
    yield b"]}"


@dataclass(frozen=True, slots=True)
class _ValidatedUploadIdentity:
    header: dict[str, str]
    commitment: tuple[int, str]
    source_count: int


def _validate_upload_identity(
    records: UploadIdentityRecords,
    root: Path,
    idempotency_key: str,
    current: _VerifiedSources,
) -> _ValidatedUploadIdentity:
    header = records.header()
    if (
        header["format"] != _FORMAT
        or header["root"] != str(root)
        or header["idempotency_key"] != idempotency_key
    ):
        raise ValueError("saved upload identity differs from the current local sources")
    count = 0
    expected_count = len(current)
    current_rows = iter(current)
    for item in records.records():
        expected = next(current_rows, None)
        if (
            count >= expected_count
            or expected is None
            or {key: item.get(key) for key in ("relative_parts", "bytes", "sha256")} != expected
        ):
            raise ValueError("saved upload identity differs from the current local sources")
        value = item.get("artifact_id")
        if (
            not isinstance(value, str)
            or len(value) != 64
            or any(character not in "0123456789abcdef" for character in value)
        ):
            raise ValueError("saved upload artifact identities are invalid")
        count += 1
    if count != expected_count or next(current_rows, None) is not None:
        raise ValueError("saved upload identity differs from the current local sources")
    if not header["source_naming_view_id"].startswith("urn:uuid:"):
        raise ValueError("saved upload source view identity is invalid")
    return _ValidatedUploadIdentity(header, records.commitment, count)


def _load_upload_identity(
    stream: BinaryIO,
    root: Path,
    validated: _ValidatedUploadIdentity,
    *,
    open_original: SourceReaderFactory,
    prepare_inventory: Callable[[Iterable[ProducerArtifactIdentity]], PreparedPrimaryInventory],
) -> LocalUpload:
    """Finish the second complete original pass into its one canonical derivation."""
    stream.seek(0)
    replay = UploadIdentityRecords(stream)
    if replay.header() != validated.header:
        raise ValueError("saved upload identity changed during replay")
    complete = False

    def members() -> Generator[ProducerArtifactIdentity, None, None]:
        nonlocal complete
        from riverhog_protocol.artifact_identity import ArtifactId

        for item in replay.records():
            yield ProducerArtifactIdentity(
                ArtifactId(cast(str, item["artifact_id"])),
                cast(int, item["bytes"]),
                cast(str, item["sha256"]),
            )
        if replay.commitment != validated.commitment:
            raise ValueError("saved upload identity changed during replay")
        complete = True

    with closing(members()) as rows:
        inventory = prepare_inventory(rows)
    if not complete or inventory.count != validated.source_count:
        raise ValueError("saved upload inventory did not consume the complete original")
    return LocalUpload(
        source_count=validated.source_count,
        source_naming_view_id=validated.header["source_naming_view_id"],
        _root=root,
        _commitment=validated.commitment,
        _reader=open_original,
        _inventory=inventory,
    )


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
    with ExitStack() as primary:
        with ExitStack() as original:
            with _verified_upload_sources(
                root, idempotency_key, byte_limit=policy.inventory_order_bytes
            ) as (root, current):
                if initialize:
                    destination = _state_path(root, idempotency_key)
                store = primary.enter_context(
                    PrimaryStore(
                        destination.with_suffix(".primary"),
                        allocation_id=destination.stem,
                        scope=policy.scope,
                        limits=policy.limits(),
                        initialize=initialize,
                    )
                )
                stream, validated, open_original = original.enter_context(
                    _open_validated_upload_identity(
                        root, idempotency_key, current, initialize=initialize
                    )
                )
            # Current-metadata scratch closes before canonical ID ordering.
            del current
            upload = _load_upload_identity(
                stream,
                root,
                validated,
                open_original=open_original,
                prepare_inventory=lambda members: prepare_primary_inventory(store, members),
            )
        yield upload, store
