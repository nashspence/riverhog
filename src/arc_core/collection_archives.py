from __future__ import annotations

import hashlib
import io
import tarfile
import tempfile
from collections.abc import Callable, Iterable, Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import yaml

from arc_core.fs_paths import normalize_collection_id, normalize_relpath
from arc_core.proofs import ProofStamper, StubProofStamper

COLLECTION_ARCHIVE_MANIFEST_SCHEMA = "collection-archive-manifest/v1"
COLLECTION_ARCHIVE_FORMAT = "tar"
COLLECTION_ARCHIVE_COMPRESSION = "none"


@dataclass(frozen=True, slots=True)
class CollectionArchiveFile:
    path: str
    content: bytes
    sha256: str

    @property
    def bytes(self) -> int:
        return len(self.content)


@dataclass(frozen=True, slots=True)
class CollectionArchiveExpectedFile:
    path: str
    bytes: int
    sha256: str


@dataclass(frozen=True, slots=True)
class CollectionArchivePackage:
    collection_id: str
    archive_bytes: bytes
    archive_sha256: str
    manifest_bytes: bytes
    manifest_sha256: str
    proof_bytes: bytes
    proof_sha256: str
    archive_format: str
    compression: str


def build_collection_archive_package(
    *,
    collection_id: str,
    files: Sequence[CollectionArchiveFile],
    stamper: ProofStamper | None = None,
) -> CollectionArchivePackage:
    normalized_collection_id = normalize_collection_id(collection_id)
    normalized_files = _normalized_files(files)
    expected_files = _expected_files_from_archive_files(normalized_files)
    return _build_collection_archive_package(
        collection_id=normalized_collection_id,
        files=expected_files,
        archive_bytes=_archive_bytes(normalized_files),
        stamper=stamper,
    )


def build_collection_archive_package_from_reader(
    *,
    collection_id: str,
    files: Sequence[CollectionArchiveExpectedFile],
    read_file: Callable[[str], bytes],
    stamper: ProofStamper | None = None,
) -> CollectionArchivePackage:
    normalized_collection_id = normalize_collection_id(collection_id)
    normalized_files = _normalized_expected_files(files)
    return _build_collection_archive_package(
        collection_id=normalized_collection_id,
        files=normalized_files,
        archive_bytes=_archive_bytes_from_reader(normalized_files, read_file),
        stamper=stamper,
    )


def _build_collection_archive_package(
    *,
    collection_id: str,
    files: Sequence[CollectionArchiveExpectedFile],
    archive_bytes: bytes,
    stamper: ProofStamper | None,
) -> CollectionArchivePackage:
    manifest = _manifest_payload(
        collection_id=collection_id,
        files=files,
    )
    manifest_bytes = yaml.safe_dump(
        manifest,
        sort_keys=False,
        allow_unicode=True,
    ).encode("utf-8")
    proof_bytes = _stamp_manifest_bytes(manifest_bytes, stamper=stamper)
    return CollectionArchivePackage(
        collection_id=collection_id,
        archive_bytes=archive_bytes,
        archive_sha256=_sha256(archive_bytes),
        manifest_bytes=manifest_bytes,
        manifest_sha256=_sha256(manifest_bytes),
        proof_bytes=proof_bytes,
        proof_sha256=_sha256(proof_bytes),
        archive_format=COLLECTION_ARCHIVE_FORMAT,
        compression=COLLECTION_ARCHIVE_COMPRESSION,
    )


def verify_collection_archive_member(
    *,
    path: str,
    content: bytes,
    expected_sha256: str,
) -> None:
    digest = _sha256(content)
    if digest != expected_sha256:
        raise ValueError(f"collection archive member sha256 mismatch: {path}")


def verify_collection_archive_manifest(
    *,
    manifest_bytes: bytes,
    expected_sha256: str,
    collection_id: str,
    files: Sequence[CollectionArchiveExpectedFile],
) -> None:
    digest = _sha256(manifest_bytes)
    if digest != expected_sha256:
        raise ValueError("collection archive manifest sha256 mismatch")
    try:
        manifest = yaml.safe_load(manifest_bytes)
    except yaml.YAMLError as exc:
        raise ValueError("collection archive manifest is not valid YAML") from exc
    if not isinstance(manifest, dict):
        raise ValueError("collection archive manifest must be a mapping")
    normalized_collection_id = normalize_collection_id(collection_id)
    if manifest.get("schema") != COLLECTION_ARCHIVE_MANIFEST_SCHEMA:
        raise ValueError("collection archive manifest schema mismatch")
    if manifest.get("collection") != normalized_collection_id:
        raise ValueError("collection archive manifest collection mismatch")

    archive = _mapping(manifest.get("archive"), "archive")
    if archive.get("format") != COLLECTION_ARCHIVE_FORMAT:
        raise ValueError("collection archive manifest format mismatch")
    if archive.get("compression") != COLLECTION_ARCHIVE_COMPRESSION:
        raise ValueError("collection archive manifest compression mismatch")

    expected_rows, expected_tree = _expected_manifest_rows(files)
    rows = _manifest_rows(manifest.get("files"))
    if rows != expected_rows:
        raise ValueError("collection archive manifest files do not match catalog")

    tree = _mapping(manifest.get("tree"), "tree")
    if tree.get("sha256") != expected_tree["sha256"]:
        raise ValueError("collection archive manifest tree sha256 mismatch")
    if tree.get("total_bytes") != expected_tree["total_bytes"]:
        raise ValueError("collection archive manifest total bytes mismatch")


def verify_collection_archive_proof(
    *,
    proof_bytes: bytes,
    expected_sha256: str,
    manifest_bytes: bytes,
) -> None:
    digest = _sha256(proof_bytes)
    if digest != expected_sha256:
        raise ValueError("collection archive proof sha256 mismatch")
    if not proof_bytes:
        raise ValueError("collection archive proof is empty")
    manifest_digest = _sha256(manifest_bytes)
    try:
        proof_text = proof_bytes.decode("utf-8")
    except UnicodeDecodeError:
        return
    if proof_text.startswith("OpenTimestamps stub proof v1\n") and (
        f"sha256: {manifest_digest}\n" not in proof_text
    ):
        raise ValueError("collection archive proof does not match manifest")


def iter_collection_archive_files(
    chunks: Iterable[bytes],
) -> Iterator[tuple[str, bytes]]:
    stream = _ChunkIteratorReader(chunks)
    with tarfile.open(fileobj=cast(Any, stream), mode="r|*") as archive:
        for member in archive:
            if not member.isfile():
                continue
            handle = archive.extractfile(member)
            if handle is None:
                continue
            yield normalize_relpath(member.name), handle.read()


def verify_collection_archive_files(
    *,
    chunks: Iterable[bytes],
    files: Sequence[CollectionArchiveExpectedFile],
) -> None:
    expected = {file.path: file for file in _normalized_expected_files(files)}
    seen: set[str] = set()
    for path, content in iter_collection_archive_files(chunks):
        if path in seen:
            raise ValueError(f"duplicate collection archive member: {path}")
        seen.add(path)
        expected_file = expected.get(path)
        if expected_file is None:
            raise ValueError(f"unexpected collection archive member: {path}")
        if len(content) != expected_file.bytes:
            raise ValueError(f"collection archive member byte count mismatch: {path}")
        verify_collection_archive_member(
            path=path,
            content=content,
            expected_sha256=expected_file.sha256,
        )
    missing = sorted(set(expected) - seen)
    if missing:
        raise ValueError(f"collection archive missing member: {missing[0]}")


def _normalized_files(
    files: Sequence[CollectionArchiveFile],
) -> tuple[CollectionArchiveFile, ...]:
    out: list[CollectionArchiveFile] = []
    seen: set[str] = set()
    for file in files:
        path = normalize_relpath(file.path)
        if path in seen:
            raise ValueError(f"duplicate collection archive path: {path}")
        seen.add(path)
        digest = _sha256(file.content)
        if digest != file.sha256:
            raise ValueError(f"collection archive file sha256 mismatch: {path}")
        out.append(CollectionArchiveFile(path=path, content=file.content, sha256=digest))
    if not out:
        raise ValueError("collection archive package requires at least one file")
    return tuple(sorted(out, key=lambda current: current.path))


def _normalized_expected_files(
    files: Sequence[CollectionArchiveExpectedFile],
) -> tuple[CollectionArchiveExpectedFile, ...]:
    out: list[CollectionArchiveExpectedFile] = []
    seen: set[str] = set()
    for file in files:
        path = normalize_relpath(file.path)
        if path in seen:
            raise ValueError(f"duplicate collection archive path: {path}")
        seen.add(path)
        out.append(
            CollectionArchiveExpectedFile(
                path=path,
                bytes=int(file.bytes),
                sha256=file.sha256,
            )
        )
    if not out:
        raise ValueError("collection archive package requires at least one file")
    return tuple(sorted(out, key=lambda current: current.path))


def _expected_files_from_archive_files(
    files: Sequence[CollectionArchiveFile],
) -> tuple[CollectionArchiveExpectedFile, ...]:
    return tuple(
        CollectionArchiveExpectedFile(path=file.path, bytes=file.bytes, sha256=file.sha256)
        for file in files
    )


def _manifest_payload(
    *,
    collection_id: str,
    files: Sequence[CollectionArchiveExpectedFile],
) -> dict[str, object]:
    tree_digest = hashlib.sha256()
    rows: list[dict[str, object]] = []
    total_bytes = 0
    for file in files:
        total_bytes += file.bytes
        rows.append(
            {
                "path": file.path,
                "bytes": file.bytes,
                "sha256": file.sha256,
            }
        )
        tree_digest.update(f"{file.path}\t{file.bytes}\t{file.sha256}\n".encode())
    return {
        "schema": COLLECTION_ARCHIVE_MANIFEST_SCHEMA,
        "collection": collection_id,
        "archive": {
            "format": COLLECTION_ARCHIVE_FORMAT,
            "compression": COLLECTION_ARCHIVE_COMPRESSION,
        },
        "tree": {
            "sha256": tree_digest.hexdigest(),
            "total_bytes": total_bytes,
        },
        "files": rows,
    }


def _expected_manifest_rows(
    files: Sequence[CollectionArchiveExpectedFile],
) -> tuple[tuple[dict[str, object], ...], dict[str, object]]:
    tree_digest = hashlib.sha256()
    rows: list[dict[str, object]] = []
    seen: set[str] = set()
    total_bytes = 0
    for file in sorted(files, key=lambda current: normalize_relpath(current.path)):
        path = normalize_relpath(file.path)
        if path in seen:
            raise ValueError(f"duplicate collection archive path: {path}")
        seen.add(path)
        total_bytes += file.bytes
        rows.append({"path": path, "bytes": file.bytes, "sha256": file.sha256})
        tree_digest.update(f"{path}\t{file.bytes}\t{file.sha256}\n".encode())
    if not rows:
        raise ValueError("collection archive manifest requires at least one file")
    return tuple(rows), {"sha256": tree_digest.hexdigest(), "total_bytes": total_bytes}


def _manifest_rows(value: object) -> tuple[dict[str, object], ...]:
    if not isinstance(value, list):
        raise ValueError("collection archive manifest files must be a list")
    rows: list[dict[str, object]] = []
    for row in value:
        mapping = _mapping(row, "file")
        path = normalize_relpath(str(mapping.get("path", "")))
        try:
            byte_count = int(mapping.get("bytes", -1))
        except (TypeError, ValueError) as exc:
            raise ValueError("collection archive manifest file has invalid byte count") from exc
        sha256 = str(mapping.get("sha256", ""))
        rows.append({"path": path, "bytes": byte_count, "sha256": sha256})
    return tuple(sorted(rows, key=lambda current: str(current["path"])))


def _mapping(value: object, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"collection archive manifest {name} must be a mapping")
    return value


def _stamp_manifest_bytes(
    manifest_bytes: bytes,
    *,
    stamper: ProofStamper | None,
) -> bytes:
    with tempfile.TemporaryDirectory(prefix="arc-collection-archive-proof-") as tmpdir:
        manifest_path = Path(tmpdir) / "manifest.yml"
        manifest_path.write_bytes(manifest_bytes)
        proof_path = (stamper or StubProofStamper()).stamp(manifest_path)
        return proof_path.read_bytes()


def _archive_bytes(files: Sequence[CollectionArchiveFile]) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as archive:
        for file in files:
            info = tarfile.TarInfo(file.path)
            info.size = file.bytes
            info.mode = 0o644
            info.uid = 0
            info.gid = 0
            info.uname = ""
            info.gname = ""
            info.mtime = 0
            archive.addfile(info, io.BytesIO(file.content))
    return buffer.getvalue()


def _archive_bytes_from_reader(
    files: Sequence[CollectionArchiveExpectedFile],
    read_file: Callable[[str], bytes],
) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as archive:
        for file in files:
            content = read_file(file.path)
            if len(content) != file.bytes:
                raise ValueError(f"collection archive file byte count mismatch: {file.path}")
            verify_collection_archive_member(
                path=file.path,
                content=content,
                expected_sha256=file.sha256,
            )
            info = tarfile.TarInfo(file.path)
            info.size = file.bytes
            info.mode = 0o644
            info.uid = 0
            info.gid = 0
            info.uname = ""
            info.gname = ""
            info.mtime = 0
            archive.addfile(info, io.BytesIO(content))
    return buffer.getvalue()


class _ChunkIteratorReader:
    def __init__(self, chunks: Iterable[bytes]) -> None:
        self._chunks = iter(chunks)
        self._buffer = bytearray()
        self._finished = False

    def read(self, size: int = -1) -> bytes:
        if size == 0:
            return b""
        if size < 0:
            while self._fill():
                pass
            out = bytes(self._buffer)
            self._buffer.clear()
            return out
        while len(self._buffer) < size and self._fill():
            pass
        out = bytes(self._buffer[:size])
        del self._buffer[:size]
        return out

    def _fill(self) -> bool:
        if self._finished:
            return False
        for chunk in self._chunks:
            if chunk:
                self._buffer.extend(chunk)
                return True
        self._finished = True
        return False


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()
