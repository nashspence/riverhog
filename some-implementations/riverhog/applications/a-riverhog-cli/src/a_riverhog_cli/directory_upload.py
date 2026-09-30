"""Durably identify local upload sources before any Riverhog registration."""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import tempfile
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from riverhog_canonical_json import canonical_json_bytes
from riverhog_client import ProducerFile
from riverhog_protocol.provenance_transport import MaterializationHintDocument

_FORMAT = "a-riverhog-cli-directory-upload/v1"


@dataclass(frozen=True, slots=True)
class LocalSource:
    path: Path
    relative_parts: tuple[str, ...]
    artifact_id: str
    bytes: int
    sha256: str

    def producer_file(self, *, observation: Any = None) -> ProducerFile:
        try:
            MaterializationHintDocument(components=list(self.relative_parts))
        except ValueError:
            return ProducerFile(
                source=self.path,
                artifact_id=self.artifact_id,
                allow_missing_materialization_hint=True,
                observation=observation,
            )
        return ProducerFile(
            source=self.path,
            artifact_id=self.artifact_id,
            materialization_hint=self.relative_parts,
            observation=observation,
        )


@dataclass(frozen=True, slots=True)
class LocalUpload:
    sources: tuple[LocalSource, ...]
    source_naming_view_id: str


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
            "bytes": path.stat().st_size,
            "sha256": _sha256(path),
        }
        for path in paths
    )


def _source_paths(root: Path) -> tuple[Path, ...]:
    paths: list[Path] = []
    for path in root.rglob("*"):
        relative = path.relative_to(root)
        if path.is_symlink():
            raise ValueError(f"upload source is a symbolic link: {relative}")
        if path.is_file():
            paths.append(path)
        elif not path.is_dir():
            raise ValueError(f"upload source is not a regular file: {relative}")
    return tuple(sorted(paths, key=lambda path: path.relative_to(root).as_posix()))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _state_path(root: Path, key: str) -> Path:
    state_root = Path(os.environ.get("XDG_STATE_HOME") or Path.home() / ".local/state")
    directory = state_root / "a-riverhog-cli" / "uploads"
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    digest = hashlib.sha256(canonical_json_bytes([str(root), key])).hexdigest()
    return directory / f"{digest}.json"


def prepare_upload(root: Path, idempotency_key: str) -> LocalUpload:
    """Reuse an exact persisted allocation or publish one before opening a session."""

    root = root.expanduser().resolve()
    if not root.is_dir():
        raise ValueError("collection source must be a directory")
    if not idempotency_key or idempotency_key != idempotency_key.strip():
        raise ValueError("upload idempotency key must be nonempty and canonical")
    current = [
        {
            "relative_parts": item["relative_components"],
            "bytes": item["bytes"],
            "sha256": item["sha256"],
        }
        for item in preview_upload(root)
    ]
    destination = _state_path(root, idempotency_key)
    if not destination.exists():
        document = {
            "format": _FORMAT,
            "root": str(root),
            "idempotency_key": idempotency_key,
            "source_naming_view_id": "urn:uuid:" + str(uuid.uuid4()),
            "sources": [{**item, "artifact_id": secrets.token_hex(32)} for item in current],
        }
        temporary: str | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb", dir=destination.parent, prefix=".upload-", delete=False
            ) as handle:
                temporary = handle.name
                handle.write(canonical_json_bytes(document))
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
    loaded = json.loads(destination.read_bytes())
    if (
        not isinstance(loaded, dict)
        or loaded.get("format") != _FORMAT
        or loaded.get("root") != str(root)
        or loaded.get("idempotency_key") != idempotency_key
        or not isinstance(loaded.get("sources"), list)
        or [
            {key: item.get(key) for key in ("relative_parts", "bytes", "sha256")}
            for item in loaded["sources"]
        ]
        != current
    ):
        raise ValueError("saved upload identity differs from the current local sources")
    view_id = loaded.get("source_naming_view_id")
    if not isinstance(view_id, str) or not view_id.startswith("urn:uuid:"):
        raise ValueError("saved upload source view identity is invalid")
    ids = [item.get("artifact_id") for item in loaded["sources"]]
    if len(ids) != len(set(ids)) or any(
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
        for value in ids
    ):
        raise ValueError("saved upload artifact identities are invalid")
    sources = tuple(
        LocalSource(
            path=root.joinpath(*item["relative_parts"]),
            relative_parts=tuple(item["relative_parts"]),
            artifact_id=item["artifact_id"],
            bytes=item["bytes"],
            sha256=item["sha256"],
        )
        for item in loaded["sources"]
    )
    return LocalUpload(sources=sources, source_naming_view_id=view_id)
