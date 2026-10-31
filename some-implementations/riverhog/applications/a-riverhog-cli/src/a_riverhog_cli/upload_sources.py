"""Scoped, verified replay of retained application source descriptors."""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Generator, Iterator
from contextlib import AbstractContextManager, closing, contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, BinaryIO, cast

from riverhog_client import ProducerFile
from riverhog_protocol.artifact_identity import ArtifactId
from riverhog_protocol.provenance_transport import MaterializationHintDocument

from a_riverhog_cli.upload_identity_records import UploadIdentityRecords

SourceReaderFactory = Callable[[], AbstractContextManager[BinaryIO]]


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
    source_count: int
    source_naming_view_id: str
    _root: Path
    _commitment: tuple[int, str]
    _reader: SourceReaderFactory = field(compare=False, repr=False)

    @contextmanager
    def open_sources(self) -> Iterator[Iterator[LocalSource]]:
        """Replay under a fresh original reader; closing never reclaims authority.

        Every traversal verifies the entire original commitment before exposing
        any descriptor. Separate traversals own separate readers. The caller
        keeps this context around its batches and closes it on interruption.
        """
        with self._reader() as stream:
            self._verify(stream)
            records = UploadIdentityRecords(stream)
            header = records.header()
            if (
                header["root"] != str(self._root)
                or header["source_naming_view_id"] != self.source_naming_view_id
            ):
                raise ValueError("saved upload identity changed during replay")
            with closing(self._sources(records)) as sources:
                yield sources

    def _verify(self, stream: BinaryIO) -> None:
        size = 0
        digest = hashlib.sha256()
        expected_bytes, expected_hash = self._commitment
        while chunk := stream.read(min(65536, expected_bytes - size + 1)):
            size += len(chunk)
            if size > expected_bytes:
                raise ValueError("saved upload identity changed during replay")
            digest.update(chunk)
        if (size, digest.hexdigest()) != (expected_bytes, expected_hash):
            raise ValueError("saved upload identity changed during replay")
        stream.seek(0)

    def _sources(self, records: UploadIdentityRecords) -> Generator[LocalSource, None, None]:
        count = 0
        for item in records.records():
            if count >= self.source_count:
                raise ValueError("saved upload identity changed during replay")
            count += 1
            yield LocalSource(
                root=self._root,
                relative_parts=tuple(cast(list[str], item["relative_parts"])),
                artifact_id=cast(str, item["artifact_id"]),
                bytes=cast(int, item["bytes"]),
                sha256=cast(str, item["sha256"]),
            )
        if count != self.source_count or records.commitment != self._commitment:
            raise ValueError("saved upload identity changed during replay")
