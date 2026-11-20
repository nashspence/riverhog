"""Small-fixture snapshots of scoped application source replay."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, cast

from a_riverhog_cli import directory_upload
from a_riverhog_cli.upload_sources import LocalSource, LocalUpload, SourceReaderFactory
from riverhog_client import PreparedPrimaryInventory, PrimaryStateError, ProducerArtifactIdentity


def read_upload_sources(upload: LocalUpload) -> tuple[LocalSource, ...]:
    with upload.open_sources() as sources:
        return tuple(sources)


def verified_upload_sources(root: Path, key: str) -> tuple[Path, list[dict[str, object]]]:
    """Snapshot small source fixtures independently of the runtime ordering scope."""
    root = directory_upload._validated_upload_root(root, key)
    return root, [
        {
            "relative_parts": row["relative_components"],
            "bytes": row["bytes"],
            "sha256": row["sha256"],
        }
        for row in directory_upload.preview_upload(root)
    ]


@dataclass(frozen=True)
class _CodecInventory:
    """Small codec fixtures isolate storage; backend qualification uses actual stores."""

    members: tuple[ProducerArtifactIdentity, ...]

    @property
    def count(self) -> int:
        return len(self.members)

    @contextmanager
    def open(self) -> Iterator[Iterator[ProducerArtifactIdentity]]:
        yield iter(self.members)


def _codec_inventory(members: Iterable[ProducerArtifactIdentity]) -> PreparedPrimaryInventory:
    rows = tuple(members)
    if len({member.artifact_id for member in rows}) != len(rows):
        raise PrimaryStateError("owner inventory contains duplicate artifact IDs")
    return cast(
        PreparedPrimaryInventory,
        _CodecInventory(tuple(sorted(rows, key=lambda member: member.artifact_id))),
    )


def load_upload_identity_fixture(
    stream: BinaryIO,
    root: Path,
    key: str,
    current,
    *,
    open_original: SourceReaderFactory,
) -> LocalUpload:
    validated = directory_upload._validate_upload_identity(
        directory_upload.UploadIdentityRecords(stream), root, key, current
    )
    return directory_upload._load_upload_identity(
        stream,
        root,
        validated,
        open_original=open_original,
        prepare_inventory=_codec_inventory,
    )


def prepare_verified_upload_fixture(
    root: Path, key: str, current, *, initialize: bool = True
) -> LocalUpload:
    with directory_upload._open_validated_upload_identity(
        root, key, current, initialize=initialize
    ) as (stream, validated, open_original):
        return directory_upload._load_upload_identity(
            stream,
            root,
            validated,
            open_original=open_original,
            prepare_inventory=_codec_inventory,
        )


def prepare_upload_fixture(root: Path, key: str, *, ordering_bytes: int) -> LocalUpload:
    with directory_upload._verified_upload_sources(root, key, byte_limit=ordering_bytes) as (
        root,
        current,
    ):
        return prepare_verified_upload_fixture(root, key, current)
