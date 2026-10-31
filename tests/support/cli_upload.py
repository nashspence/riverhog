"""Small-fixture snapshots of scoped application source replay."""

from __future__ import annotations

from pathlib import Path

from a_riverhog_cli import directory_upload
from a_riverhog_cli.upload_sources import LocalSource, LocalUpload


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
