"""Small-fixture snapshots of scoped application source replay."""

from __future__ import annotations

from a_riverhog_cli.upload_sources import LocalSource, LocalUpload


def read_upload_sources(upload: LocalUpload) -> tuple[LocalSource, ...]:
    with upload.open_sources() as sources:
        return tuple(sources)
