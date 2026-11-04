"""Operator resource grants for retained collection-upload write state."""

from __future__ import annotations

from dataclasses import dataclass

from riverhog_archive_contracts import ARCHIVE_VOLUME_DOCUMENT_BYTES_MAX


@dataclass(frozen=True, slots=True)
class ArchiveWriteGrants:
    """Calls/control bytes per construction, preparation per object, terminal per request."""

    calls: int = 64
    control_bytes: int = 1024 * 1024
    preparation_bytes: int = 16 * 1024
    terminal_bytes: int = 64 * 1024
    result_bytes: int = 64 * 1024
    metadata_preparation_bytes: int = 2 * ARCHIVE_VOLUME_DOCUMENT_BYTES_MAX

    def __post_init__(self) -> None:
        for name in (
            "calls",
            "control_bytes",
            "preparation_bytes",
            "terminal_bytes",
            "result_bytes",
            "metadata_preparation_bytes",
        ):
            value = getattr(self, name)
            if type(value) is not int or value < 1:
                raise ValueError(f"archive write {name} must be a positive integer")
