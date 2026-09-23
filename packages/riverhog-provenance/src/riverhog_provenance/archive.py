"""Portable file records that compose identity with provenance for segmented archives."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True, slots=True)
class ArchiveFileProvenanceRecord:
    path: str
    bytes: int
    sha256: str
    status: Literal["captured", "omitted"]
    journal_id: str | None = None
    current_state_id: str | None = None
    omission_reason: str | None = None


__all__ = ["ArchiveFileProvenanceRecord"]
