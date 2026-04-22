from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePosixPath

from arc_core.domain.enums import FetchState
from arc_core.domain.types import CollectionId, CopyId, FetchId, ImageId, Sha256Hex, TargetStr


@dataclass(frozen=True)
class Target:
    path: PurePosixPath
    is_dir: bool

    @property
    def canonical(self) -> str:
        canonical = str(self.path)
        if self.is_dir:
            canonical += "/"
        return canonical


@dataclass(frozen=True)
class CollectionSummary:
    id: CollectionId
    files: int
    bytes: int
    hot_bytes: int
    archived_bytes: int

    @property
    def pending_bytes(self) -> int:
        return self.bytes - self.archived_bytes


@dataclass(frozen=True)
class ImageSummary:
    id: ImageId
    bytes: int
    fill: float
    files: int
    collections: int
    iso_ready: bool


@dataclass(frozen=True)
class CopySummary:
    id: CopyId
    volume_id: str
    location: str
    created_at: str


@dataclass(frozen=True)
class FetchCopyHint:
    id: CopyId
    volume_id: str
    location: str


@dataclass(frozen=True)
class FetchSummary:
    id: FetchId
    target: TargetStr
    state: FetchState
    files: int
    bytes: int
    copies: list[FetchCopyHint]
    entries_total: int = 0
    entries_pending: int = 0
    entries_partial: int = 0
    entries_uploaded: int = 0
    uploaded_bytes: int = 0
    missing_bytes: int = 0
    upload_state_expires_at: str | None = None


@dataclass(frozen=True)
class PinSummary:
    target: TargetStr
    fetch: FetchSummary


@dataclass(frozen=True)
class FileRef:
    collection_id: CollectionId
    path: str
    bytes: int
    sha256: Sha256Hex
    copies: list[FetchCopyHint]
