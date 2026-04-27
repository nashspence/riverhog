from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath

from arc_core.domain.enums import (
    CopyState,
    FetchState,
    GlacierState,
    ProtectionState,
    VerificationState,
)
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
class GlacierArchiveStatus:
    state: GlacierState = GlacierState.PENDING
    object_path: str | None = None
    stored_bytes: int | None = None
    backend: str | None = None
    storage_class: str | None = None
    last_uploaded_at: str | None = None
    last_verified_at: str | None = None
    failure: str | None = None


@dataclass(frozen=True)
class CollectionCoverageImage:
    id: ImageId
    filename: str
    protection_state: ProtectionState
    physical_copies_required: int
    physical_copies_registered: int
    physical_copies_missing: int
    copies: list[CopySummary]
    glacier: GlacierArchiveStatus


@dataclass(frozen=True)
class CollectionSummary:
    id: CollectionId
    files: int
    bytes: int
    hot_bytes: int
    archived_bytes: int
    protection_state: ProtectionState = ProtectionState.UNPROTECTED
    protected_bytes: int = 0
    image_coverage: list[CollectionCoverageImage] = field(default_factory=list)

    @property
    def pending_bytes(self) -> int:
        return self.bytes - self.archived_bytes


@dataclass(frozen=True)
class ImageSummary:
    id: ImageId
    filename: str
    finalized_at: str
    bytes: int
    fill: float
    files: int
    collections: int
    collection_ids: list[str]
    iso_ready: bool
    protection_state: ProtectionState
    physical_copies_required: int
    physical_copies_registered: int
    physical_copies_missing: int
    glacier: GlacierArchiveStatus


@dataclass(frozen=True)
class CopyHistoryEntry:
    at: str
    event: str
    state: CopyState
    verification_state: VerificationState
    location: str | None


@dataclass(frozen=True)
class CopySummary:
    id: CopyId
    volume_id: str
    label_text: str
    location: str | None
    created_at: str
    state: CopyState = CopyState.REGISTERED
    verification_state: VerificationState = VerificationState.PENDING
    history: tuple[CopyHistoryEntry, ...] = ()


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
    entries_byte_complete: int = 0
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
