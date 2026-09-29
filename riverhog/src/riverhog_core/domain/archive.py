from __future__ import annotations

from dataclasses import dataclass

from riverhog_core.domain.retrieval_cache import RetrievalCacheReceipt


@dataclass(frozen=True, slots=True)
class ArchiveArtifact:
    artifact_id: str
    bytes: int
    sha256: str


@dataclass(frozen=True, slots=True)
class SealedProvenanceObject:
    object_id: str
    kind: str
    relative_path: str
    plaintext_bytes: int
    plaintext_sha256: str
    stored_bytes: int
    stored_sha256: str
    revision: str | None
    completed_at: str


@dataclass(frozen=True, slots=True)
class PackMemberPlan:
    artifact_id: str
    bytes: int
    sha256: str
    unit: int
    header_offset: int
    data_offset: int
    end_offset: int


@dataclass(frozen=True, slots=True)
class PackPaddingPlan:
    path: str
    header_offset: int
    payload_bytes: int
    end_offset: int


@dataclass(frozen=True, slots=True)
class PackUploadUnitPlan:
    unit: int
    plaintext_start: int
    plaintext_end: int
    sources: tuple[ArchiveArtifact, ...]
    padding: PackPaddingPlan | None = None
    includes_index: bool = False
    includes_end_markers: bool = False

    @property
    def plaintext_bytes(self) -> int:
        return self.plaintext_end - self.plaintext_start

    @property
    def payload_bytes(self) -> int:
        return sum(current.bytes for current in self.sources)

    @property
    def final(self) -> bool:
        return self.includes_end_markers


@dataclass(frozen=True, slots=True)
class PackVolumePlan:
    volume_id: str
    sequence: int
    max_member_bytes: int
    part_plaintext_bytes: int
    members: tuple[PackMemberPlan, ...]
    units: tuple[PackUploadUnitPlan, ...]
    index_bytes: bytes
    index_sha256: str
    plaintext_bytes: int
    plan_sha256: str


@dataclass(frozen=True, slots=True)
class RawVolumePlan:
    volume_id: str
    sequence: int
    artifact_id: str
    artifact_offset: int
    plaintext_bytes: int
    artifact_bytes: int
    artifact_sha256: str


@dataclass(frozen=True, slots=True)
class StoredArchivePart:
    number: int
    plaintext_start: int
    plaintext_bytes: int
    plaintext_sha256: str
    stored_bytes: int
    stored_sha256: str


@dataclass(frozen=True, slots=True)
class SealedPackVolume:
    volume_id: str
    sequence: int
    relative_path: str
    artifacts: int
    source_bytes: int
    plaintext_bytes: int
    age_state_json: str
    index_sha256: str
    plan_sha256: str
    parts: tuple[StoredArchivePart, ...]
    revision: str | None
    completed_at: str
    retrieval_cache: RetrievalCacheReceipt | None = None

    @property
    def stored_bytes(self) -> int:
        return sum(current.stored_bytes for current in self.parts)


@dataclass(frozen=True, slots=True)
class VerifiedRawArtifact:
    artifact_id: str
    bytes: int
    sha256: str
    ordered_volume_sha256: str
    verified_at: str


@dataclass(frozen=True, slots=True)
class SealedRawVolume:
    volume_id: str
    sequence: int
    relative_path: str
    artifact_id: str
    artifact_offset: int
    plaintext_bytes: int
    artifact_bytes: int
    artifact_sha256: str
    age_state_json: str
    parts: tuple[StoredArchivePart, ...]
    revision: str | None
    completed_at: str
    retrieval_cache: RetrievalCacheReceipt | None = None

    @property
    def stored_bytes(self) -> int:
        return sum(current.stored_bytes for current in self.parts)
