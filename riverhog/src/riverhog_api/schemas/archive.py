from __future__ import annotations

from typing import Annotated, Literal, Self

from http_api_contracts import BrowsePageToken
from lifecycle_events import EventContext
from pydantic import ConfigDict, Field, RootModel, model_validator
from riverhog_application_access import ApplicationKeyId, ApplicationName
from riverhog_protocol import (
    ArchiveCopyJobSort,
    ArchiveCopyJobState,
    ArchiveCopyStoreSelectionDocument,
    ArchiveStoreName,
    CollectionId,
    SortOrder,
)
from time_formats import CanonicalUtcTimestamp

from riverhog_api.schemas.common import RiverhogModel

Sha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
ObjectPath = Annotated[str, Field(min_length=1)]


class PendingArchiveRootPublicationOut(RiverhogModel):
    object_path: ObjectPath | None = None
    sha256: Sha256 | None = None
    state: Literal["pending"] = "pending"

    @model_validator(mode="after")
    def validate_manifest_pair(self) -> Self:
        if (self.object_path is None) != (self.sha256 is None):
            raise ValueError("archive-root manifest path and identity must appear together")
        return self


class UploadedArchiveRootPublicationOut(RiverhogModel):
    object_path: ObjectPath
    sha256: Sha256
    state: Literal["uploaded"]


class FailedArchiveRootPublicationOut(RiverhogModel):
    object_path: ObjectPath | None = None
    sha256: Sha256 | None = None
    state: Literal["failed"]

    @model_validator(mode="after")
    def validate_object_pairs(self) -> Self:
        if (self.object_path is None) != (self.sha256 is None):
            raise ValueError("archive-root manifest path and identity must appear together")
        return self


type _ArchiveRootPublication = Annotated[
    PendingArchiveRootPublicationOut
    | UploadedArchiveRootPublicationOut
    | FailedArchiveRootPublicationOut,
    Field(discriminator="state"),
]


class ArchiveRootPublicationOut(RootModel[_ArchiveRootPublication]):
    pass


class _ArchiveCopyBase(RiverhogModel):
    store: ArchiveStoreName
    storage_prefix: ObjectPath | None
    object_count: int = Field(ge=0)
    stored_bytes: int = Field(ge=0)
    last_uploaded_at: CanonicalUtcTimestamp | None
    last_verified_at: CanonicalUtcTimestamp | None
    archive_root: _ArchiveRootPublication


class IncompleteArchiveCopyOut(_ArchiveCopyBase):
    state: Literal["pending", "uploading", "retrying"]
    failure: None
    archive_root: PendingArchiveRootPublicationOut | UploadedArchiveRootPublicationOut


class UploadedArchiveCopyOut(_ArchiveCopyBase):
    state: Literal["uploaded"]
    storage_prefix: ObjectPath
    object_count: int = Field(ge=1)
    stored_bytes: int = Field(ge=1)
    last_uploaded_at: CanonicalUtcTimestamp
    last_verified_at: CanonicalUtcTimestamp
    failure: None
    archive_root: UploadedArchiveRootPublicationOut


class FailedArchiveCopyOut(_ArchiveCopyBase):
    state: Literal["failed"]
    failure: str = Field(min_length=1)
    archive_root: FailedArchiveRootPublicationOut


class ArchiveCopyOut(
    RootModel[
        Annotated[
            IncompleteArchiveCopyOut | UploadedArchiveCopyOut | FailedArchiveCopyOut,
            Field(discriminator="state"),
        ]
    ]
):
    pass


class CreateArchiveCopyJobRequest(ArchiveCopyStoreSelectionDocument):
    collection_id: CollectionId
    use_cache: bool | None = None
    event_context: EventContext | None = None


class ArchiveCopyJobOut(RiverhogModel):
    model_config = ConfigDict(
        json_schema_extra={
            "allOf": [
                {
                    "if": {"properties": {"state": {"enum": ["completed", "failed", "canceled"]}}},
                    "then": {"properties": {"finished_at": {"type": "string"}}},
                    "else": {"properties": {"finished_at": {"type": "null"}}},
                },
                {
                    "if": {"properties": {"state": {"const": "failed"}}},
                    "then": {"properties": {"failure": {"type": "string", "minLength": 1}}},
                    "else": {"properties": {"failure": {"type": "null"}}},
                },
            ]
        }
    )

    collection_id: CollectionId
    source_store: ArchiveStoreName
    destination_store: ArchiveStoreName
    use_cache: bool
    initiated_by_app: ApplicationName
    initiated_by_key_id: ApplicationKeyId | None
    state: ArchiveCopyJobState
    requested_at: CanonicalUtcTimestamp
    ready_at: CanonicalUtcTimestamp | None
    expires_at: CanonicalUtcTimestamp | None
    finished_at: CanonicalUtcTimestamp | None
    failure: str | None = Field(min_length=1)

    @model_validator(mode="after")
    def validate_terminal_evidence(self) -> Self:
        finished = self.state in {"completed", "failed", "canceled"}
        if (self.finished_at is not None) != finished:
            raise ValueError("archive-copy job finished_at must match terminal state")
        if (self.failure is not None) != (self.state == "failed"):
            raise ValueError("archive-copy failure evidence must match failed state")
        return self


class UploadCopyIntentOut(RiverhogModel):
    destination_store: ArchiveStoreName
    state: Literal["accepted", "pending", "handed_off", "failed", "canceled"]
    failure_code: str | None
    job_created: bool | None
    job_state: ArchiveCopyJobState | None


class UploadCopyIntentsOut(RiverhogModel):
    collection_id: CollectionId
    archive_store: ArchiveStoreName
    use_cache: bool
    copy_to: list[ArchiveStoreName]
    intents: list[UploadCopyIntentOut]


class ArchiveCopyJobListFiltersOut(RiverhogModel):
    state: ArchiveCopyJobState | None = None


class ArchiveCopyJobListOut(RiverhogModel):
    page_size: int = Field(ge=1, le=100)
    next_page_token: BrowsePageToken | None
    sort: ArchiveCopyJobSort
    order: SortOrder
    query: str | None
    filters: ArchiveCopyJobListFiltersOut
    jobs: list[ArchiveCopyJobOut]


class ArchiveCopyRetirementRequest(RiverhogModel):
    collection_id: CollectionId
    store: ArchiveStoreName


class RetireArchiveCopyRequest(ArchiveCopyRetirementRequest):
    challenge: str


class ArchiveCopyRetirementTargetOut(RiverhogModel):
    store: ArchiveStoreName
    last_verified_at: CanonicalUtcTimestamp
    remote_storage_bytes: int
    object_count: int


class ArchiveCopyRetirementRetainedOut(RiverhogModel):
    store: ArchiveStoreName
    last_verified_at: CanonicalUtcTimestamp
    remote_storage_bytes: int


class ArchiveCopyRetirementPlanOut(RiverhogModel):
    model_config = ConfigDict(
        json_schema_extra={
            "oneOf": [
                {
                    "properties": {
                        "status": {"const": "blocked"},
                        "challenge": {"type": "null"},
                        "blockers": {"minItems": 1},
                    }
                },
                {
                    "properties": {
                        "status": {"enum": ["ready", "retiring"]},
                        "challenge": {"type": "string", "minLength": 1},
                        "blockers": {"maxItems": 0},
                    }
                },
            ]
        }
    )

    status: Literal["ready", "blocked", "retiring"]
    collection_id: CollectionId
    store: ArchiveStoreName
    warning: str
    expires_at: CanonicalUtcTimestamp
    challenge: str | None
    target_copy: ArchiveCopyRetirementTargetOut
    retained_copies: list[ArchiveCopyRetirementRetainedOut]
    terminal_retrieval_job_count: int
    blockers: list[str]
    verification_note: str
    billing_note: str

    @model_validator(mode="after")
    def validate_plan_state(self) -> Self:
        if self.status == "blocked":
            if self.challenge is not None or not self.blockers:
                raise ValueError(
                    "blocked archive-copy retirement requires blockers and no challenge"
                )
        elif not self.challenge or self.blockers:
            raise ValueError("ready archive-copy retirement requires a challenge and no blockers")
        return self


class ArchiveCopyRetirementResultOut(RiverhogModel):
    status: Literal["deleted", "already_absent"]
    collection_id: CollectionId
    store: ArchiveStoreName
    remote_storage_bytes: int
    verified_store: ArchiveStoreName | None
