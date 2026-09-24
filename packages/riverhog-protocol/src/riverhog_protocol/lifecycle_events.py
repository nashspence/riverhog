"""Typed Riverhog native lifecycle-event vocabulary."""

from __future__ import annotations

from typing import Annotated, Any, Literal, Self

from lifecycle_events.models import EventContext, LifecycleEvent, normalize_event_context
from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    TypeAdapter,
    field_validator,
    model_validator,
)
from time_formats import CanonicalUtcTimestamp

from riverhog_protocol.list_controls import ArchiveCopyJobState
from riverhog_protocol.paths import CollectionId, normalize_collection_id
from riverhog_protocol.principal_ids import PrincipalId
from riverhog_protocol.storage_names import ArchiveStoreName

RIVERHOG_EVENT_TYPE_PREFIX = "io.riverhog.riverhog."
COLLECTION_FINALIZED = RIVERHOG_EVENT_TYPE_PREFIX + "collection.finalized"
COLLECTION_DELETED = RIVERHOG_EVENT_TYPE_PREFIX + "collection.deleted"
ARCHIVE_COPY_JOB_REQUESTED = RIVERHOG_EVENT_TYPE_PREFIX + "archive_copy_job.requested"
ARCHIVE_COPY_JOB_COMPLETED = RIVERHOG_EVENT_TYPE_PREFIX + "archive_copy_job.completed"
ARCHIVE_COPY_JOB_FAILED = RIVERHOG_EVENT_TYPE_PREFIX + "archive_copy_job.failed"
ARCHIVE_COPY_JOB_CANCELED = RIVERHOG_EVENT_TYPE_PREFIX + "archive_copy_job.canceled"
RETRIEVAL_REQUESTED = RIVERHOG_EVENT_TYPE_PREFIX + "retrieval.requested"
RETRIEVAL_READY = RIVERHOG_EVENT_TYPE_PREFIX + "retrieval.ready"
RETRIEVAL_RENEWED = RIVERHOG_EVENT_TYPE_PREFIX + "retrieval.renewed"
RETRIEVAL_COMPLETED = RIVERHOG_EVENT_TYPE_PREFIX + "retrieval.completed"
RETRIEVAL_CANCELED = RIVERHOG_EVENT_TYPE_PREFIX + "retrieval.canceled"
RETRIEVAL_EXPIRED = RIVERHOG_EVENT_TYPE_PREFIX + "retrieval.expired"
RETRIEVAL_ISSUE = RIVERHOG_EVENT_TYPE_PREFIX + "retrieval.issue"
RETRIEVAL_FAILED = RIVERHOG_EVENT_TYPE_PREFIX + "retrieval.failed"

RIVERHOG_EVENT_TYPES = frozenset(
    {
        COLLECTION_FINALIZED,
        COLLECTION_DELETED,
        ARCHIVE_COPY_JOB_REQUESTED,
        ARCHIVE_COPY_JOB_COMPLETED,
        ARCHIVE_COPY_JOB_FAILED,
        ARCHIVE_COPY_JOB_CANCELED,
        RETRIEVAL_REQUESTED,
        RETRIEVAL_READY,
        RETRIEVAL_RENEWED,
        RETRIEVAL_COMPLETED,
        RETRIEVAL_CANCELED,
        RETRIEVAL_EXPIRED,
        RETRIEVAL_ISSUE,
        RETRIEVAL_FAILED,
    }
)
COLLECTION_WAKE_EVENT_TYPES = frozenset({COLLECTION_FINALIZED})
MAX_LIFECYCLE_EVENT_SEQUENCE = 2**63 - 1


def validate_lifecycle_event_cursor(value: str) -> str:
    if not value.isascii() or not value.isdecimal() or str(int(value)) != value:
        raise ValueError("lifecycle-event cursor must be canonical decimal")
    if int(value) > MAX_LIFECYCLE_EVENT_SEQUENCE:
        raise ValueError("lifecycle-event cursor exceeds the v1 sequence domain")
    return value


type LifecycleEventCursor = Annotated[
    str,
    StringConstraints(min_length=1, max_length=19, pattern=r"^(?:0|[1-9][0-9]*)$"),
    AfterValidator(validate_lifecycle_event_cursor),
]


class RiverhogEventModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    def __getitem__(self, key: str) -> Any:
        return self.model_dump(mode="json", exclude_none=True)[key]

    def get(self, key: str, default: Any = None) -> Any:
        return self.model_dump(mode="json", exclude_none=True).get(key, default)


class RiverhogActor(RiverhogEventModel):
    principal_id: PrincipalId = Field(max_length=160)
    key_id: str | None = Field(default=None, min_length=1, max_length=300)


class RiverhogEventData(RiverhogEventModel):
    actor: RiverhogActor
    initiator: RiverhogActor
    context: EventContext | None = None

    @field_validator("context")
    @classmethod
    def validate_context(cls, value: dict[str, Any] | None) -> dict[str, Any] | None:
        return normalize_event_context(value)


class CollectionEventData(RiverhogEventData):
    collection_id: CollectionId
    collection_created_at: CanonicalUtcTimestamp

    @model_validator(mode="after")
    def validate_collection(self) -> Self:
        normalize_collection_id(self.collection_id)
        return self


class CollectionFinalizedData(CollectionEventData):
    files_total: int = Field(ge=0)
    bytes_total: int = Field(ge=0)
    archive_root_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class CollectionDeletedData(CollectionEventData):
    files: int = Field(ge=0)
    bytes: int = Field(ge=0)
    remote_storage_bytes: int = Field(ge=0)


class ArchiveCopyJobEventData(CollectionEventData):
    source_store: ArchiveStoreName
    destination_store: ArchiveStoreName
    state: ArchiveCopyJobState


class ArchiveCopyJobRequestedData(ArchiveCopyJobEventData):
    state: Literal["requested"]


class ArchiveCopyJobCompletedData(ArchiveCopyJobEventData):
    state: Literal["completed"]


class ArchiveCopyJobFailedData(ArchiveCopyJobEventData):
    state: Literal["failed"]
    error: str = Field(min_length=1, max_length=16384)


class ArchiveCopyJobCanceledData(ArchiveCopyJobEventData):
    state: Literal["canceled"]


RetrievalState = Literal["requested", "ready", "completed", "canceled", "expired", "failed"]


class RetrievalEventData(RiverhogEventData):
    retrieval_id: str = Field(min_length=1, max_length=300)
    collection_ids: list[CollectionId] = Field(min_length=1)
    state: RetrievalState
    collection_id: CollectionId | None = None
    collection_created_at: CanonicalUtcTimestamp | None = None

    @model_validator(mode="after")
    def validate_collections(self) -> Self:
        ids = tuple(self.collection_ids)
        if ids != tuple(sorted(set(ids))):
            raise ValueError("retrieval event collection identities must be canonical")
        if len(ids) == 1 and self.collection_id != ids[0]:
            raise ValueError("single-collection retrieval event requires its collection identity")
        if len(ids) != 1 and self.collection_id is not None:
            raise ValueError("multi-collection retrieval event cannot have a singular identity")
        if self.collection_created_at is not None and self.collection_id is None:
            raise ValueError("retrieval collection projection has no singular collection")
        return self


class RetrievalRequestedData(RetrievalEventData):
    state: Literal["requested", "ready"]
    files: int = Field(ge=1)
    objects: int = Field(ge=1)
    restore_required: bool


class RetrievalReadyData(RetrievalEventData):
    state: Literal["ready"]
    expires_at: CanonicalUtcTimestamp


class RetrievalRenewedData(RetrievalReadyData):
    pass


class RetrievalCompletedData(RetrievalEventData):
    state: Literal["completed"]


class RetrievalCanceledData(RetrievalEventData):
    state: Literal["canceled"]
    reason: str | None = Field(default=None, min_length=1, max_length=1000)


class RetrievalExpiredData(RetrievalEventData):
    state: Literal["expired"]


class RetrievalIssueData(RetrievalEventData):
    state: Literal["requested"]
    error: str = Field(min_length=1, max_length=16384)


class RetrievalFailedData(RetrievalEventData):
    state: Literal["failed"]
    error: str = Field(min_length=1, max_length=16384)


class RiverhogEvent(LifecycleEvent):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    payload: Any

    @model_validator(mode="after")
    def validate_subject_identity(self) -> Self:
        if isinstance(self.payload, CollectionEventData):
            if self.subject != str(self.payload.collection_id):
                raise ValueError("collection event subject differs from its collection identity")
        elif isinstance(self.payload, RetrievalEventData):
            if self.subject != self.payload.retrieval_id:
                raise ValueError("retrieval event subject differs from its retrieval identity")
        return self


class CollectionFinalizedEvent(RiverhogEvent):
    type: Literal["io.riverhog.riverhog.collection.finalized"]
    payload: CollectionFinalizedData


class CollectionDeletedEvent(RiverhogEvent):
    type: Literal["io.riverhog.riverhog.collection.deleted"]
    payload: CollectionDeletedData


class ArchiveCopyJobRequestedEvent(RiverhogEvent):
    type: Literal["io.riverhog.riverhog.archive_copy_job.requested"]
    payload: ArchiveCopyJobRequestedData


class ArchiveCopyJobCompletedEvent(RiverhogEvent):
    type: Literal["io.riverhog.riverhog.archive_copy_job.completed"]
    payload: ArchiveCopyJobCompletedData


class ArchiveCopyJobFailedEvent(RiverhogEvent):
    type: Literal["io.riverhog.riverhog.archive_copy_job.failed"]
    payload: ArchiveCopyJobFailedData


class ArchiveCopyJobCanceledEvent(RiverhogEvent):
    type: Literal["io.riverhog.riverhog.archive_copy_job.canceled"]
    payload: ArchiveCopyJobCanceledData


class RetrievalRequestedEvent(RiverhogEvent):
    type: Literal["io.riverhog.riverhog.retrieval.requested"]
    payload: RetrievalRequestedData


class RetrievalReadyEvent(RiverhogEvent):
    type: Literal["io.riverhog.riverhog.retrieval.ready"]
    payload: RetrievalReadyData


class RetrievalRenewedEvent(RiverhogEvent):
    type: Literal["io.riverhog.riverhog.retrieval.renewed"]
    payload: RetrievalRenewedData


class RetrievalCompletedEvent(RiverhogEvent):
    type: Literal["io.riverhog.riverhog.retrieval.completed"]
    payload: RetrievalCompletedData


class RetrievalCanceledEvent(RiverhogEvent):
    type: Literal["io.riverhog.riverhog.retrieval.canceled"]
    payload: RetrievalCanceledData


class RetrievalExpiredEvent(RiverhogEvent):
    type: Literal["io.riverhog.riverhog.retrieval.expired"]
    payload: RetrievalExpiredData


class RetrievalIssueEvent(RiverhogEvent):
    type: Literal["io.riverhog.riverhog.retrieval.issue"]
    payload: RetrievalIssueData


class RetrievalFailedEvent(RiverhogEvent):
    type: Literal["io.riverhog.riverhog.retrieval.failed"]
    payload: RetrievalFailedData


type RiverhogLifecycleEvent = Annotated[
    CollectionFinalizedEvent
    | CollectionDeletedEvent
    | ArchiveCopyJobRequestedEvent
    | ArchiveCopyJobCompletedEvent
    | ArchiveCopyJobFailedEvent
    | ArchiveCopyJobCanceledEvent
    | RetrievalRequestedEvent
    | RetrievalReadyEvent
    | RetrievalRenewedEvent
    | RetrievalCompletedEvent
    | RetrievalCanceledEvent
    | RetrievalExpiredEvent
    | RetrievalIssueEvent
    | RetrievalFailedEvent,
    Field(discriminator="type"),
]

_EVENT_ADAPTER: TypeAdapter[RiverhogLifecycleEvent] = TypeAdapter(RiverhogLifecycleEvent)


class RiverhogEventPage(RiverhogEventModel):
    events: list[RiverhogLifecycleEvent]
    next_cursor: LifecycleEventCursor
    has_more: bool

    def require_progress_after(self, cursor: str) -> None:
        if self.events and self.next_cursor == cursor:
            raise ValueError("nonempty lifecycle-event page did not advance its cursor")


def normalize_riverhog_event_type(value: str) -> str:
    normalized = (
        value
        if value.startswith(RIVERHOG_EVENT_TYPE_PREFIX)
        else (RIVERHOG_EVENT_TYPE_PREFIX + value)
    )
    if normalized not in RIVERHOG_EVENT_TYPES:
        raise ValueError(f"unknown Riverhog lifecycle event type: {value}")
    return normalized


def validate_riverhog_event(value: LifecycleEvent | dict[str, Any]) -> RiverhogLifecycleEvent:
    payload = (
        value.model_dump(mode="json", exclude_none=True)
        if isinstance(value, LifecycleEvent)
        else value
    )
    return _EVENT_ADAPTER.validate_python(payload)


def collection_id_for_event(value: LifecycleEvent | dict[str, Any]) -> int:
    event = validate_riverhog_event(value)
    if event.type not in COLLECTION_WAKE_EVENT_TYPES:
        raise ValueError("Riverhog event is not a collection wake event")
    if not isinstance(event.payload, CollectionEventData):
        raise ValueError("Riverhog collection wake event has invalid data")
    return event.payload.collection_id


__all__ = [
    "ARCHIVE_COPY_JOB_CANCELED",
    "ARCHIVE_COPY_JOB_COMPLETED",
    "ARCHIVE_COPY_JOB_FAILED",
    "ARCHIVE_COPY_JOB_REQUESTED",
    "COLLECTION_DELETED",
    "COLLECTION_FINALIZED",
    "COLLECTION_WAKE_EVENT_TYPES",
    "RETRIEVAL_CANCELED",
    "RETRIEVAL_COMPLETED",
    "RETRIEVAL_EXPIRED",
    "RETRIEVAL_FAILED",
    "RETRIEVAL_ISSUE",
    "RETRIEVAL_READY",
    "RETRIEVAL_RENEWED",
    "RETRIEVAL_REQUESTED",
    "RIVERHOG_EVENT_TYPES",
    "RIVERHOG_EVENT_TYPE_PREFIX",
    "RiverhogActor",
    "RiverhogEventPage",
    "RiverhogLifecycleEvent",
    "collection_id_for_event",
    "normalize_riverhog_event_type",
    "validate_riverhog_event",
]
