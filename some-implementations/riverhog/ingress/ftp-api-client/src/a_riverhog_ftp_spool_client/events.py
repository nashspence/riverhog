"""FTP spool owned native lifecycle-event member and cursor-feed contract."""

from __future__ import annotations

from typing import Annotated, Any, Literal, Self

from lifecycle_events import LifecycleEvent
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, model_validator

Sha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
SourceId = Annotated[
    str,
    Field(pattern=r"^[a-z0-9](?:[a-z0-9._-]{0,118}[a-z0-9])?$"),
]

CLAIM_REGISTERED = "io.riverhog.ftp_spool.claim.registered"
CUSTODY_READY = "io.riverhog.ftp_spool.claim.custody_ready"
ATTEMPT_FAILED = "io.riverhog.ftp_spool.claim.attempt_failed"
CLAIM_PUBLISHED = "io.riverhog.ftp_spool.claim.published"
FTP_EVENT_TYPES = frozenset({CLAIM_REGISTERED, CUSTODY_READY, ATTEMPT_FAILED, CLAIM_PUBLISHED})


class FtpEventPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    source_id: SourceId
    claim_id: Sha256
    source_event_id: str = Field(min_length=1, max_length=300)


class ClaimRegisteredPayload(FtpEventPayload):
    file_count: int = Field(ge=1)
    bytes: int = Field(ge=0)


class CustodyReadyPayload(FtpEventPayload):
    file_count: int = Field(ge=1)
    bytes: int = Field(ge=0)


class AttemptFailedPayload(FtpEventPayload):
    error_type: str = Field(min_length=1, max_length=160)


class ClaimPublishedPayload(FtpEventPayload):
    collection_id: str = Field(pattern=r"^[1-9][0-9]*$")
    archive_root_sha256: Sha256
    content_identity: Sha256


class FtpEvent(LifecycleEvent):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    subject: Sha256
    payload: Any

    @model_validator(mode="after")
    def exact_claim_identity(self) -> Self:
        if self.subject != self.payload.claim_id:
            raise ValueError("FTP event subject differs from its claim identity")
        return self


class ClaimRegisteredEvent(FtpEvent):
    type: Literal["io.riverhog.ftp_spool.claim.registered"]
    payload: ClaimRegisteredPayload


class CustodyReadyEvent(FtpEvent):
    type: Literal["io.riverhog.ftp_spool.claim.custody_ready"]
    payload: CustodyReadyPayload


class AttemptFailedEvent(FtpEvent):
    type: Literal["io.riverhog.ftp_spool.claim.attempt_failed"]
    payload: AttemptFailedPayload


class ClaimPublishedEvent(FtpEvent):
    type: Literal["io.riverhog.ftp_spool.claim.published"]
    payload: ClaimPublishedPayload


type FtpLifecycleEvent = Annotated[
    ClaimRegisteredEvent | CustodyReadyEvent | AttemptFailedEvent | ClaimPublishedEvent,
    Field(discriminator="type"),
]

_EVENT_ADAPTER: TypeAdapter[FtpLifecycleEvent] = TypeAdapter(FtpLifecycleEvent)


def validate_ftp_event(value: LifecycleEvent | dict[str, Any]) -> FtpLifecycleEvent:
    payload = (
        value.model_dump(mode="json", exclude_none=True)
        if isinstance(value, LifecycleEvent)
        else value
    )
    return _EVENT_ADAPTER.validate_python(payload)


class FtpEventPage(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    events: list[FtpLifecycleEvent] = Field(max_length=100)
    next_cursor: str = Field(min_length=1, max_length=220)
    has_more: bool

    def require_progress_after(self, cursor: str) -> None:
        if self.events and self.next_cursor == cursor:
            raise ValueError("nonempty FTP event page did not advance its cursor")


__all__ = [
    "CLAIM_PUBLISHED",
    "CLAIM_REGISTERED",
    "CUSTODY_READY",
    "FTP_EVENT_TYPES",
    "ATTEMPT_FAILED",
    "AttemptFailedPayload",
    "ClaimPublishedPayload",
    "ClaimRegisteredPayload",
    "CustodyReadyPayload",
    "FtpEventPage",
    "FtpLifecycleEvent",
    "validate_ftp_event",
]
