"""Provider-neutral object capabilities carried across the adapter boundary.

The models mirror Riverhog's existing object-store ports. They deliberately do
not define a transfer job, journal, archive object, collection, or provider
configuration model.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Iterator
from hashlib import sha256
from typing import Annotated, Literal, Protocol, Self, cast

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    PlainSerializer,
    StringConstraints,
    ValidationInfo,
    WithJsonSchema,
    field_validator,
    model_validator,
)
from riverhog_canonical_json import (
    canonical_json_bytes,
    format_scalar,
    parse_scalar,
    scalar_schema,
)
from time_formats import CanonicalUtcTimestamp

STORAGE_ADAPTER_PROTOCOL: Literal["riverhog-storage-adapter/v1"] = "riverhog-storage-adapter/v1"
ADAPTER_PRIVATE_ASSERTION_PREFIX = "riverhog-adapter-"
_SHA256_PATTERN = r"^[0-9a-f]{64}$"
_STORAGE_INCARNATION_ID_PATTERN = (
    r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)
_SEMANTIC_ID_PATTERN = r"^[a-z0-9](?:[a-z0-9._/-]{0,158}[a-z0-9])?$"
_METADATA_KEY_PATTERN = re.compile(r"^[a-z0-9](?:[a-z0-9._-]{0,126}[a-z0-9])?$")
_MAX_IDENTITY_ASSERTIONS_ITEMS = 64
_MAX_IDENTITY_ASSERTIONS_BYTES = 16 * 1024
MAX_WRITE_SEGMENT_PAGE_ITEMS = 128
MAX_OPEN_WRITE_PAGE_ITEMS = 128
MAX_CLEANUP_INVENTORY_PAGE_ITEMS = 128
_MAX_WRITE_COMPLETION_PRECONDITION_TOKEN_LENGTH = 4000

Sha256 = Annotated[str, StringConstraints(pattern=_SHA256_PATTERN)]
SemanticId = Annotated[str, StringConstraints(pattern=_SEMANTIC_ID_PATTERN)]
StorageIncarnationId = Annotated[str, StringConstraints(pattern=_STORAGE_INCARNATION_ID_PATTERN)]


def validate_storage_incarnation_id(value: str) -> str:
    """Require the exact canonical UUID4 used to identify one storage authority."""

    if not isinstance(value, str) or re.fullmatch(_STORAGE_INCARNATION_ID_PATTERN, value) is None:
        raise ValueError("storage incarnation ID must be a canonical lowercase UUID4")
    return value


ObjectPlacementPolicy = Annotated[
    Literal["archive_default", "immediate_default"],
    Field(
        description=(
            "Select an adapter-configured placement default. This does not establish "
            "archive membership or read readiness."
        )
    ),
]
ReadMode = Literal["immediate", "restore_required"]
StorageAdapterErrorCode = Literal[
    "unauthorized",
    "invalid_request",
    "not_found",
    "method_not_allowed",
    "length_required",
    "request_too_large",
    "insufficient_storage",
    "identity_conflict",
    "traversal_invalidated",
    "invalid_path",
    "invalid_range",
    "read_not_ready",
    "read_expired",
    "integrity_failure",
    "provider_unavailable",
    "internal_failure",
]
RequiredIdentityAssertions = Annotated[
    dict[str, str],
    Field(
        max_length=_MAX_IDENTITY_ASSERTIONS_ITEMS,
        description=(
            "Inert caller-owned facts used only to identify and reconcile an exact stored "
            "object. Adapters canonicalize, persist, return, and compare these assertions; "
            "they must not interpret them as routing, retrieval, retention, credentials, "
            "placement, or provider-control instructions. Adapters may retain additional "
            "adapter-private assertions."
        ),
        json_schema_extra={
            "x-riverhog-extent": {
                "policy": "contract_max",
                "reason": "bounded-object-identity-assertion-envelope",
            },
            "x-riverhog-encoded-bytes-max": _MAX_IDENTITY_ASSERTIONS_BYTES,
        },
    ),
]
BinaryContent = bytes | Iterable[bytes]


def _nonnegative_decimal(value: object, info: ValidationInfo) -> int:
    # Adapter implementations construct these models from native integer counts.
    # JSON transport always uses the lossless decimal string representation.
    if info.mode == "python" and type(value) is int and value >= 0:
        return value
    return parse_scalar("nonnegative", value)


type NonnegativeDecimal = Annotated[
    int,
    BeforeValidator(_nonnegative_decimal),
    PlainSerializer(lambda value: format_scalar("nonnegative", value), return_type=str),
    WithJsonSchema(scalar_schema("nonnegative")),
]


def _positive_decimal(value: object, info: ValidationInfo) -> int:
    result = _nonnegative_decimal(value, info)
    if result == 0:
        raise ValueError("positive decimal string must be greater than zero")
    return result


type PositiveDecimal = Annotated[
    int,
    BeforeValidator(_positive_decimal),
    PlainSerializer(lambda value: format_scalar("nonnegative", value), return_type=str),
    WithJsonSchema({"type": "string", "pattern": r"^[1-9][0-9]*(?![\s\S])"}),
]


def validate_object_path(value: str, *, allow_prefix: bool = False) -> str:
    """Return an unchanged canonical relative POSIX path or raise."""

    if (
        not value
        or value != value.strip()
        or value.startswith("/")
        or "\\" in value
        or "\x00" in value
        or (not allow_prefix and value.endswith("/"))
    ):
        raise ValueError("object path must be an unchanged relative POSIX path")
    candidate = value[:-1] if allow_prefix and value.endswith("/") else value
    if not candidate:
        raise ValueError("object path must not be empty")
    if any(part in {"", ".", ".."} for part in candidate.split("/")):
        raise ValueError("object path contains a forbidden segment")
    return value


def _canonical_identity_assertions(value: dict[str, str]) -> dict[str, str]:
    if len(value) > _MAX_IDENTITY_ASSERTIONS_ITEMS:
        raise ValueError("required identity assertions have too many entries")
    normalized: dict[str, str] = {}
    for raw_key, raw_value in value.items():
        key = str(raw_key).casefold()
        item = str(raw_value)
        if _METADATA_KEY_PATTERN.fullmatch(key) is None:
            raise ValueError("required identity assertions contain an invalid key")
        if key.startswith(ADAPTER_PRIVATE_ASSERTION_PREFIX):
            raise ValueError("required identity assertions use the adapter-private namespace")
        if not item or "\x00" in item:
            raise ValueError("required identity assertion values must be nonempty and NUL-free")
        if key in normalized:
            raise ValueError("required identity assertion keys collide after case folding")
        normalized[key] = item
    encoded = canonical_json_bytes(normalized)
    if len(encoded) > _MAX_IDENTITY_ASSERTIONS_BYTES:
        raise ValueError("required identity assertions exceed their encoded-size bound")
    return dict(sorted(normalized.items()))


class StorageAdapterModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class AdapterDescriptor(StorageAdapterModel):
    protocol: Literal["riverhog-storage-adapter/v1"] = STORAGE_ADAPTER_PROTOCOL
    storage_incarnation_id: StorageIncarnationId
    implementation_id: SemanticId
    implementation_version: str = Field(min_length=1, max_length=120)
    read_mode: ReadMode
    minimum_nonfinal_segment_bytes: PositiveDecimal
    maximum_segment_bytes: PositiveDecimal | None = None
    maximum_segment_count: PositiveDecimal | None = None

    @model_validator(mode="after")
    def validate_segment_limits(self) -> Self:
        if (
            self.maximum_segment_bytes is not None
            and self.minimum_nonfinal_segment_bytes > self.maximum_segment_bytes
        ):
            raise ValueError("minimum non-final segment bytes exceed maximum segment bytes")
        return self


class ObjectLocator(StorageAdapterModel):
    object_path: str = Field(min_length=1, max_length=4096)
    revision: str | None = Field(default=None, min_length=1, max_length=2000)

    @field_validator("object_path")
    @classmethod
    def canonical_path(cls, value: str) -> str:
        return validate_object_path(value)


class WriteSession(StorageAdapterModel):
    object_path: str = Field(min_length=1, max_length=4096)
    expected_bytes: PositiveDecimal = Field(
        description=(
            "Exact immutable-object byte length admitted by this write session. The value "
            "remains fixed until the write becomes terminal."
        ),
    )
    write_token: str = Field(
        min_length=1,
        max_length=4000,
        description=(
            "Opaque adapter-owned persistable continuation handle. For the same configured "
            "adapter it remains replayable across client, transport, Riverhog, and adapter "
            "process restarts until completion, explicit abort, or caller-authorized "
            "incomplete-write reclamation makes the write terminal."
        ),
    )

    @field_validator("object_path")
    @classmethod
    def canonical_path(cls, value: str) -> str:
        return validate_object_path(value)


class WriteStartRequest(StorageAdapterModel):
    """Exact authority for one idempotently established nonterminal write.

    Repeating the same canonical request against the same configured adapter while the
    write remains nonterminal returns the same continuation session. Operational
    credentials used to realize that session remain adapter-private.
    """

    object_path: str = Field(min_length=1, max_length=4096)
    expected_bytes: PositiveDecimal
    content_type: str = Field(min_length=1, max_length=255)
    required_identity_assertions: RequiredIdentityAssertions
    placement_policy: ObjectPlacementPolicy

    @field_validator("object_path")
    @classmethod
    def canonical_path(cls, value: str) -> str:
        return validate_object_path(value)

    @field_validator("required_identity_assertions")
    @classmethod
    def canonical_metadata(cls, value: dict[str, str]) -> dict[str, str]:
        return _canonical_identity_assertions(value)


class OpenWriteObservation(StorageAdapterModel):
    """One observed token, without execution closure or permission to adopt it."""

    write_token: str = Field(min_length=1, max_length=4000)
    begin_request_sha256: Sha256 | None = Field(
        default=None,
        description=(
            "The complete BeginWriteOperation commitment only when the adapter owns "
            "durable evidence of every original request field. An absent commitment "
            "does not establish that this token belongs to a caller's request."
        ),
    )


class OpenWriteListRequest(StorageAdapterModel):
    """Read one bounded live page for an exact key; observation never begins a write."""

    object_path: str = Field(min_length=1, max_length=4096)
    target_incarnation: StorageIncarnationId
    maximum_items: int = Field(
        default=MAX_OPEN_WRITE_PAGE_ITEMS, ge=1, le=MAX_OPEN_WRITE_PAGE_ITEMS
    )
    maximum_response_bytes: PositiveDecimal = Field(default=1024 * 1024)
    continuation: str | None = Field(default=None, min_length=1, max_length=65536)

    @field_validator("object_path")
    @classmethod
    def canonical_path(cls, value: str) -> str:
        return validate_object_path(value)


class OpenWritePage(StorageAdapterModel):
    """A live observation, not a snapshot or a barrier against delayed provider effects.

    scanned_items includes provider prefix candidates rejected by exact-key filtering.
    Exhaustion describes this traversal only; it never proves execution has stopped.
    """

    object_path: str = Field(min_length=1, max_length=4096)
    target_incarnation: StorageIncarnationId
    observations: tuple[OpenWriteObservation, ...] = Field(
        default=(),
        max_length=MAX_OPEN_WRITE_PAGE_ITEMS,
        json_schema_extra={
            "x-riverhog-extent": {
                "policy": "segmented_no_total_max",
                "reason": "bounded-exact-key-open-write-observation",
                "progression": "adapter-live-open-write-continuation",
            }
        },
    )
    scanned_items: NonnegativeDecimal
    continuation: str | None = Field(default=None, min_length=1, max_length=65536)
    exhausted: bool

    @field_validator("object_path")
    @classmethod
    def canonical_path(cls, value: str) -> str:
        return validate_object_path(value)

    @model_validator(mode="after")
    def exact_page(self) -> Self:
        self._require_exact_page()
        return self

    def _require_exact_page(self) -> None:
        if self.exhausted != (self.continuation is None):
            raise ValueError("open-write exhaustion differs from its continuation")
        tokens = [item.write_token for item in self.observations]
        if len(set(tokens)) != len(tokens) or self.scanned_items < len(tokens):
            raise ValueError("open-write observations must be unique within the scanned page")


def validate_open_write_page_response(
    request: OpenWriteListRequest, response: OpenWritePage
) -> None:
    response._require_exact_page()
    if (
        response.object_path != request.object_path
        or response.target_incarnation != request.target_incarnation
    ):
        raise ValueError("open-write observation differs from its exact target")
    if response.scanned_items > request.maximum_items:
        raise ValueError("open-write observation exceeds its candidate count grant")
    if response.continuation is not None and response.continuation == request.continuation:
        raise ValueError("open-write continuation did not advance")
    if (
        len(response.model_dump_json(exclude_none=True).encode("utf-8"))
        > request.maximum_response_bytes
    ):
        raise ValueError("open-write observation exceeds its encoded response grant")


CleanupCandidateKind = Literal["object_revision", "deletion_marker", "open_write"]


class CleanupCandidate(StorageAdapterModel):
    """An exact provider-private address to reconcile before separately authorized disposal.

    A candidate may survive interrupted publication or a lost cleanup reply. Neither
    its presence nor its absence proves closure of any earlier provider invocation.
    """

    object_path: str = Field(min_length=1, max_length=4096)
    kind: CleanupCandidateKind
    token: str = Field(min_length=1, max_length=4000)

    @field_validator("object_path")
    @classmethod
    def canonical_path(cls, value: str) -> str:
        return validate_object_path(value)


class CleanupInventoryRequest(StorageAdapterModel):
    """Observe bounded candidates under one exact namespace without disposing them."""

    object_prefix: str = Field(min_length=1, max_length=4096)
    target_incarnation: StorageIncarnationId
    maximum_items: int = Field(
        default=MAX_CLEANUP_INVENTORY_PAGE_ITEMS, ge=1, le=MAX_CLEANUP_INVENTORY_PAGE_ITEMS
    )
    maximum_response_bytes: PositiveDecimal = Field(default=1024 * 1024)
    continuation: str | None = Field(default=None, min_length=1, max_length=65536)

    @field_validator("object_prefix")
    @classmethod
    def canonical_prefix(cls, value: str) -> str:
        return validate_object_path(value, allow_prefix=True)


class CleanupInventoryPage(StorageAdapterModel):
    """One live inventory page, never quiescence or authority to delete its candidates."""

    object_prefix: str = Field(min_length=1, max_length=4096)
    target_incarnation: StorageIncarnationId
    candidates: tuple[CleanupCandidate, ...] = Field(
        default=(),
        max_length=MAX_CLEANUP_INVENTORY_PAGE_ITEMS,
        json_schema_extra={
            "x-riverhog-extent": {
                "policy": "segmented_no_total_max",
                "reason": "bounded-cleanup-namespace-inventory",
                "progression": "adapter-live-cleanup-inventory-continuation",
            }
        },
    )
    scanned_items: NonnegativeDecimal
    continuation: str | None = Field(default=None, min_length=1, max_length=65536)
    exhausted: bool

    @field_validator("object_prefix")
    @classmethod
    def canonical_prefix(cls, value: str) -> str:
        return validate_object_path(value, allow_prefix=True)

    @model_validator(mode="after")
    def exact_page(self) -> Self:
        self._require_exact_page()
        return self

    def _require_exact_page(self) -> None:
        if self.exhausted != (self.continuation is None):
            raise ValueError("cleanup inventory exhaustion differs from its continuation")
        identities = [(item.object_path, item.kind, item.token) for item in self.candidates]
        if len(set(identities)) != len(identities) or self.scanned_items < len(identities):
            raise ValueError("cleanup candidates must be unique within their scanned page")
        if any(not item.object_path.startswith(self.object_prefix) for item in self.candidates):
            raise ValueError("cleanup candidate is outside its exact namespace")


def validate_cleanup_inventory_response(
    request: CleanupInventoryRequest, response: CleanupInventoryPage
) -> None:
    response._require_exact_page()
    if (
        request.object_prefix != response.object_prefix
        or request.target_incarnation != response.target_incarnation
    ):
        raise ValueError("cleanup inventory differs from its exact target")
    if response.scanned_items > request.maximum_items:
        raise ValueError("cleanup inventory exceeds its candidate count grant")
    if response.continuation is not None and response.continuation == request.continuation:
        raise ValueError("cleanup inventory continuation did not advance")
    if (
        len(response.model_dump_json(exclude_none=True).encode("utf-8"))
        > request.maximum_response_bytes
    ):
        raise ValueError("cleanup inventory exceeds its encoded response grant")


class WriteSegmentReceipt(StorageAdapterModel):
    number: PositiveDecimal
    segment_token: str = Field(min_length=1, max_length=4000)
    stored_bytes: PositiveDecimal
    stored_sha256: Sha256 | None = None


def _listed_segments(
    value: tuple[WriteSegmentReceipt, ...],
) -> tuple[WriteSegmentReceipt, ...]:
    numbers = [segment.number for segment in value]
    if numbers != sorted(numbers) or len(numbers) != len(set(numbers)):
        raise ValueError("listed write segments must be unique and strictly ordered")
    return value


class WriteCompletionPrecondition(StorageAdapterModel):
    """Adapter-issued precondition for one exact active-write state.

    Consumers echo the opaque token unchanged. It is neither a credential nor a
    bearer capability; completion remains independently authorized. Once an exact
    immutable object is published, its completed-object identity supersedes this
    active-write precondition for terminal reconciliation.
    """

    segment_count: NonnegativeDecimal
    stored_bytes: NonnegativeDecimal
    state_token: str = Field(
        min_length=1,
        max_length=_MAX_WRITE_COMPLETION_PRECONDITION_TOKEN_LENGTH,
        description=(
            "Bounded opaque adapter-issued state token for the exact accepted state of an "
            "active write. The token grants no authority and must be echoed unchanged."
        ),
    )


class WriteSegmentListRequest(StorageAdapterModel):
    """Request one bounded page from an exact accepted-segment view."""

    session: WriteSession
    after_number: NonnegativeDecimal = Field(
        default=0,
        json_schema_extra={
            "x-riverhog-extent": {
                "policy": "segmented_no_total_max",
                "reason": "write-segment-history-bounded-traversal",
            }
        },
    )
    traversal_token: str | None = Field(default=None, min_length=1, max_length=4000)
    maximum_items: int = Field(
        default=MAX_WRITE_SEGMENT_PAGE_ITEMS,
        ge=1,
        le=MAX_WRITE_SEGMENT_PAGE_ITEMS,
    )


class WriteSegmentPage(StorageAdapterModel):
    """One bounded page under an adapter-owned immutable traversal view."""

    session: WriteSession
    traversal_token: str = Field(min_length=1, max_length=4000)
    segments: tuple[WriteSegmentReceipt, ...] = Field(
        default=(),
        max_length=MAX_WRITE_SEGMENT_PAGE_ITEMS,
        json_schema_extra={
            "x-riverhog-extent": {
                "policy": "segmented_no_total_max",
                "reason": "bounded-storage-write-segment-page",
                "progression": "exact-adapter-write-traversal",
            }
        },
    )
    next_after_number: PositiveDecimal | None = None
    completion: WriteCompletionPrecondition | None = None

    @field_validator("segments")
    @classmethod
    def canonical_segments(
        cls,
        value: tuple[WriteSegmentReceipt, ...],
    ) -> tuple[WriteSegmentReceipt, ...]:
        return _listed_segments(value)

    @model_validator(mode="after")
    def validate_terminal(self) -> Self:
        if self.next_after_number is not None and self.completion is not None:
            raise ValueError(
                "nonterminal write segment pages cannot carry a completion precondition"
            )
        if self.next_after_number is not None and (
            not self.segments or self.next_after_number != self.segments[-1].number
        ):
            raise ValueError("write segment page continuation must name its last segment")
        return self


class WriteSegmentRequest(StorageAdapterModel):
    session: WriteSession
    number: PositiveDecimal
    stored_bytes: PositiveDecimal


class WriteCompleteRequest(StorageAdapterModel):
    session: WriteSession
    completion: WriteCompletionPrecondition
    expected_bytes: PositiveDecimal
    expected_content_type: str = Field(min_length=1, max_length=255)
    required_identity_assertions: RequiredIdentityAssertions
    expected_placement_policy: ObjectPlacementPolicy

    @field_validator("required_identity_assertions")
    @classmethod
    def canonical_metadata(cls, value: dict[str, str]) -> dict[str, str]:
        return _canonical_identity_assertions(value)

    @model_validator(mode="after")
    def validate_bytes(self) -> Self:
        if self.expected_bytes != self.session.expected_bytes:
            raise ValueError("write completion byte count differs from its session")
        if self.completion.segment_count < 1:
            raise ValueError("write completion requires at least one segment")
        if self.completion.stored_bytes != self.expected_bytes:
            raise ValueError("write byte count does not equal its completion precondition")
        return self


class CompletedWriteLookupRequest(StorageAdapterModel):
    object_path: str = Field(min_length=1, max_length=4096)
    expected_bytes: PositiveDecimal
    expected_content_type: str = Field(min_length=1, max_length=255)
    required_identity_assertions: RequiredIdentityAssertions
    expected_placement_policy: ObjectPlacementPolicy

    @field_validator("object_path")
    @classmethod
    def canonical_path(cls, value: str) -> str:
        return validate_object_path(value)

    @field_validator("required_identity_assertions")
    @classmethod
    def canonical_metadata(cls, value: dict[str, str]) -> dict[str, str]:
        return _canonical_identity_assertions(value)


class CompletedObjectReceipt(StorageAdapterModel):
    object_path: str = Field(min_length=1, max_length=4096)
    revision: str | None = Field(default=None, min_length=1, max_length=2000)
    entity_token: str | None = Field(default=None, min_length=1, max_length=4000)
    stored_bytes: PositiveDecimal
    verified_content_type: str = Field(min_length=1, max_length=255)
    verified_identity_assertions: RequiredIdentityAssertions
    verified_placement_policy: ObjectPlacementPolicy
    completed_at: CanonicalUtcTimestamp

    @field_validator("object_path")
    @classmethod
    def canonical_path(cls, value: str) -> str:
        return validate_object_path(value)

    @field_validator("verified_identity_assertions")
    @classmethod
    def canonical_metadata(cls, value: dict[str, str]) -> dict[str, str]:
        return _canonical_identity_assertions(value)


class SmallObjectWriteRequest(StorageAdapterModel):
    object_path: str = Field(min_length=1, max_length=4096)
    content_type: str = Field(min_length=1, max_length=255)
    required_identity_assertions: RequiredIdentityAssertions
    placement_policy: ObjectPlacementPolicy
    mode: Literal["create_only", "replace_current"]
    expected_current_stored_sha256: Sha256 | None = None
    stored_bytes: NonnegativeDecimal
    stored_sha256: Sha256

    @field_validator("object_path")
    @classmethod
    def canonical_path(cls, value: str) -> str:
        return validate_object_path(value)

    @field_validator("required_identity_assertions")
    @classmethod
    def canonical_metadata(cls, value: dict[str, str]) -> dict[str, str]:
        return _canonical_identity_assertions(value)

    @model_validator(mode="after")
    def validate_replacement_fence(self) -> Self:
        if self.mode == "create_only" and self.expected_current_stored_sha256 is not None:
            raise ValueError("create-only writes must not name a current-object fence")
        return self


class ImmutableObjectReceipt(StorageAdapterModel):
    object_path: str = Field(min_length=1, max_length=4096)
    revision: str | None = Field(default=None, min_length=1, max_length=2000)
    entity_token: str | None = Field(default=None, min_length=1, max_length=4000)
    stored_bytes: NonnegativeDecimal
    stored_sha256: Sha256
    verified_content_type: str = Field(min_length=1, max_length=255)
    verified_identity_assertions: RequiredIdentityAssertions
    verified_placement_policy: ObjectPlacementPolicy
    completed_at: CanonicalUtcTimestamp

    @field_validator("object_path")
    @classmethod
    def canonical_path(cls, value: str) -> str:
        return validate_object_path(value)

    @field_validator("verified_identity_assertions")
    @classmethod
    def canonical_metadata(cls, value: dict[str, str]) -> dict[str, str]:
        return _canonical_identity_assertions(value)


class ObjectMetadataReceipt(StorageAdapterModel):
    object_path: str = Field(min_length=1, max_length=4096)
    revision: str | None = Field(default=None, min_length=1, max_length=2000)
    entity_token: str | None = Field(default=None, min_length=1, max_length=4000)
    content_type: str | None = Field(default=None, min_length=1, max_length=255)
    stored_bytes: NonnegativeDecimal
    stored_sha256: Sha256 | None = None
    observed_identity_assertions: RequiredIdentityAssertions
    verified_placement_policy: ObjectPlacementPolicy
    completed_at: CanonicalUtcTimestamp

    @field_validator("object_path")
    @classmethod
    def canonical_path(cls, value: str) -> str:
        return validate_object_path(value)

    @field_validator("observed_identity_assertions")
    @classmethod
    def canonical_metadata(cls, value: dict[str, str]) -> dict[str, str]:
        return _canonical_identity_assertions(value)


class ObjectHeadRequest(StorageAdapterModel):
    object: ObjectLocator
    expected_placement_policy: ObjectPlacementPolicy


class ObjectReadRequest(StorageAdapterModel):
    object: ObjectLocator
    expected_bytes: NonnegativeDecimal
    offset: NonnegativeDecimal | None = None
    size: NonnegativeDecimal | None = None

    @model_validator(mode="after")
    def validate_range(self) -> Self:
        if (self.offset is None) != (self.size is None):
            raise ValueError("object range requires both offset and size")
        if self.offset is not None and self.size is not None:
            if self.offset + self.size > self.expected_bytes:
                raise ValueError("object range exceeds the expected object bytes")
        return self


class ObjectReadReceipt(StorageAdapterModel):
    """Adapter-observed identity and range for one single-pass read."""

    object: ObjectLocator
    total_bytes: NonnegativeDecimal
    offset: NonnegativeDecimal
    read_bytes: NonnegativeDecimal

    @model_validator(mode="after")
    def validate_range(self) -> Self:
        if self.offset + self.read_bytes > self.total_bytes:
            raise ValueError("object read range exceeds the observed object bytes")
        return self


class ObjectReadStream:
    """A bounded receipt beside its opaque single-pass byte stream."""

    __slots__ = ("_close", "_closed", "content", "receipt")

    def __init__(
        self,
        *,
        receipt: ObjectReadReceipt,
        content: Iterator[bytes],
        close: Callable[[], None] | None = None,
    ) -> None:
        self.receipt = receipt
        self.content = content
        self._close = close
        self._closed = False

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        close_content = getattr(self.content, "close", None)
        if callable(close_content):
            close_content()
        if self._close is not None:
            self._close()

    def __enter__(self) -> ObjectReadStream:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


class DeleteObjectRequest(StorageAdapterModel):
    object: ObjectLocator
    mode: Literal["current", "exact_revision", "all_versions"]
    expected_current_stored_sha256: Sha256 | None = None

    @model_validator(mode="after")
    def validate_revision(self) -> Self:
        if self.mode == "exact_revision" and self.object.revision is None:
            raise ValueError("exact revision deletion requires a revision")
        if self.mode != "exact_revision" and self.object.revision is not None:
            raise ValueError("current/all-version deletion must not name a revision")
        if self.mode != "current" and self.expected_current_stored_sha256 is not None:
            raise ValueError("only current-object deletion may use a stored-content fence")
        return self


class DeletePrefixRequest(StorageAdapterModel):
    object_prefix: str = Field(min_length=1, max_length=4096)
    mode: Literal["all_versions"] = "all_versions"

    @field_validator("object_prefix")
    @classmethod
    def canonical_prefix(cls, value: str) -> str:
        return validate_object_path(value, allow_prefix=True)


class ReadPreparationRequest(StorageAdapterModel):
    objects: tuple[ObjectLocator, ...] = Field(min_length=1)

    @field_validator("objects")
    @classmethod
    def canonical_objects(cls, value: tuple[ObjectLocator, ...]) -> tuple[ObjectLocator, ...]:
        identities = [(item.object_path, item.revision or "") for item in value]
        if identities != sorted(identities) or len(identities) != len(set(identities)):
            raise ValueError("read objects must be unique and canonically ordered")
        return value


class ReadRequested(StorageAdapterModel):
    state: Literal["requested"] = "requested"
    estimated_ready_at: CanonicalUtcTimestamp | None = None


class ReadReady(StorageAdapterModel):
    state: Literal["ready"] = "ready"
    available_until: CanonicalUtcTimestamp | None = None


class ReadExpired(StorageAdapterModel):
    state: Literal["expired"] = "expired"


ReadReadiness = Annotated[
    ReadRequested | ReadReady | ReadExpired,
    Field(discriminator="state"),
]


class ReadStatus(StorageAdapterModel):
    objects: tuple[ObjectLocator, ...] = Field(min_length=1)
    readiness: ReadReadiness

    @field_validator("objects")
    @classmethod
    def canonical_objects(cls, value: tuple[ObjectLocator, ...]) -> tuple[ObjectLocator, ...]:
        return ReadPreparationRequest(objects=value).objects


class StorageAdapterErrorBody(StorageAdapterModel):
    code: StorageAdapterErrorCode
    message: str = Field(min_length=1, max_length=2000)


class StorageAdapterError(StorageAdapterModel):
    error: StorageAdapterErrorBody


class StorageAdapterRejection(RuntimeError):
    """Provider-neutral expected refusal from a transport-neutral adapter."""

    def __init__(self, code: StorageAdapterErrorCode, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class MaintenanceResult(StorageAdapterModel):
    affected: NonnegativeDecimal


def validate_write_session_response(
    request: WriteStartRequest,
    response: WriteSession,
) -> None:
    if (
        response.object_path != request.object_path
        or response.expected_bytes != request.expected_bytes
    ):
        raise ValueError("adapter write session differs from its request")


def validate_write_start_request(
    request: WriteStartRequest,
    descriptor: AdapterDescriptor,
) -> None:
    if (
        descriptor.maximum_segment_bytes is not None
        and descriptor.maximum_segment_count is not None
        and request.expected_bytes
        > descriptor.maximum_segment_bytes * descriptor.maximum_segment_count
    ):
        raise ValueError("write exceeds the adapter's advertised segmentation limits")


def validate_write_segment_response(
    request: WriteSegmentRequest,
    response: WriteSegmentReceipt,
) -> None:
    if response.number != request.number or response.stored_bytes != request.stored_bytes:
        raise ValueError("adapter segment receipt differs from its request")


def validate_write_segment_request(
    request: WriteSegmentRequest,
    descriptor: AdapterDescriptor,
) -> None:
    if (
        descriptor.maximum_segment_bytes is not None
        and request.stored_bytes > descriptor.maximum_segment_bytes
    ):
        raise ValueError("write segment exceeds the adapter's advertised byte limit")
    if (
        descriptor.maximum_segment_count is not None
        and request.number > descriptor.maximum_segment_count
    ):
        raise ValueError("write segment number exceeds the adapter's advertised count limit")


def _validate_segment_receipts(
    segments: tuple[WriteSegmentReceipt, ...],
    descriptor: AdapterDescriptor,
    *,
    completion: bool,
) -> None:
    if (
        descriptor.maximum_segment_count is not None
        and len(segments) > descriptor.maximum_segment_count
    ):
        raise ValueError("write segment page exceeds the adapter's advertised count limit")
    if descriptor.maximum_segment_count is not None and any(
        segment.number > descriptor.maximum_segment_count for segment in segments
    ):
        raise ValueError("write segment page exceeds the adapter's advertised count limit")
    if descriptor.maximum_segment_bytes is not None and any(
        segment.stored_bytes > descriptor.maximum_segment_bytes for segment in segments
    ):
        raise ValueError("write segment page exceeds the adapter's advertised byte limit")
    if completion and any(
        segment.stored_bytes < descriptor.minimum_nonfinal_segment_bytes
        for segment in segments[:-1]
    ):
        raise ValueError("write completion contains an undersized non-final segment")


def validate_write_completion_request(
    request: WriteCompleteRequest,
    descriptor: AdapterDescriptor,
) -> None:
    if (
        descriptor.maximum_segment_count is not None
        and request.completion.segment_count > descriptor.maximum_segment_count
    ):
        raise ValueError("write completion exceeds the adapter's advertised count limit")


def validate_write_segment_page_response(
    request: WriteSegmentListRequest,
    response: WriteSegmentPage,
    descriptor: AdapterDescriptor,
) -> None:
    if response.session != request.session:
        raise ValueError("adapter segment page differs from its write session")
    if request.traversal_token is not None and response.traversal_token != request.traversal_token:
        raise ValueError("adapter segment traversal identity changed")
    if len(response.segments) > request.maximum_items:
        raise ValueError("adapter segment page exceeds the requested item count")
    if any(segment.number <= request.after_number for segment in response.segments):
        raise ValueError("adapter segment page did not advance past its cursor")
    _validate_segment_receipts(response.segments, descriptor, completion=False)


def validate_completed_write_response(
    request: WriteCompleteRequest | CompletedWriteLookupRequest,
    response: CompletedObjectReceipt,
) -> None:
    expected_path = (
        request.session.object_path
        if isinstance(request, WriteCompleteRequest)
        else request.object_path
    )
    if response.object_path != expected_path:
        raise ValueError("adapter completed-object receipt differs from its request")
    if response.verified_identity_assertions != request.required_identity_assertions:
        raise ValueError("adapter completed-object identity assertions differ from their request")
    if response.verified_content_type != request.expected_content_type:
        raise ValueError("adapter completed-object content type differs from its request")
    if response.verified_placement_policy != request.expected_placement_policy:
        raise ValueError("adapter completed-object placement policy differs from its request")
    if response.stored_bytes != request.expected_bytes:
        raise ValueError("adapter completed-object bytes differ from their request")


def validate_small_object_response(
    request: SmallObjectWriteRequest,
    response: ImmutableObjectReceipt,
) -> None:
    if (
        response.object_path != request.object_path
        or response.stored_bytes != request.stored_bytes
        or response.stored_sha256 != request.stored_sha256
        or response.verified_content_type != request.content_type
        or response.verified_identity_assertions != request.required_identity_assertions
        or response.verified_placement_policy != request.placement_policy
    ):
        raise ValueError("adapter immutable-object receipt differs from its request")


def validate_object_metadata_response(
    request: ObjectHeadRequest,
    response: ObjectMetadataReceipt,
) -> None:
    if response.object_path != request.object.object_path:
        raise ValueError("adapter object metadata differs from its request")
    if request.object.revision is not None and response.revision != request.object.revision:
        raise ValueError("adapter object metadata revision differs from its request")
    if response.verified_placement_policy != request.expected_placement_policy:
        raise ValueError("adapter object metadata placement policy differs from its request")


def validate_object_read_response(
    request: ObjectReadRequest,
    response: ObjectReadReceipt,
) -> None:
    expected_offset = request.offset if request.offset is not None else 0
    expected_read_bytes = request.size if request.size is not None else request.expected_bytes
    if response.object.object_path != request.object.object_path:
        raise ValueError("adapter object read path differs from its request")
    if request.object.revision is not None and response.object.revision != request.object.revision:
        raise ValueError("adapter object read revision differs from its request")
    if (
        response.total_bytes != request.expected_bytes
        or response.offset != expected_offset
        or response.read_bytes != expected_read_bytes
    ):
        raise ValueError("adapter object read range differs from its request")


def validate_read_status_response(
    request: ReadPreparationRequest,
    response: ReadStatus,
) -> None:
    if response.objects != request.objects:
        raise ValueError("adapter read status differs from its request")


class BeginWriteOperation(StorageAdapterModel):
    kind: Literal["begin"] = "begin"
    request: WriteStartRequest


class WritePartOperation(StorageAdapterModel):
    kind: Literal["part"] = "part"
    request: WriteSegmentRequest
    stored_sha256: Sha256


class CompleteWriteOperation(StorageAdapterModel):
    kind: Literal["complete"] = "complete"
    request: WriteCompleteRequest


class LookupWriteOperation(StorageAdapterModel):
    kind: Literal["lookup"] = "lookup"
    request: CompletedWriteLookupRequest


class PutObjectOperation(StorageAdapterModel):
    kind: Literal["put"] = "put"
    request: SmallObjectWriteRequest


type WriteOperation = Annotated[
    BeginWriteOperation
    | WritePartOperation
    | CompleteWriteOperation
    | LookupWriteOperation
    | PutObjectOperation,
    Field(discriminator="kind"),
]


def write_operation_sha256(operation: WriteOperation) -> str:
    """Commit the complete canonical provider request, including opaque payload digest."""
    return sha256(canonical_json_bytes(operation.model_dump(mode="json"))).hexdigest()


class WriteCallIdentity(StorageAdapterModel):
    """Correlation for one already admitted invocation; none of these fields grant access."""

    call_id: Sha256
    exact_request_sha256: Sha256
    target_incarnation: StorageIncarnationId
    dispatch_owner: Sha256


class CleanupStepRequest(StorageAdapterModel):
    """An already retained exact selection in a frozen, never-reused terminal scope.

    The digest identifies the original selected terminal request; it grants no
    disposal permission or forward-execution closure. The caller owns those
    prerequisites and retains this complete request before dispatch.

    Request grants include one conservatively reserved adapter HTTP hop, even
    for direct execution, plus every attempted lower-provider request. This
    operation performs no implicit descriptor discovery. Implementations either
    account for their complete single-attempt execution profile or do not dispatch.
    Control bytes cover the serialized invocation and response JSON together.
    """

    kind: Literal["dispose"] = "dispose"
    selected_terminal_sha256: Sha256
    inventory: CleanupInventoryPage
    maximum_provider_requests: PositiveDecimal
    maximum_control_bytes: PositiveDecimal
    maximum_in_flight: PositiveDecimal


class CleanupInventoryStepRequest(StorageAdapterModel):
    """Budgeted observation in an original selected terminal scope.

    A returned page grants neither disposal authority nor closure of any earlier
    forward call. The execution envelope accounts only for this physical read.
    """

    kind: Literal["inventory"] = "inventory"
    selected_terminal_sha256: Sha256
    inventory: CleanupInventoryRequest
    maximum_provider_requests: PositiveDecimal
    maximum_control_bytes: PositiveDecimal
    maximum_in_flight: PositiveDecimal


type CleanupRequest = Annotated[
    CleanupStepRequest | CleanupInventoryStepRequest, Field(discriminator="kind")
]


class CleanupInvocation(StorageAdapterModel):
    """One already admitted physical attempt over a retained exact selection.

    A retry has a new call identity and does not close an earlier uncertain call.
    The composing owner retains every unresolved attempt and its resource charge.
    """

    call: WriteCallIdentity
    request: CleanupRequest
    adapter_hops_remaining: Literal[0, 1] = 1

    @model_validator(mode="after")
    def exact_request(self) -> Self:
        self._require_exact_request()
        return self

    def _require_exact_request(self, *, request_bytes: int | None = None) -> None:
        if isinstance(self.request, CleanupStepRequest):
            CleanupStepRequest.model_validate_json(self.request.model_dump_json())
        elif isinstance(self.request, CleanupInventoryStepRequest):
            CleanupInventoryStepRequest.model_validate_json(self.request.model_dump_json())
        else:
            raise ValueError("cleanup operation request is invalid")
        WriteCallIdentity.model_validate_json(self.call.model_dump_json())
        if type(self.adapter_hops_remaining) is not int or self.adapter_hops_remaining not in {
            0,
            1,
        }:
            raise ValueError("cleanup adapter-hop profile is invalid")
        if (
            self.call.exact_request_sha256 != cleanup_step_sha256(self.request)
            or self.call.target_incarnation != self.request.inventory.target_incarnation
        ):
            raise ValueError("cleanup invocation differs from its exact selection")
        if (
            isinstance(self.request, CleanupStepRequest)
            and self.request.inventory.scanned_items > MAX_CLEANUP_INVENTORY_PAGE_ITEMS
        ):
            raise ValueError("cleanup selection exceeds one bounded inventory page")
        for value in (
            self.request.maximum_provider_requests,
            self.request.maximum_control_bytes,
            self.request.maximum_in_flight,
        ):
            if type(value) is not int or value < 1:
                raise ValueError("cleanup resource grants must be positive integers")
        reservation = _cleanup_control_reservation(self)
        if request_bytes is not None:
            if type(request_bytes) is not int or request_bytes < 0:
                raise ValueError("cleanup request byte count is invalid")
            reservation += max(
                0, request_bytes - len(self.model_dump_json(exclude_none=True).encode())
            )
        if reservation > self.request.maximum_control_bytes:
            raise ValueError("cleanup control grant cannot retain the exact request and response")


def cleanup_step_sha256(request: CleanupRequest) -> str:
    return sha256(canonical_json_bytes(request.model_dump(mode="json"))).hexdigest()


class CleanupItemOutcome(StorageAdapterModel):
    candidate: CleanupCandidate
    outcome: Literal["disposed", "pending", "deferred", "rejected", "unknown"]


class CleanupProgress(StorageAdapterModel):
    """Exact item results, not proof that any earlier forward invocation stopped."""

    items: tuple[CleanupItemOutcome, ...] = Field(
        default=(),
        max_length=MAX_CLEANUP_INVENTORY_PAGE_ITEMS,
        json_schema_extra={
            "x-riverhog-extent": {
                "policy": "segmented_no_total_max",
                "reason": "bounded-selected-cleanup-item-outcomes",
                "progression": "adapter-retained-cleanup-selection",
            }
        },
    )
    provider_requests: NonnegativeDecimal
    selection_complete: bool
    continuation: str | None = Field(default=None, min_length=1, max_length=65536)
    exhausted: bool


class CleanupInventoryProgress(StorageAdapterModel):
    """One bounded observation and its complete transport request charge."""

    inventory: CleanupInventoryPage
    provider_requests: NonnegativeDecimal


class CleanupExecutionResult(StorageAdapterModel):
    call: WriteCallIdentity
    execution: Literal["closed", "unknown"]
    outcome: Literal["cleanup_progress", "cleanup_inventory", "unknown"]
    result: CleanupProgress | CleanupInventoryProgress | None = None

    @model_validator(mode="after")
    def result_shape(self) -> Self:
        self._require_result_shape()
        return self

    def _require_result_shape(self) -> None:
        if self.execution not in {"closed", "unknown"} or self.outcome not in {
            "cleanup_progress",
            "cleanup_inventory",
            "unknown",
        }:
            raise ValueError("cleanup execution outcome is invalid")
        if self.outcome == "unknown":
            if self.execution != "unknown" or self.result is not None:
                raise ValueError("unknown cleanup cannot claim an adopted result")
        elif self.outcome == "cleanup_progress":
            if not isinstance(self.result, CleanupProgress):
                raise ValueError("cleanup progress requires exact per-item results")
        elif not isinstance(self.result, CleanupInventoryProgress):
            raise ValueError("cleanup inventory requires an exact bounded observation")


def _cleanup_control_reservation(invocation: CleanupInvocation) -> int:
    # Reserve the longest item status, request-count decimal and continuation,
    # even when they would not occur together in an accepted result.
    request = invocation.request
    if isinstance(request, CleanupInventoryStepRequest):
        upper = {
            "call": invocation.call.model_dump(mode="json"),
            "execution": "unknown",
            "outcome": "cleanup_inventory",
            "result": {
                "inventory": None,
                "provider_requests": format_scalar(
                    "nonnegative", request.maximum_provider_requests
                ),
            },
        }
        return (
            len(invocation.model_dump_json(exclude_none=True).encode())
            + len(canonical_json_bytes(upper))
            - len(b"null")
            + request.inventory.maximum_response_bytes
        )
    upper = {
        "call": invocation.call.model_dump(mode="json"),
        "execution": "unknown",
        "outcome": "cleanup_progress",
        "result": {
            "items": [
                {"candidate": item.model_dump(mode="json"), "outcome": "rejected"}
                for item in request.inventory.candidates
            ],
            "provider_requests": format_scalar("nonnegative", request.maximum_provider_requests),
            "selection_complete": False,
            "continuation": request.inventory.continuation,
            "exhausted": False,
        },
    }
    return len(invocation.model_dump_json(exclude_none=True).encode()) + len(
        canonical_json_bytes(upper)
    )


def unknown_cleanup_execution(invocation: CleanupInvocation) -> CleanupExecutionResult:
    return CleanupExecutionResult(call=invocation.call, execution="unknown", outcome="unknown")


def cleanup_execution_result(
    invocation: CleanupInvocation,
    items: tuple[CleanupItemOutcome, ...],
    *,
    provider_requests: int,
    execution: Literal["closed", "unknown"],
) -> CleanupExecutionResult:
    request = invocation.request
    if not isinstance(request, CleanupStepRequest):
        raise ValueError("cleanup disposal result requires its exact selected page")
    complete = all(item.outcome == "disposed" for item in items)
    response = CleanupExecutionResult(
        call=invocation.call,
        execution=execution,
        outcome="cleanup_progress",
        result=CleanupProgress(
            items=items,
            provider_requests=provider_requests,
            selection_complete=complete,
            continuation=request.inventory.continuation if complete else None,
            exhausted=complete and request.inventory.exhausted,
        ),
    )
    validate_cleanup_execution(invocation, response)
    return response


def cleanup_inventory_execution_result(
    invocation: CleanupInvocation,
    page: CleanupInventoryPage,
    *,
    provider_requests: int,
    execution: Literal["closed", "unknown"],
) -> CleanupExecutionResult:
    response = CleanupExecutionResult(
        call=invocation.call,
        execution=execution,
        outcome="cleanup_inventory",
        result=CleanupInventoryProgress(inventory=page, provider_requests=provider_requests),
    )
    validate_cleanup_execution(invocation, response)
    return response


def validate_cleanup_execution(
    invocation: CleanupInvocation, response: CleanupExecutionResult
) -> None:
    invocation._require_exact_request()
    response._require_result_shape()
    if response.call != invocation.call:
        raise ValueError("cleanup execution differs from its original correlation")
    if response.outcome == "unknown":
        return
    result = response.result
    if isinstance(invocation.request, CleanupInventoryStepRequest):
        if response.outcome != "cleanup_inventory" or not isinstance(
            result, CleanupInventoryProgress
        ):
            raise ValueError("cleanup inventory response differs from its operation")
        validate_cleanup_inventory_response(invocation.request.inventory, result.inventory)
        _validate_cleanup_resources(invocation, response, result.provider_requests)
        return
    if response.outcome != "cleanup_progress" or not isinstance(result, CleanupProgress):
        raise ValueError("cleanup disposal response differs from its operation")
    selected = invocation.request.inventory
    if type(result.selection_complete) is not bool or type(result.exhausted) is not bool:
        raise ValueError("cleanup progression flags must be booleans")
    if tuple(item.candidate for item in result.items) != selected.candidates:
        raise ValueError("cleanup result differs from its exact selected items")
    if any(
        item.outcome not in {"disposed", "pending", "deferred", "rejected", "unknown"}
        for item in result.items
    ):
        raise ValueError("cleanup item outcome is invalid")
    if response.execution == "closed" and any(item.outcome == "unknown" for item in result.items):
        raise ValueError("ambiguous cleanup cannot claim execution closure")
    complete = all(item.outcome == "disposed" for item in result.items)
    if (
        result.selection_complete != complete
        or result.continuation != (selected.continuation if complete else None)
        or result.exhausted != (complete and selected.exhausted)
    ):
        raise ValueError("cleanup continuation cannot advance an incomplete selection")
    _validate_cleanup_resources(invocation, response, result.provider_requests)


def _validate_cleanup_resources(
    invocation: CleanupInvocation, response: CleanupExecutionResult, provider_requests: int
) -> None:
    if (
        type(provider_requests) is not int
        or not 1 <= provider_requests <= invocation.request.maximum_provider_requests
    ):
        raise ValueError("cleanup exceeded its complete transport request grant")
    if (
        len(invocation.model_dump_json(exclude_none=True).encode())
        + len(response.model_dump_json(exclude_none=True).encode())
        > invocation.request.maximum_control_bytes
    ):
        raise ValueError("cleanup exceeded its serialized control grant")


class WriteInvocation(StorageAdapterModel):
    call: WriteCallIdentity
    operation: WriteOperation

    @model_validator(mode="after")
    def exact_request(self) -> WriteInvocation:
        self._require_exact_request()
        return self

    def _require_exact_request(self) -> None:
        if self.call.exact_request_sha256 != write_operation_sha256(self.operation):
            raise ValueError("write invocation differs from its exact request commitment")

    @property
    def stored_bytes(self) -> int:
        if isinstance(self.operation, (WritePartOperation, PutObjectOperation)):
            return self.operation.request.stored_bytes
        return 0


class WriteExecutionResult(StorageAdapterModel):
    """An executor-owned claim, accepted only through its trusted adapter boundary.

    A positive result with unknown execution preserves observation without claiming
    that delegated work has stopped. A missing response never creates closure.
    """

    call: WriteCallIdentity
    execution: Literal["closed", "unknown"]
    outcome: Literal["no_effect", "session", "part", "completed", "unknown"]
    result: (
        WriteSession | WriteSegmentReceipt | CompletedObjectReceipt | ImmutableObjectReceipt | None
    ) = None

    @model_validator(mode="after")
    def result_shape(self) -> WriteExecutionResult:
        self._require_result_shape()
        return self

    def _require_result_shape(self) -> None:
        if self.outcome == "unknown":
            if self.execution != "unknown" or self.result is not None:
                raise ValueError("unknown execution cannot claim a confirmed outcome")
        elif self.outcome == "no_effect":
            if self.execution != "closed" or self.result is not None:
                raise ValueError("no-effect requires closed execution without a result")
        elif self.outcome == "session":
            if not isinstance(self.result, WriteSession):
                raise ValueError("session outcome requires the exact write session")
        elif self.outcome == "part":
            if not isinstance(self.result, WriteSegmentReceipt):
                raise ValueError("part outcome requires the exact segment receipt")
        elif not isinstance(self.result, (CompletedObjectReceipt, ImmutableObjectReceipt)):
            raise ValueError("completed outcome requires the exact object receipt")


def unknown_write_execution(invocation: WriteInvocation) -> WriteExecutionResult:
    return WriteExecutionResult(
        call=invocation.call, execution="unknown", outcome="unknown", result=None
    )


def validate_write_execution(invocation: WriteInvocation, response: WriteExecutionResult) -> None:
    invocation._require_exact_request()
    response._require_result_shape()
    if response.call != invocation.call:
        raise ValueError("write execution correlation differs from the admitted invocation")
    if response.outcome == "unknown":
        return
    operation, result = invocation.operation, response.result
    if isinstance(operation, BeginWriteOperation) and isinstance(result, WriteSession):
        validate_write_session_response(operation.request, result)
    elif isinstance(operation, WritePartOperation) and isinstance(result, WriteSegmentReceipt):
        validate_write_segment_response(operation.request, result)
        if result.stored_sha256 is not None and result.stored_sha256 != operation.stored_sha256:
            raise ValueError("write execution segment digest differs from the original content")
    elif isinstance(operation, (CompleteWriteOperation, LookupWriteOperation)) and isinstance(
        result, CompletedObjectReceipt
    ):
        validate_completed_write_response(operation.request, result)
    elif isinstance(operation, LookupWriteOperation) and response.outcome == "no_effect":
        return
    elif isinstance(operation, PutObjectOperation) and isinstance(result, ImmutableObjectReceipt):
        validate_small_object_response(operation.request, result)
    else:
        raise ValueError("write execution outcome differs from its operation")


class StorageAdapterPort(Protocol):
    """Transport-neutral effects required from one configured adapter target.

    Public identity assertions are inert reconciliation evidence. Write sessions are durable
    continuation identities rather than process-local handles; provider-specific state needed
    to honor them remains private to the adapter.
    """

    def descriptor(self) -> AdapterDescriptor: ...

    def execute_write(
        self, invocation: WriteInvocation, *, content: BinaryContent = b""
    ) -> WriteExecutionResult: ...

    def execute_cleanup(self, invocation: CleanupInvocation) -> CleanupExecutionResult: ...

    def begin_write(self, request: WriteStartRequest) -> WriteSession: ...

    def write_segment(
        self,
        *,
        session: WriteSession,
        number: int,
        stored_bytes: int,
        content: BinaryContent,
    ) -> WriteSegmentReceipt: ...

    def list_cleanup_candidates(self, request: CleanupInventoryRequest) -> CleanupInventoryPage: ...

    def list_open_writes(self, request: OpenWriteListRequest) -> OpenWritePage: ...

    def list_segments(self, request: WriteSegmentListRequest) -> WriteSegmentPage: ...

    def complete_write(
        self,
        request: WriteCompleteRequest,
    ) -> CompletedObjectReceipt: ...

    def find_completed_write(
        self,
        request: CompletedWriteLookupRequest,
    ) -> CompletedObjectReceipt | None: ...

    def abort_write(self, session: WriteSession) -> None: ...

    def put_small_object(
        self,
        request: SmallObjectWriteRequest,
        content: BinaryContent,
    ) -> ImmutableObjectReceipt: ...

    def head_object(self, request: ObjectHeadRequest) -> ObjectMetadataReceipt | None: ...

    def read_object(self, request: ObjectReadRequest) -> ObjectReadStream: ...

    def delete_object(self, request: DeleteObjectRequest) -> None: ...

    def delete_prefix(self, request: DeletePrefixRequest) -> int: ...

    def prepare_read(self, request: ReadPreparationRequest) -> ReadStatus: ...

    def read_status(self, request: ReadPreparationRequest) -> ReadStatus: ...

    def cleanup_read(self, request: ReadPreparationRequest) -> None: ...


class _ContentValidation:
    def __init__(self, content: BinaryContent, *, expected_bytes: int) -> None:
        self._content = content
        self.expected_bytes = expected_bytes
        self.observed_bytes = 0
        self.digest = sha256()
        self.exhausted = False
        self.started = False

    def content(self) -> BinaryContent:
        if isinstance(self._content, bytes):
            if self._content:
                self._observe(self._content)
            self.exhausted = True
            return self._content
        return self._iter_content()

    def _observe(self, chunk: bytes) -> None:
        if not isinstance(chunk, bytes) or not chunk:
            raise ValueError("adapter content chunks must be nonempty bytes")
        self.observed_bytes += len(chunk)
        if self.observed_bytes > self.expected_bytes:
            raise ValueError("adapter content exceeds its declared byte count")
        self.digest.update(chunk)

    def _iter_content(self) -> Iterator[bytes]:
        if self.started:
            raise ValueError("adapter content may be consumed only once")
        self.started = True
        for chunk in cast(Iterable[bytes], self._content):
            self._observe(chunk)
            yield chunk
        self.exhausted = True

    def require_complete(self) -> None:
        if not self.exhausted or self.observed_bytes != self.expected_bytes:
            raise ValueError("adapter did not consume the declared content bytes")


def _response[ModelT: StorageAdapterModel](
    value: object,
    expected: type[ModelT],
    label: str,
) -> ModelT:
    if not isinstance(value, expected):
        raise TypeError(f"adapter returned an invalid {label}")
    return value


class ValidatedStorageAdapterPort:
    """One exact response-acceptance domain for every transport-neutral adapter."""

    def __init__(self, adapter: StorageAdapterPort) -> None:
        self._adapter = adapter
        self._descriptor: AdapterDescriptor | None = None

    def descriptor(self) -> AdapterDescriptor:
        if self._descriptor is None:
            self._descriptor = _response(
                self._adapter.descriptor(),
                AdapterDescriptor,
                "descriptor",
            )
        return self._descriptor

    def execute_write(
        self, invocation: WriteInvocation, *, content: BinaryContent = b""
    ) -> WriteExecutionResult:
        try:
            invocation._require_exact_request()
            if self.descriptor().storage_incarnation_id != invocation.call.target_incarnation:
                return unknown_write_execution(invocation)
            validation = _ContentValidation(content, expected_bytes=invocation.stored_bytes)
            response = self._adapter.execute_write(invocation, content=validation.content())
            if not isinstance(response, WriteExecutionResult):
                return unknown_write_execution(invocation)
            validate_write_execution(invocation, response)
            if response.outcome != "unknown":
                validation.require_complete()
                operation = invocation.operation
                expected_sha256 = (
                    operation.stored_sha256
                    if isinstance(operation, WritePartOperation)
                    else operation.request.stored_sha256
                    if isinstance(operation, PutObjectOperation)
                    else None
                )
                if expected_sha256 is not None and validation.digest.hexdigest() != expected_sha256:
                    return unknown_write_execution(invocation)
            return response
        except Exception:
            return unknown_write_execution(invocation)

    def execute_cleanup(self, invocation: CleanupInvocation) -> CleanupExecutionResult:
        try:
            invocation._require_exact_request()
            response = self._adapter.execute_cleanup(invocation)
            if not isinstance(response, CleanupExecutionResult):
                return unknown_cleanup_execution(invocation)
            validate_cleanup_execution(invocation, response)
            return response
        except Exception:
            return unknown_cleanup_execution(invocation)

    def begin_write(self, request: WriteStartRequest) -> WriteSession:
        validate_write_start_request(request, self.descriptor())
        response = _response(self._adapter.begin_write(request), WriteSession, "write session")
        validate_write_session_response(request, response)
        return response

    def write_segment(
        self,
        *,
        session: WriteSession,
        number: int,
        stored_bytes: int,
        content: BinaryContent,
    ) -> WriteSegmentReceipt:
        request = WriteSegmentRequest(
            session=session,
            number=number,
            stored_bytes=stored_bytes,
        )
        descriptor = self.descriptor()
        validate_write_segment_request(request, descriptor)
        validation = _ContentValidation(content, expected_bytes=stored_bytes)
        response = _response(
            self._adapter.write_segment(
                session=session,
                number=number,
                stored_bytes=stored_bytes,
                content=validation.content(),
            ),
            WriteSegmentReceipt,
            "write segment receipt",
        )
        validation.require_complete()
        validate_write_segment_response(request, response)
        if (
            response.stored_sha256 is not None
            and response.stored_sha256 != validation.digest.hexdigest()
        ):
            raise ValueError("adapter segment digest differs from the supplied content")
        return response

    def list_cleanup_candidates(self, request: CleanupInventoryRequest) -> CleanupInventoryPage:
        response = _response(
            self._adapter.list_cleanup_candidates(request),
            CleanupInventoryPage,
            "cleanup inventory",
        )
        validate_cleanup_inventory_response(request, response)
        return response

    def list_open_writes(self, request: OpenWriteListRequest) -> OpenWritePage:
        response = _response(
            self._adapter.list_open_writes(request), OpenWritePage, "open-write page"
        )
        validate_open_write_page_response(request, response)
        return response

    def list_segments(self, request: WriteSegmentListRequest) -> WriteSegmentPage:
        response = _response(
            self._adapter.list_segments(request),
            WriteSegmentPage,
            "write segment page",
        )
        validate_write_segment_page_response(request, response, self.descriptor())
        return response

    def complete_write(self, request: WriteCompleteRequest) -> CompletedObjectReceipt:
        validate_write_completion_request(request, self.descriptor())
        response = _response(
            self._adapter.complete_write(request),
            CompletedObjectReceipt,
            "completed-object receipt",
        )
        validate_completed_write_response(request, response)
        return response

    def find_completed_write(
        self,
        request: CompletedWriteLookupRequest,
    ) -> CompletedObjectReceipt | None:
        response = self._adapter.find_completed_write(request)
        if response is None:
            return None
        validated = _response(response, CompletedObjectReceipt, "completed-object receipt")
        validate_completed_write_response(request, validated)
        return validated

    def abort_write(self, session: WriteSession) -> None:
        self._adapter.abort_write(session)

    def put_small_object(
        self,
        request: SmallObjectWriteRequest,
        content: BinaryContent,
    ) -> ImmutableObjectReceipt:
        validation = _ContentValidation(content, expected_bytes=request.stored_bytes)
        response = _response(
            self._adapter.put_small_object(request, validation.content()),
            ImmutableObjectReceipt,
            "immutable-object receipt",
        )
        validation.require_complete()
        if validation.digest.hexdigest() != request.stored_sha256:
            raise ValueError("small-object content digest differs from its request")
        validate_small_object_response(request, response)
        return response

    def head_object(self, request: ObjectHeadRequest) -> ObjectMetadataReceipt | None:
        response = self._adapter.head_object(request)
        if response is None:
            return None
        validated = _response(response, ObjectMetadataReceipt, "object metadata receipt")
        validate_object_metadata_response(request, validated)
        return validated

    def read_object(self, request: ObjectReadRequest) -> ObjectReadStream:
        response = self._adapter.read_object(request)
        if not isinstance(response, ObjectReadStream):
            raise TypeError("adapter returned an invalid object read stream")
        try:
            receipt = _response(response.receipt, ObjectReadReceipt, "object read receipt")
            validate_object_read_response(request, receipt)
        except BaseException:
            response.close()
            raise

        def validated_content() -> Iterator[bytes]:
            try:
                observed = 0
                for chunk in response.content:
                    if not isinstance(chunk, bytes) or not chunk:
                        raise ValueError("adapter read chunks must be nonempty bytes")
                    observed += len(chunk)
                    if observed > receipt.read_bytes:
                        raise ValueError("adapter read exceeds its observed byte count")
                    yield chunk
                if observed != receipt.read_bytes:
                    raise ValueError("adapter read differs from its observed byte count")
            finally:
                response.close()

        return ObjectReadStream(
            receipt=receipt,
            content=validated_content(),
            close=response.close,
        )

    def delete_object(self, request: DeleteObjectRequest) -> None:
        self._adapter.delete_object(request)

    def delete_prefix(self, request: DeletePrefixRequest) -> int:
        return self._affected(self._adapter.delete_prefix(request), "delete-prefix")

    def prepare_read(self, request: ReadPreparationRequest) -> ReadStatus:
        response = _response(
            self._adapter.prepare_read(request),
            ReadStatus,
            "read status",
        )
        validate_read_status_response(request, response)
        return response

    def read_status(self, request: ReadPreparationRequest) -> ReadStatus:
        response = _response(
            self._adapter.read_status(request),
            ReadStatus,
            "read status",
        )
        validate_read_status_response(request, response)
        return response

    def cleanup_read(self, request: ReadPreparationRequest) -> None:
        self._adapter.cleanup_read(request)

    @staticmethod
    def _affected(value: object, operation: str) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise TypeError(f"adapter {operation} response must be a nonnegative integer")
        return value


def validated_storage_adapter(adapter: StorageAdapterPort) -> ValidatedStorageAdapterPort:
    """Return the shared validation boundary without nesting it."""

    if isinstance(adapter, ValidatedStorageAdapterPort):
        return adapter
    return ValidatedStorageAdapterPort(adapter)


__all__ = [
    "CleanupStepRequest",
    "CleanupInvocation",
    "CleanupItemOutcome",
    "CleanupProgress",
    "CleanupExecutionResult",
    "cleanup_step_sha256",
    "unknown_cleanup_execution",
    "cleanup_execution_result",
    "validate_cleanup_execution",
    "BeginWriteOperation",
    "CompleteWriteOperation",
    "LookupWriteOperation",
    "PutObjectOperation",
    "WriteCallIdentity",
    "WriteExecutionResult",
    "WriteInvocation",
    "WriteOperation",
    "WritePartOperation",
    "unknown_write_execution",
    "validate_write_execution",
    "write_operation_sha256",
    "ADAPTER_PRIVATE_ASSERTION_PREFIX",
    "MAX_WRITE_SEGMENT_PAGE_ITEMS",
    "STORAGE_ADAPTER_PROTOCOL",
    "AdapterDescriptor",
    "BinaryContent",
    "CompletedObjectReceipt",
    "DeleteObjectRequest",
    "DeletePrefixRequest",
    "ImmutableObjectReceipt",
    "MaintenanceResult",
    "WriteCompleteRequest",
    "WriteCompletionPrecondition",
    "WriteStartRequest",
    "CompletedWriteLookupRequest",
    "WriteSegmentReceipt",
    "WriteSegmentListRequest",
    "WriteSegmentPage",
    "WriteSegmentRequest",
    "WriteSession",
    "ObjectLocator",
    "ObjectHeadRequest",
    "ObjectMetadataReceipt",
    "ObjectPlacementPolicy",
    "ObjectReadRequest",
    "ObjectReadReceipt",
    "ObjectReadStream",
    "RequiredIdentityAssertions",
    "ReadPreparationRequest",
    "ReadMode",
    "ReadExpired",
    "ReadReadiness",
    "ReadReady",
    "ReadRequested",
    "ReadStatus",
    "SemanticId",
    "Sha256",
    "SmallObjectWriteRequest",
    "StorageAdapterError",
    "StorageAdapterErrorBody",
    "StorageAdapterErrorCode",
    "StorageAdapterModel",
    "StorageAdapterPort",
    "ValidatedStorageAdapterPort",
    "StorageAdapterRejection",
    "validate_object_path",
    "validate_completed_write_response",
    "validate_object_metadata_response",
    "validate_object_read_response",
    "validate_read_status_response",
    "validate_small_object_response",
    "validate_write_segment_response",
    "validate_write_segment_request",
    "validate_write_segment_page_response",
    "validate_write_completion_request",
    "validate_write_start_request",
    "validate_write_session_response",
    "validated_storage_adapter",
]
