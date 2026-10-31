"""Explicit application placement and resources for a retained Windows upload."""

from __future__ import annotations

import json
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Annotated

from pydantic import BaseModel, ConfigDict, Field, field_validator
from riverhog_canonical_json import canonical_json_bytes
from riverhog_client import (
    PrimaryStateBusy,
    PrimaryStatePolicy,
    PrimaryStorage,
    PrimaryStore,
    SqliteOperationGrants,
    SqlitePrimaryLimits,
    SqliteReadinessPlan,
    WindowsSqliteProcess,
    primary_allocation_descriptor,
    sqlite_primary_preparation_step,
)

from a_riverhog_cli.upload_identity import initialize_upload_identity, read_upload_identity
from a_riverhog_cli.upload_sqlite import UploadSqliteResources

if TYPE_CHECKING:
    from a_riverhog_cli.directory_upload import LocalUpload


_PositiveGrant = Annotated[int, Field(strict=True, gt=0, le=(1 << 63) - 1)]
_NonnegativeGrant = Annotated[int, Field(strict=True, ge=0, le=(1 << 63) - 1)]


class WindowsUploadPolicy(BaseModel):
    """Explicit command input for application-owned Windows resource grants.

    Parsing selects no runtime and opens no state. The command installs one
    process policy before its consumers, then passes that same process and
    connection owner into selection. Resource grants are required inputs;
    qualification fixture values are not application defaults.
    """

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)
    parent: Path
    heap_bytes: _PositiveGrant
    native_limits: tuple[_PositiveGrant, _PositiveGrant]
    limits: SqlitePrimaryLimits
    operations: SqliteOperationGrants
    retry_profile: tuple[_NonnegativeGrant, _NonnegativeGrant]
    readiness: SqliteReadinessPlan
    identity_bytes: _PositiveGrant
    preparation_steps: _PositiveGrant

    @field_validator("parent")
    @classmethod
    def absolute_parent(cls, value: Path) -> Path:
        if not value.is_absolute():
            raise ValueError("Windows upload state requires an explicit absolute durable parent")
        return value

    def selection(
        self, process: WindowsSqliteProcess, resources: UploadSqliteResources
    ) -> WindowsUploadState:
        """Bind an already admitted process and its actual shared connection owner."""
        return WindowsUploadState(
            parent=self.parent,
            process=process,
            resources=resources,
            limits=self.limits,
            operations=self.operations,
            retry_profile=self.retry_profile,
            readiness=self.readiness,
            identity_bytes=self.identity_bytes,
            preparation_steps=self.preparation_steps,
        )


@dataclass(frozen=True, slots=True)
class WindowsUploadState:
    """Caller-selected placement and grants, without opening or preparing state.

    The parent must already be durable private local NTFS. The process owns the
    qualified engine's heap and native policy before any consumer opens. The
    same application's connection admission owns every primary pager.

    Identity bytes have their own explicit admission in addition to primary
    member admission. Physical container grants cover both. This selection
    does not bound whole source enumeration, serialization, Python or RSS.
    Preparation is execution with an explicit quantum count, never status or
    close work. Exhaustion retains state and does not increase any grant.
    """

    parent: Path
    process: WindowsSqliteProcess
    resources: UploadSqliteResources
    limits: SqlitePrimaryLimits
    operations: SqliteOperationGrants
    retry_profile: tuple[int, int]
    readiness: SqliteReadinessPlan
    identity_bytes: int
    preparation_steps: int

    def __post_init__(self) -> None:
        if not isinstance(self.parent, Path) or not self.parent.is_absolute():
            raise ValueError("Windows upload state requires an explicit absolute durable parent")
        for value in (self.identity_bytes, self.preparation_steps):
            if type(value) is not int or value < 1:
                raise ValueError(
                    "Windows upload identity and preparation admissions must be positive"
                )

    def preparation_step(self, storage: PrimaryStorage) -> bool:
        """Advance one execution quantum for the selected producer's payload.

        Pass this callback to the producer's storage_preparation port. Its
        caller schedules another execution attempt after a not-ready result;
        the callback does not drain preparation, change grants or run on close.
        """
        self.process.verify()
        return sqlite_primary_preparation_step(storage, plan=self.readiness)


@contextmanager
def prepare_windows_upload(
    root: Path,
    idempotency_key: str,
    policy: PrimaryStatePolicy,
    selection: WindowsUploadState,
    *,
    initialize: bool,
) -> Iterator[tuple[LocalUpload, PrimaryStore]]:
    """Keep allocation and source authority in the same explicit native container.

    Opening a fresh container must succeed before source IDs are generated.
    Resume reads the original identity before source enumeration and never
    initializes a missing allocation, namespace, reference or payload.
    """
    from a_riverhog_cli.directory_upload import (
        _allocation_id,
        _load_upload_identity,
        _new_upload_identity,
        _verified_upload_sources,
    )

    if type(initialize) is not bool:
        raise ValueError("upload state initialization requires explicit boolean authority")
    allocation_id = _allocation_id(root, idempotency_key)
    descriptor = primary_allocation_descriptor(
        allocation_id=allocation_id, scope=policy.scope, limits=policy.limits()
    )
    location = selection.process.primary_location(selection.parent, descriptor)
    if initialize:
        try:
            location.lstat()
        except FileNotFoundError:
            pass
        else:
            raise FileExistsError(
                "fresh upload initialization conflicts with retained primary state"
            )
    factory = selection.process.primary_factory(
        limits=selection.limits,
        operations=selection.operations,
        retry_profile=selection.retry_profile,
        connection_factory=selection.resources.connect_primary,
    )
    physical: PrimaryStorage | None = None

    def selected(
        location: Path, *, allocation_descriptor: bytes, initialize: bool
    ) -> PrimaryStorage:
        nonlocal physical
        physical = factory(
            location, allocation_descriptor=allocation_descriptor, initialize=initialize
        )
        return physical

    with PrimaryStore(
        location,
        allocation_id=allocation_id,
        scope=policy.scope,
        limits=policy.limits(),
        initialize=initialize,
        storage_factory=selected,
    ) as store:
        assert physical is not None
        identity = physical.child("directory-upload", initialize=initialize)
        if initialize:
            root, current = _verified_upload_sources(root, idempotency_key)
            original = canonical_json_bytes(_new_upload_identity(root, idempotency_key, current))

            def prepare() -> None:
                for _ in range(selection.preparation_steps):
                    if store.prepare_storage_step(selection.preparation_step):
                        return
                raise PrimaryStateBusy(
                    "upload primary preparation requires another execution grant"
                )

            body = initialize_upload_identity(
                identity,
                original,
                byte_limit=selection.identity_bytes,
                control_capacity=policy.control_bytes,
                prepare_payload=prepare,
            )
        else:
            body = read_upload_identity(identity, byte_limit=selection.identity_bytes)
            root, current = _verified_upload_sources(root, idempotency_key)
        yield _load_upload_identity(json.loads(body), root, idempotency_key, current), store
