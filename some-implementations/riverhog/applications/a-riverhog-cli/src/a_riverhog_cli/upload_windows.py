"""Explicit application placement and resources for a retained Windows upload."""

from __future__ import annotations

import json
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

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
                    if store.prepare_storage_step(
                        lambda storage: sqlite_primary_preparation_step(
                            storage, plan=selection.readiness
                        )
                    ):
                        return
                raise PrimaryStateBusy(
                    "upload primary preparation requires another execution grant"
                )

            body = initialize_upload_identity(
                identity,
                original,
                byte_limit=selection.identity_bytes,
                prepare_payload=prepare,
            )
        else:
            body = read_upload_identity(identity, byte_limit=selection.identity_bytes)
            root, current = _verified_upload_sources(root, idempotency_key)
        yield _load_upload_identity(json.loads(body), root, idempotency_key, current), store
