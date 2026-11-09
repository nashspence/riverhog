"""Keep one charged original cache session across bounded payload invocations."""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass, field
from threading import Lock
from typing import Literal

from riverhog_protocol.errors import Conflict
from sqlalchemy.orm import Session

from riverhog_core.cache_write_grants import CacheWriteGrants
from riverhog_core.catalog_models import CollectionUploadRecord
from riverhog_core.services.archive_payload_input import ArchivePayloadInput
from riverhog_core.services.archive_upload_invocations import (
    ArchiveUploadInvocationPin,
    ArchiveUploadInvocationRegistry,
)
from riverhog_core.services.archive_upload_writes import ArchiveUploadWrites
from riverhog_core.services.archive_write_admission import Guard
from riverhog_core.services.cache_population_publication import CachePopulationPublication
from riverhog_core.services.mirrored_archive_upload_writes import MirroredArchiveUploadWrites

type ArchiveCacheCreate = Callable[[ArchiveUploadWrites, Guard], MirroredArchiveUploadWrites]
type ArchiveCacheValidate = Callable[[Session, CollectionUploadRecord, str, int], None]
type _Scope = tuple[int, str, str, str, str]


class CacheFamilyNotAdmitted(Conflict):
    """No family constructor or original cache operation was entered."""


@dataclass(slots=True)
class _Family:
    pin: ArchiveUploadInvocationPin
    plan_sha256: str
    lock: Lock = field(default_factory=Lock)
    entered: bool = False
    original: object | None = None
    writes: MirroredArchiveUploadWrites | None = None
    complete: bool = False
    terminal: bool = False
    retiring: bool = False


@dataclass(frozen=True, slots=True)
class ArchivePayloadMirror:
    writes: MirroredArchiveUploadWrites
    _family: _Family = field(repr=False)
    _first: bool = False

    @property
    def has_pending(self) -> bool:
        family = self._family
        if not family.pin.active:
            return self.writes.has_pending
        return self._first or self.writes.has_pending_invocation

    def receive_complete(self, publisher: CachePopulationPublication) -> None:
        family = self._family
        with family.lock:
            family.complete = True
        self.writes.flush_cache_reports()
        if self.writes.has_transferable_cache:
            self.writes.transfer_cache(publisher)

    def drain_terminal(self, operation_id: str) -> Literal["busy", "pending", "closed"]:
        family = self._family
        if not family.pin.active:
            return self.writes.drain_terminal(operation_id)
        if not family.lock.acquire(blocking=False):
            return "busy"
        try:
            family.terminal = True
        finally:
            family.lock.release()
        status = (
            self.writes.drain_terminal(operation_id)
            if self._first
            else self.writes.drain_invocation(operation_id)
        )
        if self._first and status == "closed":
            try:
                self.writes.transfer_abandoned(operation_id)
            except Conflict:
                # Other children can still own receiving. No pin is released.
                return "pending"
        return status


type ArchivePayloadMirrorFactory = Callable[[ArchiveUploadWrites, Guard], ArchivePayloadMirror]


class ArchiveCacheFamilies:
    """The map holds only pinned parents, never an accumulating child history.

    Pinning precedes constructor entry. Each independently pending child keeps
    its own invocation reservation; healthy children can retire while the first
    reservation retains the original begin. Removing a map entry precedes
    releasing that last parent pin, after actual physical receiving.
    """

    def __init__(self, invocations: ArchiveUploadInvocationRegistry) -> None:
        self._invocations = invocations
        self._owner = object()
        self._lock = Lock()
        self._families: OrderedDict[_Scope, _Family] = OrderedDict()

    def factory(
        self,
        payload: ArchivePayloadInput,
        *,
        create: ArchiveCacheCreate,
        grants: CacheWriteGrants,
        validate_population: ArchiveCacheValidate,
    ) -> ArchivePayloadMirrorFactory:
        request = payload.request
        scope = (
            request.key.collection_id,
            request.key.construction_sha256,
            request.target_incarnation,
            request.epoch,
            request.volume_id,
        )

        def attach(archive: ArchiveUploadWrites, guard: Guard) -> ArchivePayloadMirror:
            if archive.scope != (request.key.collection_id, request.volume_id, request.epoch):
                raise Conflict("cache family differs from the original archive scope")
            with self._lock:
                family = self._families.get(scope)
                if family is None:
                    pin = self._invocations.pin_family(
                        payload.request_sha256,
                        key=request.key,
                        target_incarnation=request.target_incarnation,
                        epoch=request.epoch,
                        volume_id=request.volume_id,
                        owner=self._owner,
                    )
                    if isinstance(pin, str):
                        raise CacheFamilyNotAdmitted("cache family parent reservation unavailable")
                    family = _Family(pin, request.plan_sha256)
                    self._families[scope] = family
                if family.retiring or family.plan_sha256 != request.plan_sha256:
                    raise Conflict("cache family has another plan or is retiring")
            with family.lock:
                if family.terminal or family.complete or family.retiring:
                    raise Conflict("cache family is closed for fresh invocations")
                if not family.entered:
                    family.entered = True
                    try:
                        family.original = create(archive, guard)
                    except CacheFamilyNotAdmitted as error:
                        raise Conflict("original cache family constructor is unresolved") from error
                    if (
                        not isinstance(family.original, MirroredArchiveUploadWrites)
                        or family.original.archive is not archive
                    ):
                        raise Conflict(
                            "original cache family constructor returned no exact receiver"
                        )
                    family.writes = family.original
                    return ArchivePayloadMirror(family.writes, family, True)
                if family.writes is None:
                    raise Conflict("original cache family constructor is unresolved")
                if family.pin.request_sha256 == payload.request_sha256:
                    if family.writes.archive is not archive:
                        raise Conflict("cache family parent archive identity changed")
                    return ArchivePayloadMirror(family.writes, family, True)
                continuation = family.writes.continue_invocation(
                    archive=archive,
                    principal=request.principal,
                    grants=grants,
                    guard=guard,
                    validate_population=validate_population,
                )
                return ArchivePayloadMirror(continuation, family)

        return attach

    def retire(self, payload: ArchivePayloadInput) -> bool:
        """Point receiving only; never infer closure or run provider work."""
        request = payload.request
        scope = (
            request.key.collection_id,
            request.key.construction_sha256,
            request.target_incarnation,
            request.epoch,
            request.volume_id,
        )
        if not self._lock.acquire(blocking=False):
            return False
        try:
            family = self._families.get(scope)
            if family is None:
                return True
            return self._retire_family(scope, family)
        finally:
            self._lock.release()

    def retire_ready(self, *, limit: int) -> int:
        """Visit bounded live parents fairly; pending originals keep their charge."""
        if type(limit) is not int or limit < 1:
            raise ValueError("cache family retirement limit must be positive")
        if not self._lock.acquire(blocking=False):
            return 0
        try:
            retired = 0
            for _ in range(min(limit, len(self._families))):
                scope, family = self._families.popitem(last=False)
                self._families[scope] = family
                retired += self._retire_family(scope, family)
            return retired
        finally:
            self._lock.release()

    def _retire_family(self, scope: _Scope, family: _Family) -> bool:
        if not family.lock.acquire(blocking=False):
            return False
        try:
            if (
                not (family.complete or family.terminal)
                or family.writes is None
                or family.writes.has_pending
            ):
                return False
            family.retiring = True
            del self._families[scope]
            try:
                released = family.pin.release(owner=self._owner)
            except BaseException:
                if family.pin.active:
                    self._families[scope] = family
                raise
            if not released:
                self._families[scope] = family
                return False
            return True
        finally:
            family.lock.release()
