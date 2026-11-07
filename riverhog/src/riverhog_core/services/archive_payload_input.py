"""Retain exact plaintext input separately from ciphertext and physical slots."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from threading import Lock, RLock
from typing import Literal, Protocol

from riverhog_canonical_json import canonical_json_bytes
from riverhog_protocol.errors import BadRequest, Conflict

from riverhog_core.app_permissions import Principal
from riverhog_core.archive_input_grants import ArchiveInputGrants
from riverhog_core.services.archive_upload_execution import ArchiveUploadExecutionKey

InputRefusal = Literal["busy", "input-capacity", "target-input-capacity"]


class _PayloadHash(Protocol):
    def update(self, content: bytes) -> None: ...
    def hexdigest(self) -> str: ...


@dataclass(frozen=True, slots=True)
class ArchivePayloadRequest:
    key: ArchiveUploadExecutionKey
    target_incarnation: str
    epoch: str
    volume_id: str
    unit: int
    plan_sha256: str
    plaintext_bytes: int
    principal: Principal

    def __post_init__(self) -> None:
        if not self.target_incarnation or not self.epoch or not self.volume_id:
            raise ValueError("payload input requires its exact construction and volume")
        if type(self.unit) is not int or self.unit < 0:
            raise ValueError("payload input unit must be nonnegative")
        if type(self.plaintext_bytes) is not int or self.plaintext_bytes < 0:
            raise ValueError("payload input size must be nonnegative")
        if re.fullmatch(r"[a-f0-9]{64}", self.plan_sha256) is None:
            raise ValueError("payload input requires its exact plan")

    def commitment(self, payload_sha256: str) -> str:
        principal = self.principal
        return hashlib.sha256(
            canonical_json_bytes(
                {
                    "collection_id": self.key.collection_id,
                    "construction_sha256": self.key.construction_sha256,
                    "target_incarnation": self.target_incarnation,
                    "epoch": self.epoch,
                    "volume_id": self.volume_id,
                    "unit": self.unit,
                    "plan_sha256": self.plan_sha256,
                    "plaintext_bytes": self.plaintext_bytes,
                    "payload_sha256": payload_sha256,
                    "principal": {
                        "id": str(principal.id),
                        "key_id": principal.key_id,
                        "access": sorted(
                            [value.permission, value.resource] for value in principal.access
                        ),
                        "unrestricted_delegation": principal.unrestricted_delegation,
                        "artifact_scope_capability_id": principal.artifact_scope_capability_id,
                        "authorization_view_identity": principal.authorization_view_identity,
                    },
                }
            )
        ).hexdigest()


class ArchivePayloadInputs:
    """Reserve before reading; one service owns all targets' input counters.

    Ingress reserves two complete payloads: its mutable assembly and the final
    contiguous immutable input can coexist. Sealing drops the assembly and
    reduces that charge. Retained input never borrows the ciphertext budget.
    """

    def __init__(self, grants: ArchiveInputGrants) -> None:
        self._grants = grants
        self._lock = Lock()
        self._inputs: dict[int, ArchivePayloadInput] = {}
        self._bytes = 0
        self._constructions: dict[ArchiveUploadExecutionKey, int] = {}
        self._targets: dict[str, tuple[int, int]] = {}

    def reserve(self, request: ArchivePayloadRequest) -> ArchivePayloadInput | InputRefusal:
        amount = request.plaintext_bytes * 2
        if not self._lock.acquire(blocking=False):
            return "busy"
        try:
            count, used = self._targets.get(request.target_incarnation, (0, 0))
            if len(self._inputs) >= self._grants.inputs or (
                self._bytes + amount > self._grants.plaintext_bytes
            ):
                return "input-capacity"
            if count >= self._grants.inputs_per_target or (
                used + amount > self._grants.plaintext_bytes_per_target
            ):
                return "target-input-capacity"
            original = ArchivePayloadInput(self, request, amount)
            self._inputs[id(original)] = original
            self._bytes += amount
            self._constructions[request.key] = self._constructions.get(request.key, 0) + 1
            self._targets[request.target_incarnation] = count + 1, used + amount
            return original
        finally:
            self._lock.release()

    def status(self, key: ArchiveUploadExecutionKey) -> Literal["busy", "untracked", "retained"]:
        """Observe exact local input retention; absence grants no global closure."""
        if not self._lock.acquire(blocking=False):
            return "busy"
        try:
            return "retained" if key in self._constructions else "untracked"
        finally:
            self._lock.release()

    def _require(self, original: ArchivePayloadInput) -> None:
        with self._lock:
            if self._inputs.get(id(original)) is not original:
                raise Conflict("payload input is not this original reservation")

    def _resize(self, original: ArchivePayloadInput, amount: int, *, retire: bool = False) -> None:
        with self._lock:
            if self._inputs.get(id(original)) is not original:
                raise Conflict("payload input is not this original reservation")
            assert 0 <= amount <= original._charged
            delta = original._charged - amount
            count, used = self._targets[original.request.target_incarnation]
            self._bytes -= delta
            original._charged = amount
            count -= retire
            used -= delta
            if count:
                self._targets[original.request.target_incarnation] = count, used
            else:
                del self._targets[original.request.target_incarnation]
            if retire:
                del self._inputs[id(original)]
                key = original.request.key
                remaining = self._constructions[key] - 1
                if remaining:
                    self._constructions[key] = remaining
                else:
                    del self._constructions[key]


class ArchivePayloadInput:
    """One streamed body and its original retained receiving owner.

    No timeout, response completion or failed callback releases this input.
    Only an unused uninstalled body, or exact joined receiving after its actor
    has closed every plaintext-consuming stack, can retire the reservation.
    """

    def __init__(
        self, pool: ArchivePayloadInputs, request: ArchivePayloadRequest, charged: int
    ) -> None:
        self._request = request
        self._lock = RLock()
        self._pool = pool
        self._charged = charged
        self._buffer: bytearray | None = None
        self._content: bytes | None = None
        self._hash: _PayloadHash | None = hashlib.sha256()
        self._received = 0
        self._payload_sha256: str | None = None
        self._request_sha256: str | None = None
        self._registry: object | None = None
        self._invocation: object | None = None
        self._actor: object | None = None
        self._consumed = False
        self._released = False

    @property
    def request(self) -> ArchivePayloadRequest:
        return self._request

    @property
    def released(self) -> bool:
        return self._released

    @property
    def payload_sha256(self) -> str:
        if self._payload_sha256 is None:
            raise Conflict("payload input has not been sealed")
        return self._payload_sha256

    @property
    def request_sha256(self) -> str:
        if self._request_sha256 is None:
            raise Conflict("payload input has not been sealed")
        return self._request_sha256

    def append(self, content: bytes) -> None:
        with self._lock:
            if self._released or self._payload_sha256 is not None or self._hash is None:
                raise Conflict("payload input is no longer accepting bytes")
            self._pool._require(self)
            end = self._received + len(content)
            if end > self.request.plaintext_bytes:
                raise BadRequest("payload length differs from the exact upload unit")
            if self._buffer is None:
                self._buffer = bytearray(self.request.plaintext_bytes)
            self._buffer[self._received : end] = content
            self._hash.update(content)
            self._received = end

    def seal(self) -> None:
        with self._lock:
            if self._released or self._payload_sha256 is not None:
                raise Conflict("payload input cannot be sealed again")
            if self._received != self.request.plaintext_bytes:
                raise BadRequest("payload length differs from the exact upload unit")
            self._pool._require(self)
            self._content = bytes(self._buffer) if self._buffer is not None else b""
            self._buffer = None
            assert self._hash is not None
            self._payload_sha256 = self._hash.hexdigest()
            self._hash = None
            self._request_sha256 = self.request.commitment(self._payload_sha256)
            self._pool._resize(self, self.request.plaintext_bytes)

    def content(self) -> bytes:
        with self._lock:
            if self._content is None or self._released:
                raise Conflict("original payload input is unavailable")
            return self._content

    def require_submission(
        self,
        registry: object,
        key: ArchiveUploadExecutionKey,
        target: str,
        request_sha256: str,
    ) -> None:
        with self._lock:
            if (self._released and self._registry is not registry) or (
                self.request.key != key
                or self.request.target_incarnation != target
                or self.request_sha256 != request_sha256
                or self._registry is not None
                and self._registry is not registry
            ):
                raise Conflict("payload submission differs from the original input")
            self._registry = registry

    def retain(self, registry: object, invocation: object) -> None:
        with self._lock:
            if self._released or self._invocation is not None or self._registry is not registry:
                raise Conflict("payload input already has an original receiving owner")
            self._registry, self._invocation = registry, invocation

    def require_pool(self, pool: ArchivePayloadInputs) -> None:
        if self._pool is not pool:
            raise Conflict("payload input belongs to another service resource owner")
        self._pool._require(self)

    def require_actor(self, actor: object) -> None:
        with self._lock:
            if (
                self._released
                or self._actor is not actor
                or self._invocation is None
                or self._request_sha256 is None
            ):
                raise Conflict("payload input has no retained original invocation")
            self._pool._require(self)

    def bind_actor(self, actor: object) -> None:
        with self._lock:
            if self._released or self._actor is not None and self._actor is not actor:
                raise Conflict("payload input already belongs to another original actor")
            self._actor = actor

    def consumed(self, actor: object) -> None:
        with self._lock:
            if self._released:
                return
            if self._actor is not actor:
                raise Conflict("payload input closure is not its original actor")
            self._consumed = True

    def receive_joined(self, invocation: object, actor: object | None) -> None:
        """Called by the retaining registry after exact actual-worker receiving."""
        with self._lock:
            if self._released:
                return
            if self._invocation is not invocation or (
                actor is not None and self._actor is not actor
            ):
                raise Conflict("payload receiving is not its original invocation and actor")
            if actor is not None and self._consumed:
                self._release()

    def retire_unstarted(self, invocation: object) -> None:
        with self._lock:
            if self._released:
                return
            if self._invocation is not invocation:
                raise Conflict("payload withdrawal is not its original invocation")
            self._invocation = None
            self._registry = None
            self._release()

    def discard_unretained(self, registry: object | None = None) -> None:
        """Release only this new body when no original invocation retained it."""
        with self._lock:
            if (
                not self._released
                and self._invocation is None
                and (self._registry is None or self._registry is registry)
            ):
                self._release()

    def _release(self) -> None:
        with self._lock:
            self._content = None
            self._buffer = None
            self._hash = None
            self._released = True
            self._pool._resize(self, 0, retire=True)
            self._actor = None
