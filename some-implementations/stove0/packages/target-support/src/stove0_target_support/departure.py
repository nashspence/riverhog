"""Component-owned deferred, artifact-free departure effects."""

from __future__ import annotations

import os
import threading
import time
from collections.abc import Callable
from pathlib import Path

from http_api_contracts.control import ControlBudgetExhausted, finite_control_seconds
from http_api_contracts.metadata_contact import BoundedMetadataContacts
from pydantic import BaseModel
from stove0_extension_support import ExclusiveStateOwner, ExecutionAdmission, ExecutionPermit
from stove0_extension_support.dispatch import BoundedExecutionDispatcher, ExecutionDispatch
from stove0_extension_support.state import write_state_model
from stove0_protocol import canonical_json_bytes
from stove0_target_protocol import (
    DepartureEffectIntent,
    DepartureEffectReceipt,
    DepartureEffectStatus,
    DepartureEffectTargetDescriptor,
)

from stove0_target_support.http_binding import TargetServiceError


class DepartureExecutionSession:
    """Cancellation is a request; a durable exact receipt proves completion."""

    def __init__(
        self,
        intent: DepartureEffectIntent,
        cancellation: threading.Event,
        completed: Callable[[DepartureEffectReceipt], None],
    ) -> None:
        self.intent = intent
        self.cancellation = cancellation
        self._completed = completed

    def record_completed(self, receipt: DepartureEffectReceipt) -> None:
        if (
            receipt.departure_id != self.intent.departure_id
            or receipt.target_identity != self.intent.target_identity
        ):
            raise ValueError("departure receipt differs from the exact accepted intent")
        self._completed(receipt)


class PersistentDepartureEffectService:
    """No control handler runs an effect or waits for its execution resources.

    Fresh PUTs reauthorize queued work. A started, uncertain effect is never
    replayed automatically, including after restart. An implementation may
    reconcile it with a bounded, read-only lookup of its own exact receipt.
    The supplied executor owns effect idempotency and child containment.
    """

    def __init__(
        self,
        *,
        descriptor: DepartureEffectTargetDescriptor,
        state_root: Path,
        execute: Callable[[DepartureExecutionSession], DepartureEffectReceipt],
        authorize: Callable[[DepartureEffectIntent, float], None] | None = None,
        reconcile: Callable[[DepartureEffectIntent, float], DepartureEffectReceipt | None]
        | None = None,
        maximum_workers: int = 1,
        maximum_pending_jobs: int = 256,
        execution_admission: ExecutionAdmission | None = None,
        admission_retry_seconds: float = 1.0,
        reconciliation_seconds: float = 1.0,
    ) -> None:
        if isinstance(maximum_pending_jobs, bool) or maximum_pending_jobs < 1:
            raise ValueError("departure queue budget must be positive")
        if state_root.is_symlink():
            raise ValueError("departure state root must not be a symlink")
        self.state_root = state_root.resolve()
        self.state_root.mkdir(parents=True, mode=0o700, exist_ok=True)
        os.chmod(self.state_root, 0o700)
        self._descriptor = descriptor
        self.execute = execute
        self.authorize = authorize
        self.reconcile = reconcile
        self.reconciliation_seconds = finite_control_seconds(reconciliation_seconds)
        self._receipt_contacts = BoundedMetadataContacts(maximum_contacts=1)
        self.maximum_pending_jobs = maximum_pending_jobs
        self._lock = threading.RLock()
        self._closing = False
        self._metadata_shutdown: list[Callable[[], None]] = []
        self._shutdown: set[str] = set()
        self._nonterminal = 0
        self._owner = ExclusiveStateOwner(self.state_root)
        try:
            self._recover()
            self._dispatch = BoundedExecutionDispatcher(
                state_owner=self._owner,
                maximum_workers=maximum_workers,
                admission=execution_admission,
                retry_seconds=admission_retry_seconds,
            )
        except BaseException:
            self._owner.close()
            raise

    def descriptor(self) -> DepartureEffectTargetDescriptor:
        return self._descriptor

    def _validate(self, intent: DepartureEffectIntent) -> None:
        if intent.target_identity != self._descriptor.target_identity:
            raise TargetServiceError(409, "target_descriptor_mismatch", "departure target changed")

    def put_departure_effect(self, intent: DepartureEffectIntent) -> DepartureEffectStatus:
        self._validate(intent)
        with self._lock:
            status = self._accept(intent)
            if status.state in {"completed", "canceled"}:
                return status
            if self._closing:
                raise TargetServiceError(
                    503, "admission_unavailable", "departure service is closing"
                )
            if status.state == "queued":
                self._dispatch.enqueue(
                    intent.departure_id,
                    ExecutionDispatch(
                        invocation_id=intent.departure_id,
                        attempt=status.attempt,
                        authorize=lambda deadline: self._authorize(intent, deadline),
                        prepare=lambda canceled, permit: self._prepare(intent, canceled, permit),
                        deferred=lambda: None,
                        failed=lambda exc: self._failed(intent, exc),
                        finished=lambda: None,
                    ),
                )
        return self._reconcile_stopped(intent, status)

    def _authorize(self, intent: DepartureEffectIntent, deadline: float) -> None:
        self._validate(intent)
        if self.authorize is not None:
            self.authorize(intent, deadline)
        if time.monotonic() >= deadline:
            raise TimeoutError("departure authorization exceeded its control allowance")

    def get_departure_effect(self, departure_id: str) -> DepartureEffectStatus:
        with self._lock:
            status = self._status(departure_id)
            intent = self._load(departure_id, "accepted", DepartureEffectIntent)
        if intent is None:
            raise ValueError("departure status lost its exact accepted intent")
        return self._reconcile_stopped(intent, status)

    def _status(self, departure_id: str) -> DepartureEffectStatus:
        status = self._load(departure_id, "status", DepartureEffectStatus)
        if status is None:
            raise TargetServiceError(404, "job_not_found", "departure effect not found")
        return status

    def _reconcile_stopped(
        self, intent: DepartureEffectIntent, status: DepartureEffectStatus
    ) -> DepartureEffectStatus:
        reconcile = self.reconcile
        with self._lock:
            if (
                self._closing
                or status.state != "interrupted"
                or reconcile is None
                or intent.departure_id in self._dispatch.active_keys
            ):
                return status

        def read_receipt() -> None:
            # This owner-supplied port performs read-only receipt lookup. It
            # acquires no execution permit and runs outside the state lock.
            receipt = reconcile(intent, time.monotonic() + self.reconciliation_seconds)
            if receipt is not None:
                with self._lock:
                    if (
                        not self._closing
                        and self._status(intent.departure_id).state == "interrupted"
                    ):
                        self._complete(intent, receipt)

        try:
            self._receipt_contacts.call(
                str(self.state_root), read_receipt, maximum_seconds=self.reconciliation_seconds
            )
        except (ControlBudgetExhausted, OSError):
            # An actual outstanding lookup retains its slot. Repeated control
            # calls cannot spawn an unbounded pile of timed-out lookup workers.
            pass
        with self._lock:
            return self._status(intent.departure_id)

    def cancel_departure_effect(self, intent: DepartureEffectIntent) -> DepartureEffectStatus:
        self._validate(intent)
        with self._lock:
            existing = self._load(intent.departure_id, "accepted", DepartureEffectIntent)
            if existing is not None and existing != intent:
                raise TargetServiceError(409, "job_request_mismatch", "departure intent changed")
            # Write the fence first: a crash or a lost first PUT cannot revive it.
            self._write(intent.departure_id, "cancel", intent)
            status = self._accept(intent, cancellation=True)
            self._dispatch.cancel(intent.departure_id)
            if status.state == "queued":
                status = self._transition(status, "canceled")
            elif status.state == "running":
                status = self._transition(status, "canceling")
            self._write(intent.departure_id, "status", status)
        return self._reconcile_stopped(intent, status)

    def _accept(
        self, intent: DepartureEffectIntent, *, cancellation: bool = False
    ) -> DepartureEffectStatus:
        existing = self._load(intent.departure_id, "accepted", DepartureEffectIntent)
        if existing is not None and existing != intent:
            raise TargetServiceError(409, "job_request_mismatch", "departure intent changed")
        status = self._load(intent.departure_id, "status", DepartureEffectStatus)
        if status is not None:
            return status
        if not cancellation and self._nonterminal >= self.maximum_pending_jobs:
            raise TargetServiceError(
                503, "admission_unavailable", "departure admission queue is full"
            )
        self._write(intent.departure_id, "accepted", intent)
        canceled = self._load(intent.departure_id, "cancel", DepartureEffectIntent)
        if canceled is not None and canceled != intent:
            raise ValueError("departure cancellation fence differs from accepted intent")
        status = DepartureEffectStatus(
            departure_id=intent.departure_id,
            target_identity=intent.target_identity,
            attempt=1,
            state="canceled" if canceled is not None else "queued",
        )
        if status.state == "queued":
            self._nonterminal += 1
        self._write(intent.departure_id, "status", status)
        return status

    def _prepare(
        self,
        intent: DepartureEffectIntent,
        cancellation: threading.Event,
        _permit: ExecutionPermit,
    ) -> Callable[[], object] | None:
        with self._lock:
            status = self._status(intent.departure_id)
            if self._closing or cancellation.is_set() or status.state != "queued":
                return None
            # This fsynced marker precedes every possible external effect.
            self._write(intent.departure_id, "status", self._transition(status, "running"))
        return lambda: self._run(intent, cancellation)

    def _run(self, intent: DepartureEffectIntent, cancellation: threading.Event) -> None:
        session = DepartureExecutionSession(
            intent, cancellation, lambda receipt: self._complete(intent, receipt)
        )
        try:
            receipt = self.execute(session)
            session.record_completed(receipt)
        except BaseException as exc:
            self._failed(intent, exc)
        finally:
            with self._lock:
                status = self._status(intent.departure_id)
                if status.state in {"running", "canceling"}:
                    self._write(
                        intent.departure_id, "status", self._transition(status, "interrupted")
                    )

    def _complete(self, intent: DepartureEffectIntent, receipt: DepartureEffectReceipt) -> None:
        receipt = DepartureEffectReceipt.model_validate_json(
            canonical_json_bytes(receipt.model_dump(mode="json", by_alias=True))
        )
        if (
            receipt.departure_id != intent.departure_id
            or receipt.target_identity != intent.target_identity
        ):
            raise ValueError("departure completion receipt differs from accepted intent")
        with self._lock:
            status = self._status(intent.departure_id)
            if status.state == "completed":
                if status.receipt != receipt:
                    raise ValueError("completed departure receipt changed")
                return
            if status.state not in {"running", "canceling", "interrupted"}:
                raise ValueError("departure completion has no durable start boundary")
            self._write(
                intent.departure_id,
                "status",
                DepartureEffectStatus(
                    departure_id=intent.departure_id,
                    target_identity=intent.target_identity,
                    attempt=status.attempt,
                    state="completed",
                    receipt=receipt,
                ),
            )
            self._nonterminal -= 1

    def _failed(self, intent: DepartureEffectIntent, exc: BaseException) -> None:
        with self._lock:
            status = self._status(intent.departure_id)
            if status.state in {"completed", "canceled"}:
                return
            # No receipt proves neither completion nor absence of an effect.
            self._write(
                intent.departure_id,
                "status",
                self._transition(
                    status,
                    "interrupted",
                    failure=f"{type(exc).__name__}: {exc}"[:1000],
                ),
            )

    def _transition(
        self, status: DepartureEffectStatus, state: str, *, failure: str | None = None
    ) -> DepartureEffectStatus:
        if state == "canceled" and status.state != "canceled":
            self._nonterminal -= 1
        return DepartureEffectStatus.model_validate(
            {**status.model_dump(mode="json"), "state": state, "failure": failure}
        )

    def _path(self, departure_id: str, kind: str) -> Path:
        if len(departure_id) != 64 or any(
            character not in "0123456789abcdef" for character in departure_id
        ):
            raise TargetServiceError(400, "invalid_target_request", "invalid departure identity")
        return self.state_root / f"{departure_id}.{kind}.json"

    def _load[Model: BaseModel](
        self, departure_id: str, kind: str, model: type[Model]
    ) -> Model | None:
        path = self._path(departure_id, kind)
        if path.is_symlink():
            raise ValueError("departure state must not be a symlink")
        return model.model_validate_json(path.read_bytes()) if path.exists() else None

    def _write(self, departure_id: str, kind: str, model: BaseModel) -> None:
        write_state_model(self.state_root, self._path(departure_id, kind), model)

    def _recover(self) -> None:
        for path in self.state_root.glob("*.cancel.json"):
            intent = DepartureEffectIntent.model_validate_json(path.read_bytes())
            if path != self._path(intent.departure_id, "cancel") or path.is_symlink():
                raise ValueError("departure cancellation file differs from its identity")
            self._accept(intent, cancellation=True)
        for path in self.state_root.glob("*.accepted.json"):
            intent = DepartureEffectIntent.model_validate_json(path.read_bytes())
            if path != self._path(intent.departure_id, "accepted") or path.is_symlink():
                raise ValueError("departure declaration file differs from its identity")
            status = self._load(intent.departure_id, "status", DepartureEffectStatus)
            if status is None:
                self._accept(intent)
                status = self._status(intent.departure_id)
            if (
                status.departure_id != intent.departure_id
                or status.target_identity != intent.target_identity
            ):
                raise ValueError("departure state differs from accepted intent")
            if status.state in {"running", "canceling"}:
                self._write(
                    intent.departure_id,
                    "status",
                    self._transition(
                        status,
                        "interrupted",
                        failure="departure execution interrupted by process restart",
                    ),
                )
        for path in self.state_root.glob("*.status.json"):
            status = DepartureEffectStatus.model_validate_json(path.read_bytes())
            if (
                path != self._path(status.departure_id, "status")
                or path.is_symlink()
                or not self._path(status.departure_id, "accepted").exists()
            ):
                raise ValueError("departure status has no exact accepted declaration")
        self._nonterminal = sum(
            self._status(path.name[:64]).state not in {"completed", "canceled"}
            for path in self.state_root.glob("*.status.json")
        )

    def register_metadata_shutdown(self, close: Callable[[], None]) -> None:
        with self._lock:
            if self._closing:
                raise RuntimeError("component metadata ownership is shutting down")
            self._metadata_shutdown.append(close)

    def close(self) -> None:
        with self._lock:
            if self._closing:
                return
            self._closing = True
            for key in self._dispatch.active_keys:
                self._shutdown.add(key)
                self._dispatch.cancel(key)
        try:
            for close in self._metadata_shutdown:
                close()
            self._dispatch.close()
        finally:
            self._owner.close()


__all__ = ["DepartureExecutionSession", "PersistentDepartureEffectService"]
