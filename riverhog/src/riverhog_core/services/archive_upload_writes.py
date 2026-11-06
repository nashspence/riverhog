"""One live volume actor's admitted writes and atomic checkpoint reports."""

from __future__ import annotations

import hashlib
import secrets
import threading
from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Literal

from riverhog_protocol.errors import Conflict, NotFound
from riverhog_storage_adapter_protocol import (
    WriteCallIdentity,
    WriteExecutionResult,
    WriteInvocation,
    WriteOperation,
    WritePartOperation,
    validate_write_execution,
    write_operation_sha256,
)
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from riverhog_core.archive_write_grants import ArchiveWriteGrants
from riverhog_core.catalog_archive_write_models import ArchiveUploadTerminalRecord
from riverhog_core.catalog_db import SessionFactory, catalog_write_contention, session_scope
from riverhog_core.catalog_models import CollectionArchiveObjectUploadRecord, CollectionUploadRecord
from riverhog_core.ports.archive_objects import ArchiveObjectWriteExecutor
from riverhog_core.provider_write_dispatch import (
    ArmedProviderWrite,
    ClosedProviderWrite,
    ProviderWriteReport,
    WithdrawnProviderWrite,
)
from riverhog_core.services.archive_upload_drain import inspect_terminal_write_records
from riverhog_core.services.archive_write_admission import (
    ArchiveWriteAdmission,
    ArchiveWriteRefused,
    Guard,
)
from riverhog_core.stores.sqlalchemy_archive_upload_checkpoints import (
    SqlAlchemyArchiveUploadCheckpointStore,
)


@dataclass(frozen=True, slots=True)
class AdmittedArchiveUploadWrite:
    """One actor's original admitted invocation, before physical dispatch."""

    ticket: ArmedProviderWrite = field(repr=False)
    invocation: WriteInvocation = field(repr=False)
    _nonce: str = field(repr=False)
    _request_sha256: str = field(repr=False)


class ArchiveUploadWrites:
    """Retain each request before dispatch; report retries never invoke the provider.

    The owning uploader saves after begin, each logical part, and completion.
    A logical part can contain several separately admitted provider segments.
    This actor is scoped to that uploader invocation, not shared between units.
    """

    def __init__(
        self,
        factory: SessionFactory,
        *,
        admission: ArchiveWriteAdmission,
        checkpoints: SqlAlchemyArchiveUploadCheckpointStore,
        executor: ArchiveObjectWriteExecutor,
        collection_id: int,
        object_id: str,
        epoch: str,
        grants: ArchiveWriteGrants,
        guard: Guard,
    ) -> None:
        self._factory = factory
        self._admission = admission
        self._checkpoints = checkpoints
        self._executor = executor
        self._collection_id = collection_id
        self._object_id = object_id
        self._epoch = epoch
        self._grants = grants
        self._result_capacity_bytes = grants.result_bytes
        self._guard = guard
        self._executor_id = secrets.token_hex(32)
        self._armed: dict[str, AdmittedArchiveUploadWrite] = {}
        self._lock = threading.RLock()
        self._retained: dict[str, ArmedProviderWrite] = {}
        self._admission_pending = False
        self._running = 0
        self._terminal_operation_id: str | None = None
        self._terminal_drained = False
        self._report_input: tuple[str | None, str | None] | None = None
        self._withdrawals: deque[WithdrawnProviderWrite] = deque()
        self._pending: list[tuple[ArmedProviderWrite, WriteExecutionResult]] = []
        self._retire: deque[ArmedProviderWrite] = deque()
        self._confirmed_checkpoint: tuple[str | None, str | None] | None = None

    @property
    def has_pending(self) -> bool:
        """Local receiving/accounting obligations, never global quiescence."""
        return self._admission_pending or bool(self._retained) or self._running != 0

    def drain_terminal(self, operation_id: str) -> Literal["busy", "pending", "closed"]:
        """Fence this original actor without inventing a format checkpoint.

        One turn withdraws at most one unused invocation and receives at most one
        retained checkpoint batch. Unknown execution/admission stays charged.
        Closed grants no worker join, session abort, disposal or global drainage.
        """
        if not self._lock.acquire(blocking=False):
            return "busy"
        try:
            if self._terminal_operation_id is not None:
                if self._terminal_operation_id != operation_id:
                    raise Conflict("archive upload actor has another terminal request")
                if self._terminal_drained:
                    return "closed"
            else:
                try:
                    with session_scope(self._factory) as session:
                        inspect_terminal_write_records(
                            session, self._collection_id, operation_id, nowait=True
                        )
                        terminal = session.get(ArchiveUploadTerminalRecord, operation_id)
                        assert terminal is not None
                        if terminal.original_epoch != self._epoch:
                            raise Conflict(
                                "archive upload actor epoch differs from terminal selection"
                            )
                except OperationalError as error:
                    if catalog_write_contention(error):
                        return "busy"
                    raise
                self._terminal_operation_id = operation_id
            if self._running:
                return "busy"
            if self._admission_pending:
                return "pending"
            if self._armed:
                admitted = next(iter(self._armed.values()))
                proof = admitted.ticket.withdraw()
                self._withdrawals.append(proof)
                self._armed.pop(admitted.ticket.call_id)
            self.flush_withdrawals(limit=1)
            if self._report_input is not None:
                checkpoint, kind = self._report_input
                self._report(checkpoint, kind=kind)
            if self.has_pending:
                return "pending"
            self._terminal_drained = True
            return "closed"
        finally:
            self._lock.release()

    @property
    def scope(self) -> tuple[int, str, str]:
        """Describe this actor without granting admission or dispatch authority."""
        return self._collection_id, self._object_id, self._epoch

    def execute_write_operation(
        self, operation: WriteOperation, *, content: bytes = b""
    ) -> WriteExecutionResult:
        admitted = self.arm_write_operation(operation)
        return self.execute_admitted_write(admitted, content=content)

    def arm_write_operation(self, operation: WriteOperation) -> AdmittedArchiveUploadWrite:
        """Commit the original child intent without starting its provider."""
        with self._lock:
            if self._terminal_operation_id is not None:
                raise Conflict("archive upload actor was stopped for its terminal request")
            if self._admission_pending:
                raise Conflict("original archive upload admission remains unconfirmed")
            if self._report_input is not None:
                raise Conflict("original archive upload checkpoint report remains unconfirmed")
            self.flush_withdrawals()
            self._retire_confirmed()
            arguments: dict[str, Any] = {
                "operation": operation.model_dump(mode="json"),
                "provider_request_sha256": write_operation_sha256(operation),
            }
            if isinstance(operation, WritePartOperation):
                arguments["number"] = operation.request.number
                resource = f"part:{operation.request.number}"
            else:
                resource = operation.kind
            self._admission_pending = True
            try:
                ticket = self._admission.arm(
                    self._collection_id,
                    self._object_id,
                    epoch=self._epoch,
                    kind=operation.kind,
                    resource=resource,
                    arguments=arguments,
                    executor_id=self._executor_id,
                    result_capacity_bytes=self._result_capacity_bytes,
                    grants=self._grants,
                    guard=self._guard,
                )
            except ArchiveWriteRefused:
                self._admission_pending = False
                raise
            self._retained[ticket.call_id] = ticket
            self._admission_pending = False
            invocation = WriteInvocation(
                call=WriteCallIdentity(
                    call_id=ticket.call_id,
                    exact_request_sha256=arguments["provider_request_sha256"],
                    target_incarnation=ticket.target_incarnation,
                    dispatch_owner=ticket.executor_id,
                ),
                operation=operation,
            )
            admitted = AdmittedArchiveUploadWrite(
                ticket=ticket,
                invocation=WriteInvocation.model_validate_json(invocation.model_dump_json()),
                _nonce=ticket._nonce,
                _request_sha256=ticket.request_sha256,
            )
            self._armed[ticket.call_id] = admitted
            return admitted

    def _require_admitted(self, admitted: AdmittedArchiveUploadWrite) -> None:
        ticket, invocation = admitted.ticket, admitted.invocation
        if (
            self._armed.get(ticket.call_id) is not admitted
            or ticket.collection_id != self._collection_id
            or ticket.object_id != self._object_id
            or ticket.call_id != invocation.call.call_id
            or ticket.executor_id != invocation.call.dispatch_owner
            or ticket.target_incarnation != invocation.call.target_incarnation
            or ticket._nonce != admitted._nonce
            or ticket.request_sha256 != admitted._request_sha256
        ):
            raise Conflict("archive invocation was not admitted by this live actor")
        invocation._require_exact_request()

    def execute_admitted_write(
        self, admitted: AdmittedArchiveUploadWrite, *, content: bytes = b""
    ) -> WriteExecutionResult:
        with self._lock:
            self._require_admitted(admitted)
            if self._terminal_operation_id is not None:
                raise Conflict("archive upload actor was stopped for its terminal request")
            ticket, invocation = admitted.ticket, admitted.invocation
            if isinstance(invocation.operation, WritePartOperation):
                if (
                    len(content) != invocation.operation.request.stored_bytes
                    or hashlib.sha256(content).hexdigest() != invocation.operation.stored_sha256
                ):
                    raise Conflict("archive ciphertext differs from its admitted commitment")
            elif content:
                raise Conflict("archive control invocation cannot accept payload bytes")
            self._armed.pop(ticket.call_id)
            self._running += 1
        try:
            try:
                response = ticket.dispatch(
                    lambda: self._executor.execute_write(invocation, content=content)
                )
                validate_write_execution(invocation, response)
            except BaseException:
                self._admission.record_unknown(ticket)
                raise
            if response.outcome == "unknown":
                self._admission.record_unknown(ticket)
                raise Conflict("archive write execution remains unknown")
            with self._lock:
                self._pending.append((ticket, response))
                if response.outcome == "no_effect":
                    self._report(None, kind=None)
            return response
        finally:
            with self._lock:
                self._running -= 1

    def withdraw_admitted_write(self, admitted: AdmittedArchiveUploadWrite) -> None:
        """Retain proof across settlement failures when a sibling cannot admit."""
        with self._lock:
            self._require_admitted(admitted)
            proof = admitted.ticket.withdraw()
            self._withdrawals.append(proof)
            self._armed.pop(admitted.ticket.call_id)
            self.flush_withdrawals()

    def flush_withdrawals(self, *, limit: int | None = None) -> None:
        """Retry only retained original proofs; never reconstruct or resend work."""
        with self._lock:
            settled = 0
            while limit is None or settled < limit:
                if not self._withdrawals:
                    break
                proof = self._withdrawals[0]
                self._admission.settle_withdrawn(proof)
                self._retire.append(proof.ticket)
                self._withdrawals.popleft()
                settled += 1
            self._retire_confirmed(limit=limit)

    @staticmethod
    def _report_result(
        response: WriteExecutionResult, checkpoint_sha256: str | None
    ) -> Mapping[str, Any]:
        # Observation and later closure commit the same outcome, independently of
        # execution status. The catalog and provider request hashes have different preimages.
        return {
            "call": response.call.model_dump(mode="json"),
            "outcome": response.outcome,
            "result": response.result.model_dump(mode="json") if response.result else None,
            "checkpoint_sha256": checkpoint_sha256,
        }

    def _report(self, checkpoint_json: str | None, *, kind: str | None) -> str | None:
        with self._lock:
            checkpoint_identity = (
                kind,
                hashlib.sha256(checkpoint_json.encode()).hexdigest()
                if checkpoint_json is not None
                else None,
            )
            reports = []
            for ticket, response in self._pending:
                result = self._report_result(response, checkpoint_identity[1])
                closure = (
                    ClosedProviderWrite(
                        call_id=ticket.call_id,
                        request_sha256=ticket.request_sha256,
                        executor_id=ticket.executor_id,
                        target_incarnation=ticket.target_incarnation,
                        result=result,
                    )
                    if response.execution == "closed"
                    else None
                )
                reports.append(ProviderWriteReport(ticket, result, closure))
            if not reports:
                if checkpoint_json is None or self._confirmed_checkpoint != checkpoint_identity:
                    raise Conflict("archive upload checkpoint has no admitted write outcome")
                self._retire_confirmed()
                return self._load_confirmed_checkpoint(kind)
            report_input = (checkpoint_json, kind)
            if self._report_input is not None and self._report_input != report_input:
                raise Conflict("original archive upload checkpoint report changed")
            self._report_input = report_input
            saved = []

            def apply(
                session: Session,
                upload: CollectionUploadRecord,
                volume: CollectionArchiveObjectUploadRecord,
                result: Mapping[str, Any],
            ) -> None:
                if checkpoint_json is None:
                    return
                save = (
                    self._checkpoints.apply_pack_upload_checkpoint
                    if kind == "pack"
                    else self._checkpoints.apply_raw_upload_checkpoint
                )
                saved.append(
                    save(
                        session=session,
                        upload=upload,
                        record=volume,
                        checkpoint_json=checkpoint_json,
                    )
                )

            self._admission.apply_reports(tuple(reports), apply_result=apply)
            # Only confirmed settlement can retire a reservation. A lost report
            # acknowledgement leaves the same pending reports available for retry.
            self._retire.extend(report.ticket for report in reports if report.closure is not None)
            self._pending.clear()
            self._report_input = None
            self._confirmed_checkpoint = checkpoint_identity
            self._retire_confirmed()
            if checkpoint_json is None:
                return None
            if saved:
                return saved[0]
            return self._load_confirmed_checkpoint(kind)

    def _load_confirmed_checkpoint(self, kind: str | None) -> str:
        load = (
            self._checkpoints.load_pack_upload_checkpoint
            if kind == "pack"
            else self._checkpoints.load_raw_upload_checkpoint
        )
        current = load(collection_id=self._collection_id, volume_id=self._object_id)
        if current is None:
            raise RuntimeError("confirmed archive write checkpoint is missing")
        return current

    def _retire_confirmed(self, *, limit: int | None = None) -> None:
        with self._lock:
            retired = 0
            while self._retire and (limit is None or retired < limit):
                ticket = self._retire[0]
                try:
                    self._admission.acknowledge_report(ticket)
                except NotFound:
                    # This live actor already received exact settlement confirmation;
                    # absence here is only a lost acknowledgement of its own retirement.
                    pass
                self._retire.popleft()
                self._retained.pop(ticket.call_id)
                retired += 1

    def _identity(self, collection_id: int, volume_id: str) -> None:
        if collection_id != self._collection_id or volume_id != self._object_id:
            raise Conflict("archive upload write actor identity changed")

    def load_pack_upload_checkpoint(self, *, collection_id: int, volume_id: str) -> str | None:
        self._identity(collection_id, volume_id)
        return self._checkpoints.load_pack_upload_checkpoint(
            collection_id=collection_id, volume_id=volume_id
        )

    def load_raw_upload_checkpoint(self, *, collection_id: int, volume_id: str) -> str | None:
        self._identity(collection_id, volume_id)
        return self._checkpoints.load_raw_upload_checkpoint(
            collection_id=collection_id, volume_id=volume_id
        )

    def merge_pack_upload_checkpoint(
        self, *, collection_id: int, volume_id: str, checkpoint_json: str
    ) -> str:
        self._identity(collection_id, volume_id)
        result = self._report(checkpoint_json, kind="pack")
        assert result is not None
        return result

    def merge_raw_upload_checkpoint(
        self, *, collection_id: int, volume_id: str, checkpoint_json: str
    ) -> str:
        self._identity(collection_id, volume_id)
        result = self._report(checkpoint_json, kind="segment")
        assert result is not None
        return result

    def delete_pack_upload_checkpoint(self, *, collection_id: int, volume_id: str) -> None:
        raise Conflict("forward archive write actor cannot dispose checkpoints")

    def delete_raw_upload_checkpoint(self, *, collection_id: int, volume_id: str) -> None:
        raise Conflict("forward archive write actor cannot dispose checkpoints")
