"""One live volume actor's admitted writes and atomic checkpoint reports."""

from __future__ import annotations

import hashlib
import secrets
import threading
from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

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
from sqlalchemy.orm import Session

from riverhog_core.archive_write_grants import ArchiveWriteGrants
from riverhog_core.catalog_models import CollectionArchiveObjectUploadRecord, CollectionUploadRecord
from riverhog_core.ports.archive_objects import ArchiveObjectWriteExecutor
from riverhog_core.provider_write_dispatch import (
    ArmedProviderWrite,
    ClosedProviderWrite,
    ProviderWriteReport,
    WithdrawnProviderWrite,
)
from riverhog_core.services.archive_write_admission import (
    ArchiveWriteAdmission,
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
        self._armed_lock = threading.Lock()
        self._withdrawal_lock = threading.Lock()
        self._retirement_lock = threading.Lock()
        self._withdrawals: deque[WithdrawnProviderWrite] = deque()
        self._pending: list[tuple[ArmedProviderWrite, WriteExecutionResult]] = []
        self._retire: deque[ArmedProviderWrite] = deque()
        self._confirmed_checkpoint: tuple[str | None, str | None] | None = None

    def execute_write_operation(
        self, operation: WriteOperation, *, content: bytes = b""
    ) -> WriteExecutionResult:
        admitted = self.arm_write_operation(operation)
        return self.execute_admitted_write(admitted, content=content)

    def arm_write_operation(self, operation: WriteOperation) -> AdmittedArchiveUploadWrite:
        """Commit the original child intent without starting its provider."""
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
        with self._armed_lock:
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
        with self._armed_lock:
            self._require_admitted(admitted)
        ticket, invocation = admitted.ticket, admitted.invocation
        if isinstance(invocation.operation, WritePartOperation):
            if (
                len(content) != invocation.operation.request.stored_bytes
                or hashlib.sha256(content).hexdigest() != invocation.operation.stored_sha256
            ):
                raise Conflict("archive ciphertext differs from its admitted commitment")
        elif content:
            raise Conflict("archive control invocation cannot accept payload bytes")
        with self._armed_lock:
            self._require_admitted(admitted)
            self._armed.pop(ticket.call_id)
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
        self._pending.append((ticket, response))
        if response.outcome == "no_effect":
            self._report(None, kind=None)
        return response

    def withdraw_admitted_write(self, admitted: AdmittedArchiveUploadWrite) -> None:
        """Retain proof across settlement failures when a sibling cannot admit."""
        with self._armed_lock:
            self._require_admitted(admitted)
            proof = admitted.ticket.withdraw()
            self._withdrawals.append(proof)
            self._armed.pop(admitted.ticket.call_id)
        self.flush_withdrawals()

    def flush_withdrawals(self) -> None:
        """Retry only retained original proofs; never reconstruct or resend work."""
        with self._withdrawal_lock:
            while True:
                with self._armed_lock:
                    if not self._withdrawals:
                        break
                    proof = self._withdrawals[0]
                self._admission.settle_withdrawn(proof)
                self._retire.append(proof.ticket)
                with self._armed_lock:
                    self._withdrawals.popleft()
        self._retire_confirmed()

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

    def _retire_confirmed(self) -> None:
        with self._retirement_lock:
            while self._retire:
                ticket = self._retire[0]
                try:
                    self._admission.acknowledge_report(ticket)
                except NotFound:
                    # This live actor already received exact settlement confirmation;
                    # absence here is only a lost acknowledgement of its own retirement.
                    pass
                self._retire.popleft()

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
