"""Original immutable metadata calls under the archive construction interlock."""

from __future__ import annotations

import json
import secrets
import threading
from collections.abc import Callable
from dataclasses import dataclass, field

from riverhog_protocol.errors import Conflict, NotFound
from riverhog_storage_adapter_protocol import (
    ImmutableObjectReceipt,
    WriteCallIdentity,
    WriteExecutionResult,
    WriteInvocation,
    validate_write_execution,
    write_operation_sha256,
)
from sqlalchemy import select
from sqlalchemy.orm import Session

from riverhog_core.archive_write_grants import ArchiveWriteGrants
from riverhog_core.catalog_archive_write_models import (
    ArchiveWriteControlRecord,
    ArchiveWriteEffectRecord,
    ArchiveWritePreparationRecord,
)
from riverhog_core.catalog_db import SessionFactory, session_scope
from riverhog_core.catalog_models import StorageIncarnationRecord
from riverhog_core.immutable_archive_identity import require_immutable_archive_receipt
from riverhog_core.ports.archive_objects import ArchiveObjectWriteExecutor
from riverhog_core.provider_write_dispatch import ArmedProviderWrite, WithdrawnProviderWrite
from riverhog_core.services.archive_write_admission import (
    ArchiveWriteAdmission,
    Guard,
    _json,
    _positive,
    _sha,
    _writer,
)
from riverhog_core.services.immutable_archive_preparation import (
    ImmutableArchivePreparation,
    PreparedImmutableArchiveObject,
    ValidateMetadata,
)
from riverhog_core.storage_incarnations import require_storage_incarnation

ApplyImmutableReceipt = Callable[
    [Session, PreparedImmutableArchiveObject, ImmutableObjectReceipt], None
]


@dataclass(frozen=True, slots=True)
class AdmittedImmutableArchiveWrite:
    """The original actor's one-use right, distinct from retained preparation."""

    ticket: ArmedProviderWrite = field(repr=False)
    prepared: PreparedImmutableArchiveObject = field(repr=False)
    invocation_json: str = field(repr=False)

    @property
    def invocation(self) -> WriteInvocation:
        return WriteInvocation.model_validate_json(self.invocation_json)


class ImmutableArchiveAdmission:
    """Share retained-call grants without borrowing a payload-volume identity.

    Current caller guards govern admission. Original returned outcomes can be
    reported after revocation, terminal selection or parent retirement; that
    report grants no new dispatch, finalization or cleanup authority.
    """

    def __init__(self, factory: SessionFactory) -> None:
        self._factory = factory
        self._live: dict[str, AdmittedImmutableArchiveWrite] = {}
        self._live_lock = threading.Lock()

    def arm(
        self,
        prepared: PreparedImmutableArchiveObject,
        *,
        epoch: str,
        executor_id: str,
        grants: ArchiveWriteGrants,
        guard: Guard,
        validate_metadata: ValidateMetadata,
    ) -> AdmittedImmutableArchiveWrite:
        if len(executor_id) != 64 or any(c not in "0123456789abcdef" for c in executor_id):
            raise ValueError("immutable archive executor identity is invalid")
        _positive(grants.result_bytes)
        nonce = secrets.token_hex(32)
        with session_scope(self._factory) as session:
            _writer(session)
            guard(session)
            upload = ArchiveWriteAdmission._upload(session, prepared.collection_id)
            control = ArchiveWriteAdmission._control(session, upload)
            ArchiveWriteAdmission._require_epoch(control, epoch)
            ArchiveWriteAdmission._terminal_lock(session, control)
            if upload.state not in {"open", "closing", "uploading", "finalizing"}:
                raise Conflict("immutable archive construction cannot admit metadata")
            # Configuration reconciliation uses the same binding row lock. Hold
            # it through the reservation commit, including any capacity wait.
            session.scalar(
                select(StorageIncarnationRecord)
                .where(
                    StorageIncarnationRecord.kind == "archive",
                    StorageIncarnationRecord.name == upload.archive_store,
                )
                .with_for_update()
                .execution_options(populate_existing=True)
            )
            if (
                require_storage_incarnation(session, "archive", upload.archive_store)
                != upload.archive_incarnation_id
            ):
                raise Conflict("immutable archive original target incarnation changed")
            record = session.get(
                ArchiveWritePreparationRecord, (prepared.collection_id, prepared.object_id)
            )
            if record is None:
                raise Conflict("immutable archive original preparation is missing")
            original = ImmutableArchivePreparation._original(
                prepared.collection_id, prepared.object_id, control, record
            )
            if original != prepared:
                raise Conflict("immutable archive admitted preparation changed")
            operation = original.operation
            if not upload.archive_storage_prefix or not operation.request.object_path.startswith(
                upload.archive_storage_prefix + "/"
            ):
                raise Conflict("immutable archive object is outside its original construction")
            validate_metadata(session, upload, operation.model_copy(deep=True))
            call_id, nonce, request_sha256 = ArchiveWriteAdmission._reserve_effect(
                session,
                control,
                record,
                collection_id=prepared.collection_id,
                object_id=prepared.object_id,
                kind="put",
                resource="put",
                arguments={
                    "operation": operation.model_dump(mode="json"),
                    "provider_request_sha256": write_operation_sha256(operation),
                },
                executor_id=executor_id,
                result_capacity_bytes=grants.result_bytes,
                grants=grants,
                nonce=nonce,
            )
            incarnation = upload.archive_incarnation_id
        # The original preparation epoch is data identity. The newly admitted
        # call belongs to the separately checked current construction epoch.
        ticket = ArmedProviderWrite(
            call_id=call_id,
            collection_id=prepared.collection_id,
            object_id=prepared.object_id,
            nonce=nonce,
            request_sha256=request_sha256,
            executor_id=executor_id,
            target_incarnation=incarnation,
        )
        invocation = WriteInvocation(
            call=WriteCallIdentity(
                call_id=call_id,
                exact_request_sha256=write_operation_sha256(operation),
                target_incarnation=incarnation,
                dispatch_owner=executor_id,
            ),
            operation=operation,
        )
        admitted = AdmittedImmutableArchiveWrite(ticket, original, invocation.model_dump_json())
        with self._live_lock:
            self._live[call_id] = admitted
        return admitted

    @staticmethod
    def _original_context(
        session: Session, admitted: AdmittedImmutableArchiveWrite
    ) -> ArchiveWriteEffectRecord:
        ticket, prepared, invocation = admitted.ticket, admitted.prepared, admitted.invocation
        # Keep construction-before-child ordering when the source parent still
        # exists. The independent control retains ownership after its retirement.
        upload = session.scalar(ArchiveWriteAdmission._upload_query(ticket.collection_id))
        control = session.get(
            ArchiveWriteControlRecord,
            ticket.collection_id,
            with_for_update=True,
            populate_existing=True,
        )
        if control is None:
            raise NotFound("immutable archive original construction is unavailable")
        if upload is not None:
            ArchiveWriteAdmission._control(session, upload)
        ArchiveWriteAdmission._terminal_lock(session, control)
        record = session.get(
            ArchiveWritePreparationRecord, (ticket.collection_id, ticket.object_id)
        )
        if record is None:
            raise NotFound("immutable archive original preparation is unavailable")
        original = ImmutableArchivePreparation._original(
            ticket.collection_id, ticket.object_id, control, record
        )
        effect = session.get(
            ArchiveWriteEffectRecord, ticket.call_id, with_for_update=True, populate_existing=True
        )
        if effect is None:
            raise NotFound("immutable archive original effect is unavailable")
        arguments = {
            "operation": original.operation.model_dump(mode="json"),
            "provider_request_sha256": write_operation_sha256(original.operation),
        }
        request_json = _json(
            {
                "kind": "put",
                "resource": "put",
                "identity": json.loads(record.identity_json),
                "preparation_sha256": _sha(record.preparation_json),
                "arguments": arguments,
            }
        )
        if (
            original != prepared
            or ticket.collection_id != prepared.collection_id
            or ticket.object_id != prepared.object_id
            or effect.collection_id != prepared.collection_id
            or effect.object_id != prepared.object_id
            or effect.kind != "put"
            or effect.resource != "put"
            or effect.dispatch_sha256 != _sha(ticket._nonce)
            or effect.request_json != request_json
            or effect.request_sha256 != _sha(request_json)
            or ticket.request_sha256 != effect.request_sha256
            or ticket.executor_id != effect.executor_id
            or invocation.call
            != WriteCallIdentity(
                call_id=effect.id,
                exact_request_sha256=write_operation_sha256(original.operation),
                target_incarnation=json.loads(control.construction_json)["archive_incarnation_id"],
                dispatch_owner=effect.executor_id,
            )
            or invocation.operation != original.operation
            or ticket.target_incarnation != invocation.call.target_incarnation
        ):
            raise Conflict("immutable archive original invocation changed")
        return effect

    def execute(
        self, admitted: AdmittedImmutableArchiveWrite, executor: ArchiveObjectWriteExecutor
    ) -> WriteExecutionResult:
        """Dispatch once outside catalog transactions, retaining unknown failures."""
        with self._live_lock:
            self._consume_live(admitted)
        invocation = admitted.invocation
        try:
            response = admitted.ticket.dispatch(
                lambda: executor.execute_write(invocation, content=admitted.prepared.content)
            )
            validate_write_execution(invocation, response)
        except BaseException:
            try:
                self.record_unknown(admitted)
            except BaseException:
                # The original committed unresolved row already retains the
                # charge. A failed status write cannot replace the actor's error.
                pass
            raise
        # Return the exact response even when execution is unknown. Only the
        # report owner can merge its positive observation with a source checkpoint.
        return response

    def _consume_live(self, admitted: AdmittedImmutableArchiveWrite) -> None:
        if self._live.get(admitted.ticket.call_id) is not admitted:
            raise Conflict("immutable archive invocation belongs to another or used live actor")
        self._live.pop(admitted.ticket.call_id)

    def withdraw(self, admitted: AdmittedImmutableArchiveWrite) -> WithdrawnProviderWrite:
        """Consume the original unused capability; retain this proof until acknowledged."""
        with self._live_lock:
            if self._live.get(admitted.ticket.call_id) is not admitted:
                raise Conflict("immutable archive invocation belongs to another or used live actor")
            proof = admitted.ticket.withdraw()
            self._live.pop(admitted.ticket.call_id)
        return proof

    def settle_withdrawal(self, evidence: WithdrawnProviderWrite) -> None:
        """Report only the original never-started proof, independently of provider facts."""
        ArchiveWriteAdmission(self._factory).settle_withdrawn(evidence)

    def report(
        self,
        admitted: AdmittedImmutableArchiveWrite,
        response: WriteExecutionResult,
        *,
        apply_receipt: ApplyImmutableReceipt,
    ) -> None:
        """Atomically retain an original outcome and its source-owned checkpoint."""
        admitted.ticket._close_dispatch()
        # Take a fresh immutable snapshot of adapter-owned mutable dictionaries.
        response = WriteExecutionResult.model_validate_json(response.model_dump_json())
        validate_write_execution(admitted.invocation, response)
        encoded = _json(
            {
                "call": response.call.model_dump(mode="json"),
                "outcome": response.outcome,
                "result": response.result.model_dump(mode="json") if response.result else None,
            }
        )
        with session_scope(self._factory) as session:
            _writer(session)
            effect = self._original_context(session, admitted)
            if len(encoded.encode()) > effect.result_capacity_bytes:
                raise Conflict("immutable archive result exceeds its reserved byte grant")
            if effect.state == "settled" and effect.result_json != encoded:
                raise Conflict("immutable archive confirmed outcome changed")
            if effect.observation_json not in {None, encoded}:
                raise Conflict("immutable archive report conflicts with its observed result")
            if effect.state != "settled" and effect.observation_json is None:
                if isinstance(response.result, ImmutableObjectReceipt):
                    apply_receipt(session, admitted.prepared, response.result)
            if response.execution == "closed":
                effect.state, effect.unresolved, effect.result_json = "settled", False, encoded
            elif effect.state != "settled":
                effect.state = "uncertain"
                if response.result is not None:
                    effect.observation_json = encoded

    def accept_existing(
        self,
        admitted: AdmittedImmutableArchiveWrite,
        evidence: WithdrawnProviderWrite,
        receipt: ImmutableObjectReceipt,
        *,
        apply_receipt: ApplyImmutableReceipt,
    ) -> None:
        """Receive object authority separately from an original unused dispatch.

        The observed object may have different randomized ciphertext. Its receipt
        is never represented as execution of the admitted exact Put request.
        """
        if evidence.ticket is not admitted.ticket:
            raise Conflict("immutable archive unused proof belongs to another invocation")
        admitted.ticket._require_withdrawal(evidence)
        receipt = ImmutableObjectReceipt.model_validate_json(receipt.model_dump_json())
        require_immutable_archive_receipt(admitted.prepared.operation.request, receipt)
        encoded = _json(
            {"dispatch": "never-started", "observed_object": receipt.model_dump(mode="json")}
        )
        with session_scope(self._factory) as session:
            _writer(session)
            effect = self._original_context(session, admitted)
            if len(encoded.encode()) > effect.result_capacity_bytes:
                raise Conflict("immutable archive observation exceeds its reserved byte grant")
            if effect.observation_json is not None or (
                effect.state == "settled" and effect.result_json != encoded
            ):
                raise Conflict("immutable archive unused observation conflicts with its outcome")
            if effect.state != "settled":
                apply_receipt(session, admitted.prepared, receipt)
            effect.state, effect.unresolved, effect.result_json = "settled", False, encoded

    def record_unknown(self, admitted: AdmittedImmutableArchiveWrite) -> None:
        with session_scope(self._factory) as session:
            _writer(session)
            effect = self._original_context(session, admitted)
            if effect.unresolved:
                effect.state = "uncertain"

    def acknowledge_report(
        self,
        admitted: AdmittedImmutableArchiveWrite,
        *,
        verify_receipt: ApplyImmutableReceipt | None = None,
    ) -> None:
        """Retire a closed call only against its receiving owner's durable fact.

        A known unused withdrawal has no provider receipt to transfer. Completed
        writes require an exact source-owned receipt check in this transaction;
        parent retirement alone cannot authorize deleting the original report.
        """
        with session_scope(self._factory) as session:
            _writer(session)
            effect = self._original_context(session, admitted)
            if effect.state != "settled" or effect.result_json is None:
                raise Conflict("unresolved immutable archive write must retain its reservation")
            result = json.loads(effect.result_json)
            if result != {"dispatch": "never-started"}:
                if verify_receipt is None:
                    raise Conflict("immutable archive original receipt has no receiving proof")
                if result.get("dispatch") == "never-started" and "observed_object" in result:
                    receipt = ImmutableObjectReceipt.model_validate(result["observed_object"])
                    require_immutable_archive_receipt(admitted.prepared.operation.request, receipt)
                elif result.get("outcome") == "completed":
                    receipt = ImmutableObjectReceipt.model_validate(result["result"])
                else:
                    raise Conflict("immutable archive original receipt has no receiving proof")
                verify_receipt(session, admitted.prepared, receipt)
            session.delete(effect)
