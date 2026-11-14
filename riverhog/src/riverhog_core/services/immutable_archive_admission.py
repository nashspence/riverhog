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

from riverhog_core.archive_custody_revision import advance_archive_custody_revision
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


@dataclass(frozen=True, slots=True)
class SettledImmutableArchiveWrite:
    """Original closed data; neither a live ticket nor a fresh dispatch right."""

    prepared: PreparedImmutableArchiveObject = field(repr=False)
    call_id: str
    invocation_json: str = field(repr=False)
    result_json: str = field(repr=False)
    record_sha256: str = field(repr=False)

    @property
    def receipt(self) -> ImmutableObjectReceipt | None:
        result = json.loads(self.result_json)
        if result == {"dispatch": "never-started"}:
            return None
        value = (
            result["observed_object"]
            if result.get("dispatch") == "never-started"
            else result["result"]
        )
        return ImmutableObjectReceipt.model_validate(value)


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
    def _retained_context(
        session: Session,
        collection_id: int,
        object_id: str,
        *,
        call_id: str | None,
    ) -> tuple[PreparedImmutableArchiveObject, ArchiveWriteEffectRecord, WriteInvocation] | None:
        # Parent-before-child ordering also applies to cold receivers. The
        # independent construction retains original facts after parent retirement.
        upload = session.scalar(ArchiveWriteAdmission._upload_query(collection_id))
        control = session.get(
            ArchiveWriteControlRecord,
            collection_id,
            with_for_update=True,
            populate_existing=True,
        )
        if control is None:
            raise NotFound("immutable archive original construction is unavailable")
        if upload is not None:
            ArchiveWriteAdmission._control(session, upload)
        ArchiveWriteAdmission._terminal_lock(session, control)
        record = session.get(ArchiveWritePreparationRecord, (collection_id, object_id))
        if record is None:
            if call_id is None:
                return None
            raise NotFound("immutable archive original preparation is unavailable")
        original = ImmutableArchivePreparation._original(collection_id, object_id, control, record)
        if call_id is None:
            # Read at most one already closed call from this object's indexed
            # retained controls. Absence says nothing about provider execution.
            call_id = session.scalar(
                select(ArchiveWriteEffectRecord.id)
                .where(
                    ArchiveWriteEffectRecord.collection_id == collection_id,
                    ArchiveWriteEffectRecord.object_id == object_id,
                    ArchiveWriteEffectRecord.unresolved.is_(False),
                    ArchiveWriteEffectRecord.resource == "put",
                    ArchiveWriteEffectRecord.state == "settled",
                )
                .limit(1)
            )
            if call_id is None:
                return None
        effect = session.get(
            ArchiveWriteEffectRecord, call_id, with_for_update=True, populate_existing=True
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
            effect.collection_id != collection_id
            or effect.object_id != object_id
            or effect.kind != "put"
            or effect.resource != "put"
            or effect.request_json != request_json
            or effect.request_sha256 != _sha(request_json)
        ):
            raise Conflict("immutable archive original invocation changed")
        invocation = WriteInvocation(
            call=WriteCallIdentity(
                call_id=effect.id,
                exact_request_sha256=write_operation_sha256(original.operation),
                target_incarnation=json.loads(control.construction_json)["archive_incarnation_id"],
                dispatch_owner=effect.executor_id,
            ),
            operation=original.operation,
        )
        return original, effect, invocation

    @staticmethod
    def _original_context(
        session: Session, admitted: AdmittedImmutableArchiveWrite
    ) -> ArchiveWriteEffectRecord:
        ticket = admitted.ticket
        context = ImmutableArchiveAdmission._retained_context(
            session, ticket.collection_id, ticket.object_id, call_id=ticket.call_id
        )
        assert context is not None
        original, effect, invocation = context
        if (
            original != admitted.prepared
            or effect.dispatch_sha256 != _sha(ticket._nonce)
            or ticket.request_sha256 != effect.request_sha256
            or ticket.executor_id != effect.executor_id
            or ticket.target_incarnation != invocation.call.target_incarnation
            or admitted.invocation != invocation
        ):
            raise Conflict("immutable archive original invocation changed")
        return effect

    @staticmethod
    def _settled_snapshot(
        prepared: PreparedImmutableArchiveObject,
        effect: ArchiveWriteEffectRecord,
        invocation: WriteInvocation,
    ) -> SettledImmutableArchiveWrite:
        if effect.state != "settled" or effect.unresolved or effect.result_json is None:
            raise Conflict("unresolved immutable archive write must retain its reservation")
        if len(
            effect.result_json.encode()
        ) > effect.result_capacity_bytes or effect.observation_json not in {
            None,
            effect.result_json,
        }:
            raise Conflict("immutable archive retained outcome changed")
        result = json.loads(effect.result_json)
        if not isinstance(result, dict) or _json(result) != effect.result_json:
            raise Conflict("immutable archive retained outcome encoding changed")
        if result.get("dispatch") == "never-started":
            if set(result) == {"dispatch", "observed_object"}:
                receipt = ImmutableObjectReceipt.model_validate(result["observed_object"])
                require_immutable_archive_receipt(prepared.operation.request, receipt)
            elif set(result) != {"dispatch"}:
                raise Conflict("immutable archive retained unused proof changed")
        else:
            if set(result) != {"call", "outcome", "result"}:
                raise Conflict("immutable archive retained execution shape changed")
            response = WriteExecutionResult.model_validate({**result, "execution": "closed"})
            validate_write_execution(invocation, response)
            if response.outcome != "completed" or not isinstance(
                response.result, ImmutableObjectReceipt
            ):
                raise Conflict("immutable archive retained execution has no receipt")
        commitment = _json(
            {
                "preparation_sha256": _sha(prepared.preparation_json),
                "invocation": invocation.model_dump(mode="json"),
                "sequence": effect.sequence,
                "epoch": effect.epoch,
                "dispatch_sha256": effect.dispatch_sha256,
                "request_sha256": effect.request_sha256,
                "control_bytes": effect.control_bytes,
                "result_capacity_bytes": effect.result_capacity_bytes,
                "observation_json": effect.observation_json,
                "result_json": effect.result_json,
            }
        )
        return SettledImmutableArchiveWrite(
            prepared, effect.id, invocation.model_dump_json(), effect.result_json, _sha(commitment)
        )

    @staticmethod
    def _settled_context(
        session: Session, settled: SettledImmutableArchiveWrite
    ) -> ArchiveWriteEffectRecord:
        context = ImmutableArchiveAdmission._retained_context(
            session,
            settled.prepared.collection_id,
            settled.prepared.object_id,
            call_id=settled.call_id,
        )
        assert context is not None
        original, effect, invocation = context
        if ImmutableArchiveAdmission._settled_snapshot(original, effect, invocation) != settled:
            raise Conflict("immutable archive receiving proof changed")
        return effect

    def load_next_report(
        self, collection_id: int, object_path: str
    ) -> SettledImmutableArchiveWrite | None:
        """Read one original closed fact, including its original preparation epoch.

        No current forward permission is granted. An absent fact neither observes
        provider absence nor permits replacement of an unknown original call.
        """
        with session_scope(self._factory) as session:
            _writer(session)
            context = self._retained_context(
                session,
                collection_id,
                ImmutableArchivePreparation.object_id(object_path),
                call_id=None,
            )
            if context is None:
                return None
            original, effect, invocation = context
            return self._settled_snapshot(original, effect, invocation)

    def load_next_collection_report(
        self, collection_id: int
    ) -> SettledImmutableArchiveWrite | None:
        """Read one closed metadata report from the finite retained call pool.

        Selection does not depend on the source workflow's current pointer.
        Pending payload calls share this pool but retain their own receiving owner.
        """
        with session_scope(self._factory) as session:
            _writer(session)
            selected = session.execute(
                select(ArchiveWriteEffectRecord.id, ArchiveWriteEffectRecord.object_id)
                .where(
                    ArchiveWriteEffectRecord.collection_id == collection_id,
                    ArchiveWriteEffectRecord.unresolved.is_(False),
                    ArchiveWriteEffectRecord.resource == "put",
                    ArchiveWriteEffectRecord.state == "settled",
                )
                .order_by(ArchiveWriteEffectRecord.id)
                .limit(1)
            ).first()
            if selected is None:
                return None
            context = self._retained_context(
                session, collection_id, selected.object_id, call_id=selected.id
            )
            assert context is not None
            return self._settled_snapshot(*context)

    def receive_settled(
        self,
        settled: SettledImmutableArchiveWrite,
        *,
        apply_receipt: ApplyImmutableReceipt,
        verify_receipt: ApplyImmutableReceipt,
    ) -> None:
        """Ensure and verify the receiving owner's idempotent original checkpoint.

        No provider call or forward authorization occurs here. The source owner
        must ensure the same receipt on retries, including lost commit replies.
        A separate positive acknowledgement retires the original call control.
        """
        with session_scope(self._factory) as session:
            _writer(session)
            self._settled_context(session, settled)
            receipt = settled.receipt
            if receipt is not None:
                apply_receipt(session, settled.prepared, receipt)
                # The receiver cannot change the original result passed to its
                # verifier through a mutable assertion dictionary.
                original = settled.receipt
                assert original is not None
                verify_receipt(session, settled.prepared, original)

    def acknowledge_settled(
        self,
        settled: SettledImmutableArchiveWrite,
        *,
        verify_receipt: ApplyImmutableReceipt | None = None,
    ) -> None:
        """Retire only the exact original closed control against durable ownership."""
        with session_scope(self._factory) as session:
            _writer(session)
            effect = self._settled_context(session, settled)
            self._verify_receiving(settled, session, verify_receipt)
            session.delete(effect)

    @staticmethod
    def _verify_receiving(
        settled: SettledImmutableArchiveWrite,
        session: Session,
        verify_receipt: ApplyImmutableReceipt | None,
    ) -> None:
        receipt = settled.receipt
        if receipt is not None:
            if verify_receipt is None:
                raise Conflict("immutable archive original receipt has no receiving proof")
            verify_receipt(session, settled.prepared, receipt)

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
                    advance_archive_custody_revision(session, admitted.ticket.collection_id)
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
                advance_archive_custody_revision(session, admitted.ticket.collection_id)
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
            settled = self._settled_snapshot(admitted.prepared, effect, admitted.invocation)
            self._verify_receiving(settled, session, verify_receipt)
            session.delete(effect)
