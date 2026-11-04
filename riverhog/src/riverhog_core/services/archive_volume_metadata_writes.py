"""One original live metadata actor, retained through receiving acknowledgements."""

from __future__ import annotations

import hashlib
import secrets
import threading

from riverhog_protocol.errors import Conflict, NotFound
from riverhog_storage_adapter_protocol import (
    ImmutableObjectReceipt as AdapterImmutableObjectReceipt,
)
from riverhog_storage_adapter_protocol import (
    ObjectPlacementPolicy,
    PutObjectOperation,
    SmallObjectWriteRequest,
    WriteExecutionResult,
)

from riverhog_core.archive_write_grants import ArchiveWriteGrants
from riverhog_core.catalog_db import SessionFactory, session_scope
from riverhog_core.ports.archive_objects import (
    ArchiveObjectWriteExecutor,
    ImmutableArchiveObjectObserver,
    ImmutableObjectReceipt,
)
from riverhog_core.provider_write_dispatch import WithdrawnProviderWrite
from riverhog_core.services.archive_volume_metadata_checkpoint import ArchiveVolumeMetadataSource
from riverhog_core.services.immutable_archive_admission import (
    AdmittedImmutableArchiveWrite,
    ImmutableArchiveAdmission,
)
from riverhog_core.services.immutable_archive_preparation import (
    ImmutableArchivePreparation,
    PreparedImmutableArchiveObject,
)


class ArchiveVolumeMetadataWrites:
    """Keep the exact live invocation and returned data across publisher retries.

    The caller retains this actor until receiving and retirement are acknowledged.
    Cold rows can recover settled facts but cannot recreate this actor's ticket.
    """

    def __init__(
        self,
        factory: SessionFactory,
        *,
        source: ArchiveVolumeMetadataSource,
        epoch: str,
        grants: ArchiveWriteGrants,
        executor: ArchiveObjectWriteExecutor,
        observer: ImmutableArchiveObjectObserver,
    ) -> None:
        self._factory = factory
        self._source = source
        self._epoch = epoch
        self._grants = grants
        self._executor = executor
        self._observer = observer
        self._preparation = ImmutableArchivePreparation(factory)
        self._admission = ImmutableArchiveAdmission(factory)
        self._executor_id = secrets.token_hex(32)
        self._lock = threading.Lock()
        self._prepared: PreparedImmutableArchiveObject | None = None
        self._admitted: AdmittedImmutableArchiveWrite | None = None
        self._response_json: str | None = None
        self._observed_json: str | None = None
        self._withdrawal: WithdrawnProviderWrite | None = None
        self._admission_pending = False
        self._started = False
        self._report_confirmed = False
        self._retired = False

    @property
    def has_pending(self) -> bool:
        return self._admission_pending or (self._admitted is not None and not self._retired)

    def _receipt(self) -> AdapterImmutableObjectReceipt | None:
        if self._observed_json is not None:
            return AdapterImmutableObjectReceipt.model_validate_json(self._observed_json)
        if self._response_json is not None:
            result = WriteExecutionResult.model_validate_json(self._response_json).result
            if isinstance(result, AdapterImmutableObjectReceipt):
                return result
        return None

    @staticmethod
    def _project(receipt: AdapterImmutableObjectReceipt) -> ImmutableObjectReceipt:
        return ImmutableObjectReceipt(
            object_path=receipt.object_path,
            revision=receipt.revision,
            entity_token=receipt.entity_token,
            stored_bytes=receipt.stored_bytes,
            stored_sha256=receipt.stored_sha256,
            completed_at=receipt.completed_at,
        )

    def flush(self) -> ImmutableObjectReceipt | None:
        """Retry only original reports/proofs; never observe or invoke a provider."""
        with self._lock:
            if self._admission_pending and self._admitted is None:
                raise Conflict("original archive metadata admission remains unconfirmed")
            return self._flush()

    def _flush(self) -> ImmutableObjectReceipt | None:
        admitted = self._admitted
        if admitted is None:
            return None
        receipt = self._receipt()
        if self._retired:
            return self._project(receipt) if receipt is not None else None
        response = (
            WriteExecutionResult.model_validate_json(self._response_json)
            if self._response_json is not None
            else None
        )
        if response is None and self._withdrawal is None:
            if self._started:
                raise Conflict("original archive metadata execution remains unknown")
            return None
        if not self._report_confirmed:
            try:
                if response is not None:
                    self._admission.report(
                        admitted, response, apply_receipt=self._source.apply_prepared_receipt
                    )
                elif self._observed_json is not None:
                    assert self._withdrawal is not None and receipt is not None
                    self._admission.accept_existing(
                        admitted,
                        self._withdrawal,
                        receipt,
                        apply_receipt=self._source.apply_prepared_receipt,
                    )
                else:
                    assert self._withdrawal is not None
                    self._admission.settle_withdrawal(self._withdrawal)
            except NotFound:
                # Another receiving owner may have drained the same report
                # after a lost reply. Only actual source proof can replace the
                # missing report acknowledgement for a positive original result.
                if receipt is None:
                    if self._withdrawal is None:
                        raise
                    # The actor still owns its minted never-started proof;
                    # there is no positive object receipt to fabricate.
                    admitted.ticket._require_withdrawal(self._withdrawal)
                else:
                    if response is not None and response.execution != "closed":
                        raise
                    with session_scope(self._factory) as session:
                        self._source.verify_prepared_receipt(session, admitted.prepared, receipt)
            self._report_confirmed = True
        if response is not None and (
            response.execution != "closed" or response.outcome != "completed"
        ):
            raise Conflict("original archive metadata execution remains unknown")
        try:
            self._admission.acknowledge_report(
                admitted,
                verify_receipt=self._source.verify_prepared_receipt
                if receipt is not None
                else None,
            )
        except NotFound:
            # A confirmed original report and actual positive source proof
            # account for a lost retirement reply without fresh execution.
            if receipt is not None:
                with session_scope(self._factory) as session:
                    self._source.verify_prepared_receipt(session, admitted.prepared, receipt)
        self._retired = True
        return self._project(receipt) if receipt is not None else None

    def _reset_unused(self) -> None:
        assert self._retired and self._withdrawal is not None and self._receipt() is None
        self._admitted = None
        self._withdrawal = None
        self._report_confirmed = self._retired = False

    def put_immutable_object(
        self,
        *,
        object_path: str,
        content: bytes,
        content_type: str,
        required_identity_assertions: dict[str, str],
        placement_policy: ObjectPlacementPolicy,
    ) -> ImmutableObjectReceipt:
        if not isinstance(content, bytes):
            raise TypeError("archive metadata ciphertext must be immutable bytes")
        request = SmallObjectWriteRequest(
            object_path=object_path,
            content_type=content_type,
            required_identity_assertions=dict(required_identity_assertions),
            placement_policy=placement_policy,
            mode="create_only",
            stored_bytes=len(content),
            stored_sha256=hashlib.sha256(content).hexdigest(),
        )
        self._source.publication.require_request(request)
        with self._lock:
            received = self._flush()
            if received is not None:
                return received
            if self._retired:
                self._reset_unused()
            if self._prepared is None:
                self._prepared = self._preparation.prepare(
                    self._source.collection_id,
                    operation=PutObjectOperation(request=request),
                    content=content,
                    epoch=self._epoch,
                    grants=self._grants,
                    guard=self._source.guard,
                    validate_metadata=self._source.validate_metadata,
                )
            if self._admitted is None:
                # A failed commit reply supplies no live ticket and cannot
                # expose an empty actor as a quiescence acknowledgement.
                self._admission_pending = True
                self._admitted = self._admission.arm(
                    self._prepared,
                    epoch=self._epoch,
                    executor_id=self._executor_id,
                    grants=self._grants,
                    guard=self._source.guard,
                    validate_metadata=self._source.validate_metadata,
                )
                self._admission_pending = False
            admitted = self._admitted
            if self._observed_json is None:
                try:
                    observed = self._observer.observe_immutable_object(
                        admitted.prepared.operation.request
                    )
                except BaseException:
                    try:
                        self._withdrawal = self._admission.withdraw(admitted)
                        self._flush()
                    except BaseException:
                        # Cleanup bookkeeping cannot replace the original
                        # observer interruption; its intent/proof stays owned.
                        pass
                    raise
                if observed is not None:
                    self._observed_json = observed.model_dump_json()
            if self._observed_json is not None:
                self._withdrawal = self._admission.withdraw(admitted)
            else:
                self._started = True
                response = self._admission.execute(admitted, self._executor)
                self._response_json = response.model_dump_json()
            received = self._flush()
            if received is None:
                raise Conflict("original archive metadata has no received object")
            return received
