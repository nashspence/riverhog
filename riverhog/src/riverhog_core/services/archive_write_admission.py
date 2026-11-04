"""Durable provider-effect admission inside the collection-upload owner.

The composing owner supplies current permission/claim guards before construction
locks and supplies adapter-owned execution-closure evidence. Observations never
supply closure. Provider calls run through one-use live dispatch capabilities,
outside these short catalog transactions.
"""

from __future__ import annotations

import hashlib
import json
import secrets
from collections.abc import Callable, Mapping
from typing import Any

from riverhog_canonical_json import format_scalar, parse_scalar
from riverhog_protocol.errors import Conflict, NotFound
from riverhog_protocol.pack_ingress import canonical_json_bytes
from sqlalchemy import Select, select
from sqlalchemy.orm import Session, load_only
from state_schema import read_snapshot

from riverhog_core.archive_write_grants import ArchiveWriteGrants
from riverhog_core.catalog_archive_write_models import (
    ArchiveUploadTerminalRecord,
    ArchiveWriteControlRecord,
    ArchiveWriteEffectRecord,
    ArchiveWritePreparationRecord,
)
from riverhog_core.catalog_db import SessionFactory, session_scope
from riverhog_core.catalog_models import CollectionArchiveObjectUploadRecord, CollectionUploadRecord
from riverhog_core.collection_construction import upload_construction
from riverhog_core.provider_write_dispatch import (
    ArmedProviderWrite,
    ClosedProviderWrite,
    ProviderWriteReport,
    WithdrawnProviderWrite,
)
from riverhog_core.services.collection_registration_cleanup import freeze_registration_execution
from riverhog_core.services.collection_upload_authority import require_archive_write_control

Guard = Callable[[Session], None]
ApplyResult = Callable[
    [Session, CollectionUploadRecord, CollectionArchiveObjectUploadRecord, Mapping[str, Any]], None
]


def _json(value: object) -> str:
    return canonical_json_bytes(value).decode("utf-8")


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _writer(session: Session) -> None:
    if session.get_bind().dialect.name == "sqlite":
        session.connection(execution_options={"sqlite_immediate_writer": True})


def _positive(value: int) -> None:
    if type(value) is not int or value < 1:
        raise ValueError("archive write grants must be positive integers")


def initialize_archive_write_control(session: Session, upload: CollectionUploadRecord) -> None:
    """Called only in the transaction that allocates a new upload construction."""
    session.add(
        ArchiveWriteControlRecord(
            collection_id=upload.collection_id,
            construction_json=upload_construction(upload),
            epoch=secrets.token_hex(32),
            gate="open",
            next_call="0",
        )
    )


class ArchiveWriteAdmission:
    def __init__(self, session_factory: SessionFactory) -> None:
        self._factory = session_factory

    @staticmethod
    def _upload_query(collection_id: int) -> Select[tuple[CollectionUploadRecord]]:
        return (
            select(CollectionUploadRecord)
            .options(
                load_only(
                    CollectionUploadRecord.archive_generation,
                    CollectionUploadRecord.archive_incarnation_id,
                    CollectionUploadRecord.archive_storage_prefix,
                    CollectionUploadRecord.archive_store,
                    CollectionUploadRecord.collection_id,
                    CollectionUploadRecord.copy_to_json,
                    CollectionUploadRecord.creation_identity_sha256,
                    CollectionUploadRecord.custody_mode,
                    CollectionUploadRecord.delivery_context_id,
                    CollectionUploadRecord.encryption_format,
                    CollectionUploadRecord.event_context_json,
                    CollectionUploadRecord.idempotency_key,
                    CollectionUploadRecord.ingest_source,
                    CollectionUploadRecord.initial_tag_set_identity,
                    CollectionUploadRecord.lease_expires_at,
                    CollectionUploadRecord.initiated_by_key_id,
                    CollectionUploadRecord.initiated_by_principal_id,
                    CollectionUploadRecord.passphrase_id,
                    CollectionUploadRecord.state,
                    CollectionUploadRecord.use_cache,
                    raiseload=True,
                )
            )
            .where(CollectionUploadRecord.collection_id == collection_id)
            .with_for_update()
        )

    @classmethod
    def _upload(cls, session: Session, collection_id: int) -> CollectionUploadRecord:
        upload = session.scalar(cls._upload_query(collection_id))
        if upload is None:
            raise NotFound("archive write construction is unavailable")
        return upload

    @staticmethod
    def _control(session: Session, upload: CollectionUploadRecord) -> ArchiveWriteControlRecord:
        return require_archive_write_control(session, upload)

    @staticmethod
    def _volume(
        session: Session, collection_id: int, object_id: str
    ) -> CollectionArchiveObjectUploadRecord:
        volume = session.scalar(
            select(CollectionArchiveObjectUploadRecord)
            .options(
                load_only(
                    CollectionArchiveObjectUploadRecord.collection_id,
                    CollectionArchiveObjectUploadRecord.object_id,
                    CollectionArchiveObjectUploadRecord.kind,
                    CollectionArchiveObjectUploadRecord.object_path,
                    CollectionArchiveObjectUploadRecord.plan_sha256,
                )
            )
            .where(
                CollectionArchiveObjectUploadRecord.collection_id == collection_id,
                CollectionArchiveObjectUploadRecord.object_id == object_id,
            )
            .with_for_update()
        )
        if volume is None:
            raise NotFound("archive write volume is unavailable")
        return volume

    @staticmethod
    def _terminal_lock(session: Session, control: ArchiveWriteControlRecord) -> None:
        if control.terminal_id is not None:
            terminal = session.scalar(
                select(ArchiveUploadTerminalRecord)
                .where(ArchiveUploadTerminalRecord.id == control.terminal_id)
                .with_for_update()
            )
            if terminal is None:
                raise RuntimeError("archive write terminal intent is missing")

    @staticmethod
    def require_no_unresolved_calls(session: Session, collection_id: int) -> None:
        """Require closure while the caller retains the construction write lock."""
        if (
            session.scalar(
                select(ArchiveWriteEffectRecord.id)
                .where(
                    ArchiveWriteEffectRecord.collection_id == collection_id,
                    ArchiveWriteEffectRecord.unresolved.is_(True),
                )
                .limit(1)
            )
            is not None
        ):
            raise Conflict("archive construction has unresolved provider writes")

    def bind_epoch(self, collection_id: int, *, guard: Guard) -> str:
        """Bind one authorized actor before it prepares or admits forward work."""
        with session_scope(self._factory) as session:
            _writer(session)
            guard(session)
            upload = self._upload(session, collection_id)
            control = self._control(session, upload)
            if control.gate != "open":
                raise Conflict("archive write admission is closed")
            return control.epoch

    @staticmethod
    def _require_epoch(control: ArchiveWriteControlRecord, epoch: str) -> None:
        if control.gate != "open":
            raise Conflict("archive write admission is closed")
        if control.epoch != epoch:
            raise Conflict("archive write admission epoch changed")

    @classmethod
    def require_forward_construction(
        cls, session: Session, collection_id: int, *, epoch: str
    ) -> tuple[CollectionUploadRecord, ArchiveWriteControlRecord]:
        """Retain the parent upload interlock before a child owner admits work."""
        upload = cls._upload(session, collection_id)
        control = cls._control(session, upload)
        cls._require_epoch(control, epoch)
        if upload.state not in {"open", "closing", "uploading"}:
            raise Conflict("archive write construction cannot accept payload")
        return upload, control

    def _preparation_record(
        self,
        session: Session,
        collection_id: int,
        object_id: str,
        *,
        epoch: str,
        identity: Mapping[str, Any],
    ) -> ArchiveWritePreparationRecord | None:
        upload = self._upload(session, collection_id)
        control = self._control(session, upload)
        self._require_epoch(control, epoch)
        self._terminal_lock(session, control)
        volume = self._volume(session, collection_id, object_id)
        expected = {
            "kind": volume.kind,
            "object_path": volume.object_path,
            "plan_sha256": volume.plan_sha256,
        }
        if dict(identity) != expected:
            raise Conflict("archive write preparation identity changed")
        stored = session.get(ArchiveWritePreparationRecord, (collection_id, object_id))
        if stored is not None and stored.identity_json != _json(identity):
            raise Conflict("archive write preparation identity changed")
        return stored

    def load_preparation(
        self,
        collection_id: int,
        object_id: str,
        *,
        epoch: str,
        identity: Mapping[str, Any],
        guard: Guard,
    ) -> str | None:
        """Read an actor's original preparation without creating or admitting work."""
        with session_scope(self._factory) as session:
            _writer(session)
            guard(session)
            stored = self._preparation_record(
                session, collection_id, object_id, epoch=epoch, identity=identity
            )
            return stored.preparation_json if stored is not None else None

    def prepare(
        self,
        collection_id: int,
        object_id: str,
        *,
        epoch: str,
        identity: Mapping[str, Any],
        preparation: Mapping[str, Any],
        grants: ArchiveWriteGrants,
        guard: Guard,
    ) -> str:
        identity_json = _json(identity)
        preparation_json = _json(preparation)
        if len(identity_json.encode()) + len(preparation_json.encode()) > grants.preparation_bytes:
            raise Conflict("archive write preparation exceeds its byte grant")
        with session_scope(self._factory) as session:
            _writer(session)
            guard(session)
            stored = self._preparation_record(
                session, collection_id, object_id, epoch=epoch, identity=identity
            )
            if stored is not None:
                return stored.preparation_json
            session.add(
                ArchiveWritePreparationRecord(
                    collection_id=collection_id,
                    object_id=object_id,
                    identity_json=identity_json,
                    preparation_json=preparation_json,
                )
            )
        return preparation_json

    def arm(
        self,
        collection_id: int,
        object_id: str,
        *,
        epoch: str,
        kind: str,
        resource: str,
        arguments: Mapping[str, Any],
        executor_id: str,
        result_capacity_bytes: int,
        grants: ArchiveWriteGrants,
        guard: Guard,
    ) -> ArmedProviderWrite:
        if kind not in {"begin", "part", "complete", "lookup"}:
            raise ValueError("archive write effect kind/resource is invalid")
        number = arguments.get("number") if kind == "part" else None
        if kind == "part" and (type(number) is not int or number < 1):
            raise ValueError("archive write part identity is invalid")
        if resource != (f"part:{number}" if kind == "part" else kind):
            raise ValueError("archive write effect resource does not match its arguments")
        if len(executor_id) != 64 or any(c not in "0123456789abcdef" for c in executor_id):
            raise ValueError("archive write executor identity is invalid")
        _positive(result_capacity_bytes)
        nonce = secrets.token_hex(32)
        with session_scope(self._factory) as session:
            _writer(session)
            guard(session)
            upload, control = self.require_forward_construction(session, collection_id, epoch=epoch)
            volume = self._volume(session, collection_id, object_id)
            operation = arguments.get("operation")
            if operation is not None:
                if not isinstance(operation, Mapping) or operation.get("kind") != kind:
                    raise Conflict("archive write provider operation changed")
                request = operation.get("request")
                if not isinstance(request, Mapping):
                    raise Conflict("archive write provider request is invalid")
                target = request if kind in {"begin", "lookup"} else request.get("session")
                if (
                    not isinstance(target, Mapping)
                    or target.get("object_path") != volume.object_path
                ):
                    raise Conflict("archive write provider target differs from the original volume")
            preparation = session.get(ArchiveWritePreparationRecord, (collection_id, object_id))
            if preparation is None:
                raise Conflict("archive write original preparation is missing")
            if preparation.identity_json != _json(
                {
                    "kind": volume.kind,
                    "object_path": volume.object_path,
                    "plan_sha256": volume.plan_sha256,
                }
            ):
                raise Conflict("archive write prepared volume identity changed")
            call_id, nonce, request_sha256 = self._reserve_effect(
                session,
                control,
                preparation,
                collection_id=collection_id,
                object_id=object_id,
                kind=kind,
                resource=resource,
                arguments=arguments,
                executor_id=executor_id,
                result_capacity_bytes=result_capacity_bytes,
                grants=grants,
                nonce=nonce,
            )
        # No capability exists until the transaction's commit returned successfully.
        return ArmedProviderWrite(
            call_id=call_id,
            collection_id=collection_id,
            object_id=object_id,
            nonce=nonce,
            request_sha256=request_sha256,
            executor_id=executor_id,
            target_incarnation=str(json.loads(control.construction_json)["archive_incarnation_id"]),
        )

    @staticmethod
    def _reserve_effect(
        session: Session,
        control: ArchiveWriteControlRecord,
        preparation: ArchiveWritePreparationRecord,
        *,
        collection_id: int,
        object_id: str,
        kind: str,
        resource: str,
        arguments: Mapping[str, Any],
        executor_id: str,
        result_capacity_bytes: int,
        grants: ArchiveWriteGrants,
        nonce: str,
    ) -> tuple[str, str, str]:
        """Reserve shared bounded call capacity under the construction lock.

        Returned facts are inert; the caller mints a live capability only after
        its admission transaction has returned an unambiguous commit.
        """
        request_json = _json(
            {
                "kind": kind,
                "resource": resource,
                "identity": json.loads(preparation.identity_json),
                "preparation_sha256": _sha(preparation.preparation_json),
                "arguments": arguments,
            }
        )
        control_bytes = len(request_json.encode()) + 2 * result_capacity_bytes
        retained = list(
            session.execute(
                select(ArchiveWriteEffectRecord.id, ArchiveWriteEffectRecord.control_bytes)
                .where(ArchiveWriteEffectRecord.collection_id == collection_id)
                .order_by(ArchiveWriteEffectRecord.id)
                .limit(grants.calls)
            )
        )
        if len(retained) >= grants.calls or (
            sum(row.control_bytes for row in retained) + control_bytes > grants.control_bytes
        ):
            raise Conflict("archive write retained-effect grant is exhausted")
        overlap = select(ArchiveWriteEffectRecord.id).where(
            ArchiveWriteEffectRecord.collection_id == collection_id,
            ArchiveWriteEffectRecord.object_id == object_id,
            ArchiveWriteEffectRecord.unresolved.is_(True),
        )
        if kind == "part":
            overlap = overlap.where(
                (ArchiveWriteEffectRecord.resource == resource)
                | ArchiveWriteEffectRecord.kind.in_(("begin", "complete", "lookup"))
            )
        if session.scalar(overlap.limit(1)) is not None:
            raise Conflict("archive write effect is already unresolved")
        call_id = secrets.token_hex(32)
        sequence = parse_scalar("nonnegative", control.next_call)
        control.next_call = format_scalar("nonnegative", sequence + 1)
        session.add(
            ArchiveWriteEffectRecord(
                id=call_id,
                collection_id=collection_id,
                object_id=object_id,
                sequence=format_scalar("nonnegative", sequence),
                epoch=control.epoch,
                executor_id=executor_id,
                dispatch_sha256=_sha(nonce),
                request_sha256=_sha(request_json),
                request_json=request_json,
                resource=resource,
                kind=kind,
                state="armed",
                unresolved=True,
                control_bytes=control_bytes,
                result_capacity_bytes=result_capacity_bytes,
            )
        )
        return call_id, nonce, _sha(request_json)

    def _report_context(
        self, session: Session, ticket: ArmedProviderWrite
    ) -> tuple[
        CollectionUploadRecord, CollectionArchiveObjectUploadRecord, ArchiveWriteEffectRecord, str
    ]:
        upload = self._upload(session, ticket.collection_id)
        control = self._control(session, upload)
        self._terminal_lock(session, control)
        volume = self._volume(session, ticket.collection_id, ticket.object_id)
        effect = session.scalar(
            select(ArchiveWriteEffectRecord)
            .where(ArchiveWriteEffectRecord.id == ticket.call_id)
            .with_for_update()
        )
        if effect is None:
            raise NotFound("archive write report identity is unavailable")
        if (
            effect.collection_id != ticket.collection_id
            or effect.object_id != ticket.object_id
            or effect.dispatch_sha256 != _sha(ticket._nonce)
        ):
            raise Conflict("archive write report identity changed")
        incarnation = str(json.loads(control.construction_json)["archive_incarnation_id"])
        return upload, volume, effect, incarnation

    def settle_closed(
        self,
        ticket: ArmedProviderWrite,
        evidence: ClosedProviderWrite,
        *,
        apply_result: ApplyResult,
    ) -> None:
        self.apply_reports(
            (ProviderWriteReport(ticket, evidence.result, evidence),),
            apply_result=apply_result,
        )

    def apply_reports(
        self,
        reports: tuple[ProviderWriteReport, ...],
        *,
        apply_result: ApplyResult,
    ) -> None:
        """Apply one checkpoint with its exact bounded batch of returned outcomes.

        All effect reservations already exist. Duplicate reports cannot reapply a
        checkpoint, and unknown execution retains its original unresolved charge.
        """
        if not reports:
            raise ValueError("archive write report batch is empty")
        first = reports[0].ticket
        identities = set()
        for report in reports:
            ticket = report.ticket
            if (
                ticket.collection_id != first.collection_id
                or ticket.object_id != first.object_id
                or ticket.call_id in identities
            ):
                raise ValueError("archive write report batch identity changed")
            identities.add(ticket.call_id)
            ticket._close_dispatch()
        with session_scope(self._factory) as session:
            _writer(session)
            updates = []
            changed = False
            for report in reports:
                upload, volume, effect, incarnation = self._report_context(session, report.ticket)
                encoded = _json(report.result)
                if (
                    report.ticket.request_sha256 != effect.request_sha256
                    or report.ticket.executor_id != effect.executor_id
                    or report.ticket.target_incarnation != incarnation
                ):
                    raise Conflict("archive write report commitment changed")
                if len(encoded.encode()) > effect.result_capacity_bytes:
                    raise Conflict("archive write result exceeds its reserved byte grant")
                if report.closure is not None:
                    evidence = report.closure
                    if (
                        evidence.call_id != effect.id
                        or evidence.request_sha256 != effect.request_sha256
                        or evidence.executor_id != effect.executor_id
                        or evidence.target_incarnation != incarnation
                        or _json(evidence.result) != encoded
                    ):
                        raise Conflict("archive write closure evidence identity changed")
                if effect.state == "settled" and effect.result_json != encoded:
                    raise Conflict("archive write confirmed outcome changed")
                if effect.observation_json not in {None, encoded}:
                    raise Conflict("archive write closure conflicts with its observed result")
                changed = changed or (
                    effect.state != "settled"
                    if report.closure is not None
                    else effect.state != "settled" and effect.observation_json is None
                )
                updates.append((effect, report, encoded))
            if changed:
                apply_result(session, upload, volume, reports[-1].result)
            for effect, report, encoded in updates:
                if report.closure is not None:
                    effect.result_json = encoded
                    effect.state = "settled"
                    effect.unresolved = False
                elif effect.state != "settled":
                    effect.observation_json = encoded
                    effect.state = "uncertain"

    def observe(
        self,
        collection_id: int,
        call_id: str,
        observation: Mapping[str, Any] | None,
        *,
        apply_observation: ApplyResult,
    ) -> None:
        encoded = _json(observation) if observation is not None else None
        with session_scope(self._factory) as session:
            _writer(session)
            upload = self._upload(session, collection_id)
            control = self._control(session, upload)
            self._terminal_lock(session, control)
            object_id = session.scalar(
                select(ArchiveWriteEffectRecord.object_id).where(
                    ArchiveWriteEffectRecord.id == call_id,
                    ArchiveWriteEffectRecord.collection_id == collection_id,
                )
            )
            if object_id is None:
                raise NotFound("archive write observation identity is unavailable")
            volume = self._volume(session, collection_id, object_id)
            effect = session.scalar(
                select(ArchiveWriteEffectRecord)
                .where(ArchiveWriteEffectRecord.id == call_id)
                .with_for_update()
            )
            if effect is None:
                raise NotFound("archive write observation identity is unavailable")
            if encoded is None:
                return
            if len(encoded.encode()) > effect.result_capacity_bytes:
                raise Conflict("archive write observation exceeds its reserved byte grant")
            if effect.state == "settled" and effect.result_json != encoded:
                raise Conflict("archive write observation conflicts with the confirmed result")
            if effect.observation_json not in {None, encoded}:
                raise Conflict("archive write observation changed")
            assert observation is not None
            apply_observation(session, upload, volume, observation)
            effect.observation_json = encoded

    def settle_withdrawn(self, evidence: WithdrawnProviderWrite) -> None:
        """Retain a never-started dispatch result without applying a checkpoint.

        Original ownership survives parent/volume retirement. Current forward
        authority and provider closure are not needed to report this local proof.
        """
        ticket = evidence.ticket
        ticket._require_withdrawal(evidence)
        encoded = _json({"dispatch": "never-started"})
        with session_scope(self._factory) as session:
            _writer(session)
            control = session.get(
                ArchiveWriteControlRecord,
                evidence.collection_id,
                with_for_update=True,
                populate_existing=True,
            )
            if control is None:
                raise NotFound("archive write original construction is unavailable")
            self._terminal_lock(session, control)
            effect = session.get(
                ArchiveWriteEffectRecord,
                evidence.call_id,
                with_for_update=True,
                populate_existing=True,
            )
            if effect is None:
                raise NotFound("archive write original effect is unavailable")
            if (
                effect.collection_id != evidence.collection_id
                or effect.object_id != evidence.object_id
                or effect.dispatch_sha256 != _sha(evidence._nonce)
                or effect.request_sha256 != evidence.request_sha256
                or _sha(effect.request_json) != effect.request_sha256
                or effect.executor_id != evidence.executor_id
                or json.loads(control.construction_json)["archive_incarnation_id"]
                != evidence.target_incarnation
            ):
                raise Conflict("archive write withdrawal original invocation changed")
            if len(encoded.encode()) > effect.result_capacity_bytes:
                raise Conflict("archive write withdrawal exceeds its reserved byte grant")
            if effect.observation_json is not None or (
                effect.state == "settled" and effect.result_json != encoded
            ):
                raise Conflict("archive write withdrawal conflicts with its original outcome")
            effect.state, effect.unresolved, effect.result_json = "settled", False, encoded

    def record_unknown(self, ticket: ArmedProviderWrite) -> None:
        with session_scope(self._factory) as session:
            _writer(session)
            _, _, effect, _ = self._report_context(session, ticket)
            if effect.unresolved:
                effect.state = "uncertain"

    def acknowledge_report(self, ticket: ArmedProviderWrite) -> None:
        """Retire only after the live dispatcher received exact settlement confirmation."""
        with session_scope(self._factory) as session:
            _writer(session)
            _, _, effect, _ = self._report_context(session, ticket)
            if effect.state != "settled":
                raise Conflict("unresolved archive write cannot release its reservation")
            session.delete(effect)

    def unresolved(self, collection_id: int, *, limit: int) -> tuple[str, ...]:
        _positive(limit)
        with read_snapshot(self._factory) as session:
            return tuple(
                session.scalars(
                    select(ArchiveWriteEffectRecord.id)
                    .where(
                        ArchiveWriteEffectRecord.collection_id == collection_id,
                        ArchiveWriteEffectRecord.unresolved.is_(True),
                    )
                    .order_by(ArchiveWriteEffectRecord.id)
                    .limit(limit)
                )
            )

    def select_terminal(
        self,
        collection_id: int,
        *,
        kind: str,
        request: Mapping[str, Any],
        principal_id: str,
        grants: ArchiveWriteGrants,
        guard: Guard,
        validate_selection: Callable[[Session, CollectionUploadRecord], None],
    ) -> str:
        """Persist selection only; the terminal owner adjudicates rights before cleanup."""
        if kind not in {"cancel", "discard"} or not principal_id:
            raise ValueError("archive upload terminal selection is invalid")
        with session_scope(self._factory) as session:
            _writer(session)
            guard(session)
            upload = session.scalar(self._upload_query(collection_id))
            control = session.get(ArchiveWriteControlRecord, collection_id)
            if upload is not None:
                control = self._control(session, upload)
            if control is None:
                raise NotFound("archive write construction is unavailable")
            encoded = _json(
                {
                    "construction": json.loads(control.construction_json),
                    "kind": kind,
                    "principal_id": principal_id,
                    "request": request,
                }
            )
            if len(encoded.encode()) > grants.terminal_bytes:
                raise Conflict("archive upload terminal request exceeds its byte grant")
            operation_id = _sha(encoded)
            prior = session.get(ArchiveUploadTerminalRecord, operation_id)
            if prior is not None:
                if prior.request_json != encoded:
                    raise Conflict("archive upload terminal request preimage changed")
                return prior.id
            if upload is None:
                raise NotFound("archive upload terminal outcome is unavailable")
            if control.gate != "open" or control.terminal_id is not None:
                raise Conflict("a different archive upload terminal request was selected")
            validate_selection(session, upload)
            freeze_registration_execution(
                session, upload, construction_json=control.construction_json
            )
            session.add(
                ArchiveUploadTerminalRecord(
                    id=operation_id,
                    collection_id=collection_id,
                    request_json=encoded,
                    principal_id=principal_id,
                    kind=kind,
                    phase="draining",
                    frontier=control.next_call,
                    original_epoch=control.epoch,
                )
            )
            control.gate = "closed"
            control.epoch = secrets.token_hex(32)
            control.terminal_id = operation_id
            return operation_id
