from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_age import encrypt_age_scrypt
from riverhog_archive_contracts import provenance_structure_identity
from riverhog_core.archive_provenance import ArchiveProvenancePublisher
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import (
    CollectionUploadProvenanceStructureRecord as Structure,
)
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.services import immutable_archive_admission as admission_module
from riverhog_core.services.archive_provenance_structure_checkpoint import (
    ArchiveProvenanceStructureSource as Source,
)
from riverhog_core.services.immutable_archive_admission import ImmutableArchiveAdmission
from riverhog_core.services.immutable_archive_preparation import ImmutableArchivePreparation
from riverhog_core.services.immutable_archive_receiving import drain_original_immutable_report
from riverhog_core.services.immutable_archive_writes import ImmutableArchiveWrites
from riverhog_core.stores.storage_adapter_archive_objects import (
    StorageAdapterImmutableArchiveObjectStore,
)
from riverhog_protocol.errors import Conflict, NotFound
from riverhog_storage_adapter_protocol import PutObjectOperation
from sqlalchemy import delete, event, select

from tests.unit.test_archive_provenance_publication import _case as _format_case
from tests.unit.test_archive_volume_metadata_receiving import _count
from tests.unit.test_archive_write_execution import _executor
from tests.unit.test_finalization_claim_readiness import _fresh_coordinator
from tests.unit.test_immutable_archive_preparation import _metadata

_KINDS = ("history", "record-page", "source-proof")


@contextmanager
def _structure_source(tmp_path, monkeypatch, kind="history", mode="direct", *, database_url=None):
    with _metadata(tmp_path, monkeypatch, database_url=database_url) as fixture:
        case = fixture.actor.case
        original, *_ = _format_case(kind)
        identity = provenance_structure_identity(original.plaintext)
        with session_scope(case.factory) as session:
            session.add(
                Structure(
                    collection_id=case.collection_id,
                    object_id=identity.object_id,
                    kind=identity.kind,
                    relative_path=identity.relative_path,
                    content=original.plaintext,
                )
            )
        with session_scope(case.factory) as session:
            source = Source.load(
                session, case.collection_id, identity.object_id, forward_guard=fixture.guard
            )
        with _executor(fixture.actor.adapter, mode) as transport:
            physical = StorageAdapterImmutableArchiveObjectStore(transport)
            calls = {"observe": 0, "execute": 0}

            class Ports:
                def observe_immutable_object(self, request):
                    calls["observe"] += 1
                    return physical.observe_immutable_object(request)

                def execute_write(self, invocation, *, content):
                    # A separate writer observes the actual admission while
                    # taking the parent NOWAIT: no provider wait owns its lock.
                    with session_scope(case.factory) as session:
                        if session.get_bind().dialect.name == "sqlite":
                            session.connection(execution_options={"sqlite_immediate_writer": True})
                        assert (
                            session.scalar(
                                select(Upload)
                                .where(Upload.collection_id == case.collection_id)
                                .with_for_update(nowait=True)
                            )
                            is not None
                        )
                    calls["execute"] += 1
                    return physical.execute_write(invocation, content=content)

            ports = Ports()
            grants = replace(fixture.grants, calls=1)
            owner = ImmutableArchiveAdmission(case.factory)
            actor = ImmutableArchiveWrites(
                case.factory,
                source=source,
                epoch=fixture.epoch,
                grants=grants,
                executor=ports,
                observer=ports,
            )
            yield SimpleNamespace(
                case=case,
                source=source,
                actor=actor,
                ports=ports,
                physical=physical,
                owner=owner,
                calls=calls,
                grants=grants,
                epoch=fixture.epoch,
                forward_guard=fixture.guard,
            )


def _publish(f):
    received = f.actor.resume_original()
    if received is not None:
        return f.source.publication.sealed_receipt(received)
    return ArchiveProvenancePublisher(
        object_store=f.actor, passphrase="fixture", scrypt_log_n=1
    ).publish_object(f.source.publication)


def _admit(f):
    content = encrypt_age_scrypt(f.source.publication.plaintext, "fixture", log_n=1)
    prepared = ImmutableArchivePreparation(f.case.factory).prepare(
        f.case.collection_id,
        operation=PutObjectOperation(request=f.source.publication.request(content)),
        content=content,
        epoch=f.epoch,
        grants=f.grants,
        guard=f.source.guard,
        validate_metadata=f.source.validate_metadata,
    )
    admitted = f.owner.arm(
        prepared,
        epoch=f.epoch,
        executor_id="e" * 64,
        grants=f.grants,
        guard=f.source.guard,
        validate_metadata=f.source.validate_metadata,
    )
    response = f.owner.execute(admitted, f.ports)
    assert response.execution == "closed"
    return admitted, response


def _received(f):
    with session_scope(f.case.factory) as session:
        source = Source.load(session, f.case.collection_id, f.source.object_id)
        return source.received(session)


@pytest.mark.parametrize("kind", _KINDS)
@pytest.mark.parametrize("mode", ("direct", "http"))
def test_actual_staged_structure_has_one_original_actor_and_exact_receiving_proof(
    tmp_path, monkeypatch, kind, mode
):
    with _structure_source(tmp_path, monkeypatch, kind, mode) as f:
        first = _publish(f)
        assert _publish(f) == first == _received(f)
        assert f.calls == {"observe": 1, "execute": 1}
        assert not f.actor.has_pending and _count(f.case) == 0
        with session_scope(f.case.factory) as session:
            cold = Source.load(session, f.case.collection_id, f.source.object_id)
            assert cold.forward_guard is None
            with pytest.raises(Conflict, match="current forward authority"):
                cold.guard(session)


@pytest.mark.parametrize("kind", _KINDS)
@pytest.mark.parametrize("phase", ("report", "retirement"))
@pytest.mark.parametrize("committed", (False, True))
def test_original_structure_result_survives_receiving_or_retirement_reply_loss(
    tmp_path, monkeypatch, kind, phase, committed
):
    with _structure_source(tmp_path, monkeypatch, kind) as f:
        transaction = admission_module.session_scope
        calls = 0
        position = 2 if phase == "report" else 3

        @contextmanager
        def lost(factory):
            nonlocal calls
            calls += 1
            with transaction(factory) as session:
                yield session
                if calls == position and not committed:
                    raise OSError("injected provenance rollback")
            if calls == position and committed:
                raise OSError("injected provenance commit reply loss")

        with monkeypatch.context() as patch:
            patch.setattr(admission_module, "session_scope", lost)
            with pytest.raises(OSError, match="injected provenance"):
                _publish(f)
        assert f.actor.has_pending
        original = f.actor._response_json
        assert original is not None
        with session_scope(f.case.factory) as session:
            upload = session.get(Upload, f.case.collection_id)
            upload.state = upload.archive_phase = "orphaned"

        def forbidden(*args, **kwargs):
            raise AssertionError("original receiving must precede fresh publisher work")

        with monkeypatch.context() as patch:
            patch.setattr(ArchiveProvenancePublisher, "publish_object", forbidden)
            receipt = _publish(f)
            assert _publish(f) == receipt
        assert receipt == _received(f) and f.actor._response_json == original
        assert not f.actor.has_pending and _count(f.case) == 0
        assert f.calls == {"observe": 1, "execute": 1}


@pytest.mark.parametrize("kind", _KINDS)
@pytest.mark.parametrize("mode", ("direct", "http"))
def test_maintained_cold_drain_receives_original_structure_after_forward_authority_loss(
    tmp_path, monkeypatch, kind, mode
):
    with _structure_source(tmp_path, monkeypatch, kind, mode) as f:
        admitted, response = _admit(f)
        f.owner.report(admitted, response, apply_receipt=lambda *args: None)
        assert _received(f) is None and _count(f.case) == 1
        with session_scope(f.case.factory) as session:
            upload = session.get(Upload, f.case.collection_id)
            upload.state = upload.archive_phase = "orphaned"
            lease = upload.lease_expires_at
        cold = _fresh_coordinator(f.case)
        assert cold._drain_archive_immutable_report(f.case.collection_id)
        assert not cold._drain_archive_immutable_report(f.case.collection_id)
        assert _count(f.case) == 0
        with session_scope(f.case.factory) as session:
            f.source.verify_prepared_receipt(session, admitted.prepared, response.result)
            upload = session.get(Upload, f.case.collection_id)
            assert upload.state == "orphaned" and upload.lease_expires_at == lease


@pytest.mark.parametrize("kind", _KINDS)
def test_positive_unknown_structure_data_never_proves_execution_closure(
    tmp_path, monkeypatch, kind
):
    with _structure_source(tmp_path, monkeypatch, kind) as f:
        admitted, response = _admit(f)
        f.owner.report(
            admitted,
            response.model_copy(update={"execution": "unknown"}),
            apply_receipt=f.source.apply_prepared_receipt,
        )
        assert _received(f) is not None and _count(f.case) == 1
        assert not drain_original_immutable_report(f.case.factory, f.case.collection_id)
        with session_scope(f.case.factory) as session:
            with pytest.raises(Conflict, match="unresolved provider writes"):
                f.source.guard(session)


@pytest.mark.parametrize("change", ("construction", "staged", "path", "kind", "policy"))
def test_original_structure_receiving_rejects_changed_source_and_keeps_report(
    tmp_path, monkeypatch, change
):
    with _structure_source(tmp_path, monkeypatch) as f:
        admitted, response = _admit(f)
        f.owner.report(admitted, response, apply_receipt=lambda *args: None)
        with session_scope(f.case.factory) as session:
            upload = session.get(Upload, f.case.collection_id)
            row = session.get(Structure, (f.case.collection_id, f.source.object_id))
            if change == "construction":
                upload.archive_generation = "f" * 64
            elif change == "staged":
                row.content = row.content.replace(b'"bytes":"7"', b'"bytes":"8"')
                assert row.content != f.source.publication.plaintext
            elif change == "path":
                row.relative_path = "provenance/other.json.age"
            elif change == "kind":
                row.kind = "source-proof"
            else:
                import json

                value = json.loads(upload.planner_checkpoint_json)
                value["policy"]["pack_part_plaintext_bytes"] = str(
                    int(value["policy"]["pack_part_plaintext_bytes"]) + 1
                )
                upload.planner_checkpoint_json = json.dumps(value)
        with pytest.raises((Conflict, ValueError)):
            drain_original_immutable_report(f.case.factory, f.case.collection_id)
        assert _count(f.case) == 1
        with session_scope(f.case.factory) as session:
            assert (
                session.get(Structure, (f.case.collection_id, f.source.object_id)).receipt_json
                is None
            )


def test_missing_structure_parent_cannot_acknowledge_original_live_result(tmp_path, monkeypatch):
    with _structure_source(tmp_path, monkeypatch) as f:
        report = f.actor._admission.report

        def lost(*args, **kwargs):
            report(*args, **kwargs)
            raise OSError("injected original provenance report reply loss")

        with monkeypatch.context() as patch:
            patch.setattr(f.actor._admission, "report", lost)
            with pytest.raises(OSError, match="report reply loss"):
                _publish(f)
        with session_scope(f.case.factory) as session:
            session.execute(delete(Upload).where(Upload.collection_id == f.case.collection_id))
        with pytest.raises(NotFound, match="parent is unavailable"):
            _publish(f)
        assert f.actor.has_pending and f.actor._response_json is not None
        assert f.calls == {"observe": 1, "execute": 1}


def test_receiving_proof_requires_exact_original_projection_and_retained_bytes(
    tmp_path, monkeypatch
):
    with _structure_source(tmp_path, monkeypatch) as f:
        receipt = _publish(f)
        for changes in (
            {"object_id": "provenance-history-" + "f" * 64},
            {"plaintext_bytes": receipt.plaintext_bytes + 1},
            {"plaintext_sha256": "f" * 64},
            {"relative_path": "provenance/other.json.age"},
        ):
            with session_scope(f.case.factory) as session:
                with pytest.raises(Conflict, match="receipt differs"):
                    f.source.verify_receipt(session, replace(receipt, **changes))
        with session_scope(f.case.factory) as session:
            row = session.get(Structure, (f.case.collection_id, f.source.object_id))
            row.receipt_json = row.receipt_json.replace(receipt.stored_sha256, "f" * 64)
        with session_scope(f.case.factory) as session:
            with pytest.raises(Conflict, match="receiving proof changed"):
                f.source.verify_receipt(session, receipt)


def test_structure_receiving_replay_uses_parent_and_source_point_lookups(tmp_path, monkeypatch):
    with _structure_source(tmp_path, monkeypatch) as f:
        receipt = _publish(f)
        engine = f.case.factory.kw["bind"]
        statements = []

        def capture(connection, cursor, statement, *args):
            if statement.lstrip().upper().startswith("SELECT"):
                statements.append(statement)

        event.listen(engine, "before_cursor_execute", capture)
        try:
            counts = []
            for repeats in (1, 4, 8):
                statements.clear()
                for _ in range(repeats):
                    with session_scope(f.case.factory) as session:
                        f.source.apply_receipt(session, receipt)
                counts.append(len(statements))
                assert all("WHERE" in statement for statement in statements)
                assert all(
                    "collection_uploads" in statement
                    or "collection_upload_provenance_structure" in statement
                    for statement in statements
                )
            assert counts == [2, 8, 16]
        finally:
            event.remove(engine, "before_cursor_execute", capture)
