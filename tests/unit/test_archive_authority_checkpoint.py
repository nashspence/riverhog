from __future__ import annotations

import json
from contextlib import contextmanager
from dataclasses import asdict, replace

import pytest
from riverhog_age import encrypt_age_scrypt
from riverhog_core.archive_provenance import SealedArchiveProvenance
from riverhog_core.archive_recovery_descriptor import ArchiveRecoveryDescriptorPublisher
from riverhog_core.archive_root import ArchiveRootPublisher
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.domain.archive import SealedProvenanceObject
from riverhog_core.ports.archive_objects import ImmutableObjectReceipt
from riverhog_core.services.archive_authority_checkpoint import (
    ArchiveRecoverySource,
    ArchiveRootSource,
)
from riverhog_core.services.archive_volume_metadata_checkpoint import ArchiveVolumeMetadataSource
from riverhog_core.services.immutable_archive_admission import ImmutableArchiveAdmission
from riverhog_core.services.immutable_archive_preparation import ImmutableArchivePreparation
from riverhog_protocol.errors import Conflict, NotFound
from riverhog_storage_adapter_protocol import PutObjectOperation
from sqlalchemy import delete, event

from tests.unit.test_archive_volume_metadata_receiving import _count, _metadata_source


@contextmanager
def _authority_source(
    tmp_path, monkeypatch, mode="direct", *, database_url=None, configure_service=None
):
    with _metadata_source(
        tmp_path,
        monkeypatch,
        mode=mode,
        database_url=database_url,
        configure_service=configure_service,
    ) as fixture:
        publisher = ArchiveRootPublisher(
            object_store=fixture.physical, passphrase="fixture", scrypt_log_n=1
        )
        for sequence in range(3):
            with session_scope(fixture.case.factory) as session:
                source = ArchiveVolumeMetadataSource.load(
                    session, fixture.case.collection_id, sequence
                )
            if source.object_id is None:
                receipt = publisher.publish_terminal_metadata(
                    archive_storage_prefix=source.publication.archive_storage_prefix,
                    document=source.document,
                )
            else:
                receipt = publisher.publish_volume_metadata(
                    archive_storage_prefix=source.publication.archive_storage_prefix,
                    document=source.document,
                )
            with session_scope(fixture.case.factory) as session:
                source.apply_receipt(session, receipt)
        # These sealed provenance facts are the root source's prior input.
        # Provenance graph publication has its own maintained qualification.
        provenance = SealedProvenanceObject(
            object_id="provenance-root",
            kind="provenance-root",
            relative_path="provenance/root.json.age",
            plaintext_bytes=32,
            plaintext_sha256="b" * 64,
            stored_bytes=64,
            stored_sha256="c" * 64,
            revision="provenance-v1",
            completed_at="2026-08-03T00:00:00Z",
        )
        with session_scope(fixture.case.factory) as session:
            upload = session.get(Upload, fixture.case.collection_id)
            upload.provenance_identity = provenance.plaintext_sha256
            prior = SealedArchiveProvenance(provenance.plaintext_sha256, provenance)
            upload.provenance_archive_root_receipt_json = json.dumps(
                {"identity": prior.identity, "root": asdict(prior.root)},
                sort_keys=True,
                separators=(",", ":"),
            )
        yield fixture


def _source(fixture, kind):
    cls = ArchiveRootSource if kind == "root" else ArchiveRecoverySource
    with session_scope(fixture.case.factory) as session:
        return cls.load(session, fixture.case.collection_id)


def _admit(fixture, source):
    if isinstance(source, ArchiveRootSource):
        content = encrypt_age_scrypt(source.publication.plaintext, "fixture", log_n=1)
        request = source.publication.request(content)
    else:
        content = source.publication.content
        request = source.publication.request()
    prepared = ImmutableArchivePreparation(fixture.case.factory).prepare(
        fixture.case.collection_id,
        operation=PutObjectOperation(request=request),
        content=content,
        epoch=fixture.epoch,
        grants=fixture.grants,
        guard=source.guard,
        validate_metadata=source.validate_metadata,
    )
    owner = ImmutableArchiveAdmission(fixture.case.factory)
    admitted = owner.arm(
        prepared,
        epoch=fixture.epoch,
        executor_id="e" * 64,
        grants=fixture.grants,
        guard=source.guard,
        validate_metadata=source.validate_metadata,
    )
    return owner, admitted


def _write(fixture, source, *, receive=True, observed=False, unknown=False):
    owner, admitted = _admit(fixture, source)
    callback = source.apply_prepared_receipt if receive else lambda *args: None
    if observed:
        assert isinstance(source, ArchiveRootSource)
        fixture.physical.put_immutable_object(
            object_path=source.publication.object_path,
            content=encrypt_age_scrypt(source.publication.plaintext, "fixture", log_n=1),
            content_type=admitted.prepared.operation.request.content_type,
            required_identity_assertions=source.publication.required_identity_assertions,
            placement_policy="immediate_default",
        )
        receipt = fixture.physical.observe_immutable_object(admitted.prepared.operation.request)
        assert receipt.stored_sha256 != admitted.prepared.operation.request.stored_sha256
        owner.accept_existing(admitted, owner.withdraw(admitted), receipt, apply_receipt=callback)
    else:
        response = owner.execute(admitted, fixture.physical)
        assert response.execution == "closed"
        if unknown:
            response = response.model_copy(update={"execution": "unknown"})
        owner.report(admitted, response, apply_receipt=callback)
        receipt = response.result
    return owner, admitted, receipt


def _snapshot(fixture):
    with session_scope(fixture.case.factory) as session:
        upload = session.get(Upload, fixture.case.collection_id)
        return upload.archive_root_receipt_json, upload.final_authority_json


@pytest.mark.parametrize("mode", ["direct", "http"])
@pytest.mark.parametrize("observed", [False, True])
def test_actual_root_retirement_precedes_exact_stored_root_recovery_under_one_call_grant(
    tmp_path, monkeypatch, mode, observed
):
    with _authority_source(tmp_path, monkeypatch, mode) as fixture:
        root_source = _source(fixture, "root")
        owner, admitted, receipt = _write(fixture, root_source, observed=observed)
        root_json, authority = _snapshot(fixture)
        assert root_json is not None and authority is None and _count(fixture.case) == 1
        with session_scope(fixture.case.factory) as session:
            root = root_source.received(session)
            assert root.stored_sha256 == receipt.stored_sha256
        owner.acknowledge_report(admitted, verify_receipt=root_source.verify_prepared_receipt)
        assert _count(fixture.case) == 0
        recovery = _source(fixture, "recovery")
        assert recovery.publication.descriptor.root.stored_sha256 == receipt.stored_sha256
        assert recovery.publication.descriptor.root.stored_bytes == receipt.stored_bytes
        owner, admitted, descriptor = _write(fixture, recovery)
        owner.acknowledge_report(admitted, verify_receipt=recovery.verify_prepared_receipt)
        final = json.loads(_snapshot(fixture)[1])
        assert final["root"] == json.loads(root_json)
        assert final["recovery"]["sha256"] == descriptor.stored_sha256
        assert _count(fixture.case) == 0
        with session_scope(fixture.case.factory) as session:
            root_source.verify_prepared_receipt(
                session,
                prepared=ImmutableArchivePreparation(fixture.case.factory).load(
                    fixture.case.collection_id,
                    root_source.publication.object_path,
                    epoch=fixture.epoch,
                ),
                receipt=receipt,
            )
        cold = _source(fixture, "recovery")
        with session_scope(fixture.case.factory) as session:
            cold.verify_prepared_receipt(session, admitted.prepared, descriptor)
            with pytest.raises(Conflict, match="claim is not current"):
                cold.guard(session)


@pytest.mark.parametrize("kind", ["root", "recovery"])
@pytest.mark.parametrize("mode", ["direct", "http"])
def test_original_closed_report_receives_after_forward_claim_loss_and_cold_restart(
    tmp_path, monkeypatch, kind, mode
):
    with _authority_source(tmp_path, monkeypatch, mode) as fixture:
        root = _source(fixture, "root")
        if kind == "recovery":
            owner, admitted, _ = _write(fixture, root)
            owner.acknowledge_report(admitted, verify_receipt=root.verify_prepared_receipt)
        source = _source(fixture, kind)
        _, admitted, receipt = _write(fixture, source, receive=False)
        with session_scope(fixture.case.factory) as session:
            upload = session.get(Upload, fixture.case.collection_id)
            upload.state = upload.archive_phase = "orphaned"
        with session_scope(fixture.case.factory) as session:
            with pytest.raises(Conflict, match="claim is not current"):
                source.guard(session)
        cold = ImmutableArchiveAdmission(fixture.case.factory)
        settled = cold.load_next_collection_report(fixture.case.collection_id)
        assert settled is not None and settled.receipt is not None
        cls = ArchiveRootSource if kind == "root" else ArchiveRecoverySource

        def apply(session, prepared, returned):
            cls.from_prepared(session, prepared).apply_prepared_receipt(session, prepared, returned)

        def verify(session, prepared, returned):
            cls.from_prepared(session, prepared).verify_prepared_receipt(
                session, prepared, returned
            )

        cold.receive_settled(settled, apply_receipt=apply, verify_receipt=verify)
        cold.acknowledge_settled(settled, verify_receipt=verify)
        assert _count(fixture.case) == 0
        with session_scope(fixture.case.factory) as session:
            source.verify_prepared_receipt(session, admitted.prepared, receipt)


@pytest.mark.parametrize("kind", ["root", "recovery"])
@pytest.mark.parametrize("committed", [False, True])
def test_actual_receiving_rollback_or_lost_commit_reply_keeps_original_projection(
    tmp_path, monkeypatch, kind, committed
):
    with _authority_source(tmp_path, monkeypatch) as fixture:
        root = _source(fixture, "root")
        if kind == "recovery":
            owner, admitted, _ = _write(fixture, root)
            owner.acknowledge_report(admitted, verify_receipt=root.verify_prepared_receipt)
        source = _source(fixture, kind)
        owner, admitted, receipt = _write(fixture, source, receive=False)
        with pytest.raises(OSError, match="injected"):
            with session_scope(fixture.case.factory) as session:
                source.apply_prepared_receipt(session, admitted.prepared, receipt)
                if not committed:
                    raise OSError("injected receiving rollback")
            raise OSError("injected lost commit reply")
        before = _snapshot(fixture)
        assert (before[0 if kind == "root" else 1] is not None) == committed
        with session_scope(fixture.case.factory) as session:
            source.apply_prepared_receipt(session, admitted.prepared, receipt)
            source.verify_prepared_receipt(session, admitted.prepared, receipt)
        after = _snapshot(fixture)
        if committed:
            assert before == after
        owner.acknowledge_report(admitted, verify_receipt=source.verify_prepared_receipt)
        assert _count(fixture.case) == 0


def test_positive_unknown_root_cannot_grant_recovery_admission(tmp_path, monkeypatch):
    with _authority_source(tmp_path, monkeypatch) as fixture:
        root = _source(fixture, "root")
        _, admitted, receipt = _write(fixture, root, unknown=True)
        assert _snapshot(fixture)[0] is not None and _count(fixture.case) == 1
        recovery = _source(fixture, "recovery")
        assert recovery.publication.descriptor.root.stored_sha256 == receipt.stored_sha256
        with session_scope(fixture.case.factory) as session:
            root.verify_prepared_receipt(session, admitted.prepared, receipt)
            with pytest.raises(Conflict, match="unresolved provider writes"):
                recovery.guard(session)
        with pytest.raises(Conflict, match="unresolved provider writes"):
            _admit(fixture, recovery)
        assert _snapshot(fixture)[1] is None and _count(fixture.case) == 1


@pytest.mark.parametrize("kind", ["root", "recovery"])
@pytest.mark.parametrize(
    "field", ["tree", "ordered", "prefix", "passphrase", "creation", "count", "provenance"]
)
def test_original_authority_receiving_rejects_changed_source_after_capture(
    tmp_path, monkeypatch, kind, field
):
    with _authority_source(tmp_path, monkeypatch) as fixture:
        root = _source(fixture, "root")
        if kind == "recovery":
            owner, admitted, _ = _write(fixture, root)
            owner.acknowledge_report(admitted, verify_receipt=root.verify_prepared_receipt)
        source = _source(fixture, kind)
        _, admitted, receipt = _write(fixture, source, receive=False)
        with session_scope(fixture.case.factory) as session:
            upload = session.get(Upload, fixture.case.collection_id)
            attribute, value = {
                "tree": ("archive_tree_sha256", "f" * 64),
                "ordered": ("archive_ordered_volume_sha256", "f" * 64),
                "prefix": ("archive_storage_prefix", "archive/other"),
                "passphrase": ("passphrase_id", "different-passphrase-id"),
                "creation": ("creation_identity_sha256", "f" * 64),
                "count": ("artifact_count", upload.artifact_count + 1),
                "provenance": ("provenance_identity", "f" * 64),
            }[field]
            setattr(upload, attribute, value)
        before = _snapshot(fixture)
        with session_scope(fixture.case.factory) as session:
            with pytest.raises(Conflict, match="original construction changed"):
                source.apply_prepared_receipt(session, admitted.prepared, receipt)
        assert _snapshot(fixture) == before and _count(fixture.case) == 1


def test_original_stored_root_cannot_change_after_recovery_capture(tmp_path, monkeypatch):
    with _authority_source(tmp_path, monkeypatch) as fixture:
        root = _source(fixture, "root")
        owner, admitted, _ = _write(fixture, root)
        owner.acknowledge_report(admitted, verify_receipt=root.verify_prepared_receipt)
        source = _source(fixture, "recovery")
        _, admitted, receipt = _write(fixture, source, receive=False)
        with session_scope(fixture.case.factory) as session:
            upload = session.get(Upload, fixture.case.collection_id)
            value = json.loads(upload.archive_root_receipt_json)
            value["stored_sha256"] = "f" * 64
            upload.archive_root_receipt_json = json.dumps(
                value, sort_keys=True, separators=(",", ":")
            )
        for method in (
            source.guard,
            lambda session: source.apply_prepared_receipt(session, admitted.prepared, receipt),
        ):
            with session_scope(fixture.case.factory) as session:
                with pytest.raises(Conflict, match="original stored root changed"):
                    method(session)
        assert _snapshot(fixture)[1] is None and _count(fixture.case) == 1


@pytest.mark.parametrize("kind", ["root", "recovery"])
def test_missing_authority_parent_is_not_positive_receiving_proof(tmp_path, monkeypatch, kind):
    with _authority_source(tmp_path, monkeypatch) as fixture:
        root = _source(fixture, "root")
        if kind == "recovery":
            owner, admitted, _ = _write(fixture, root)
            owner.acknowledge_report(admitted, verify_receipt=root.verify_prepared_receipt)
        source = _source(fixture, kind)
        owner, admitted, receipt = _write(fixture, source, receive=False)
        with session_scope(fixture.case.factory) as session:
            session.execute(
                delete(Upload).where(Upload.collection_id == fixture.case.collection_id)
            )
        with pytest.raises(NotFound, match="receiving source is unavailable"):
            owner.acknowledge_report(admitted, verify_receipt=source.verify_prepared_receipt)
        assert _count(fixture.case) == 1


@pytest.mark.parametrize(
    "field",
    [
        "relative_path",
        "plaintext_sha256",
        "artifact_count",
        "manifest_bytes",
        "stored_sha256",
        "revision",
    ],
)
def test_root_received_proof_is_exact_and_replay_cannot_replace_it(tmp_path, monkeypatch, field):
    with _authority_source(tmp_path, monkeypatch) as fixture:
        source = _source(fixture, "root")
        owner, admitted, _ = _write(fixture, source)
        with session_scope(fixture.case.factory) as session:
            original = source.received(session)
        owner.acknowledge_report(admitted, verify_receipt=source.verify_prepared_receipt)
        before = _snapshot(fixture)
        value = getattr(original, field)
        changed = replace(
            original,
            **{
                field: value + 1
                if type(value) is int
                else b"changed"
                if type(value) is bytes
                else "changed"
            },
        )
        for method in (source.apply_receipt, source.verify_receipt):
            with session_scope(fixture.case.factory) as session:
                with pytest.raises(Conflict):
                    method(session, changed)
        assert _snapshot(fixture) == before


def test_recovery_requires_positive_root_receiving_and_complete_pair(tmp_path, monkeypatch):
    with _authority_source(tmp_path, monkeypatch) as fixture:
        with pytest.raises(Conflict, match="not durably received"):
            _source(fixture, "recovery")
        root = _source(fixture, "root")
        owner, admitted, _ = _write(fixture, root)
        owner.acknowledge_report(admitted, verify_receipt=root.verify_prepared_receipt)
        source = _source(fixture, "recovery")
        receipt = ArchiveRecoveryDescriptorPublisher(object_store=fixture.physical).publish(
            archive_storage_prefix=source.publication.archive_storage_prefix,
            root=_received_root(fixture, root),
            encryption=source.publication.descriptor.encryption,
        )
        with session_scope(fixture.case.factory) as session:
            with pytest.raises(Conflict, match="not durably received"):
                source.verify_receipt(session, receipt)
            source.apply_receipt(session, receipt)
            source.verify_receipt(session, receipt)
        assert _snapshot(fixture)[1] is not None
        changed = replace(receipt, content=b"changed")
        with session_scope(fixture.case.factory) as session:
            with pytest.raises(Conflict, match="differs from its source"):
                source.apply_receipt(session, changed)


def _received_root(fixture, source):
    with session_scope(fixture.case.factory) as session:
        return source.received(session)


@pytest.mark.parametrize("replays", [1, 4, 8])
def test_root_and_recovery_receiving_replays_use_only_indexed_parent_facts(
    tmp_path, monkeypatch, record_property, replays
):
    with _authority_source(tmp_path, monkeypatch) as fixture:
        root = _source(fixture, "root")
        owner, admitted, root_receipt = _write(fixture, root)
        owner.acknowledge_report(admitted, verify_receipt=root.verify_prepared_receipt)
        recovery = _source(fixture, "recovery")
        owner, admitted, receipt = _write(fixture, recovery)
        owner.acknowledge_report(admitted, verify_receipt=recovery.verify_prepared_receipt)
        statements = []

        def record(_conn, _cursor, statement, _parameters, _context, _many):
            if statement.lstrip().upper().startswith("SELECT"):
                statements.append(statement)

        engine = fixture.case.factory.kw["bind"]
        event.listen(engine, "before_cursor_execute", record)
        try:
            for _ in range(replays):
                with session_scope(fixture.case.factory) as session:
                    root.verify_receipt(
                        session,
                        root.publication.sealed_receipt(
                            # Root receives original ciphertext; descriptor identity is separate.
                            ImmutableObjectReceipt(
                                object_path=root_receipt.object_path,
                                revision=root_receipt.revision,
                                entity_token=None,
                                stored_bytes=root_receipt.stored_bytes,
                                stored_sha256=root_receipt.stored_sha256,
                                completed_at=root_receipt.completed_at,
                            )
                        ),
                    )
                    recovery.verify_prepared_receipt(session, admitted.prepared, receipt)
        finally:
            event.remove(engine, "before_cursor_execute", record)
        assert len(statements) == 2 * replays
        assert all("collection_uploads" in sql and "collection_id =" in sql for sql in statements)
        record_property(
            "authority_receiving_work",
            {"replays": replays, "parent_selects": len(statements), "volume_or_member_censuses": 0},
        )
