from __future__ import annotations

import hashlib
import json
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_archive_contracts import PROVENANCE_JOURNAL_SEGMENT_BYTES_MAX
from riverhog_core.archive_provenance import ArchiveProvenancePublisher
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import (
    CollectionUploadProvenanceCustodyObjectRecord as Custody,
)
from riverhog_core.catalog_models import CollectionUploadProvenanceJournalChunkRecord as Chunk
from riverhog_core.catalog_models import CollectionUploadProvenanceJournalRecord as Journal
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.catalog_provenance_publication_models import (
    CollectionUploadProvenanceJournalPublicationRecord as Publication,
)
from riverhog_core.checkpoint_sha256 import CheckpointSHA256
from riverhog_core.services import immutable_archive_admission as admission_module
from riverhog_core.services.archive_provenance_journal_checkpoint import (
    ArchiveProvenanceJournalSource as Source,
)
from riverhog_core.services.archive_provenance_journal_checkpoint import ProvenanceJournalRange
from riverhog_core.services.immutable_archive_admission import ImmutableArchiveAdmission
from riverhog_core.services.immutable_archive_receiving import drain_original_immutable_report
from riverhog_core.services.immutable_archive_writes import ImmutableArchiveWrites
from riverhog_core.stores.storage_adapter_archive_objects import (
    StorageAdapterImmutableArchiveObjectStore,
)
from riverhog_protocol import COLLECTION_UPLOAD_PROVENANCE_APPEND_BYTES_MAX as CHUNK_BYTES
from riverhog_protocol.errors import Conflict, NotFound
from sqlalchemy import delete, event, func, select

from tests.unit.test_archive_provenance_structure_checkpoint import _admit
from tests.unit.test_archive_volume_metadata_receiving import _count
from tests.unit.test_archive_write_execution import _executor
from tests.unit.test_finalization_claim_readiness import _fresh_coordinator
from tests.unit.test_immutable_archive_preparation import _metadata

_JOURNAL = "urn:uuid:11111111-1111-4111-8111-111111111111"
_OTHER = "urn:uuid:22222222-2222-4222-8222-222222222222"


def _stage_journal(f, journal_id, content):
    with session_scope(f.case.factory) as session:
        # This publication witness starts from an accepted sealed source.
        # Native semantic acceptance is qualified by its own maintained owner.
        session.add(
            Journal(
                collection_id=f.case.collection_id,
                journal_id=journal_id,
                bytes=len(content),
                sha256=hashlib.sha256(content).hexdigest(),
                state="sealed",
                accepted_bytes=len(content),
                next_chunk_ordinal=(len(content) + CHUNK_BYTES - 1) // CHUNK_BYTES,
                content_hash_state=CheckpointSHA256().export_state(),
            )
        )
        session.flush()
        for offset in range(0, len(content), CHUNK_BYTES):
            session.add(
                Chunk(
                    collection_id=f.case.collection_id,
                    journal_id=journal_id,
                    ordinal=offset // CHUNK_BYTES,
                    byte_offset=offset,
                    content=content[offset : offset + CHUNK_BYTES],
                )
            )


@contextmanager
def _journal_source(
    tmp_path, monkeypatch, mode="direct", *, database_url=None, content=b"abcdefghabcdefgh"
):
    with _metadata(tmp_path, monkeypatch, database_url=database_url) as fixture:
        case = fixture.actor.case
        grants = replace(fixture.grants, calls=1)
        f = SimpleNamespace(case=case)
        _stage_journal(f, _JOURNAL, content)
        with session_scope(case.factory) as session:
            source = Source.stage(
                session,
                case.collection_id,
                journal_id=_JOURNAL,
                offset=0,
                size=len(content) // 2,
                grants=grants,
                forward_guard=fixture.guard,
            )
        with _executor(fixture.actor.adapter, mode) as transport:
            physical = StorageAdapterImmutableArchiveObjectStore(transport)
            calls = {"observe": 0, "execute": 0}

            class Ports:
                def observe_immutable_object(self, request):
                    calls["observe"] += 1
                    return physical.observe_immutable_object(request)

                def execute_write(self, invocation, *, content):
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
            effective = source.write_grants(grants)
            f = SimpleNamespace(
                case=case,
                source=source,
                ports=ports,
                physical=physical,
                calls=calls,
                epoch=fixture.epoch,
                original_grants=grants,
                grants=effective,
                forward_guard=fixture.guard,
                owner=ImmutableArchiveAdmission(case.factory),
                actor=ImmutableArchiveWrites(
                    case.factory,
                    source=source,
                    epoch=fixture.epoch,
                    grants=effective,
                    executor=ports,
                    observer=ports,
                ),
            )
            yield f


def _publish(f):
    received = f.actor.resume_original()
    if received is not None:
        return f.source.publication.sealed_receipt(received)
    return ArchiveProvenancePublisher(
        object_store=f.actor, passphrase="fixture", scrypt_log_n=1
    ).publish_object(f.source.publication)


def _received(f):
    with session_scope(f.case.factory) as session:
        return Source.load(
            session, f.case.collection_id, f.source.publication.plaintext_sha256
        ).received(session)


@pytest.mark.parametrize("mode", ("direct", "http"))
def test_actual_original_journal_actor_receives_once_and_never_recreates_dispatch(
    tmp_path, monkeypatch, mode
):
    with _journal_source(tmp_path, monkeypatch, mode) as f:
        first = _publish(f)
        assert _publish(f) == first == _received(f)
        assert f.calls == {"observe": 1, "execute": 1}
        assert _count(f.case) == 0 and not f.actor.has_pending
        with session_scope(f.case.factory) as session:
            cold = Source.from_prepared(session, f.actor._prepared)
            assert cold.journal_range == f.source.journal_range
            assert cold.forward_guard is None
            with pytest.raises(Conflict, match="current forward authority"):
                cold.guard(session)


@pytest.mark.parametrize("mode", ("direct", "http"))
def test_shared_ciphertext_keeps_first_origin_and_receives_other_positions_without_calls(
    tmp_path, monkeypatch, mode
):
    with _journal_source(tmp_path, monkeypatch, mode) as f:
        first = _publish(f)
        _stage_journal(f, _OTHER, b"abcdefghabcdefgh")
        for journal, offset in [(_JOURNAL, 8), (_OTHER, 0), (_OTHER, 8)]:
            with session_scope(f.case.factory) as session:
                original = Source.stage(
                    session,
                    f.case.collection_id,
                    journal_id=journal,
                    offset=offset,
                    size=8,
                    grants=f.original_grants,
                    forward_guard=f.forward_guard,
                )
                selected, _ = ProvenanceJournalRange.capture(
                    session, f.case.collection_id, journal, offset=offset, size=8
                )
                assert original.source_json == f.source.source_json
                assert original.journal_range.journal_id == _JOURNAL
                assert original.journal_range.offset == 0
                assert original.receive_association(session, selected) == first
                assert original.receive_association(session, selected) == first
        assert f.calls == {"observe": 1, "execute": 1} and _count(f.case) == 0
        with session_scope(f.case.factory) as session:
            assert session.scalar(select(func.count()).select_from(Publication)) == 1
            assert session.scalar(select(func.count()).select_from(Custody)) == 4


@pytest.mark.parametrize("phase", ("report", "retirement"))
@pytest.mark.parametrize("committed", (False, True))
def test_original_journal_result_survives_receiving_and_retirement_acknowledgement_loss(
    tmp_path, monkeypatch, phase, committed
):
    with _journal_source(tmp_path, monkeypatch) as f:
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
                    raise OSError("injected original journal rollback")
            if calls == position and committed:
                raise OSError("injected original journal commit reply loss")

        with monkeypatch.context() as patch:
            patch.setattr(admission_module, "session_scope", lost)
            with pytest.raises(OSError, match="injected original journal"):
                _publish(f)
        assert f.actor.has_pending and f.actor._response_json is not None
        original = f.actor._response_json
        with session_scope(f.case.factory) as session:
            upload = session.get(Upload, f.case.collection_id)
            upload.state = upload.archive_phase = "orphaned"

        def forbidden(*args, **kwargs):
            raise AssertionError("original result must precede new journal publisher work")

        with monkeypatch.context() as patch:
            patch.setattr(ArchiveProvenancePublisher, "publish_object", forbidden)
            assert _publish(f) == _received(f)
        assert f.actor._response_json == original and not f.actor.has_pending
        assert f.calls == {"observe": 1, "execute": 1} and _count(f.case) == 0


@pytest.mark.parametrize("mode", ("direct", "http"))
def test_maintained_cold_journal_receiving_uses_retained_origin_after_forward_loss(
    tmp_path, monkeypatch, mode
):
    with _journal_source(tmp_path, monkeypatch, mode) as f:
        admitted, response = _admit(f)
        f.owner.report(admitted, response, apply_receipt=lambda *args: None)
        assert _received(f) is None and _count(f.case) == 1
        with session_scope(f.case.factory) as session:
            upload = session.get(Upload, f.case.collection_id)
            upload.state = upload.archive_phase = "orphaned"
            lease = upload.lease_expires_at
        coordinator = _fresh_coordinator(f.case)
        assert coordinator._drain_archive_immutable_report(f.case.collection_id)
        assert not coordinator._drain_archive_immutable_report(f.case.collection_id)
        assert _count(f.case) == 0 and f.calls == {"observe": 0, "execute": 1}
        with session_scope(f.case.factory) as session:
            f.source.verify_prepared_receipt(session, admitted.prepared, response.result)
            upload = session.get(Upload, f.case.collection_id)
            assert upload.state == "orphaned" and upload.lease_expires_at == lease


def test_positive_unknown_journal_data_and_associations_never_prove_closure(tmp_path, monkeypatch):
    with _journal_source(tmp_path, monkeypatch) as f:
        admitted, response = _admit(f)
        f.owner.report(
            admitted,
            response.model_copy(update={"execution": "unknown"}),
            apply_receipt=f.source.apply_prepared_receipt,
        )
        assert _received(f) is not None and _count(f.case) == 1
        with session_scope(f.case.factory) as session:
            selected, _ = ProvenanceJournalRange.capture(
                session, f.case.collection_id, _JOURNAL, offset=8, size=8
            )
            f.source.receive_association(session, selected)
        assert _count(f.case) == 1
        assert not drain_original_immutable_report(f.case.factory, f.case.collection_id)
        with session_scope(f.case.factory) as session:
            with pytest.raises(Conflict, match="unresolved provider writes"):
                f.source.guard(session)


@pytest.mark.parametrize("change", ("parent", "journal", "chunk", "context", "origin", "receipt"))
def test_cold_receiving_refuses_changed_original_source_without_releasing_control(
    tmp_path, monkeypatch, change
):
    with _journal_source(tmp_path, monkeypatch) as f:
        admitted, response = _admit(f)
        f.owner.report(admitted, response, apply_receipt=lambda *args: None)
        with session_scope(f.case.factory) as session:
            if change == "parent":
                session.get(Upload, f.case.collection_id).archive_generation = "f" * 64
            elif change == "journal":
                session.get(Journal, (f.case.collection_id, _JOURNAL)).sha256 = "f" * 64
            elif change == "chunk":
                session.get(Chunk, (f.case.collection_id, _JOURNAL, 0)).content = b"x" * 16
            else:
                row = session.get(
                    Publication, (f.case.collection_id, f.source.publication.plaintext_sha256)
                )
                if change == "context":
                    row.source_json += " "
                elif change == "origin":
                    value = json.loads(row.source_json)
                    value["range"]["offset"] = 8
                    row.source_json = json.dumps(value, sort_keys=True, separators=(",", ":"))
                else:
                    row.receipt_json = "{}"
        with pytest.raises((Conflict, TypeError)):
            drain_original_immutable_report(f.case.factory, f.case.collection_id)
        assert _count(f.case) == 1


def test_missing_parent_keeps_original_journal_result_owned(tmp_path, monkeypatch):
    with _journal_source(tmp_path, monkeypatch) as f:
        original = f.actor._admission.report

        def lost(*args, **kwargs):
            original(*args, **kwargs)
            raise OSError("injected original journal report reply loss")

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


def test_inconsistent_logical_association_cannot_receive_original_physical_receipt(
    tmp_path, monkeypatch
):
    with _journal_source(tmp_path, monkeypatch) as f:
        _publish(f)
        _stage_journal(f, _OTHER, b"other---other---")
        with session_scope(f.case.factory) as session:
            selected, _ = ProvenanceJournalRange.capture(
                session, f.case.collection_id, _OTHER, offset=0, size=8
            )
            with pytest.raises(Conflict, match="does not identify original bytes"):
                f.source.receive_association(session, selected)
        with session_scope(f.case.factory) as session:
            assert (
                session.get(Custody, (f.case.collection_id, _OTHER, 0, selected.plaintext_sha256))
                is None
            )


def test_source_and_exact_original_preparation_share_one_operator_byte_grant(
    tmp_path, monkeypatch, record_property
):
    content = b"x" * (2 * PROVENANCE_JOURNAL_SEGMENT_BYTES_MAX)
    with _journal_source(tmp_path, monkeypatch, content=content) as f:
        receipt = _publish(f)
        prepared = f.actor._prepared
        assert receipt.plaintext_bytes == PROVENANCE_JOURNAL_SEGMENT_BYTES_MAX
        assert _received(f) == receipt and _count(f.case) == 0
        assert (
            f.grants.metadata_preparation_bytes + f.source.reserved_source_bytes
            == f.original_grants.metadata_preparation_bytes
        )
        assert prepared._value()["capacity_bytes"] == f.grants.metadata_preparation_bytes
        assert len(prepared.content) > PROVENANCE_JOURNAL_SEGMENT_BYTES_MAX
        with pytest.raises(Conflict, match="source exceeds its preparation grant"):
            f.source.write_grants(
                replace(
                    f.original_grants,
                    metadata_preparation_bytes=f.source.reserved_source_bytes,
                )
            )
        for key, value in {
            "plaintext_bytes": receipt.plaintext_bytes,
            "stored_bytes": receipt.stored_bytes,
            "source_metadata_bytes": f.source.reserved_source_bytes,
            "preparation_grant_bytes": f.grants.metadata_preparation_bytes,
            "operator_grant_bytes": f.original_grants.metadata_preparation_bytes,
        }.items():
            record_property(key, value)


def test_cold_origin_lookup_does_not_search_journals_or_logical_associations(tmp_path, monkeypatch):
    with _journal_source(tmp_path, monkeypatch) as f:
        admitted, response = _admit(f)
        f.owner.report(admitted, response, apply_receipt=lambda *args: None)
        for index in range(17):
            _stage_journal(f, "unrelated-" + str(index), b"unrelated bytes")
        engine = f.case.factory.kw["bind"]
        statements = []

        def capture(connection, cursor, statement, *args):
            if statement.lstrip().upper().startswith("SELECT"):
                statements.append(statement)

        event.listen(engine, "before_cursor_execute", capture)
        try:
            with session_scope(f.case.factory) as session:
                cold = Source.from_prepared(session, admitted.prepared)
                assert cold.journal_range.journal_id == _JOURNAL
            assert len(statements) == 4
            assert all("WHERE" in statement for statement in statements)
            assert not any("collection_upload_provenance_custody_objects" in s for s in statements)
        finally:
            event.remove(engine, "before_cursor_execute", capture)


def test_preparation_capacity_refusal_can_resume_with_larger_grant_and_same_source(
    tmp_path, monkeypatch
):
    with _journal_source(tmp_path, monkeypatch) as f:
        original_context = f.source.source_json
        small = replace(
            f.original_grants,
            metadata_preparation_bytes=f.source.reserved_source_bytes + 1,
        )
        f.actor = ImmutableArchiveWrites(
            f.case.factory,
            source=f.source,
            epoch=f.epoch,
            grants=f.source.write_grants(small),
            executor=f.ports,
            observer=f.ports,
        )
        with pytest.raises(Conflict, match="preparation exceeds its byte grant"):
            _publish(f)
        assert not f.actor.has_pending and f.calls == {"observe": 0, "execute": 0}
        assert _count(f.case) == 0
        with session_scope(f.case.factory) as session:
            source = Source.stage(
                session,
                f.case.collection_id,
                journal_id=_JOURNAL,
                offset=0,
                size=8,
                grants=f.original_grants,
                forward_guard=f.forward_guard,
            )
        assert source.source_json == original_context
        f.actor = ImmutableArchiveWrites(
            f.case.factory,
            source=source,
            epoch=f.epoch,
            grants=source.write_grants(f.original_grants),
            executor=f.ports,
            observer=f.ports,
        )
        assert _publish(f) == _received(f)
        assert f.calls == {"observe": 1, "execute": 1} and _count(f.case) == 0
