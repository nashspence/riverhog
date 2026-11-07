from __future__ import annotations

import hashlib
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_archive_contracts import (
    PROVENANCE_JOURNAL_SEGMENT_BYTES_MAX,
    MemberBindingTreeCheckpoint,
    MemberHistoryBinding,
)
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.archive_provenance import ArchiveProvenancePublisher
from riverhog_core.catalog_archive_write_models import ArchiveWritePreparationRecord
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadArtifactRecord as Artifact
from riverhog_core.catalog_models import CollectionUploadMemberHistoryRecord as History
from riverhog_core.catalog_models import (
    CollectionUploadProvenanceArchiveVolumeRecord as FinalVolume,
)
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.catalog_provenance_publication_models import (
    CollectionUploadProvenanceVolumePublicationRecord as Volume,
)
from riverhog_core.services.archive_provenance_volume_checkpoint import (
    ArchiveProvenanceVolumeSource as Source,
)
from riverhog_core.services.immutable_archive_admission import ImmutableArchiveAdmission
from riverhog_core.services.immutable_archive_receiving import drain_original_immutable_report
from riverhog_core.services.immutable_archive_writes import ImmutableArchiveWrites
from riverhog_protocol.errors import Conflict
from sqlalchemy import func, select

from tests.unit.test_archive_provenance_journal_checkpoint import _JOURNAL, _stage_journal
from tests.unit.test_archive_provenance_journal_writer import _lose_ack
from tests.unit.test_archive_provenance_structure_checkpoint import _admit
from tests.unit.test_archive_volume_metadata_receiving import _count, _metadata_source


@contextmanager
def _volume_source(
    tmp_path,
    monkeypatch,
    mode="direct",
    kind="bindings",
    *,
    database_url=None,
    journal_content=b"original journal corpus",
    binding_count=None,
):
    with _metadata_source(tmp_path, monkeypatch, mode=mode, database_url=database_url) as fixture:
        case = fixture.case
        with session_scope(case.factory) as session:
            # This receiving witness starts from accepted final binding rows.
            # Native history validation remains qualified by its maintained owner.
            upload = session.get(Upload, case.collection_id)
            upload.provenance_closure_validated = True
            artifacts = list(
                session.scalars(
                    select(Artifact)
                    .where(Artifact.collection_id == case.collection_id)
                    .order_by(Artifact.artifact_id)
                )
            )
            if binding_count is not None:
                first = artifacts[0]
                original_count = len(artifacts)
                assert binding_count >= original_count
                for index in range(binding_count - original_count):
                    artifact = Artifact(
                        collection_id=case.collection_id,
                        artifact_id=hashlib.sha256(
                            f"accepted binding {index}".encode()
                        ).hexdigest(),
                        artifact_order=original_count + index,
                        bytes=first.bytes,
                        sha256=first.sha256,
                    )
                    session.add(artifact)
                    artifacts.append(artifact)
                session.flush()
                artifacts.sort(key=lambda artifact: artifact.artifact_id)
                upload.artifact_count = binding_count
            checkpoint = MemberBindingTreeCheckpoint()
            for artifact in artifacts:
                binding = MemberHistoryBinding(
                    artifact.artifact_id,
                    artifact.bytes,
                    artifact.sha256,
                    hashlib.sha256(artifact.artifact_id.encode()).hexdigest(),
                    42,
                )
                checkpoint = checkpoint.append(binding)
                session.add(
                    History(
                        collection_id=case.collection_id,
                        artifact_id=artifact.artifact_id,
                        history_sha256=binding.history_sha256,
                        history_bytes=binding.history_bytes,
                        binding_json=canonical_json_bytes(binding.to_mapping()).decode(),
                    )
                )
            if kind == "journal":
                upload.provenance_archive_next_artifact_order = upload.artifact_count
                upload.provenance_archive_after_artifact_id = checkpoint.previous_artifact_id
                upload.provenance_archive_binding_tree_state = checkpoint.to_json_bytes().decode()
        f = SimpleNamespace(case=case)
        if kind == "journal":
            _stage_journal(f, _JOURNAL, journal_content)
        with session_scope(case.factory) as session:
            source = Source.stage_next(session, case.collection_id, grants=fixture.grants)
        calls = {"observe": 0, "execute": 0}

        class Ports:
            def observe_immutable_object(self, request):
                calls["observe"] += 1
                return fixture.physical.observe_immutable_object(request)

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
                return fixture.physical.execute_write(invocation, content=content)

        yield SimpleNamespace(
            case=case,
            metadata=source,
            ports=Ports(),
            physical=fixture.physical,
            calls=calls,
            epoch=fixture.epoch,
            original_grants=fixture.grants,
            owner=ImmutableArchiveAdmission(case.factory),
            actors={},
        )


def _select(f, role):
    with session_scope(f.case.factory) as session:
        source = Source.load(session, f.case.collection_id, 0, role=role)
    return SimpleNamespace(
        case=f.case,
        source=source,
        epoch=f.epoch,
        grants=source.write_grants(f.original_grants),
        owner=f.owner,
        ports=f.ports,
    )


def _publish(f, role):
    selected = _select(f, role)
    actor = f.actors.get(role)
    if actor is None:
        actor = f.actors[role] = ImmutableArchiveWrites(
            f.case.factory,
            source=selected.source,
            epoch=f.epoch,
            grants=selected.grants,
            executor=f.ports,
            observer=f.ports,
        )
    received = actor.resume_original()
    if received is not None:
        return selected.source.publication.sealed_receipt(received)
    return ArchiveProvenancePublisher(
        object_store=actor, passphrase="fixture", scrypt_log_n=1
    ).publish_object(selected.source.publication)


def _received(f, role):
    selected = _select(f, role)
    with session_scope(f.case.factory) as session:
        return selected.source.received(session)


@pytest.mark.parametrize("mode", ("direct", "http"))
def test_actual_corpus_volume_has_two_independent_original_physical_calls(
    tmp_path, monkeypatch, mode
):
    with _volume_source(tmp_path, monkeypatch, mode) as f:
        with session_scope(f.case.factory) as session:
            with pytest.raises(Conflict, match="payload is not received"):
                f.metadata.guard(session)
        payload = _publish(f, "bindings")
        assert payload == _received(f, "bindings") and _received(f, "metadata") is None
        assert f.calls == {"observe": 1, "execute": 1} and _count(f.case) == 0
        metadata = _publish(f, "metadata")
        assert metadata == _received(f, "metadata")
        assert f.calls == {"observe": 2, "execute": 2} and _count(f.case) == 0
        assert _publish(f, "bindings") == payload and _publish(f, "metadata") == metadata
        with session_scope(f.case.factory) as session:
            upload = session.get(Upload, f.case.collection_id)
            assert upload.provenance_archive_next_sequence == 0
            assert upload.provenance_archive_next_artifact_order == 0
            assert session.scalar(select(func.count()).select_from(FinalVolume)) == 0
            assert session.scalar(select(func.count()).select_from(Volume)) == 1
        with session_scope(f.case.factory) as session:
            staged = Source.stage_next(session, f.case.collection_id, grants=f.original_grants)
            assert staged == f.metadata


def _cold_receiving(f, role):
    if role == "metadata":
        _publish(f, "bindings")
    selected = _select(f, role)
    admitted, response = _admit(selected)
    f.owner.report(admitted, response, apply_receipt=lambda *args: None)
    assert _received(f, role) is None and _count(f.case) == 1
    with session_scope(f.case.factory) as session:
        upload = session.get(Upload, f.case.collection_id)
        upload.state = upload.archive_phase = "orphaned"
        upload.provenance_archive_next_sequence = 9
        lease = upload.lease_expires_at
    assert drain_original_immutable_report(f.case.factory, f.case.collection_id)
    assert _received(f, role).stored_sha256 == response.result.stored_sha256
    assert _count(f.case) == 0
    with session_scope(f.case.factory) as session:
        upload = session.get(Upload, f.case.collection_id)
        assert upload.state == "orphaned" and upload.lease_expires_at == lease
        assert upload.provenance_archive_next_sequence == 9


@pytest.mark.parametrize("role", ("bindings", "metadata"))
@pytest.mark.parametrize("mode", ("direct", "http"))
def test_maintained_cold_corpus_receiving_uses_original_point_source_after_forward_loss(
    tmp_path, monkeypatch, role, mode
):
    with _volume_source(tmp_path, monkeypatch, mode) as f:
        _cold_receiving(f, role)


@pytest.mark.parametrize("role", ("bindings", "metadata"))
@pytest.mark.parametrize("phase", ("report", "retirement"))
@pytest.mark.parametrize("committed", (False, True))
def test_original_corpus_result_survives_receiving_and_retirement_reply_loss(
    tmp_path, monkeypatch, role, phase, committed
):
    with _volume_source(tmp_path, monkeypatch) as f:
        if role == "metadata":
            _publish(f, "bindings")
        with _lose_ack(monkeypatch, phase, committed):
            with pytest.raises(OSError, match="injected journal writer"):
                _publish(f, role)
        actor = f.actors[role]
        response = actor._response_json
        calls = dict(f.calls)
        assert actor.has_pending and response is not None
        with session_scope(f.case.factory) as session:
            upload = session.get(Upload, f.case.collection_id)
            upload.state = upload.archive_phase = "orphaned"
            upload.provenance_archive_next_sequence = 9

        def forbidden(*args, **kwargs):
            raise AssertionError("original corpus result must precede fresh publisher work")

        with monkeypatch.context() as patch:
            patch.setattr(ArchiveProvenancePublisher, "publish_object", forbidden)
            assert _publish(f, role) == _received(f, role)
        assert actor._response_json == response and not actor.has_pending
        assert _count(f.case) == 0 and f.calls == calls


@pytest.mark.parametrize("role", ("bindings", "metadata"))
def test_positive_unknown_corpus_data_never_allows_the_next_physical_call(
    tmp_path, monkeypatch, role
):
    with _volume_source(tmp_path, monkeypatch) as f:
        if role == "metadata":
            _publish(f, "bindings")
        selected = _select(f, role)
        admitted, response = _admit(selected)
        f.owner.report(
            admitted,
            response.model_copy(update={"execution": "unknown"}),
            apply_receipt=selected.source.apply_prepared_receipt,
        )
        assert _received(f, role) is not None and _count(f.case) == 1
        with session_scope(f.case.factory) as session:
            with pytest.raises(Conflict, match="unresolved provider writes"):
                f.metadata.guard(session)
        assert not drain_original_immutable_report(f.case.factory, f.case.collection_id)
        assert _count(f.case) == 1


@pytest.mark.parametrize("change", ("construction", "binding", "document", "context", "receipt"))
def test_changed_original_corpus_source_retains_original_report_without_acknowledgement(
    tmp_path, monkeypatch, change
):
    with _volume_source(tmp_path, monkeypatch) as f:
        selected = _select(f, "bindings")
        admitted, response = _admit(selected)
        f.owner.report(admitted, response, apply_receipt=lambda *args: None)
        with session_scope(f.case.factory) as session:
            if change == "construction":
                session.get(Upload, f.case.collection_id).archive_generation = "f" * 64
            elif change == "binding":
                session.scalar(
                    select(History)
                    .where(History.collection_id == f.case.collection_id)
                    .order_by(History.artifact_id)
                ).history_sha256 = "f" * 64
            else:
                row = session.get(Volume, (f.case.collection_id, 0))
                if change == "document":
                    row.document_json += " "
                elif change == "context":
                    row.source_json += " "
                else:
                    row.payload_receipt_json = "{}"
        with pytest.raises((Conflict, TypeError)):
            drain_original_immutable_report(f.case.factory, f.case.collection_id)
        assert _count(f.case) == 1


def test_original_corpus_cursor_is_live_authority_only_for_fresh_writes(tmp_path, monkeypatch):
    with _volume_source(tmp_path, monkeypatch) as f:
        source = _select(f, "bindings").source
        with session_scope(f.case.factory) as session:
            session.get(Upload, f.case.collection_id).provenance_archive_after_artifact_id = (
                "f" * 64
            )
        with session_scope(f.case.factory) as session:
            with pytest.raises(Conflict, match="original cursor changed"):
                source.guard(session)
        assert f.calls == {"observe": 0, "execute": 0}


@pytest.mark.parametrize("mode", ("direct", "http"))
def test_corpus_journal_payload_reuses_original_journal_receiving_without_a_second_call(
    tmp_path, monkeypatch, mode
):
    with _volume_source(tmp_path, monkeypatch, mode, kind="journal") as f:
        with session_scope(f.case.factory) as session:
            journal = f.metadata.stage_journal_payload(session, grants=f.original_grants)
        actor = ImmutableArchiveWrites(
            f.case.factory,
            source=journal,
            epoch=f.epoch,
            grants=journal.write_grants(f.metadata.write_grants(f.original_grants)),
            executor=f.ports,
            observer=f.ports,
        )
        payload = ArchiveProvenancePublisher(
            object_store=actor, passphrase="fixture", scrypt_log_n=1
        ).publish_object(journal.publication)
        with session_scope(f.case.factory) as session:
            assert f.metadata.receive_journal_payload(session) == payload
            assert f.metadata.receive_journal_payload(session) == payload
            with pytest.raises(Conflict, match="original journal owner"):
                Source.load(session, f.case.collection_id, 0, role="bindings")
        metadata = _publish(f, "metadata")
        assert _received(f, "metadata") == metadata
        assert f.calls == {"observe": 2, "execute": 2} and _count(f.case) == 0


def test_full_corpus_journal_frame_charges_both_original_sources_to_one_preparation_grant(
    tmp_path, monkeypatch
):
    with _volume_source(
        tmp_path,
        monkeypatch,
        kind="journal",
        journal_content=b"x" * PROVENANCE_JOURNAL_SEGMENT_BYTES_MAX,
    ) as f:
        with session_scope(f.case.factory) as session:
            journal = f.metadata.stage_journal_payload(session, grants=f.original_grants)
        grants = journal.write_grants(f.metadata.write_grants(f.original_grants))
        actor = ImmutableArchiveWrites(
            f.case.factory,
            source=journal,
            epoch=f.epoch,
            grants=grants,
            executor=f.ports,
            observer=f.ports,
        )
        payload = ArchiveProvenancePublisher(
            object_store=actor, passphrase="fixture", scrypt_log_n=1
        ).publish_object(journal.publication)
        with session_scope(f.case.factory) as session:
            prepared = session.scalar(
                select(ArchiveWritePreparationRecord).where(
                    ArchiveWritePreparationRecord.collection_id == f.case.collection_id
                )
            )
            retained = len(prepared.preparation_json.encode()) + len(
                prepared.identity_json.encode()
            )
            assert retained + journal.reserved_source_bytes + f.metadata.reserved_source_bytes <= (
                f.original_grants.metadata_preparation_bytes
            )
            assert f.metadata.receive_journal_payload(session) == payload
        assert payload.plaintext_bytes == PROVENANCE_JOURNAL_SEGMENT_BYTES_MAX
        assert f.calls == {"observe": 1, "execute": 1} and _count(f.case) == 0


def test_corpus_preparation_capacity_refusal_can_resume_with_larger_operator_grant(
    tmp_path, monkeypatch
):
    with _volume_source(tmp_path, monkeypatch) as f:
        source = _select(f, "bindings").source
        small = replace(
            f.original_grants, metadata_preparation_bytes=source.reserved_source_bytes + 500
        )
        actor = ImmutableArchiveWrites(
            f.case.factory,
            source=source,
            epoch=f.epoch,
            grants=source.write_grants(small),
            executor=f.ports,
            observer=f.ports,
        )
        with pytest.raises(Conflict, match="preparation.*grant"):
            ArchiveProvenancePublisher(
                object_store=actor, passphrase="fixture", scrypt_log_n=1
            ).publish_object(source.publication)
        assert f.calls == {"observe": 0, "execute": 0} and _count(f.case) == 0
        assert _publish(f, "bindings") == _received(f, "bindings")
        assert f.calls == {"observe": 1, "execute": 1} and _count(f.case) == 0
