from __future__ import annotations

import json

import pytest
from riverhog_archive_contracts import (
    PROVENANCE_JOURNAL_SEGMENT_BYTES_MAX,
    MemberBindingTreeCheckpoint,
    ProvenanceTerminalDocument,
    binding_tree_commitment,
    ordered_provenance_commitment,
    update_provenance_commitment,
)
from riverhog_core import provenance_volume_source as volumes
from riverhog_core.archive_provenance import ArchiveProvenancePublisher
from riverhog_core.catalog_archive_write_models import ArchiveWriteControlRecord as Control
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadMemberHistoryRecord as History
from riverhog_core.catalog_models import (
    CollectionUploadProvenanceArchiveVolumeRecord as FinalVolume,
)
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.catalog_provenance_publication_models import (
    CollectionUploadProvenanceVolumePublicationRecord as Volume,
)
from riverhog_core.checkpoint_sha256 import CheckpointSHA256
from riverhog_core.services.archive_provenance_volume_checkpoint import (
    ArchiveProvenanceVolumeSource as Source,
)
from riverhog_core.services.immutable_archive_writes import ImmutableArchiveWrites
from riverhog_protocol.errors import Conflict
from sqlalchemy import func, select

from tests.unit.test_archive_provenance_journal_checkpoint import _JOURNAL, _stage_journal
from tests.unit.test_archive_provenance_structure_checkpoint import _admit
from tests.unit.test_archive_provenance_volume_checkpoint import _publish, _select, _volume_source
from tests.unit.test_archive_volume_metadata_receiving import _count


def _ready(f):
    _publish(f, "bindings")
    _publish(f, "metadata")


def _commit(f, source=None):
    with session_scope(f.case.factory) as session:
        return (source or f.metadata).commit_volume(session)


def _snapshot(f):
    with session_scope(f.case.factory) as session:
        upload = session.get(Upload, f.case.collection_id)
        row = session.get(Volume, (f.case.collection_id, 0))
        return (
            upload.provenance_archive_next_sequence,
            upload.provenance_archive_hash_state,
            upload.provenance_archive_after_artifact_id,
            upload.provenance_archive_next_artifact_order,
            upload.provenance_archive_last_journal_id,
            upload.provenance_archive_current_journal_id,
            upload.provenance_archive_current_journal_offset,
            upload.provenance_archive_binding_tree_state,
            upload.provenance_archive_journal_count,
            row.progress_receipt_json,
            session.scalar(select(func.count()).select_from(FinalVolume)),
        )


def _publish_source(f, source):
    actor = ImmutableArchiveWrites(
        f.case.factory,
        source=source,
        epoch=f.epoch,
        grants=source.write_grants(f.original_grants),
        executor=f.ports,
        observer=f.ports,
    )
    return ArchiveProvenancePublisher(
        object_store=actor, passphrase="fixture", scrypt_log_n=1
    ).publish_object(source.publication)


def _journal_pair(f, source):
    with session_scope(f.case.factory) as session:
        journal = source.stage_journal_payload(session, grants=f.original_grants)
    actor = ImmutableArchiveWrites(
        f.case.factory,
        source=journal,
        epoch=f.epoch,
        grants=journal.write_grants(source.write_grants(f.original_grants)),
        executor=f.ports,
        observer=f.ports,
    )
    payload = ArchiveProvenancePublisher(
        object_store=actor, passphrase="fixture", scrypt_log_n=1
    ).publish_object(journal.publication)
    with session_scope(f.case.factory) as session:
        assert source.receive_journal_payload(session) == payload
    _publish_source(f, source)


@pytest.mark.parametrize("mode", ("direct", "http"))
def test_actual_original_pair_advances_corpus_cursor_and_hash_exactly_once(
    tmp_path, monkeypatch, mode
):
    with _volume_source(tmp_path, monkeypatch, mode) as f:
        _ready(f)
        assert _commit(f)
        first = _snapshot(f)
        assert first[0] == 1 and first[2] == f.metadata.document.last_artifact_id
        assert first[3] == f.metadata.document.binding_count and first[-1] == 1
        assert not _commit(f) and _snapshot(f) == first
        with session_scope(f.case.factory) as session:
            f.metadata.verify_progress(session)
            with pytest.raises(Conflict, match="metadata source"):
                Source.load(session, f.case.collection_id, 0, role="bindings").verify_progress(
                    session
                )
        assert f.calls == {"observe": 2, "execute": 2} and _count(f.case) == 0


@pytest.mark.parametrize("committed", (False, True))
def test_original_progress_retains_atomicity_and_acknowledges_commit_reply_loss(
    tmp_path, monkeypatch, committed
):
    with _volume_source(tmp_path, monkeypatch) as f:
        _ready(f)
        before = _snapshot(f)
        with pytest.raises(OSError, match="original progress"):
            with session_scope(f.case.factory) as session:
                assert f.metadata.commit_volume(session)
                if not committed:
                    raise OSError("injected original progress rollback")
            raise OSError("injected original progress commit reply loss")
        if committed:
            after = _snapshot(f)
            assert after != before
            with session_scope(f.case.factory) as session:
                upload = session.get(Upload, f.case.collection_id)
                upload.state = upload.archive_phase = "orphaned"
                lease = upload.lease_expires_at
                session.get(Control, f.case.collection_id).gate = "closed"
            assert not _commit(f) and _snapshot(f) == after
            with session_scope(f.case.factory) as session:
                upload = session.get(Upload, f.case.collection_id)
                assert upload.state == "orphaned" and upload.lease_expires_at == lease
        else:
            assert _snapshot(f) == before and _commit(f)
            after = _snapshot(f)
            assert not _commit(f) and _snapshot(f) == after
        assert f.calls == {"observe": 2, "execute": 2} and _count(f.case) == 0


@pytest.mark.parametrize("change", ("orphan", "cursor", "hash", "gate"))
def test_fresh_progress_requires_current_claim_cursor_hash_and_open_gate(
    tmp_path, monkeypatch, change
):
    with _volume_source(tmp_path, monkeypatch) as f:
        _ready(f)
        with session_scope(f.case.factory) as session:
            upload = session.get(Upload, f.case.collection_id)
            if change == "orphan":
                upload.state = upload.archive_phase = "orphaned"
            elif change == "cursor":
                upload.provenance_archive_next_artifact_order += 1
            elif change == "hash":
                changed = CheckpointSHA256()
                changed.update(b"changed")
                upload.provenance_archive_hash_state = changed.export_state()
            else:
                session.get(Control, f.case.collection_id).gate = "closed"
        before = _snapshot(f)
        with pytest.raises(Conflict):
            _commit(f)
        assert _snapshot(f) == before and before[-1] == 0 and before[-2] is None
        assert f.calls == {"observe": 2, "execute": 2}


@pytest.mark.parametrize("role", ("bindings", "metadata"))
def test_positive_unknown_original_data_never_commits_corpus_progress(tmp_path, monkeypatch, role):
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
        before = _snapshot(f)
        with pytest.raises(Conflict, match="unresolved provider writes"):
            _commit(f)
        assert _snapshot(f) == before and _count(f.case) == 1


@pytest.mark.parametrize(
    "change", ("marker", "final", "cursor", "receipt", "binding_tree", "journals")
)
def test_progress_acknowledgement_requires_its_actual_original_proof(tmp_path, monkeypatch, change):
    with _volume_source(tmp_path, monkeypatch) as f:
        _ready(f)
        assert _commit(f)
        with session_scope(f.case.factory) as session:
            if change == "marker":
                session.get(Volume, (f.case.collection_id, 0)).progress_receipt_json += " "
            elif change == "final":
                session.get(FinalVolume, (f.case.collection_id, 0)).metadata_receipt_json = "{}"
            elif change == "cursor":
                session.get(Upload, f.case.collection_id).provenance_archive_next_sequence = 0
            elif change == "binding_tree":
                session.get(
                    Upload, f.case.collection_id
                ).provenance_archive_binding_tree_state = None
            elif change == "journals":
                session.get(Upload, f.case.collection_id).provenance_archive_journal_count = 1
            else:
                session.get(Volume, (f.case.collection_id, 0)).payload_receipt_json = "{}"
        before = _snapshot(f)
        with pytest.raises((Conflict, TypeError)):
            _commit(f)
        assert _snapshot(f) == before and f.calls == {"observe": 2, "execute": 2}


def test_progress_does_not_adopt_an_unmarked_final_row(tmp_path, monkeypatch):
    with _volume_source(tmp_path, monkeypatch) as f:
        _ready(f)
        with session_scope(f.case.factory) as session:
            row = session.get(Volume, (f.case.collection_id, 0))
            session.add(
                FinalVolume(
                    collection_id=f.case.collection_id,
                    sequence=0,
                    kind=f.metadata.document.payload.kind,
                    document_json=row.document_json,
                    payload_receipt_json=row.payload_receipt_json,
                    metadata_receipt_json=row.metadata_receipt_json,
                )
            )
        before = _snapshot(f)
        with pytest.raises(Conflict, match="no original progress proof"):
            _commit(f)
        assert _snapshot(f) == before


def test_payload_receipt_alone_never_advances_corpus_progress(tmp_path, monkeypatch):
    with _volume_source(tmp_path, monkeypatch) as f:
        _publish(f, "bindings")
        before = _snapshot(f)
        with pytest.raises(Conflict, match="metadata is not received"):
            _commit(f)
        assert _snapshot(f) == before


def test_actual_binding_then_journal_progress_keeps_original_prior_proof_point_readable(
    tmp_path, monkeypatch
):
    with _volume_source(tmp_path, monkeypatch) as f:
        _stage_journal(f, _JOURNAL, b"original corpus journal")
        _ready(f)
        first = f.metadata
        assert _commit(f)
        with session_scope(f.case.factory) as session:
            second = Source.stage_next(session, f.case.collection_id, grants=f.original_grants)
        assert second.sequence == 1 and second.document.payload.kind == "journal"
        assert json.loads(second.source_json)["hash_state"] is not None
        _journal_pair(f, second)
        assert _commit(f, second)
        before = _snapshot(f)
        assert not _commit(f, first) and _snapshot(f) == before
        assert before[0] == 2 and before[4] == _JOURNAL and before[5:7] == (None, 0)
        assert before[8] == 1
        tree = MemberBindingTreeCheckpoint.from_json_bytes(before[7].encode())
        assert tree.count == first.document.binding_count
        terminal = ProvenanceTerminalDocument(
            archive_generation=first.document.archive_generation,
            artifact_set_sha256=first.document.artifact_set_sha256,
            sequence=2,
        )
        actual = CheckpointSHA256.from_state(before[1])
        update_provenance_commitment(actual, terminal)
        assert actual.hexdigest() == ordered_provenance_commitment(
            (first.document, second.document, terminal)
        )
        assert f.calls == {"observe": 4, "execute": 4} and _count(f.case) == 0


def test_actual_multi_frame_journal_progress_preserves_exact_continuation_and_terminal_hash(
    tmp_path, monkeypatch
):
    content = b"x" * PROVENANCE_JOURNAL_SEGMENT_BYTES_MAX + b"end"
    with _volume_source(tmp_path, monkeypatch, kind="journal", journal_content=content) as f:
        first = f.metadata
        _journal_pair(f, first)
        assert _commit(f)
        before = _snapshot(f)
        assert before[4:7] == (None, _JOURNAL, PROVENANCE_JOURNAL_SEGMENT_BYTES_MAX)
        assert before[8] == 0
        with session_scope(f.case.factory) as session:
            second = Source.stage_next(session, f.case.collection_id, grants=f.original_grants)
        assert second.document.journal_offset == PROVENANCE_JOURNAL_SEGMENT_BYTES_MAX
        assert second.document.payload.bytes == 3
        _journal_pair(f, second)
        assert _commit(f, second)
        after = _snapshot(f)
        assert after[0] == 2 and after[4:7] == (_JOURNAL, None, 0)
        assert after[8] == 1 and after[7] == before[7]
        terminal = ProvenanceTerminalDocument(
            archive_generation=first.document.archive_generation,
            artifact_set_sha256=first.document.artifact_set_sha256,
            sequence=2,
        )
        actual = CheckpointSHA256.from_state(after[1])
        update_provenance_commitment(actual, terminal)
        assert actual.hexdigest() == ordered_provenance_commitment(
            (first.document, second.document, terminal)
        )
        assert f.calls == {"observe": 4, "execute": 4} and _count(f.case) == 0


@pytest.mark.parametrize("change", ("binding_tree", "journal_count"))
def test_new_corpus_selection_refuses_root_input_checkpoint_drift(tmp_path, monkeypatch, change):
    with _volume_source(tmp_path, monkeypatch) as f:
        with session_scope(f.case.factory) as session:
            upload = session.get(Upload, f.case.collection_id)
            if change == "binding_tree":
                upload.provenance_archive_binding_tree_state = "{}"
            else:
                upload.provenance_archive_journal_count = 1
        with pytest.raises(Conflict, match="binding tree checkpoint|journal count"):
            with session_scope(f.case.factory) as session:
                Source.stage_next(session, f.case.collection_id, grants=f.original_grants)
        assert f.calls == {"observe": 0, "execute": 0}


def _multi_page_binding_progress(f, monkeypatch):
    with session_scope(f.case.factory) as session:
        bindings = [
            volumes.member_history_binding_row(row)
            for row in session.scalars(
                select(History)
                .where(History.collection_id == f.case.collection_id)
                .order_by(History.artifact_id)
            )
        ]
    expected = binding_tree_commitment(bindings)
    _ready(f)
    assert _commit(f)
    first = _snapshot(f)
    checkpoint = MemberBindingTreeCheckpoint.from_json_bytes(first[7].encode())
    assert checkpoint.count == 512 and checkpoint.previous_artifact_id == bindings[511].artifact_id
    assert not _commit(f) and _snapshot(f) == first
    calls = []
    convert = volumes.member_history_binding_row

    def original_page(row):
        calls.append(row.artifact_id)
        return convert(row)

    with monkeypatch.context() as patch:
        patch.setattr(volumes, "member_history_binding_row", original_page)
        with session_scope(f.case.factory) as session:
            metadata = Source.stage_next(session, f.case.collection_id, grants=f.original_grants)
        with session_scope(f.case.factory) as session:
            payload = Source.load(session, f.case.collection_id, 1, role="bindings")
        _publish_source(f, payload)
        _publish_source(f, metadata)
        assert _commit(f, metadata)
    assert calls and set(calls) == {bindings[-1].artifact_id}
    after = _snapshot(f)
    checkpoint = MemberBindingTreeCheckpoint.from_json_bytes(after[7].encode())
    assert checkpoint.count == expected.count == 513
    assert checkpoint.root_sha256 == expected.root_sha256
    assert after[8] == 0 and after[0] == after[-1] == 2
    assert not _commit(f, metadata) and _snapshot(f) == after
    assert not _commit(f) and _snapshot(f) == after
    assert f.calls == {"observe": 4, "execute": 4} and _count(f.case) == 0


def test_actual_multi_page_binding_progress_retains_exact_root_without_prefix_replay(
    tmp_path, monkeypatch
):
    with _volume_source(tmp_path, monkeypatch, binding_count=513) as f:
        _multi_page_binding_progress(f, monkeypatch)


@pytest.mark.parametrize("count", (2**63, 2**255 - 1))
def test_completed_journal_checkpoint_preserves_v1_sequence_extent(tmp_path, monkeypatch, count):
    with _volume_source(tmp_path, monkeypatch) as f:
        with session_scope(f.case.factory) as session:
            session.get(Upload, f.case.collection_id).provenance_archive_journal_count = count
        with session_scope(f.case.factory) as session:
            assert (
                session.get(Upload, f.case.collection_id).provenance_archive_journal_count == count
            )
