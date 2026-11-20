from __future__ import annotations

import hashlib
import json
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_archive_contracts import (
    ProvenanceRootDocument,
    ProvenanceTerminalDocument,
    update_provenance_commitment,
)
from riverhog_core.archive_provenance import ArchiveProvenancePublisher
from riverhog_core.catalog_archive_write_models import ArchiveWriteControlRecord as Control
from riverhog_core.catalog_archive_write_models import ArchiveWritePreparationRecord as Preparation
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.catalog_provenance_publication_models import (
    CollectionUploadProvenanceAuthorityPublicationRecord as Authority,
)
from riverhog_core.catalog_provenance_publication_models import (
    CollectionUploadProvenanceVolumePublicationRecord as Volume,
)
from riverhog_core.checkpoint_sha256 import CheckpointSHA256
from riverhog_core.services.archive_provenance_authority_checkpoint import (
    ArchiveProvenanceAuthoritySource as Source,
)
from riverhog_core.services.archive_provenance_volume_checkpoint import (
    ArchiveProvenanceVolumeSource,
)
from riverhog_core.services.immutable_archive_receiving import drain_original_immutable_report
from riverhog_core.services.immutable_archive_writes import ImmutableArchiveWrites
from riverhog_protocol.errors import Conflict
from sqlalchemy import select

from tests.unit.test_archive_provenance_journal_writer import _lose_ack
from tests.unit.test_archive_provenance_structure_checkpoint import _admit
from tests.unit.test_archive_provenance_volume_checkpoint import _volume_source
from tests.unit.test_archive_provenance_volume_finalizer import _two_volumes
from tests.unit.test_archive_volume_metadata_receiving import _count

_ROLES = ("terminal", "root")


def _publish(f, source=None):
    source = source or f.authority
    actor = f.authority_actors.get(source.role)
    if actor is None:
        actor = f.authority_actors[source.role] = ImmutableArchiveWrites(
            f.case.factory,
            source=source,
            epoch=f.epoch,
            grants=source.write_grants(f.original_grants),
            executor=f.ports,
            observer=f.ports,
        )
    original = actor.resume_original()
    if original is not None:
        return source.publication.sealed_receipt(original)
    return ArchiveProvenancePublisher(
        object_store=actor, passphrase="fixture", scrypt_log_n=1
    ).publish_object(source.publication)


def _commit(f, source=None):
    with session_scope(f.case.factory) as session:
        return (source or f.authority).commit_progress(session)


def _stage(f):
    with session_scope(f.case.factory) as session:
        return Source.stage_next(session, f.case.collection_id, grants=f.original_grants)


def _snapshot(f):
    with session_scope(f.case.factory) as session:
        upload = session.get(Upload, f.case.collection_id)
        return {
            "terminal": upload.provenance_archive_terminal_receipt_json,
            "ordered": upload.provenance_archive_ordered_sha256,
            "prefix": upload.provenance_archive_hash_state,
            "root": upload.provenance_archive_root_receipt_json,
            "identity": upload.provenance_identity,
            "sequence": upload.provenance_archive_next_sequence,
            "progress": {
                row.role: row.progress_receipt_json
                for row in session.scalars(
                    select(Authority).where(Authority.collection_id == f.case.collection_id)
                )
            },
        }


def _received(f):
    with session_scope(f.case.factory) as session:
        return f.authority.received(session)


def _orphan(f):
    with session_scope(f.case.factory) as session:
        upload = session.get(Upload, f.case.collection_id)
        upload.state = upload.archive_phase = "orphaned"
        session.get(Control, f.case.collection_id).gate = "closed"
        return upload.lease_expires_at


@contextmanager
def _authority_source(tmp_path, monkeypatch, role="terminal", mode="direct", *, database_url=None):
    with _volume_source(tmp_path, monkeypatch, mode, database_url=database_url) as f:
        _two_volumes(f)
        f.authority_actors = {}
        f.authority = _stage(f)
        assert f.authority.role == "terminal"
        with session_scope(f.case.factory) as session:
            f.last_volume = ArchiveProvenanceVolumeSource.load(
                session, f.case.collection_id, 1, role="metadata"
            )
        if role == "root":
            _publish(f)
            assert _commit(f)
            f.authority = _stage(f)
            assert f.authority.role == "root"
        f.before_calls = dict(f.calls)
        yield f


def _original(f):
    return SimpleNamespace(
        case=f.case,
        source=f.authority,
        epoch=f.epoch,
        grants=f.authority.write_grants(f.original_grants),
        ports=f.ports,
        owner=f.owner,
    )


def _one_call(f):
    return {name: value + 1 for name, value in f.before_calls.items()}


def _complete_pair(f):
    role = f.authority.role
    before = _snapshot(f)
    receipt = _publish(f)
    assert receipt == _received(f) and _snapshot(f) == before
    assert f.calls == _one_call(f) and _count(f.case) == 0
    assert _commit(f)
    after = _snapshot(f)
    assert after != before and after["sequence"] == before["sequence"] == 2
    assert after["prefix"] == before["prefix"]
    assert not _commit(f) and _snapshot(f) == after
    assert _publish(f) == receipt and f.calls == _one_call(f)
    with session_scope(f.case.factory) as session:
        assert not f.last_volume.commit_volume(session)
    if role == "terminal":
        document = ProvenanceTerminalDocument.from_json_bytes(f.authority.publication.plaintext)
        digest = CheckpointSHA256.from_state(before["prefix"])
        update_provenance_commitment(digest, document)
        assert after["ordered"] == digest.hexdigest()
        assert _stage(f).role == "root"
    else:
        document = ProvenanceRootDocument.from_json_bytes(f.authority.publication.plaintext)
        assert after["identity"] == document.identity
        assert json.loads(after["root"]) == {
            "identity": document.identity,
            "root": json.loads(f.authority._projection(receipt)),
        }
        assert _stage(f) is None


@pytest.mark.parametrize("role", _ROLES)
@pytest.mark.parametrize("mode", ("direct", "http"))
def test_actual_terminal_and_root_separate_physical_receiving_from_once_only_progress(
    tmp_path, monkeypatch, role, mode
):
    with _authority_source(tmp_path, monkeypatch, role, mode) as f:
        _complete_pair(f)


@pytest.mark.parametrize("role", _ROLES)
@pytest.mark.parametrize("phase", ("report", "retirement"))
@pytest.mark.parametrize("committed", (False, True))
def test_original_authority_receiving_survives_lost_replies_and_forward_authority(
    tmp_path, monkeypatch, role, phase, committed
):
    with _authority_source(tmp_path, monkeypatch, role) as f:
        before = _snapshot(f)
        with _lose_ack(monkeypatch, phase, committed):
            with pytest.raises(OSError, match="injected journal writer"):
                _publish(f)
        actor = f.authority_actors[role]
        response = actor._response_json
        assert actor.has_pending and response is not None
        lease = _orphan(f)

        def forbidden(*args, **kwargs):
            raise AssertionError("original authority receiving precedes fresh forward dispatch")

        with monkeypatch.context() as patch:
            patch.setattr(ArchiveProvenancePublisher, "publish_object", forbidden)
            patch.setattr(Source, "guard", forbidden)
            assert _publish(f) == _received(f)
        assert actor._response_json == response and not actor.has_pending
        assert f.calls == _one_call(f) and _count(f.case) == 0 and _snapshot(f) == before
        with session_scope(f.case.factory) as session:
            upload = session.get(Upload, f.case.collection_id)
            assert upload.state == "orphaned" and upload.lease_expires_at == lease
        with pytest.raises(Conflict, match="claim is not current"):
            _commit(f)


@pytest.mark.parametrize("role", _ROLES)
@pytest.mark.parametrize("committed", (False, True))
def test_original_authority_progress_is_atomic_and_acks_old_commit_after_orphaning(
    tmp_path, monkeypatch, role, committed
):
    with _authority_source(tmp_path, monkeypatch, role) as f:
        _publish(f)
        before = _snapshot(f)
        with pytest.raises(OSError, match="original authority progress"):
            with session_scope(f.case.factory) as session:
                assert f.authority.commit_progress(session)
                if not committed:
                    raise OSError("injected original authority progress rollback")
            raise OSError("injected original authority progress commit reply loss")
        if committed:
            after = _snapshot(f)
            assert after != before
            _orphan(f)
        else:
            assert _snapshot(f) == before
            assert _commit(f)
            after = _snapshot(f)
        assert not _commit(f) and _snapshot(f) == after
        with session_scope(f.case.factory) as session:
            assert not f.last_volume.commit_volume(session)
        assert f.calls == _one_call(f) and _count(f.case) == 0


def _cold_receive(f):
    selected = _original(f)
    admitted, response = _admit(selected)
    f.owner.report(admitted, response, apply_receipt=lambda *args: None)
    assert _received(f) is None and _count(f.case) == 1
    _orphan(f)
    assert drain_original_immutable_report(f.case.factory, f.case.collection_id)
    assert _received(f).stored_sha256 == response.result.stored_sha256 and _count(f.case) == 0
    with session_scope(f.case.factory) as session:
        f.authority.verify_prepared_receipt(session, admitted.prepared, response.result)
    assert f.calls == {
        "observe": f.before_calls["observe"],
        "execute": f.before_calls["execute"] + 1,
    }


@pytest.mark.parametrize("role", _ROLES)
@pytest.mark.parametrize("mode", ("direct", "http"))
def test_actual_cold_authority_routing_receives_closed_original_after_claim_loss(
    tmp_path, monkeypatch, role, mode
):
    with _authority_source(tmp_path, monkeypatch, role, mode) as f:
        before = _snapshot(f)
        _cold_receive(f)
        assert _snapshot(f) == before


@pytest.mark.parametrize("role", _ROLES)
def test_positive_unknown_authority_data_cannot_advance_progress_or_select_next_role(
    tmp_path, monkeypatch, role
):
    with _authority_source(tmp_path, monkeypatch, role) as f:
        admitted, response = _admit(_original(f))
        f.owner.report(
            admitted,
            response.model_copy(update={"execution": "unknown"}),
            apply_receipt=f.authority.apply_prepared_receipt,
        )
        assert _received(f) is not None and _count(f.case) == 1
        before = _snapshot(f)
        with pytest.raises(Conflict, match="unresolved provider writes"):
            _commit(f)
        with pytest.raises(Conflict, match="unresolved provider writes"):
            _stage(f)
        assert _snapshot(f) == before
        assert f.calls == {
            "observe": f.before_calls["observe"],
            "execute": f.before_calls["execute"] + 1,
        }


@pytest.mark.parametrize("role", _ROLES)
@pytest.mark.parametrize("change", ("orphan", "gate", "hash", "cursor"))
def test_new_authority_effect_requires_current_claim_gate_and_original_cursor(
    tmp_path, monkeypatch, role, change
):
    with _authority_source(tmp_path, monkeypatch, role) as f:
        with session_scope(f.case.factory) as session:
            upload = session.get(Upload, f.case.collection_id)
            if change == "orphan":
                upload.state = upload.archive_phase = "orphaned"
            elif change == "gate":
                session.get(Control, f.case.collection_id).gate = "closed"
            elif change == "hash":
                changed = CheckpointSHA256()
                changed.update(b"changed original prefix")
                upload.provenance_archive_hash_state = changed.export_state()
            else:
                upload.provenance_archive_next_artifact_order += 1
        with pytest.raises(Conflict):
            _publish(f)
        assert f.calls == f.before_calls and _count(f.case) == 0


@pytest.mark.parametrize("role", _ROLES)
@pytest.mark.parametrize("change", ("source", "document", "tail"))
def test_authority_original_receiving_refuses_changed_retained_source(
    tmp_path, monkeypatch, role, change
):
    with _authority_source(tmp_path, monkeypatch, role) as f:
        admitted, response = _admit(_original(f))
        with session_scope(f.case.factory) as session:
            row = session.get(Authority, (f.case.collection_id, role))
            if change == "source":
                row.source_json += " "
            elif change == "document":
                row.document_json += " "
            else:
                session.get(Volume, (f.case.collection_id, 1)).progress_receipt_json += " "
        with pytest.raises(Conflict):
            f.owner.report(admitted, response, apply_receipt=f.authority.apply_prepared_receipt)
        assert _count(f.case) == 1


@pytest.mark.parametrize("role", _ROLES)
@pytest.mark.parametrize("change", ("marker", "projection"))
def test_old_authority_progress_requires_its_exact_positive_proof(
    tmp_path, monkeypatch, role, change
):
    with _authority_source(tmp_path, monkeypatch, role) as f:
        _publish(f)
        assert _commit(f)
        with session_scope(f.case.factory) as session:
            if change == "marker":
                session.get(Authority, (f.case.collection_id, role)).progress_receipt_json += " "
            elif role == "terminal":
                session.get(Upload, f.case.collection_id).provenance_archive_ordered_sha256 = (
                    "f" * 64
                )
            else:
                session.get(Upload, f.case.collection_id).provenance_identity = "f" * 64
        with pytest.raises(Conflict):
            _commit(f)
        assert f.calls == _one_call(f) and _count(f.case) == 0


def test_root_effect_is_bound_to_the_actual_original_terminal_progress(tmp_path, monkeypatch):
    with _authority_source(tmp_path, monkeypatch, "root") as f:
        with session_scope(f.case.factory) as session:
            session.get(Authority, (f.case.collection_id, "terminal")).progress_receipt_json += " "
        with pytest.raises(Conflict, match="original construction changed"):
            _publish(f)
        assert f.calls == f.before_calls and _count(f.case) == 0


@pytest.mark.parametrize("role", _ROLES)
def test_authority_source_and_original_preparation_share_one_operator_grant(
    tmp_path, monkeypatch, role
):
    with _authority_source(tmp_path, monkeypatch, role) as f:
        small = replace(
            f.original_grants, metadata_preparation_bytes=f.authority.reserved_source_bytes
        )
        with pytest.raises(Conflict, match="source exceeds its preparation grant"):
            with session_scope(f.case.factory) as session:
                Source.stage_next(session, f.case.collection_id, grants=small)
        assert _stage(f) == f.authority and f.calls == f.before_calls
        _publish(f)
        with session_scope(f.case.factory) as session:
            prepared = session.scalar(
                select(Preparation).where(
                    Preparation.collection_id == f.case.collection_id,
                    Preparation.object_id == f.authority_actors[role]._prepared.object_id,
                )
            )
            retained = len(prepared.preparation_json.encode()) + len(
                prepared.identity_json.encode()
            )
            assert json.loads(prepared.preparation_json)["capacity_bytes"] == (
                f.original_grants.metadata_preparation_bytes - f.authority.reserved_source_bytes
            )
            assert (
                retained + f.authority.reserved_source_bytes
                <= f.original_grants.metadata_preparation_bytes
            )
        assert f.calls == _one_call(f) and _count(f.case) == 0


@pytest.mark.parametrize("role", _ROLES)
def test_new_authority_effect_refuses_an_unmarked_root_identity(tmp_path, monkeypatch, role):
    with _authority_source(tmp_path, monkeypatch, role) as f:
        with session_scope(f.case.factory) as session:
            session.get(Upload, f.case.collection_id).provenance_identity = "f" * 64
        with pytest.raises(Conflict, match="root progress is already committed"):
            _publish(f)
        with pytest.raises(Conflict, match="root has no original progress proof"):
            _stage(f)
        assert f.calls == f.before_calls and _count(f.case) == 0


@pytest.mark.parametrize("role", _ROLES)
def test_existing_compatible_authority_ciphertext_is_preserved_by_actual_receiving(
    tmp_path, monkeypatch, role
):
    with _authority_source(tmp_path, monkeypatch, role) as f:
        first = ArchiveProvenancePublisher(
            object_store=f.physical, passphrase="fixture", scrypt_log_n=1
        ).publish_object(f.authority.publication)
        receipt = _publish(f)
        assert receipt == first == _received(f)
        assert f.calls == {
            "observe": f.before_calls["observe"] + 1,
            "execute": f.before_calls["execute"],
        }
        assert _count(f.case) == 0 and _commit(f) and not _commit(f)


def test_cold_root_receiving_requires_the_exact_original_terminal_basis(tmp_path, monkeypatch):
    with _authority_source(tmp_path, monkeypatch, "root") as f:
        admitted, _ = _admit(_original(f))
        with session_scope(f.case.factory) as session:
            row = session.get(Authority, (f.case.collection_id, "root"))
            original = json.loads(row.source_json)
            changed = CheckpointSHA256()
            changed.update(b"changed retained root basis")
            original["hash_state"] = changed.export_state()
            row.source_json = json.dumps(original, sort_keys=True, separators=(",", ":"))
            row.source_sha256 = hashlib.sha256(row.source_json.encode()).hexdigest()
        with pytest.raises(Conflict, match="original terminal progress changed"):
            with session_scope(f.case.factory) as session:
                Source.from_prepared(session, admitted.prepared, role="root")


@pytest.mark.parametrize("role", _ROLES)
def test_original_authority_preparation_is_bound_to_the_exact_construction(
    tmp_path, monkeypatch, role
):
    with _authority_source(tmp_path, monkeypatch, role) as f:
        admitted, _ = _admit(_original(f))
        value = json.loads(admitted.prepared.preparation_json)
        value["construction_sha256"] = "f" * 64
        changed = replace(
            admitted.prepared,
            preparation_json=json.dumps(value, sort_keys=True, separators=(",", ":")),
        )
        with pytest.raises(Conflict, match="preparation belongs to another construction"):
            with session_scope(f.case.factory) as session:
                Source.from_prepared(session, changed, role=role)
