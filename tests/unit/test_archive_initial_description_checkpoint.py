from __future__ import annotations

import json
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_core.archive_collection_metadata import ArchiveCollectionMetadataPublisher
from riverhog_core.catalog_archive_write_models import ArchiveWriteControlRecord as Control
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_initial_metadata_models import (
    CollectionUploadInitialDescriptionRecord as Description,
)
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.services.archive_initial_description_checkpoint import (
    ArchiveInitialDescriptionSource as Source,
)
from riverhog_core.services.collection_uploads import _initial_description_receipt
from riverhog_core.services.immutable_archive_admission import ImmutableArchiveAdmission
from riverhog_core.services.immutable_archive_receiving import drain_original_immutable_report
from riverhog_core.services.immutable_archive_writes import ImmutableArchiveWrites
from riverhog_protocol import CollectionDescriptionDocument, collection_description_identity
from riverhog_protocol.errors import Conflict

from tests.unit.test_archive_authority_checkpoint import _authority_source
from tests.unit.test_archive_authority_finalizer import _owner as _authority_owner
from tests.unit.test_archive_provenance_structure_checkpoint import _admit as _admit_source
from tests.unit.test_archive_volume_metadata_receiving import _count


@contextmanager
def _description_source(
    tmp_path, monkeypatch, description="initial description", mode="direct", *, database_url=None
):
    with _authority_source(tmp_path, monkeypatch, mode, database_url=database_url) as fixture:
        prior = _authority_owner(fixture)
        assert prior.owner.advance() and prior.owner.advance()
        with session_scope(fixture.case.factory) as session:
            session.get(Upload, fixture.case.collection_id).description = description
        with session_scope(fixture.case.factory) as session:
            source = Source.stage_next(session, fixture.case.collection_id, grants=fixture.grants)
        assert source is not None
        actor = (
            None
            if not source.has_publication
            else ImmutableArchiveWrites(
                fixture.case.factory,
                source=source,
                epoch=fixture.epoch,
                grants=source.write_grants(fixture.grants),
                executor=prior.ports,
                observer=prior.ports,
            )
        )
        yield SimpleNamespace(
            case=fixture.case,
            fixture=fixture,
            source=source,
            actor=actor,
            owner=ImmutableArchiveAdmission(fixture.case.factory),
            epoch=fixture.epoch,
            grants=fixture.grants,
            ports=prior.ports,
            calls=prior.calls,
            prior=prior,
            physical=fixture.physical,
            before_calls={k: len(v) for k, v in prior.calls.items()},
        )


def _admit(f):
    selected = SimpleNamespace(**vars(f))
    selected.grants = f.source.write_grants(f.grants)
    return _admit_source(selected)


def _snapshot(f):
    with session_scope(f.case.factory) as session:
        upload = session.get(Upload, f.case.collection_id)
        row = session.get(Description, f.case.collection_id)
        return (
            upload.description_revision,
            upload.description_identity,
            upload.description_publication_receipt_json,
            row.progress_receipt_json,
        )


def _received(f):
    with session_scope(f.case.factory) as session:
        return f.source.received(session)


def _publish(f):
    received = f.actor.resume_original()
    if received is not None:
        return f.source.publication.receipt(received)
    return ArchiveCollectionMetadataPublisher(
        object_store=f.actor,
        passphrase="fixture",
        scrypt_log_n=1,
    ).publish_object(f.source.publication)


def _commit(f):
    with session_scope(f.case.factory) as session:
        return f.source.commit_progress(session)


def _stage(f):
    with session_scope(f.case.factory) as session:
        return Source.stage_next(session, f.case.collection_id, grants=f.grants)


def _orphan(f):
    with session_scope(f.case.factory) as session:
        upload = session.get(Upload, f.case.collection_id)
        upload.state = upload.archive_phase = "orphaned"
        session.get(Control, f.case.collection_id).gate = "closed"


def _complete(f):
    before = _snapshot(f)
    if f.source.has_publication:
        receipt = _publish(f)
        assert receipt == _received(f) and _snapshot(f) == before
        document = CollectionDescriptionDocument.from_json_bytes(f.source.publication.plaintext)
        assert (
            document.revision == 1
            and document.description == json.loads(f.source.source_json)["description"]
        )
    else:
        with pytest.raises(Conflict, match="no provider write"):
            _ = f.source.publication
        assert _received(f) is None
    assert _commit(f)
    after = _snapshot(f)
    assert after != before and not _commit(f) and _snapshot(f) == after and _stage(f) is None
    with session_scope(f.case.factory) as session:
        f.source.verify_progress(session)
        upload = session.get(Upload, f.case.collection_id)
        revision = 1 if f.source.has_publication else 0
        assert upload.description_revision == revision
        catalog = _initial_description_receipt(upload)
        physical = f.source.received(session)
        if physical is None:
            assert catalog is None
        else:
            assert catalog is not None
            assert catalog["object_path"] == physical.object_path
            assert catalog["provider_revision"] == physical.revision
            assert catalog["stored_bytes"] == physical.stored_bytes
            assert catalog["stored_sha256"] == physical.stored_sha256
            assert catalog["published_at"] == physical.published_at
        original = json.loads(f.source.source_json)
        assert upload.description_identity == collection_description_identity(
            archive_root_sha256=original["archive_root_sha256"],
            revision=revision,
            description=original["description"],
        )
    difference = 1 if f.source.has_publication else 0
    assert {k: len(v) for k, v in f.calls.items()} == {
        k: v + difference for k, v in f.before_calls.items()
    }
    assert _count(f.case) == 0


@pytest.mark.parametrize("description", (None, "a", "first \u00e9 caption"))
@pytest.mark.parametrize("mode", ("direct", "http"))
def test_actual_description_physical_receiving_is_separate_from_once_only_progress(
    tmp_path, monkeypatch, description, mode
):
    with _description_source(tmp_path, monkeypatch, description, mode) as f:
        _complete(f)


def _cold_receive(f):
    admitted, response = _admit(f)
    f.owner.report(admitted, response, apply_receipt=lambda *args: None)
    assert _received(f) is None and _count(f.case) == 1
    _orphan(f)
    with session_scope(f.case.factory) as session:
        upload = session.get(Upload, f.case.collection_id)
        upload.description = "a later current selection"
        upload.final_authority_json = None
    before = _snapshot(f)
    assert drain_original_immutable_report(f.case.factory, f.case.collection_id)
    assert _received(f).stored_sha256 == response.result.stored_sha256
    assert _snapshot(f) == before and _count(f.case) == 0
    with session_scope(f.case.factory) as session:
        f.source.verify_prepared_receipt(session, admitted.prepared, response.result)


@pytest.mark.parametrize("mode", ("direct", "http"))
def test_actual_cold_description_receiving_ignores_current_forward_selection(
    tmp_path, monkeypatch, mode
):
    with _description_source(tmp_path, monkeypatch, mode=mode) as f:
        _cold_receive(f)


def test_positive_unknown_description_data_does_not_authorize_progress(tmp_path, monkeypatch):
    with _description_source(tmp_path, monkeypatch) as f:
        admitted, response = _admit(f)
        f.owner.report(
            admitted,
            response.model_copy(update={"execution": "unknown"}),
            apply_receipt=f.source.apply_prepared_receipt,
        )
        assert _received(f) is not None and _count(f.case) == 1
        before = _snapshot(f)
        for step in (_commit, _stage):
            with pytest.raises(Conflict, match="unresolved provider writes"):
                step(f)
        assert _snapshot(f) == before


@pytest.mark.parametrize("description", (None, "present"))
@pytest.mark.parametrize("committed", (False, True))
def test_description_progress_is_atomic_and_old_ack_precedes_new_authority(
    tmp_path, monkeypatch, description, committed
):
    with _description_source(tmp_path, monkeypatch, description) as f:
        if f.source.has_publication:
            _publish(f)
        before = _snapshot(f)
        with pytest.raises(OSError, match="description progress"):
            with session_scope(f.case.factory) as session:
                assert f.source.commit_progress(session)
                if not committed:
                    raise OSError("injected description progress rollback")
            raise OSError("injected description progress commit reply loss")
        if committed:
            after = _snapshot(f)
            _orphan(f)
            with session_scope(f.case.factory) as session:
                upload = session.get(Upload, f.case.collection_id)
                upload.description = "current selection changed"
                upload.final_authority_json = None
            assert not _commit(f) and _snapshot(f) == after
        else:
            assert _snapshot(f) == before and _commit(f)


@pytest.mark.parametrize("change", ("orphan", "gate", "description", "authority"))
@pytest.mark.parametrize("phase", ("publish", "progress"))
def test_fresh_description_steps_require_current_claim_gate_and_original_inputs(
    tmp_path, monkeypatch, change, phase
):
    with _description_source(tmp_path, monkeypatch) as f:
        if phase == "progress":
            _publish(f)
        before = _snapshot(f)
        with session_scope(f.case.factory) as session:
            upload = session.get(Upload, f.case.collection_id)
            if change == "orphan":
                upload.state = upload.archive_phase = "orphaned"
            elif change == "gate":
                session.get(Control, f.case.collection_id).gate = "closed"
            elif change == "description":
                upload.description = "new current text"
            else:
                upload.final_authority_json = None
        with pytest.raises(Conflict):
            (_publish if phase == "publish" else _commit)(f)
        assert _snapshot(f) == before


@pytest.mark.parametrize("change", ("source", "document", "root"))
def test_original_description_receiving_refuses_changed_factual_source(
    tmp_path, monkeypatch, change
):
    with _description_source(tmp_path, monkeypatch) as f:
        admitted, response = _admit(f)
        with session_scope(f.case.factory) as session:
            row = session.get(Description, f.case.collection_id)
            if change == "source":
                row.source_json += " "
            elif change == "document":
                row.document_json += " "
            else:
                upload = session.get(Upload, f.case.collection_id)
                value = json.loads(upload.archive_root_receipt_json)
                value["revision"] = "changed-original-root"
                upload.archive_root_receipt_json = json.dumps(
                    value, sort_keys=True, separators=(",", ":")
                )
        with pytest.raises(Conflict):
            f.owner.report(admitted, response, apply_receipt=f.source.apply_prepared_receipt)
        assert _count(f.case) == 1


@pytest.mark.parametrize(
    "field",
    ("description_identity", "description_revision", "description_publication_receipt_json"),
)
def test_description_progress_proof_refuses_changed_positive_projection(
    tmp_path, monkeypatch, field
):
    with _description_source(tmp_path, monkeypatch) as f:
        _publish(f)
        assert _commit(f)
        with session_scope(f.case.factory) as session:
            upload = session.get(Upload, f.case.collection_id)
            setattr(
                upload,
                field,
                2
                if field == "description_revision"
                else ("d" * 64 if field == "description_identity" else "{}"),
            )
        with pytest.raises(Conflict, match="progress proof"):
            _commit(f)


def test_description_source_and_preparation_share_one_byte_grant(tmp_path, monkeypatch):
    with _description_source(tmp_path, monkeypatch) as f:
        assert f.source.reserved_source_bytes > len(f.source.publication.plaintext)
        smaller = replace(f.grants, metadata_preparation_bytes=f.source.reserved_source_bytes)
        with pytest.raises(Conflict, match="preparation grant"):
            f.source.write_grants(smaller)
        assert f.source.write_grants(f.grants).metadata_preparation_bytes == (
            f.grants.metadata_preparation_bytes - f.source.reserved_source_bytes
        )


def test_initial_description_rejects_unmarked_progress_and_changed_prepared_construction(
    tmp_path, monkeypatch
):
    with _description_source(tmp_path, monkeypatch) as f:
        admitted, response = _admit(f)
        value = admitted.prepared._value()
        value["construction_sha256"] = "f" * 64
        changed = replace(
            admitted.prepared,
            preparation_json=json.dumps(value, sort_keys=True, separators=(",", ":")),
        )
        with session_scope(f.case.factory) as session:
            with pytest.raises(Conflict, match="another source"):
                f.source.apply_prepared_receipt(session, changed, response.result)
        f.owner.report(admitted, response, apply_receipt=f.source.apply_prepared_receipt)
        f.owner.acknowledge_report(admitted, verify_receipt=f.source.verify_prepared_receipt)
        with session_scope(f.case.factory) as session:
            session.get(Upload, f.case.collection_id).description_identity = "d" * 64
        with pytest.raises(Conflict, match="progress proof"):
            _stage(f)
