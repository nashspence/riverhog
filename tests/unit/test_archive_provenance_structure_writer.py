from __future__ import annotations

import json
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_archive_contracts import provenance_structure_identity
from riverhog_core.archive_provenance import ArchiveProvenancePublisher
from riverhog_core.archive_store_registry import ArchiveStoreRegistry
from riverhog_core.catalog_archive_write_models import ArchiveWritePreparationRecord
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadProvenanceStructureRecord as Structure
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.services.archive_provenance_structure_checkpoint import (
    ArchiveProvenanceStructureSource as Source,
)
from riverhog_core.services.archive_provenance_structure_writer import (
    ArchiveProvenanceStructureWriter,
)
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission
from riverhog_protocol.errors import Conflict, ServiceUnavailable
from sqlalchemy import select, text

from tests.unit.test_archive_provenance_journal_writer import _lose_ack
from tests.unit.test_archive_provenance_publication import _case as _format_case
from tests.unit.test_archive_provenance_structure_checkpoint import _received, _structure_source
from tests.unit.test_archive_volume_metadata_receiving import _count

_KINDS = ("history", "record-page", "source-proof")


def _owner(f):
    with session_scope(f.case.factory) as session:
        name = session.get(Upload, f.case.collection_id).archive_store
    binding = replace(
        f.case.service._archive_stores.require(name),
        immutable_objects=f.physical,
        immutable_writes=f.ports,
    )
    config = replace(f.case.service._config, archive_write=f.grants)
    registry = ArchiveStoreRegistry({name: binding})
    owner = ArchiveProvenanceStructureWriter(
        f.case.factory,
        collection_id=f.case.collection_id,
        config=config,
        archive_stores=registry,
        forward_guard=f.forward_guard,
    )
    return SimpleNamespace(
        owner=owner, config=config, registry=registry, name=name, binding=binding
    )


def _closure(f, value=True):
    with session_scope(f.case.factory) as session:
        session.get(Upload, f.case.collection_id).provenance_closure_validated = value


def _orphan(f):
    with session_scope(f.case.factory) as session:
        upload = session.get(Upload, f.case.collection_id)
        upload.state = upload.archive_phase = "orphaned"
        return upload.lease_expires_at


def _advance(owner, f):
    return owner.advance(selected_object_id=f.source.object_id)


@pytest.mark.parametrize("kind", _KINDS)
@pytest.mark.parametrize("mode", ("direct", "http"))
def test_actual_retained_structure_writer_preserves_exact_original_receipt(
    tmp_path, monkeypatch, kind, mode
):
    with _structure_source(tmp_path, monkeypatch, kind, mode) as f:
        original = _owner(f)
        assert _advance(original.owner, f)
        assert _received(f) is not None and not original.owner.has_pending
        assert not _advance(original.owner, f)
        assert f.calls == {"observe": 1, "execute": 1} and _count(f.case) == 0


@pytest.mark.parametrize("kind", _KINDS)
@pytest.mark.parametrize("phase", ("report", "retirement"))
@pytest.mark.parametrize("committed", (False, True))
def test_original_structure_actor_precedes_new_key_config_target_and_selection_after_reply_loss(
    tmp_path, monkeypatch, kind, phase, committed
):
    with _structure_source(tmp_path, monkeypatch, kind) as f:
        original = _owner(f)
        with _lose_ack(monkeypatch, phase, committed):
            with pytest.raises(OSError, match="injected journal writer"):
                _advance(original.owner, f)
        actor = original.owner._actor
        response = actor._response_json
        assert original.owner.has_pending and response is not None
        original.owner._config = original.owner._archive_stores = None
        lease = _orphan(f)

        def forbidden(*args, **kwargs):
            raise AssertionError("original structural receiving precedes fresh publisher authority")

        original.owner._forward_guard = forbidden
        with monkeypatch.context() as patch:
            patch.setattr(ArchiveProvenancePublisher, "publish_object", forbidden)
            assert original.owner.advance(selected_object_id="another-source")
        assert not original.owner.has_pending and actor._response_json == response
        assert original.owner._source is original.owner._actor is None
        assert original.owner._passphrase is original.owner._scrypt_log_n is None
        assert _received(f) is not None and _count(f.case) == 0
        assert f.calls == {"observe": 1, "execute": 1}
        with session_scope(f.case.factory) as session:
            upload = session.get(Upload, f.case.collection_id)
            assert upload.state == "orphaned" and upload.lease_expires_at == lease


@pytest.mark.parametrize("kind", _KINDS)
def test_unknown_structural_execution_stays_with_original_actor_and_blocks_fresh_selection(
    tmp_path, monkeypatch, kind
):
    with _structure_source(tmp_path, monkeypatch, kind) as f:
        original = _owner(f)
        execute = f.ports.execute_write

        def unknown(invocation, *, content):
            return execute(invocation, content=content).model_copy(update={"execution": "unknown"})

        monkeypatch.setattr(f.ports, "execute_write", unknown)
        with pytest.raises(Conflict, match="execution remains unknown"):
            _advance(original.owner, f)
        actor = original.owner._actor
        assert original.owner.has_pending and _count(f.case) == 1
        with pytest.raises(Conflict, match="execution remains unknown"):
            _advance(original.owner, f)
        with pytest.raises(Conflict, match="unresolved provider writes"):
            _advance(_owner(f).owner, f)
        assert original.owner._actor is actor and f.calls == {"observe": 1, "execute": 1}


def _cold_and_live(f, monkeypatch):
    original = _owner(f)
    with _lose_ack(monkeypatch, "report", True):
        with pytest.raises(OSError, match="injected journal writer"):
            _advance(original.owner, f)
    _orphan(f)
    cold = _owner(f).owner
    cold._config = cold._archive_stores = None
    assert cold.advance() and _count(f.case) == 0
    original.owner._config = original.owner._archive_stores = None
    assert original.owner.advance() and not original.owner.has_pending
    assert _received(f) is not None and f.calls == {"observe": 1, "execute": 1}


@pytest.mark.parametrize("kind", _KINDS)
def test_cold_and_live_structure_writers_converge_on_original_closed_fact(
    tmp_path, monkeypatch, kind
):
    with _structure_source(tmp_path, monkeypatch, kind) as f:
        _cold_and_live(f, monkeypatch)


@pytest.mark.parametrize("kind", _KINDS)
def test_default_structural_selection_waits_for_closure_but_exact_early_source_keeps_caller_guard(
    tmp_path, monkeypatch, kind
):
    with _structure_source(tmp_path, monkeypatch, kind) as f:
        original = _owner(f)
        assert not original.owner.advance()
        assert f.calls == {"observe": 0, "execute": 0}
        assert _advance(original.owner, f)
        assert not original.owner.advance() and _received(f) is not None


def _complete_all(f):
    with session_scope(f.case.factory) as session:
        session.get(Upload, f.case.collection_id).provenance_closure_validated = True
        for kind in ("record-page", "source-proof"):
            publication, *_ = _format_case(kind)
            identity = provenance_structure_identity(publication.plaintext)
            session.add(
                Structure(
                    collection_id=f.case.collection_id,
                    object_id=identity.object_id,
                    kind=identity.kind,
                    relative_path=identity.relative_path,
                    content=publication.plaintext,
                )
            )
    original = _owner(f)
    seen = []
    for _ in range(3):
        assert original.owner.advance() and not original.owner.has_pending
        with session_scope(f.case.factory) as session:
            rows = list(
                session.scalars(
                    select(Structure)
                    .where(
                        Structure.collection_id == f.case.collection_id,
                        Structure.receipt_json.is_not(None),
                    )
                    .order_by(Structure.object_id)
                )
            )
            assert len(rows) == len(seen) + 1
            current = [r.object_id for r in rows]
            assert current[:-1] == seen
            seen = current
    assert not original.owner.advance()
    assert _count(f.case) == 0 and f.calls == {"observe": 3, "execute": 3}


@pytest.mark.parametrize("mode", ("direct", "http"))
def test_pending_structure_selection_visits_each_staged_kind_once(tmp_path, monkeypatch, mode):
    with _structure_source(tmp_path, monkeypatch, mode=mode) as f:
        _complete_all(f)


def test_actual_pending_selection_uses_its_partial_index_after_published_prefix(
    tmp_path, monkeypatch
):
    with _structure_source(tmp_path, monkeypatch) as f:
        _closure(f)
        original = _owner(f)
        assert original.owner.advance()
        with session_scope(f.case.factory) as session:
            plan = list(
                session.execute(
                    text(
                        "EXPLAIN QUERY PLAN SELECT object_id "
                        "FROM collection_upload_provenance_structure "
                        "WHERE collection_id = :cid AND receipt_json IS NULL "
                        "ORDER BY object_id LIMIT 1"
                    ),
                    {"cid": f.case.collection_id},
                )
            )
        assert any("ix_upload_structure_publication" in row[-1] for row in plan)
        assert not original.owner.advance() and f.calls == {"observe": 1, "execute": 1}


@pytest.mark.parametrize("change", ("capability", "incarnation"))
def test_new_structural_effect_requires_exact_selected_explicit_ports(
    tmp_path, monkeypatch, change
):
    with _structure_source(tmp_path, monkeypatch) as f:
        original = _owner(f)
        binding = replace(
            original.binding,
            **(
                {"immutable_writes": None}
                if change == "capability"
                else {"incarnation_id": "f" * 64}
            ),
        )
        original.owner._archive_stores = ArchiveStoreRegistry({original.name: binding})
        with pytest.raises(ServiceUnavailable):
            _advance(original.owner, f)
        assert not original.owner.has_pending and _count(f.case) == 0
        assert f.calls == {"observe": 0, "execute": 0}


@pytest.mark.parametrize("kind", _KINDS)
def test_structure_source_and_actual_preparation_share_one_operator_grant(
    tmp_path, monkeypatch, kind
):
    with _structure_source(tmp_path, monkeypatch, kind) as f:
        original = _owner(f)
        small = replace(f.grants, metadata_preparation_bytes=f.source.reserved_source_bytes)
        original.owner._config = replace(original.config, archive_write=small)
        with pytest.raises(Conflict, match="source exceeds its preparation grant"):
            _advance(original.owner, f)
        assert not original.owner.has_pending and f.calls == {"observe": 0, "execute": 0}
        original.owner._config = original.config
        assert _advance(original.owner, f)
        with session_scope(f.case.factory) as session:
            source = Source.load(session, f.case.collection_id, f.source.object_id)
            assert source.publication == f.source.publication
            prepared = session.scalar(
                select(ArchiveWritePreparationRecord).where(
                    ArchiveWritePreparationRecord.collection_id == f.case.collection_id
                )
            )
            retained = len(prepared.preparation_json.encode()) + len(
                prepared.identity_json.encode()
            )
            assert json.loads(prepared.preparation_json)["capacity_bytes"] == (
                f.grants.metadata_preparation_bytes - source.reserved_source_bytes
            )
            assert retained + source.reserved_source_bytes <= f.grants.metadata_preparation_bytes
        assert f.calls == {"observe": 1, "execute": 1} and _count(f.case) == 0


@pytest.mark.parametrize("change", ("caller", "closure"))
def test_structural_current_authority_is_rechecked_before_new_original_actor(
    tmp_path, monkeypatch, change
):
    with _structure_source(tmp_path, monkeypatch) as f:
        original = _owner(f)
        _closure(f)
        if change == "caller":

            def refuse(session):
                raise Conflict("injected current structural authority refusal")

            original.owner._forward_guard = refuse
        else:
            bind = ArchiveWriteAdmission.bind_epoch

            def revoke(self, collection_id, *, guard):
                _closure(f, False)
                return bind(self, collection_id, guard=guard)

            monkeypatch.setattr(ArchiveWriteAdmission, "bind_epoch", revoke)
        with pytest.raises(Conflict, match="authority refusal|closure is no longer validated"):
            original.owner.advance()
        assert not original.owner.has_pending and f.calls == {"observe": 0, "execute": 0}
