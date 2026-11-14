from __future__ import annotations

import json
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_archive_contracts import PROVENANCE_JOURNAL_SEGMENT_BYTES_MAX
from riverhog_core.archive_provenance import ArchiveProvenancePublisher
from riverhog_core.archive_store_registry import ArchiveStoreRegistry
from riverhog_core.catalog_archive_write_models import ArchiveWritePreparationRecord
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import (
    CollectionUploadProvenanceCustodyObjectRecord as Custody,
)
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.services import archive_provenance_journal_writer as writer_module
from riverhog_core.services import immutable_archive_admission as admission_module
from riverhog_core.services.archive_provenance_journal_checkpoint import (
    ArchiveProvenanceJournalSource as Source,
)
from riverhog_core.services.archive_provenance_journal_checkpoint import ProvenanceJournalRange
from riverhog_core.services.archive_provenance_journal_writer import ArchiveProvenanceJournalWriter
from riverhog_core.services.immutable_archive_admission import ImmutableArchiveAdmission
from riverhog_protocol.errors import Conflict, NotFound, ServiceUnavailable
from sqlalchemy import delete, select

from tests.unit.test_archive_provenance_journal_checkpoint import (
    _JOURNAL,
    _OTHER,
    _journal_source,
    _publish,
    _stage_journal,
)
from tests.unit.test_archive_volume_metadata_receiving import _count


def _owner(f):
    with session_scope(f.case.factory) as session:
        name = session.get(Upload, f.case.collection_id).archive_store
    binding = replace(f.case.service._archive_stores.require(name), immutable_writes=f.ports)
    registry = ArchiveStoreRegistry({name: binding})
    config = replace(f.case.service._config, archive_write=f.original_grants)
    owner = ArchiveProvenanceJournalWriter(
        f.case.factory,
        collection_id=f.case.collection_id,
        config=config,
        archive_stores=registry,
        forward_guard=f.forward_guard,
    )
    return SimpleNamespace(
        owner=owner, config=config, registry=registry, name=name, binding=binding
    )


def _advance(owner, *, journal_id=_JOURNAL, offset=0, size=8):
    return owner.advance(journal_id=journal_id, offset=offset, size=size)


def _association(f, *, journal_id=_JOURNAL, offset=0, size=8):
    with session_scope(f.case.factory) as session:
        selected, _ = ProvenanceJournalRange.capture(
            session, f.case.collection_id, journal_id, offset=offset, size=size
        )
        return f.source.received_association(session, selected)


@contextmanager
def _lose_ack(monkeypatch, phase, committed):
    module = writer_module if phase == "association" else admission_module
    transaction = module.session_scope
    state = {"active": False, "fired": False, "association": False}
    owner = Source if phase == "association" else ImmutableArchiveAdmission
    name = {
        "association": "receive_association",
        "report": "report",
        "retirement": "acknowledge_report",
    }[phase]
    original = getattr(owner, name)

    def selected(*args, **kwargs):
        state["active"] = True
        try:
            result = original(*args, **kwargs)
            state["association"] = phase == "association"
            return result
        finally:
            state["active"] = False

    @contextmanager
    def lost(factory):
        active = state["active"]
        with transaction(factory) as session:
            yield session
            chosen = not state["fired"] and (active or state["association"])
            if chosen and not committed:
                state["fired"] = True
                raise OSError("injected journal writer rollback")
        if chosen and committed:
            state["fired"] = True
            raise OSError("injected journal writer commit reply loss")

    with monkeypatch.context() as patch:
        patch.setattr(owner, name, selected)
        patch.setattr(module, "session_scope", lost)
        yield state
    assert state["fired"]


@pytest.mark.parametrize("mode", ("direct", "http"))
def test_actual_journal_writer_shares_one_physical_call_across_selected_positions(
    tmp_path, monkeypatch, mode
):
    with _journal_source(tmp_path, monkeypatch, mode) as f:
        original = _owner(f)
        assert _advance(original.owner)
        first = _association(f)
        _stage_journal(f, _OTHER, b"abcdefghabcdefgh")
        for journal, offset in [(_JOURNAL, 8), (_OTHER, 0), (_OTHER, 8)]:
            assert _advance(original.owner, journal_id=journal, offset=offset)
            assert _association(f, journal_id=journal, offset=offset) == first
            assert not _advance(original.owner, journal_id=journal, offset=offset)
            assert not original.owner.has_pending
        assert f.calls == {"observe": 1, "execute": 1} and _count(f.case) == 0


@pytest.mark.parametrize("phase", ("report", "retirement", "association"))
@pytest.mark.parametrize("committed", (False, True))
def test_original_journal_writer_precedes_fresh_inputs_after_each_acknowledgement_loss(
    tmp_path, monkeypatch, phase, committed
):
    with _journal_source(tmp_path, monkeypatch) as f:
        original = _owner(f)
        with _lose_ack(monkeypatch, phase, committed):
            with pytest.raises(OSError, match="injected journal writer"):
                _advance(original.owner, offset=8)
        actor = original.owner._actor
        assert original.owner.has_pending and actor is not None
        response = actor._response_json
        assert response is not None
        original.owner._config = None
        original.owner._archive_stores = None

        def forbidden(*args, **kwargs):
            raise AssertionError("original receiving cannot consult fresh forward inputs")

        original.owner._forward_guard = forbidden
        with session_scope(f.case.factory) as session:
            upload = session.get(Upload, f.case.collection_id)
            upload.state = upload.archive_phase = "orphaned"
            lease = upload.lease_expires_at
        # New arguments cannot replace the retained original association.
        assert _advance(original.owner, journal_id="unavailable", offset=-1, size=0)
        assert _association(f, offset=8) == _association(f)
        assert not original.owner.has_pending and actor._response_json == response
        assert original.owner._actor is original.owner._selected is None
        assert original.owner._source is original.owner._passphrase is None
        assert original.owner._scrypt_log_n is None
        assert f.calls == {"observe": 1, "execute": 1} and _count(f.case) == 0
        with session_scope(f.case.factory) as session:
            assert session.get(Upload, f.case.collection_id).lease_expires_at == lease


@pytest.mark.parametrize("committed", (False, True))
def test_shared_receipt_only_writer_retains_association_after_reply_loss(
    tmp_path, monkeypatch, committed
):
    with _journal_source(tmp_path, monkeypatch) as f:
        first = _publish(f)
        original = _owner(f)
        with _lose_ack(monkeypatch, "association", committed):
            with pytest.raises(OSError, match="injected journal writer"):
                _advance(original.owner, offset=8)
        assert original.owner.has_pending and original.owner._actor is None
        original.owner._config = original.owner._archive_stores = None
        with session_scope(f.case.factory) as session:
            upload = session.get(Upload, f.case.collection_id)
            upload.state = upload.archive_phase = "orphaned"
        assert _advance(original.owner, journal_id="unavailable")
        assert _association(f, offset=8) == first and not original.owner.has_pending
        assert f.calls == {"observe": 1, "execute": 1} and _count(f.case) == 0


@pytest.mark.parametrize("mode", ("direct", "http"))
def test_positive_unknown_journal_writer_data_stays_owned_and_blocks_new_selection(
    tmp_path, monkeypatch, mode
):
    with _journal_source(tmp_path, monkeypatch, mode) as f:
        original = _owner(f)
        execute = f.ports.execute_write

        def unknown(invocation, *, content):
            return execute(invocation, content=content).model_copy(update={"execution": "unknown"})

        monkeypatch.setattr(f.ports, "execute_write", unknown)
        with pytest.raises(Conflict, match="execution remains unknown"):
            _advance(original.owner)
        assert _association(f) is not None and _count(f.case) == 1
        original.owner._config = original.owner._archive_stores = None
        with pytest.raises(Conflict, match="execution remains unknown"):
            _advance(original.owner, offset=8)
        with pytest.raises(Conflict, match="unresolved provider writes"):
            _advance(_owner(f).owner, offset=8)
        assert original.owner.has_pending and _association(f, offset=8) is None
        assert f.calls == {"observe": 1, "execute": 1} and _count(f.case) == 1


@pytest.mark.parametrize("mode", ("direct", "http"))
def test_existing_compatible_ciphertext_is_received_by_actual_journal_writer(
    tmp_path, monkeypatch, mode
):
    with _journal_source(tmp_path, monkeypatch, mode) as f:
        prior = ArchiveProvenancePublisher(
            object_store=f.physical, passphrase="fixture", scrypt_log_n=1
        ).publish_object(f.source.publication)
        original = _owner(f)
        assert _advance(original.owner, offset=8)
        assert _association(f) == _association(f, offset=8) == prior
        assert f.calls == {"observe": 1, "execute": 0} and _count(f.case) == 0
        assert not original.owner.has_pending


def _cold_and_live(f, monkeypatch):
    original = _owner(f)
    with _lose_ack(monkeypatch, "report", True):
        with pytest.raises(OSError, match="injected journal writer"):
            _advance(original.owner, offset=8)
    actor = original.owner._actor
    cold = _owner(f).owner
    with session_scope(f.case.factory) as session:
        upload = session.get(Upload, f.case.collection_id)
        upload.state = upload.archive_phase = "orphaned"
        lease = upload.lease_expires_at
    cold._config = cold._archive_stores = None
    assert _advance(cold, journal_id="unavailable")
    assert _count(f.case) == 0 and _association(f, offset=8) is None
    original.owner._config = original.owner._archive_stores = None
    assert _advance(original.owner, journal_id="unavailable")
    assert actor._response_json is not None and not original.owner.has_pending
    assert _association(f, offset=8) == _association(f)
    assert f.calls == {"observe": 1, "execute": 1} and _count(f.case) == 0
    with session_scope(f.case.factory) as session:
        assert session.get(Upload, f.case.collection_id).lease_expires_at == lease


def test_live_journal_writer_and_cold_receiving_converge_after_forward_loss(tmp_path, monkeypatch):
    with _journal_source(tmp_path, monkeypatch) as f:
        _cold_and_live(f, monkeypatch)


@pytest.mark.parametrize("change", ("capability", "incarnation"))
def test_fresh_journal_writer_requires_exact_selected_immutable_ports(
    tmp_path, monkeypatch, change
):
    with _journal_source(tmp_path, monkeypatch) as f:
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
            _advance(original.owner)
        assert not original.owner.has_pending and _count(f.case) == 0
        assert f.calls == {"observe": 0, "execute": 0}


def test_fresh_journal_writer_preserves_caller_owned_forward_authority(tmp_path, monkeypatch):
    with _journal_source(tmp_path, monkeypatch) as f:
        original = _owner(f)

        def refused(session):
            raise Conflict("current independent custody authority unavailable")

        original.owner._forward_guard = refused
        with pytest.raises(Conflict, match="current independent custody authority"):
            _advance(original.owner)
        assert not original.owner.has_pending and _count(f.case) == 0
        assert f.calls == {"observe": 0, "execute": 0}


def test_missing_parent_cannot_acknowledge_original_journal_writer(tmp_path, monkeypatch):
    with _journal_source(tmp_path, monkeypatch) as f:
        original = _owner(f)
        with _lose_ack(monkeypatch, "report", True):
            with pytest.raises(OSError, match="injected journal writer"):
                _advance(original.owner)
        actor = original.owner._actor
        with session_scope(f.case.factory) as session:
            session.execute(delete(Upload).where(Upload.collection_id == f.case.collection_id))
        with pytest.raises(NotFound):
            _advance(original.owner)
        assert original.owner.has_pending and original.owner._actor is actor
        assert actor._response_json is not None and f.calls == {"observe": 1, "execute": 1}


def test_actual_full_journal_writer_charges_source_and_preparation_to_one_grant(
    tmp_path, monkeypatch
):
    content = b"x" * PROVENANCE_JOURNAL_SEGMENT_BYTES_MAX
    with _journal_source(tmp_path, monkeypatch, content=content * 2) as f:
        original = _owner(f)
        assert _advance(original.owner, size=len(content))
        receipt = _association(f, size=len(content))
        with session_scope(f.case.factory) as session:
            prepared = session.scalar(
                select(ArchiveWritePreparationRecord).where(
                    ArchiveWritePreparationRecord.collection_id == f.case.collection_id
                )
            )
            retained = len(prepared.preparation_json.encode()) + len(
                prepared.identity_json.encode()
            )
            assert json.loads(prepared.preparation_json)["capacity_bytes"] == (
                f.original_grants.metadata_preparation_bytes - f.source.reserved_source_bytes
            )
        assert (
            retained + f.source.reserved_source_bytes
            <= f.original_grants.metadata_preparation_bytes
        )
        assert receipt.plaintext_bytes == len(content) and receipt.stored_bytes > len(content)
        assert f.calls == {"observe": 1, "execute": 1} and not original.owner.has_pending


@pytest.mark.parametrize("change", ("receipt", "size"))
def test_existing_selected_journal_association_requires_actual_original_proof(
    tmp_path, monkeypatch, change
):
    with _journal_source(tmp_path, monkeypatch) as f:
        original = _owner(f)
        assert _advance(original.owner) and _advance(original.owner, offset=8)
        with session_scope(f.case.factory) as session:
            row = session.get(
                Custody,
                (f.case.collection_id, _JOURNAL, 8, f.source.publication.plaintext_sha256),
            )
            if change == "receipt":
                row.receipt_json = "{}"
            else:
                row.plaintext_bytes += 1
        with pytest.raises(Conflict, match="association proof changed"):
            _advance(_owner(f).owner, offset=8)
        assert f.calls == {"observe": 1, "execute": 1} and _count(f.case) == 0


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("journal_id", _OTHER),
        ("journal_bytes", 99),
        ("journal_sha256", "f" * 64),
        ("offset", 1),
        ("size", 7),
        ("plaintext_sha256", "f" * 64),
    ),
)
def test_verified_journal_range_is_checked_before_staging_or_provider_execution(
    tmp_path, monkeypatch, field, value
):
    with _journal_source(tmp_path, monkeypatch) as f:
        original = _owner(f)
        selected = f.source.journal_range
        changed = replace(selected, **{field: value})
        with pytest.raises(Conflict, match="verified artifact custody journal selection changed"):
            original.owner.advance(
                journal_id=selected.journal_id,
                offset=selected.offset,
                size=selected.size,
                expected_range=changed,
            )
        assert f.calls == {"observe": 0, "execute": 0}
        assert not original.owner.has_pending and _association(f) is None
        assert original.owner.advance(
            journal_id=selected.journal_id,
            offset=selected.offset,
            size=selected.size,
            expected_range=selected,
        )
        assert _association(f) is not None and f.calls == {"observe": 1, "execute": 1}
