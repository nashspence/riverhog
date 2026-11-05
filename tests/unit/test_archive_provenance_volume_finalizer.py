from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_core import archive_provenance
from riverhog_core.archive_provenance import ArchiveProvenancePublisher
from riverhog_core.archive_store_registry import ArchiveStoreRegistry
from riverhog_core.catalog_archive_write_models import ArchiveWriteControlRecord as Control
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.services import archive_provenance_volume_finalizer as finalizer_module
from riverhog_core.services.archive_provenance_volume_checkpoint import (
    ArchiveProvenanceVolumeSource as Source,
)
from riverhog_core.services.archive_provenance_volume_finalizer import (
    ArchiveProvenanceVolumeFinalizer,
)
from riverhog_core.services.immutable_archive_writes import ImmutableArchiveWrites
from riverhog_protocol.errors import Conflict, ServiceUnavailable

from tests.unit.test_archive_provenance_journal_checkpoint import _JOURNAL, _stage_journal
from tests.unit.test_archive_provenance_journal_writer import _lose_ack
from tests.unit.test_archive_provenance_volume_checkpoint import _volume_source
from tests.unit.test_archive_provenance_volume_progress import _snapshot
from tests.unit.test_archive_volume_metadata_receiving import _count


def _owner(f):
    with session_scope(f.case.factory) as session:
        name = session.get(Upload, f.case.collection_id).archive_store
    binding = replace(
        f.case.service._archive_stores.require(name),
        immutable_objects=f.physical,
        immutable_writes=f.ports,
    )
    registry = ArchiveStoreRegistry({name: binding})
    config = replace(f.case.service._config, archive_write=f.original_grants)
    owner = ArchiveProvenanceVolumeFinalizer(
        f.case.factory,
        collection_id=f.case.collection_id,
        config=config,
        archive_stores=registry,
    )
    return SimpleNamespace(
        owner=owner, config=config, registry=registry, name=name, binding=binding
    )


def _prior(owner, phase):
    if phase == "metadata":
        assert owner.advance()


def _orphan(f):
    with session_scope(f.case.factory) as session:
        upload = session.get(Upload, f.case.collection_id)
        upload.state = upload.archive_phase = "orphaned"
        return upload.lease_expires_at


@contextmanager
def _lose_data_ack(monkeypatch, phase, committed):
    transaction = finalizer_module.session_scope
    method = "commit_volume" if phase == "commit" else "receive_journal_payload"
    original = getattr(Source, method)
    state = {"selected": False, "fired": False}

    def selected(*args, **kwargs):
        result = original(*args, **kwargs)
        state["selected"] = True
        return result

    @contextmanager
    def lost(factory):
        with transaction(factory) as session:
            yield session
            chosen = state["selected"] and not state["fired"]
            if chosen and not committed:
                state["fired"] = True
                raise OSError("injected original corpus data rollback")
        if chosen and committed:
            state["fired"] = True
            raise OSError("injected original corpus data commit reply loss")

    with monkeypatch.context() as patch:
        patch.setattr(Source, method, selected)
        patch.setattr(finalizer_module, "session_scope", lost)
        yield
    assert state["fired"]


def _journal_receipt(f):
    with session_scope(f.case.factory) as session:
        source = f.metadata.stage_journal_payload(session, grants=f.original_grants)
    actor = ImmutableArchiveWrites(
        f.case.factory,
        source=source,
        epoch=f.epoch,
        grants=source.write_grants(f.metadata.write_grants(f.original_grants)),
        executor=f.ports,
        observer=f.ports,
    )
    return ArchiveProvenancePublisher(
        object_store=actor, passphrase="fixture", scrypt_log_n=1
    ).publish_object(source.publication)


@pytest.mark.parametrize("kind", ("bindings", "journal"))
@pytest.mark.parametrize("mode", ("direct", "http"))
def test_actual_retained_corpus_finalizer_completes_two_effects_and_one_progress_step(
    tmp_path, monkeypatch, kind, mode
):
    with _volume_source(tmp_path, monkeypatch, mode, kind) as f:
        original = _owner(f)
        assert original.owner.advance()
        assert f.calls == {"observe": 1, "execute": 1}
        assert _snapshot(f)[0] == 0 and not original.owner.has_pending
        assert original.owner.advance()
        assert f.calls == {"observe": 2, "execute": 2}
        assert _snapshot(f)[0] == 0 and not original.owner.has_pending
        assert original.owner.advance()
        after = _snapshot(f)
        assert after[0] == after[-1] == 1 and not original.owner.has_pending
        assert not original.owner.advance()
        assert _snapshot(f) == after and _count(f.case) == 0


@pytest.mark.parametrize("phase", ("bindings", "journal", "metadata"))
@pytest.mark.parametrize("ack", ("report", "retirement"))
@pytest.mark.parametrize("committed", (False, True))
def test_retained_corpus_actor_precedes_fresh_config_key_target_or_source_after_reply_loss(
    tmp_path, monkeypatch, phase, ack, committed
):
    with _volume_source(
        tmp_path, monkeypatch, kind="journal" if phase == "journal" else "bindings"
    ) as f:
        original = _owner(f)
        _prior(original.owner, phase)
        with _lose_ack(monkeypatch, ack, committed):
            with pytest.raises(OSError, match="injected journal writer"):
                original.owner.advance()
        actor = original.owner._actor
        assert original.owner.has_pending and actor._response_json is not None
        response = actor._response_json
        original.owner._config = original.owner._archive_stores = None
        lease = _orphan(f)
        calls = dict(f.calls)

        def forbidden(*args, **kwargs):
            raise AssertionError("original corpus receiving precedes fresh publisher inputs")

        with monkeypatch.context() as patch:
            patch.setattr(ArchiveProvenancePublisher, "publish_object", forbidden)
            assert original.owner.advance()
        assert actor._response_json == response and not original.owner.has_pending
        assert original.owner._actor is original.owner._volume is original.owner._source is None
        assert (
            original.owner._phase
            is original.owner._passphrase
            is original.owner._scrypt_log_n
            is None
        )
        assert f.calls == calls and _count(f.case) == 0
        with session_scope(f.case.factory) as session:
            upload = session.get(Upload, f.case.collection_id)
            assert upload.state == "orphaned" and upload.lease_expires_at == lease


@pytest.mark.parametrize("committed", (False, True))
@pytest.mark.parametrize("existing", (False, True))
def test_retained_journal_association_acknowledgement_outlives_physical_retirement(
    tmp_path, monkeypatch, committed, existing
):
    with _volume_source(tmp_path, monkeypatch, kind="journal") as f:
        prior = _journal_receipt(f) if existing else None
        original = _owner(f)
        with _lose_data_ack(monkeypatch, "journal", committed):
            with pytest.raises(OSError, match="original corpus data"):
                original.owner.advance()
        assert original.owner.has_pending
        assert (original.owner._actor is None) == existing
        if not existing:
            assert not original.owner._actor.has_pending
        original.owner._config = original.owner._archive_stores = None
        _orphan(f)
        calls = dict(f.calls)
        assert original.owner.advance() and not original.owner.has_pending
        with session_scope(f.case.factory) as session:
            receipt = f.metadata.received_payload(session)
            assert receipt is not None and (prior is None or receipt == prior)
        assert f.calls == calls == {"observe": 1, "execute": 1} and _count(f.case) == 0


@pytest.mark.parametrize("committed", (False, True))
def test_retained_original_progress_acknowledgement_precedes_new_selection(
    tmp_path, monkeypatch, committed
):
    with _volume_source(tmp_path, monkeypatch) as f:
        original = _owner(f)
        assert original.owner.advance() and original.owner.advance()
        with _lose_data_ack(monkeypatch, "commit", committed):
            with pytest.raises(OSError, match="original corpus data"):
                original.owner.advance()
        assert original.owner.has_pending and original.owner._actor is None
        original.owner._config = original.owner._archive_stores = None
        if committed:
            _orphan(f)
            with session_scope(f.case.factory) as session:
                session.get(Control, f.case.collection_id).gate = "closed"
        assert original.owner.advance() and not original.owner.has_pending
        assert _snapshot(f)[0] == _snapshot(f)[-1] == 1
        assert f.calls == {"observe": 2, "execute": 2} and _count(f.case) == 0


@pytest.mark.parametrize("phase", ("bindings", "journal", "metadata"))
def test_unknown_corpus_execution_keeps_original_actor_and_blocks_fresh_finalizers(
    tmp_path, monkeypatch, phase
):
    with _volume_source(
        tmp_path, monkeypatch, kind="journal" if phase == "journal" else "bindings"
    ) as f:
        original = _owner(f)
        _prior(original.owner, phase)
        execute = f.ports.execute_write

        def unknown(invocation, *, content):
            return execute(invocation, content=content).model_copy(update={"execution": "unknown"})

        monkeypatch.setattr(f.ports, "execute_write", unknown)
        with pytest.raises(Conflict, match="execution remains unknown"):
            original.owner.advance()
        assert original.owner.has_pending and _count(f.case) == 1
        calls = dict(f.calls)
        original.owner._config = original.owner._archive_stores = None
        with pytest.raises(Conflict, match="execution remains unknown"):
            original.owner.advance()
        with pytest.raises(Conflict, match="unresolved provider writes"):
            _owner(f).owner.advance()
        assert f.calls == calls and _count(f.case) == 1 and _snapshot(f)[0] == 0


def _cold_and_live(f, monkeypatch, phase):
    original = _owner(f)
    _prior(original.owner, phase)
    with _lose_ack(monkeypatch, "report", True):
        with pytest.raises(OSError, match="injected journal writer"):
            original.owner.advance()
    assert original.owner.has_pending
    calls = dict(f.calls)
    _orphan(f)
    cold = _owner(f).owner
    cold._config = cold._archive_stores = None
    assert cold.advance() and _count(f.case) == 0
    original.owner._config = original.owner._archive_stores = None
    assert original.owner.advance() and not original.owner.has_pending
    assert f.calls == calls
    if phase == "journal":
        with session_scope(f.case.factory) as session:
            assert f.metadata.received_payload(session) is not None


@pytest.mark.parametrize("phase", ("bindings", "journal", "metadata"))
def test_cold_and_live_corpus_finalizers_converge_on_original_closed_facts(
    tmp_path, monkeypatch, phase
):
    with _volume_source(
        tmp_path, monkeypatch, kind="journal" if phase == "journal" else "bindings"
    ) as f:
        _cold_and_live(f, monkeypatch, phase)


@pytest.mark.parametrize("change", ("capability", "incarnation"))
def test_new_corpus_effect_requires_exact_explicit_selected_ports(tmp_path, monkeypatch, change):
    with _volume_source(tmp_path, monkeypatch) as f:
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
            original.owner.advance()
        assert not original.owner.has_pending and _count(f.case) == 0
        assert f.calls == {"observe": 0, "execute": 0}


def test_original_preparation_retry_keeps_selected_crypto_inputs_before_any_effect(
    tmp_path, monkeypatch
):
    with _volume_source(tmp_path, monkeypatch) as f:
        original = _owner(f)
        encrypt = archive_provenance.encrypt_age_scrypt
        inputs = []

        def fail_once(content, passphrase, *, log_n):
            inputs.append((passphrase, log_n))
            if len(inputs) == 1:
                raise OSError("injected encryption interruption")
            return encrypt(content, passphrase, log_n=log_n)

        with monkeypatch.context() as patch:
            patch.setattr(archive_provenance, "encrypt_age_scrypt", fail_once)
            with pytest.raises(OSError, match="encryption interruption"):
                original.owner.advance()
            assert original.owner.has_pending and not original.owner._actor.has_pending
            original.owner._config = original.owner._archive_stores = None
            assert original.owner.advance()
        assert len(inputs) == 2 and inputs[0] == inputs[1]
        assert f.calls == {"observe": 1, "execute": 1} and _count(f.case) == 0


def _two_volumes(f):
    _stage_journal(f, _JOURNAL, b"original retained corpus journal")
    original = _owner(f)
    for _ in range(6):
        assert original.owner.advance()
        assert not original.owner.has_pending
    assert not original.owner.advance()
    assert _snapshot(f)[0] == _snapshot(f)[-1] == 2
    assert f.calls == {"observe": 4, "execute": 4} and _count(f.case) == 0
