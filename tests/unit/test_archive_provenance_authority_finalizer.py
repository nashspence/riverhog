from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_core import archive_provenance
from riverhog_core.archive_provenance import ArchiveProvenancePublisher
from riverhog_core.archive_store_registry import ArchiveStoreRegistry
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.services import archive_provenance_authority_finalizer as finalizer_module
from riverhog_core.services.archive_provenance_authority_checkpoint import (
    ArchiveProvenanceAuthoritySource as Source,
)
from riverhog_core.services.archive_provenance_authority_finalizer import (
    ArchiveProvenanceAuthorityFinalizer,
)
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission
from riverhog_core.services.immutable_archive_preparation import ImmutableArchivePreparation
from riverhog_protocol.errors import Conflict, ServiceUnavailable

from tests.unit.finalization_claim_fixtures import _different_claim, _replace_claim
from tests.unit.test_archive_provenance_authority_checkpoint import (
    _authority_source,
    _commit,
    _orphan,
    _publish,
    _snapshot,
)
from tests.unit.test_archive_provenance_journal_writer import _lose_ack
from tests.unit.test_archive_volume_metadata_receiving import _count
from tests.unit.test_finalization_claim_readiness import _current_claim


def _owner(f, *, forward_guard=None):
    with session_scope(f.case.factory) as session:
        name = session.get(Upload, f.case.collection_id).archive_store
    binding = replace(
        f.case.service._archive_stores.require(name),
        immutable_objects=f.physical,
        immutable_writes=f.ports,
    )
    config = replace(f.case.service._config, archive_write=f.original_grants)
    registry = ArchiveStoreRegistry({name: binding})
    owner = ArchiveProvenanceAuthorityFinalizer(
        f.case.factory,
        collection_id=f.case.collection_id,
        config=config,
        archive_stores=registry,
        forward_guard=forward_guard,
    )
    return SimpleNamespace(
        owner=owner, config=config, binding=binding, registry=registry, name=name
    )


def _complete_owner(f):
    original = _owner(f)
    physical = 2 if f.authority.role == "terminal" else 1
    for _ in range(physical * 2):
        assert original.owner.advance() and not original.owner.has_pending
    assert not original.owner.advance()
    after = _snapshot(f)
    assert after["root"] is not None and after["identity"] is not None
    assert f.calls == {name: value + physical for name, value in f.before_calls.items()}
    assert _count(f.case) == 0
    with session_scope(f.case.factory) as session:
        assert not f.last_volume.commit_volume(session)


@pytest.mark.parametrize("role", ("terminal", "root"))
@pytest.mark.parametrize("mode", ("direct", "http"))
def test_actual_retained_authority_finalizer_finishes_physical_and_progress_steps(
    tmp_path, monkeypatch, role, mode
):
    with _authority_source(tmp_path, monkeypatch, role, mode) as f:
        _complete_owner(f)


@pytest.mark.parametrize("role", ("terminal", "root"))
@pytest.mark.parametrize("phase", ("report", "retirement"))
@pytest.mark.parametrize("committed", (False, True))
def test_original_authority_actor_precedes_new_config_target_key_and_source_after_reply_loss(
    tmp_path, monkeypatch, role, phase, committed
):
    with _authority_source(tmp_path, monkeypatch, role) as f:
        claim = _current_claim(f.case)
        original = _owner(f, forward_guard=claim.guard)
        before = _snapshot(f)
        with _lose_ack(monkeypatch, phase, committed):
            with pytest.raises(OSError, match="injected journal writer"):
                original.owner.advance()
        actor = original.owner._actor
        response = actor._response_json
        assert original.owner.has_pending and response is not None
        original.owner._config = original.owner._archive_stores = None
        _orphan(f)

        def forbidden(*args, **kwargs):
            raise AssertionError("retained authority receiving precedes fresh publisher inputs")

        with monkeypatch.context() as patch:
            patch.setattr(ArchiveProvenancePublisher, "publish_object", forbidden)
            assert original.owner.advance()
        assert actor._response_json == response and not original.owner.has_pending
        assert original.owner._source is original.owner._phase is original.owner._actor is None
        assert original.owner._passphrase is original.owner._scrypt_log_n is None
        assert _snapshot(f) == before and _count(f.case) == 0
        assert f.calls == {name: value + 1 for name, value in f.before_calls.items()}


@contextmanager
def _lose_progress_ack(monkeypatch, committed):
    transaction = finalizer_module.session_scope
    method = Source.commit_progress
    state = {"selected": False, "fired": False}

    def selected(*args, **kwargs):
        value = method(*args, **kwargs)
        state["selected"] = True
        return value

    @contextmanager
    def lost(factory):
        with transaction(factory) as session:
            yield session
            chosen = state["selected"] and not state["fired"]
            if chosen and not committed:
                state["fired"] = True
                raise OSError("injected authority progress rollback")
        if chosen and committed:
            state["fired"] = True
            raise OSError("injected authority progress commit reply loss")

    with monkeypatch.context() as patch:
        patch.setattr(Source, "commit_progress", selected)
        patch.setattr(finalizer_module, "session_scope", lost)
        yield
    assert state["fired"]


@pytest.mark.parametrize("role", ("terminal", "root"))
@pytest.mark.parametrize("committed", (False, True))
def test_retained_original_authority_progress_ack_precedes_fresh_configuration(
    tmp_path, monkeypatch, role, committed
):
    with _authority_source(tmp_path, monkeypatch, role) as f:
        claim = _current_claim(f.case)
        original = _owner(f, forward_guard=claim.guard)
        assert original.owner.advance()
        with _lose_progress_ack(monkeypatch, committed):
            with pytest.raises(OSError, match="authority progress"):
                original.owner.advance()
        assert original.owner.has_pending and original.owner._actor is None
        original.owner._config = original.owner._archive_stores = None
        before = _snapshot(f)
        _replace_claim(f.case, _different_claim(claim))
        if not committed:
            with pytest.raises(Conflict, match="original finalization claim"):
                original.owner.advance()
            assert original.owner.has_pending and _snapshot(f) == before
            _replace_claim(f.case, claim.claim_id)
        if committed:
            _orphan(f)
        calls = dict(f.calls)
        assert original.owner.advance() and not original.owner.has_pending
        assert f.calls == calls and _count(f.case) == 0
        assert not _commit(f)


@pytest.mark.parametrize("role", ("terminal", "root"))
def test_known_original_authority_data_commits_without_key_or_registry_lookup(
    tmp_path, monkeypatch, role
):
    with _authority_source(tmp_path, monkeypatch, role) as f:
        _publish(f)
        calls = dict(f.calls)
        original = _owner(f)
        original.owner._archive_stores = None
        original.owner._config = SimpleNamespace(archive_write=f.original_grants)
        assert original.owner.advance() and not original.owner.has_pending
        assert not _commit(f) and f.calls == calls


@pytest.mark.parametrize("role", ("terminal", "root"))
def test_unknown_authority_execution_retains_same_actor_and_refuses_fresh_finalizers(
    tmp_path, monkeypatch, role
):
    with _authority_source(tmp_path, monkeypatch, role) as f:
        original = _owner(f)
        execute = f.ports.execute_write

        def unknown(invocation, *, content):
            return execute(invocation, content=content).model_copy(update={"execution": "unknown"})

        monkeypatch.setattr(f.ports, "execute_write", unknown)
        with pytest.raises(Conflict, match="execution remains unknown"):
            original.owner.advance()
        actor = original.owner._actor
        assert original.owner.has_pending and _count(f.case) == 1
        original.owner._config = original.owner._archive_stores = None
        with pytest.raises(Conflict, match="execution remains unknown"):
            original.owner.advance()
        with pytest.raises(Conflict, match="unresolved provider writes"):
            _owner(f).owner.advance()
        assert original.owner._actor is actor
        assert f.calls == {name: value + 1 for name, value in f.before_calls.items()}


def _cold_and_live(f, monkeypatch):
    original = _owner(f)
    with _lose_ack(monkeypatch, "report", True):
        with pytest.raises(OSError, match="injected journal writer"):
            original.owner.advance()
    _orphan(f)
    cold = _owner(f).owner
    cold._config = cold._archive_stores = None
    assert cold.advance() and _count(f.case) == 0
    original.owner._config = original.owner._archive_stores = None
    assert original.owner.advance() and not original.owner.has_pending
    assert f.calls == {name: value + 1 for name, value in f.before_calls.items()}


@pytest.mark.parametrize("role", ("terminal", "root"))
def test_cold_and_live_authority_finalizers_converge_on_original_fact(tmp_path, monkeypatch, role):
    with _authority_source(tmp_path, monkeypatch, role) as f:
        _cold_and_live(f, monkeypatch)


@pytest.mark.parametrize("change", ("capability", "incarnation"))
def test_fresh_authority_effect_requires_exact_selected_explicit_ports(
    tmp_path, monkeypatch, change
):
    with _authority_source(tmp_path, monkeypatch) as f:
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
        assert not original.owner.has_pending and f.calls == f.before_calls and _count(f.case) == 0


def test_original_authority_preparation_retry_retains_selected_crypto_inputs(tmp_path, monkeypatch):
    with _authority_source(tmp_path, monkeypatch) as f:
        original = _owner(f)
        encrypt = archive_provenance.encrypt_age_scrypt
        inputs = []

        def interrupted(content, passphrase, *, log_n):
            inputs.append((passphrase, log_n))
            if len(inputs) == 1:
                raise OSError("injected authority encryption interruption")
            return encrypt(content, passphrase, log_n=log_n)

        with monkeypatch.context() as patch:
            patch.setattr(archive_provenance, "encrypt_age_scrypt", interrupted)
            with pytest.raises(OSError, match="encryption interruption"):
                original.owner.advance()
            assert original.owner.has_pending and not original.owner._actor.has_pending
            original.owner._config = original.owner._archive_stores = None
            assert original.owner.advance()
        assert len(inputs) == 2 and inputs[0] == inputs[1]
        assert f.calls == {name: value + 1 for name, value in f.before_calls.items()}


@pytest.mark.parametrize("role", ("terminal", "root"))
def test_provenance_authority_staging_requires_original_claim(tmp_path, monkeypatch, role):
    with _authority_source(tmp_path, monkeypatch, role) as f:
        claim = _current_claim(f.case)
        original = _owner(f, forward_guard=claim.guard)
        before = _snapshot(f)
        calls = dict(f.calls)
        _replace_claim(f.case, _different_claim(claim))
        with pytest.raises(Conflict, match="original finalization claim"):
            original.owner.advance()
        assert not original.owner.has_pending and _snapshot(f) == before
        assert f.calls == calls and _count(f.case) == 0


@pytest.mark.parametrize("role", ("terminal", "root"))
@pytest.mark.parametrize("boundary", ("preparation", "admission"))
def test_provenance_authority_rechecks_original_claim_after_source_selection(
    tmp_path, monkeypatch, role, boundary
):
    with _authority_source(tmp_path, monkeypatch, role) as f:
        claim = _current_claim(f.case)
        original = _owner(f, forward_guard=claim.guard)
        calls = dict(f.calls)
        owner_type, method = (
            (ArchiveWriteAdmission, "bind_epoch")
            if boundary == "preparation"
            else (ImmutableArchivePreparation, "prepare")
        )
        advance = getattr(owner_type, method)

        def changed(owner, *args, **kwargs):
            result = advance(owner, *args, **kwargs)
            _replace_claim(f.case, _different_claim(claim))
            return result

        with monkeypatch.context() as patch:
            patch.setattr(owner_type, method, changed)
            with pytest.raises(Conflict, match="original finalization claim"):
                original.owner.advance()
        actor = original.owner._actor
        assert actor is not None and original.owner.has_pending
        assert (actor._prepared is None) == (boundary == "preparation")
        assert f.calls == calls and _count(f.case) == 0
        _replace_claim(f.case, claim.claim_id)
        assert original.owner.advance() and not original.owner.has_pending
        assert f.calls == {name: value + 1 for name, value in calls.items()}
