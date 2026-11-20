from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_core import archive_collection_metadata as metadata_module
from riverhog_core.archive_collection_metadata import ArchiveCollectionMetadataPublisher
from riverhog_core.archive_store_registry import ArchiveStoreRegistry
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.services import archive_initial_tag_finalizer as finalizer_module
from riverhog_core.services.archive_initial_tag_finalizer import (
    ArchiveInitialTagFinalizer,
)
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission
from riverhog_protocol import collection_tag_set_identity
from riverhog_protocol.errors import Conflict, ServiceUnavailable

from tests.unit.finalization_claim_fixtures import _different_claim, _replace_claim
from tests.unit.test_archive_initial_tag_checkpoint import (
    _orphan,
    _publish,
    _received,
    _recover,
    _snapshot,
    _tag_source,
    _tag_stage,
)
from tests.unit.test_archive_provenance_journal_writer import _lose_ack
from tests.unit.test_archive_volume_metadata_receiving import _count
from tests.unit.test_finalization_claim_readiness import _current_claim


def _owner(f, *, forward_guard=None):
    return ArchiveInitialTagFinalizer(
        f.case.factory,
        collection_id=f.case.collection_id,
        config=f.prior.config,
        archive_stores=f.prior.registry,
        forward_guard=forward_guard,
    )


def _complete_owner(f):
    owner = _owner(f)
    for _ in range(2 if f.source.has_publication else 1):
        assert owner.advance() and not owner.has_pending
    with session_scope(f.case.factory) as session:
        f.source.verify_progress(session)
    assert _count(f.case) == 0
    change = 1 if f.source.has_publication else 0
    assert {k: len(v) for k, v in f.calls.items()} == {
        k: v + change for k, v in f.before_calls.items()
    }


@pytest.mark.parametrize("role", ("state", "expand", "node", "head"))
@pytest.mark.parametrize("mode", ("direct", "http"))
def test_actual_retained_tag_finalizer_separates_physical_and_progress_steps(
    tmp_path, monkeypatch, role, mode
):
    with _tag_source(tmp_path, monkeypatch, role, mode) as f:
        _complete_owner(f)


@pytest.mark.parametrize("phase", ("report", "retirement"))
@pytest.mark.parametrize("committed", (False, True))
def test_tag_original_actor_precedes_new_key_config_target_and_selection(
    tmp_path, monkeypatch, phase, committed
):
    with _tag_source(tmp_path, monkeypatch, "node") as f:
        owner = _owner(f, forward_guard=_current_claim(f.case).guard)
        before = _snapshot(f)
        with _lose_ack(monkeypatch, phase, committed):
            with pytest.raises(OSError, match="injected journal writer"):
                owner.advance()
        actor = owner._actor
        response = actor._response_json
        assert owner.has_pending and response is not None
        _orphan(f)
        with session_scope(f.case.factory) as session:
            upload = session.get(Upload, f.case.collection_id)
            upload.tag_staging_root_sha256 = None
            upload.tag_staging_set_identity = collection_tag_set_identity(None)
            upload.final_authority_json = None
        owner._config = owner._archive_stores = None

        def forbidden(*args, **kwargs):
            raise AssertionError("original tag receiving precedes fresh publisher inputs")

        with monkeypatch.context() as patch:
            patch.setattr(ArchiveCollectionMetadataPublisher, "publish_object", forbidden)
            assert owner.advance() and not owner.has_pending
        assert actor._response_json == response and _received(f) is not None
        assert _snapshot(f) == before and _count(f.case) == 0


@pytest.mark.parametrize("role", ("state", "expand", "node", "head"))
@pytest.mark.parametrize("committed", (False, True))
def test_tag_finalizer_retains_logical_progress_ack_before_fresh_inputs(
    tmp_path, monkeypatch, role, committed
):
    with _tag_source(tmp_path, monkeypatch, role) as f:
        if f.source.has_publication:
            _publish(f)
        claim = _current_claim(f.case)
        owner = _owner(f, forward_guard=claim.guard)
        transaction = finalizer_module.session_scope
        calls = 0

        @contextmanager
        def lost(factory):
            nonlocal calls
            calls += 1
            with transaction(factory) as session:
                yield session
                if calls == 2 and not committed:
                    raise OSError("injected tag progress rollback")
            if calls == 2 and committed:
                raise OSError("injected tag progress reply loss")

        with monkeypatch.context() as patch:
            patch.setattr(finalizer_module, "session_scope", lost)
            with pytest.raises(OSError, match="tag progress"):
                owner.advance()
        assert owner.has_pending and owner._phase == "progress"
        owner._config = owner._archive_stores = None
        before = _snapshot(f)
        _replace_claim(f.case, _different_claim(claim))
        if committed:
            _orphan(f)
        else:
            with pytest.raises(Conflict, match="original finalization claim"):
                owner.advance()
            assert owner.has_pending and _snapshot(f) == before
            _replace_claim(f.case, claim.claim_id)
        assert owner.advance() and not owner.has_pending
        with session_scope(f.case.factory) as session:
            f.source.verify_progress(session)
        assert _count(f.case) == 0


@pytest.mark.parametrize("role", ("state", "expand", "node", "head"))
def test_known_tag_data_and_logical_steps_need_no_key_or_registry(tmp_path, monkeypatch, role):
    with _tag_source(tmp_path, monkeypatch, role) as f:
        if f.source.has_publication:
            _publish(f)
        owner = _owner(f)
        owner._config = SimpleNamespace(archive_write=f.grants)
        owner._archive_stores = None
        assert owner.advance() and not owner.has_pending
        with session_scope(f.case.factory) as session:
            f.source.verify_progress(session)


def _cold_and_live(f, monkeypatch):
    live = _owner(f)
    with _lose_ack(monkeypatch, "report", True):
        with pytest.raises(OSError, match="injected journal writer"):
            live.advance()
    _orphan(f)
    cold = _owner(f)
    cold._config = cold._archive_stores = None
    assert cold.advance() and _count(f.case) == 0
    live._config = live._archive_stores = None
    assert live.advance() and not live.has_pending and _received(f) is not None


def test_live_and_cold_tag_finalizers_converge_on_original_data(tmp_path, monkeypatch):
    with _tag_source(tmp_path, monkeypatch, "node") as f:
        _cold_and_live(f, monkeypatch)


@pytest.mark.parametrize("change", ("capability", "incarnation"))
def test_fresh_tag_finalizer_requires_explicit_exact_ports(tmp_path, monkeypatch, change):
    with _tag_source(tmp_path, monkeypatch, "node") as f:
        owner = _owner(f)
        binding = replace(
            f.prior.binding,
            **(
                {"immutable_writes": None}
                if change == "capability"
                else {"incarnation_id": "f" * 64}
            ),
        )
        owner._archive_stores = ArchiveStoreRegistry({f.prior.name: binding})
        with pytest.raises(ServiceUnavailable):
            owner.advance()
        assert not owner.has_pending and _count(f.case) == 0
        assert {k: len(v) for k, v in f.calls.items()} == f.before_calls


def test_tag_encryption_retry_retains_original_crypto_inputs(tmp_path, monkeypatch):
    with _tag_source(tmp_path, monkeypatch, "node") as f:
        owner = _owner(f)
        encrypt = metadata_module.encrypt_age_scrypt
        calls = []

        def interrupted(content, passphrase, *, log_n):
            calls.append((content, passphrase, log_n))
            if len(calls) == 1:
                raise OSError("injected tag encryption")
            return encrypt(content, passphrase, log_n=log_n)

        with monkeypatch.context() as patch:
            patch.setattr(metadata_module, "encrypt_age_scrypt", interrupted)
            with pytest.raises(OSError, match="injected"):
                owner.advance()
            actor = owner._actor
            assert owner.has_pending
            owner._config = owner._archive_stores = None
            assert owner.advance() and not owner.has_pending
        assert calls[0] == calls[1] and actor is not None and _received(f) is not None
        assert _count(f.case) == 0


def test_tag_preparation_grant_refusal_dispatches_nothing_and_keeps_actor(tmp_path, monkeypatch):
    with _tag_source(tmp_path, monkeypatch, "node") as f:
        owner = _owner(f)
        grant = replace(
            f.grants,
            metadata_preparation_bytes=(
                f.source.reserved_source_bytes + len(f.source.publication.plaintext)
            ),
        )
        owner._config = replace(f.prior.config, archive_write=grant)
        with pytest.raises(Conflict, match="preparation.*grant"):
            owner.advance()
        assert owner.has_pending and _count(f.case) == 0
        assert {k: len(v) for k, v in f.calls.items()} == f.before_calls


def test_tag_unknown_execution_retains_original_actor_and_resources(tmp_path, monkeypatch):
    with _tag_source(tmp_path, monkeypatch, "node") as f:
        owner = _owner(f)

        class Unknown:
            def observe_immutable_object(self, request):
                return f.ports.observe_immutable_object(request)

            def execute_write(self, invocation, *, content):
                return f.ports.execute_write(invocation, content=content).model_copy(
                    update={"execution": "unknown"}
                )

        owner._archive_stores = ArchiveStoreRegistry(
            {f.prior.name: replace(f.prior.binding, immutable_writes=Unknown())}
        )
        with pytest.raises((Conflict, ServiceUnavailable)):
            owner.advance()
        actor = owner._actor
        assert owner.has_pending and _count(f.case) == 1 and _received(f) is not None
        owner._config = owner._archive_stores = None
        with pytest.raises((Conflict, ServiceUnavailable)):
            owner.advance()
        assert owner._actor is actor and owner.has_pending and _count(f.case) == 1
        assert len(f.calls["execute"]) == f.before_calls["execute"] + 1


@pytest.mark.parametrize("mode", ("direct", "http"))
def test_retained_tag_finalizer_completes_entire_encrypted_tree(tmp_path, monkeypatch, mode):
    with _tag_source(tmp_path, monkeypatch, mode=mode) as f:
        from riverhog_core.catalog_models import CollectionTagNodeRecord as Node
        from sqlalchemy import select

        with session_scope(f.case.factory) as session:
            count = len(list(session.scalars(select(Node.digest))))
            passphrase_id = session.get(Upload, f.case.collection_id).passphrase_id
        owner = _owner(f)
        for _ in range(3 * count + 3):
            if not owner.advance():
                break
            assert not owner.has_pending
        else:
            assert not owner.advance()
        assert _tag_stage(f) is None and not owner.has_pending and _count(f.case) == 0
        assert (
            0
            < _recover(f, passphrase=f.prior.config.archive_passphrase_for(passphrase_id))
            <= count
        )


@pytest.mark.parametrize("role", ("state", "expand", "node", "head"))
def test_initial_tag_staging_requires_its_exact_original_claim(tmp_path, monkeypatch, role):
    with _tag_source(tmp_path, monkeypatch, role) as f:
        claim = _current_claim(f.case)
        owner = _owner(f, forward_guard=claim.guard)
        before = _snapshot(f)
        calls = {k: len(v) for k, v in f.calls.items()}
        _replace_claim(f.case, _different_claim(claim))
        with pytest.raises(Conflict, match="original finalization claim"):
            owner.advance()
        assert not owner.has_pending and _snapshot(f) == before and _count(f.case) == 0
        assert {k: len(v) for k, v in f.calls.items()} == calls


@pytest.mark.parametrize("role", ("node", "head"))
def test_initial_tag_preparation_rechecks_the_original_claim(tmp_path, monkeypatch, role):
    with _tag_source(tmp_path, monkeypatch, role) as f:
        _claim_changes_before_preparation(f, monkeypatch)


def _claim_changes_before_preparation(f, monkeypatch):
    claim = _current_claim(f.case)
    owner = _owner(f, forward_guard=claim.guard)
    calls = {k: len(v) for k, v in f.calls.items()}
    bind = ArchiveWriteAdmission.bind_epoch

    def changed(admission, *args, **kwargs):
        epoch = bind(admission, *args, **kwargs)
        _replace_claim(f.case, _different_claim(claim))
        return epoch

    with monkeypatch.context() as patch:
        patch.setattr(ArchiveWriteAdmission, "bind_epoch", changed)
        with pytest.raises(Conflict, match="original finalization claim"):
            owner.advance()
    actor = owner._actor
    assert actor is not None and actor._prepared is None and owner.has_pending
    assert _count(f.case) == 0 and {k: len(v) for k, v in f.calls.items()} == calls
    _replace_claim(f.case, claim.claim_id)
    assert owner.advance() and not owner.has_pending
    assert len(f.calls["execute"]) == calls["execute"] + 1
