from __future__ import annotations

import json
from contextlib import contextmanager
from dataclasses import replace

import pytest
from riverhog_core.archive_store_registry import ArchiveStoreRegistry
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.services import immutable_archive_admission as admission_module
from riverhog_core.services.archive_authority_finalizer import ArchiveAuthorityFinalizer
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission
from riverhog_core.services.immutable_archive_preparation import ImmutableArchivePreparation
from riverhog_protocol.errors import Conflict, NotFound, ServiceUnavailable
from sqlalchemy import delete, event, update

from tests.unit.test_archive_authority_checkpoint import _authority_source, _snapshot
from tests.unit.test_archive_volume_metadata_finalizer import _owner as _volume_owner
from tests.unit.test_archive_volume_metadata_receiving import _count
from tests.unit.test_finalization_claim_readiness import _current_claim, _fresh_coordinator


def _owner(fixture, *, forward_guard=None):
    original = _volume_owner(fixture)
    original.owner = ArchiveAuthorityFinalizer(
        original.case.factory,
        collection_id=original.case.collection_id,
        config=original.config,
        archive_stores=original.registry,
        forward_guard=forward_guard,
    )
    return original


def _fresh(original):
    return ArchiveAuthorityFinalizer(
        original.case.factory,
        collection_id=original.case.collection_id,
        config=original.config,
        archive_stores=original.registry,
    )


def _prior_root(original, kind):
    if kind == "recovery":
        assert original.owner.advance()


@pytest.mark.parametrize("mode", ["direct", "http"])
def test_actual_authority_finalizer_keeps_one_call_through_root_recovery_and_requeue(
    tmp_path, monkeypatch, mode
):
    with _authority_source(tmp_path, monkeypatch, mode) as fixture:
        original = _owner(fixture)
        for kind in ("root", "recovery"):
            assert original.owner.advance()
            assert not original.owner.has_pending and _count(fixture.case) == 0
            before = _snapshot(fixture)
            assert before[0] is not None and (before[1] is not None) == (kind == "recovery")
            coordinator = _fresh_coordinator(original.case)
            coordinator._requeue_finalization_step(_current_claim(original.case))
            assert (
                coordinator._claim_due_finalization().collection_id == original.case.collection_id
            )
        assert not original.owner.advance()
        assert len(original.calls["execute"]) == len(original.calls["observe"]) == 2


@pytest.mark.parametrize("kind", ["root", "recovery"])
@pytest.mark.parametrize("phase", ["report", "retirement"])
@pytest.mark.parametrize("committed", [False, True])
def test_authority_finalizer_retains_exact_actor_after_advanced_receiving_and_reply_loss(
    tmp_path, monkeypatch, kind, phase, committed
):
    with _authority_source(tmp_path, monkeypatch) as fixture:
        original = _owner(fixture)
        _prior_root(original, kind)
        transaction = admission_module.session_scope
        calls = 0
        position = 3 if phase == "report" else 4

        @contextmanager
        def lost(factory):
            nonlocal calls
            calls += 1
            selected = calls == position
            with transaction(factory) as session:
                yield session
                if selected and not committed:
                    raise OSError("injected authority finalizer rollback")
            if selected and committed:
                raise OSError("injected authority finalizer reply loss")

        with monkeypatch.context() as patch:
            patch.setattr(admission_module, "session_scope", lost)
            with pytest.raises(OSError, match="injected authority finalizer"):
                original.owner.advance()
        actor = original.owner._actor
        assert original.owner.has_pending and actor is not None
        response = actor._response_json
        assert response is not None
        executed = len(original.calls["execute"])
        original.owner._config = None
        original.owner._archive_stores = None
        with session_scope(fixture.case.factory) as session:
            upload = session.get(Upload, fixture.case.collection_id)
            upload.state = upload.archive_phase = "orphaned"
        assert original.owner.advance()
        assert actor._response_json == response and not original.owner.has_pending
        assert _count(fixture.case) == 0 and len(original.calls["execute"]) == executed
        assert original.owner._passphrase is None and original.owner._source is None


@pytest.mark.parametrize("kind", ["root", "recovery"])
def test_positive_unknown_authority_data_does_not_allow_fresh_finalizer_progress(
    tmp_path, monkeypatch, kind
):
    with _authority_source(tmp_path, monkeypatch) as fixture:
        original = _owner(fixture)
        _prior_root(original, kind)
        execute = original.ports.execute_write

        def unknown(invocation, *, content):
            return execute(invocation, content=content).model_copy(update={"execution": "unknown"})

        monkeypatch.setattr(original.ports, "execute_write", unknown)
        with pytest.raises(Conflict, match="execution remains unknown"):
            original.owner.advance()
        before = _snapshot(fixture)
        assert before[0] is not None and (before[1] is not None) == (kind == "recovery")
        executed = len(original.calls["execute"])
        with pytest.raises(Conflict, match="execution remains unknown"):
            original.owner.advance()
        with pytest.raises(Conflict, match="unresolved provider writes"):
            _fresh(original).advance()
        assert _snapshot(fixture) == before
        assert original.owner.has_pending and _count(fixture.case) == 1
        assert len(original.calls["execute"]) == len(original.calls["observe"]) == executed


@pytest.mark.parametrize("kind", ["root", "recovery"])
def test_live_and_cold_authority_finalizers_converge_without_redispatch(
    tmp_path, monkeypatch, kind
):
    with _authority_source(tmp_path, monkeypatch) as fixture:
        original = _owner(fixture)
        _prior_root(original, kind)
        calls_before = len(original.calls["execute"])
        # The new actor is installed before report; patch the owning transaction
        # to lose the original positive receiving commit reply.
        transaction = admission_module.session_scope
        calls = 0

        @contextmanager
        def lost(factory):
            nonlocal calls
            calls += 1
            with transaction(factory) as session:
                yield session
            if calls == 3:
                raise OSError("injected original receiving reply loss")

        with monkeypatch.context() as patch:
            patch.setattr(admission_module, "session_scope", lost)
            with pytest.raises(OSError, match="original receiving reply loss"):
                original.owner.advance()
        assert original.owner.has_pending and _count(fixture.case) == 1
        cold = _fresh(original)
        assert cold.advance()
        assert _count(fixture.case) == 0
        assert len(original.calls["execute"]) == calls_before + 1
        assert original.owner.advance()
        assert not original.owner.has_pending and _count(fixture.case) == 0
        assert len(original.calls["execute"]) == calls_before + 1


def test_authority_finalizer_requires_explicit_selected_write_capability(tmp_path, monkeypatch):
    with _authority_source(tmp_path, monkeypatch) as fixture:
        original = _owner(fixture)
        original.owner._archive_stores = ArchiveStoreRegistry(
            {original.name: replace(original.binding, immutable_writes=None)}
        )
        with pytest.raises(ServiceUnavailable, match="immutable"):
            original.owner.advance()
        assert not original.owner.has_pending and _count(fixture.case) == 0
        assert original.calls == {"observe": [], "execute": []}


@pytest.mark.parametrize("kind", ["root", "recovery"])
def test_missing_parent_keeps_original_authority_result_and_actor_owned(
    tmp_path, monkeypatch, kind
):
    with _authority_source(tmp_path, monkeypatch) as fixture:
        original = _owner(fixture)
        _prior_root(original, kind)
        transaction = admission_module.session_scope
        calls = 0

        @contextmanager
        def lost(factory):
            nonlocal calls
            calls += 1
            with transaction(factory) as session:
                yield session
            if calls == 3:
                raise OSError("injected original receiving reply loss")

        with monkeypatch.context() as patch:
            patch.setattr(admission_module, "session_scope", lost)
            with pytest.raises(OSError, match="original receiving reply loss"):
                original.owner.advance()
        actor = original.owner._actor
        with session_scope(fixture.case.factory) as session:
            session.execute(
                delete(Upload).where(Upload.collection_id == fixture.case.collection_id)
            )
        with pytest.raises(NotFound):
            original.owner.advance()
        assert original.owner._actor is actor and original.owner.has_pending
        assert actor._response_json is not None and _count(fixture.case) == 1


@pytest.mark.parametrize("change", ["root-missing", "pair-changed"])
def test_completed_authority_requires_its_actual_original_pair_proof(tmp_path, monkeypatch, change):
    with _authority_source(tmp_path, monkeypatch) as fixture:
        original = _owner(fixture)
        assert original.owner.advance() and original.owner.advance()
        assert not original.owner.advance()
        with session_scope(fixture.case.factory) as session:
            upload = session.get(Upload, fixture.case.collection_id)
            if change == "root-missing":
                upload.archive_root_receipt_json = None
            else:
                value = json.loads(upload.final_authority_json)
                value["root"]["artifact_count"] += 1
                upload.final_authority_json = json.dumps(
                    value, sort_keys=True, separators=(",", ":")
                )
        with pytest.raises(Conflict, match="receiving proof"):
            _fresh(original).advance()
        assert len(original.calls["execute"]) == 2 and _count(fixture.case) == 0


def _different_claim(claim):
    return ("0" if claim.claim_id[0] != "0" else "1") + claim.claim_id[1:]


def _replace_claim(case, claim_id):
    with session_scope(case.factory) as session:
        upload = session.get(Upload, case.collection_id)
        upload.archive_claim_id = claim_id


@pytest.mark.parametrize("kind", ["root", "recovery"])
def test_authority_selection_requires_the_exact_original_finalization_claim(
    tmp_path, monkeypatch, kind
):
    with _authority_source(tmp_path, monkeypatch) as fixture:
        claim = _current_claim(fixture.case)
        original = _owner(fixture, forward_guard=claim.guard)
        _prior_root(original, kind)
        before = _snapshot(fixture)
        calls = {name: len(values) for name, values in original.calls.items()}
        _replace_claim(fixture.case, _different_claim(claim))
        with pytest.raises(Conflict, match="original finalization claim"):
            original.owner.advance()
        assert not original.owner.has_pending and _snapshot(fixture) == before
        assert {name: len(values) for name, values in original.calls.items()} == calls


@pytest.mark.parametrize("kind", ["root", "recovery"])
@pytest.mark.parametrize("boundary", ["preparation", "admission"])
def test_changed_claim_after_selection_cannot_prepare_or_admit_authority_publication(
    tmp_path, monkeypatch, kind, boundary
):
    with _authority_source(tmp_path, monkeypatch) as fixture:
        claim = _current_claim(fixture.case)
        original = _owner(fixture, forward_guard=claim.guard)
        _prior_root(original, kind)
        calls = {name: len(values) for name, values in original.calls.items()}
        before = _snapshot(fixture)
        owner_type, method = (
            (ArchiveWriteAdmission, "bind_epoch")
            if boundary == "preparation"
            else (ImmutableArchivePreparation, "prepare")
        )
        advance = getattr(owner_type, method)

        def changed(owner, *args, **kwargs):
            result = advance(owner, *args, **kwargs)
            _replace_claim(fixture.case, _different_claim(claim))
            return result

        with monkeypatch.context() as patch:
            patch.setattr(owner_type, method, changed)
            with pytest.raises(Conflict, match="original finalization claim"):
                original.owner.advance()
        actor = original.owner._actor
        assert actor is not None and original.owner.has_pending
        assert actor._response_json is None and _count(fixture.case) == 0
        assert (actor._prepared is None) == (boundary == "preparation")
        assert _snapshot(fixture) == before
        assert {name: len(values) for name, values in original.calls.items()} == calls
        _replace_claim(fixture.case, claim.claim_id)
        assert original.owner.advance()
        assert not original.owner.has_pending and _count(fixture.case) == 0
        assert len(original.calls["execute"]) == calls["execute"] + 1


@pytest.mark.parametrize("kind", ["root", "recovery"])
@pytest.mark.parametrize("committed", [False, True])
def test_original_authority_receiving_does_not_reacquire_a_replaced_claim(
    tmp_path, monkeypatch, kind, committed
):
    with _authority_source(tmp_path, monkeypatch) as fixture:
        claim = _current_claim(fixture.case)
        original = _owner(fixture, forward_guard=claim.guard)
        _prior_root(original, kind)
        transaction = admission_module.session_scope
        calls = 0

        @contextmanager
        def lost(factory):
            nonlocal calls
            calls += 1
            selected = calls == 3
            with transaction(factory) as session:
                yield session
                if selected and not committed:
                    raise OSError("injected guarded authority rollback")
            if selected and committed:
                raise OSError("injected guarded authority reply loss")

        with monkeypatch.context() as patch:
            patch.setattr(admission_module, "session_scope", lost)
            with pytest.raises(OSError, match="guarded authority"):
                original.owner.advance()
        actor = original.owner._actor
        assert actor is not None and actor._response_json is not None
        executed = len(original.calls["execute"])
        _replace_claim(fixture.case, _different_claim(claim))
        original.owner._config = original.owner._archive_stores = None
        assert original.owner.advance()
        assert not original.owner.has_pending and _count(fixture.case) == 0
        assert len(original.calls["execute"]) == executed
        with session_scope(fixture.case.factory) as session:
            assert session.get(Upload, fixture.case.collection_id).archive_claim_id == (
                _different_claim(claim)
            )


def test_original_claim_guard_reads_only_its_bounded_workflow_fields(tmp_path, monkeypatch):
    with _authority_source(tmp_path, monkeypatch) as fixture:
        claim = _current_claim(fixture.case)
        engine = fixture.case.factory.kw["bind"]
        observed = []

        def selected(connection, cursor, statement, parameters, context, executemany):
            compiled = context.compiled
            if compiled is not None and compiled.statement.is_select:
                observed.append(tuple(compiled.statement.selected_columns.keys()))

        event.listen(engine, "before_cursor_execute", selected)
        try:
            with session_scope(fixture.case.factory) as session:
                claim.guard(session)
        finally:
            event.remove(engine, "before_cursor_execute", selected)
        assert observed == [("state", "archive_phase", "archive_claim_id")]


def test_original_claim_guard_rechecks_database_despite_cached_upload(tmp_path, monkeypatch):
    with _authority_source(tmp_path, monkeypatch) as fixture:
        claim = _current_claim(fixture.case)
        with session_scope(fixture.case.factory) as session:
            cached = session.get(Upload, fixture.case.collection_id)
            session.execute(
                update(Upload)
                .where(Upload.collection_id == claim.collection_id)
                .values(archive_claim_id=_different_claim(claim))
                .execution_options(synchronize_session=False)
            )
            assert cached.archive_claim_id == claim.claim_id
            with pytest.raises(Conflict, match="original finalization claim"):
                claim.guard(session)
