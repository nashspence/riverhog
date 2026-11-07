from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

import pytest
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.services import archive_catalog_finalizer as module
from riverhog_core.services import collection_uploads as service_module
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission
from riverhog_protocol.errors import Conflict

from tests.unit.test_collection_upload_preparation import _case
from tests.unit.test_collection_uploads import _bind_primary
from tests.unit.test_finalization_claim_readiness import _claim_snapshot, _seal

_KINDS = ("member-histories", "provenance-closure", "artifact-tree")


@contextmanager
def _original(tmp_path, monkeypatch, kind, *, database_url=None):
    monkeypatch.setattr(service_module, "_FINALIZATION_FILE_BATCH", 1)
    with _case(tmp_path, "segment", artifact_count=2, database_url=database_url) as case:
        for index in (1, 2):
            _bind_primary(case.service, case.collection_id, format(index, "064x"), case.content)
        _seal(case)
        claim = case.service._claim_due_finalization()
        assert claim is not None
        if kind == "provenance-closure":
            assert case.service._stage_next_member_histories(case.collection_id)
            assert case.service._stage_next_member_histories(case.collection_id)
            assert case.service._stage_next_member_histories(case.collection_id)
            assert not case.service._stage_next_member_histories(case.collection_id)
        owner = case.service._catalog_finalization_owner(claim, kind=kind)
        yield case, claim, owner


def _cursor(case, owner):
    with session_scope(case.factory) as session:
        return owner._cursor(session.get(Upload, case.collection_id))


@contextmanager
def _lose_commit_reply(monkeypatch, *, committed):
    actual = module.session_scope

    @contextmanager
    def lose_reply(factory):
        with actual(factory) as session:
            yield session
            if not committed:
                raise OSError("synthetic catalog commit acknowledgement")
        raise OSError("synthetic catalog commit acknowledgement")

    with monkeypatch.context() as patch:
        patch.setattr(module, "session_scope", lose_reply)
        yield


@pytest.mark.parametrize("kind", _KINDS)
@pytest.mark.parametrize("committed", (False, True))
def test_original_catalog_batch_survives_commit_reply_loss(tmp_path, monkeypatch, kind, committed):
    _assert_original_catalog_batch(tmp_path, monkeypatch, kind, committed)


def _assert_original_catalog_batch(tmp_path, monkeypatch, kind, committed, *, database_url=None):
    with _original(tmp_path, monkeypatch, kind, database_url=database_url) as (case, claim, owner):
        actual = owner._advance
        calls = []

        def advance(session, upload):
            calls.append(owner._cursor(upload))
            return actual(session, upload)

        owner._advance = advance
        before = _cursor(case, owner)
        with _lose_commit_reply(monkeypatch, committed=committed):
            with pytest.raises(OSError, match="catalog commit acknowledgement"):
                owner.advance()
        assert owner.has_pending and len(calls) == 1
        after = owner._after
        assert after != before
        assert _cursor(case, owner) == (after if committed else before)
        assert owner.advance() is True
        assert not owner.has_pending and _cursor(case, owner) == after
        assert len(calls) == (1 if committed else 2)
        # A duplicate cannot silently select the second real member.
        assert owner.advance() is True and _cursor(case, owner) == after
        assert len(calls) == (1 if committed else 2)
        replacement = case.service._catalog_finalization_owner(claim, kind=kind)
        assert replacement.advance() is True
        assert _cursor(case, replacement) != after
        assert not replacement.has_pending


@pytest.mark.parametrize("kind", _KINDS)
@pytest.mark.parametrize("committed", (False, True))
def test_unchanged_false_result_is_retained_without_fresh_work(
    tmp_path, monkeypatch, kind, committed
):
    with _original(tmp_path, monkeypatch, kind) as (case, claim, owner):
        with session_scope(case.factory) as session:
            upload = session.get(Upload, case.collection_id)
            if kind == "member-histories":
                upload.provenance_histories_sealed = True
            elif kind == "provenance-closure":
                upload.provenance_closure_validated = True
            else:
                upload.archive_tree_sha256 = "a" * 64
        actual = owner._advance
        calls = []

        def advance(session, upload):
            calls.append(1)
            return actual(session, upload)

        owner._advance = advance
        with _lose_commit_reply(monkeypatch, committed=committed):
            with pytest.raises(OSError, match="catalog commit acknowledgement"):
                owner.advance()
        assert owner.has_pending and owner._result is False
        assert owner._before == owner._after
        assert owner.advance() is False and not owner.has_pending
        assert owner.advance() is False and calls == [1]


@pytest.mark.parametrize("kind", _KINDS)
def test_original_transaction_failure_keeps_its_exact_cursor(tmp_path, monkeypatch, kind):
    with _original(tmp_path, monkeypatch, kind) as (case, claim, owner):
        actual = owner._advance
        before = _cursor(case, owner)
        calls = []

        def advance(session, upload):
            calls.append(owner._cursor(upload))
            progressed = actual(session, upload)
            if len(calls) == 1:
                raise OSError("synthetic failure before commit")
            return progressed

        owner._advance = advance
        with pytest.raises(OSError, match="failure before commit"):
            owner.advance()
        assert owner.has_pending and owner._after is None
        assert _cursor(case, owner) == before
        assert owner.advance() is True and calls == [before, before]
        assert not owner.has_pending


@pytest.mark.parametrize("kind", _KINDS)
def test_pending_original_cannot_advance_a_replacement_claim(tmp_path, monkeypatch, kind):
    with _original(tmp_path, monkeypatch, kind) as (case, claim, owner):
        with _lose_commit_reply(monkeypatch, committed=False):
            with pytest.raises(OSError):
                owner.advance()
        case.service._requeue_finalization_step(claim)
        replacement = case.service._claim_due_finalization()
        assert replacement.claim_id != claim.claim_id
        before = _cursor(case, owner), _claim_snapshot(case)
        with pytest.raises(Conflict, match="original catalog finalization claim"):
            owner.advance()
        assert owner.has_pending
        assert (_cursor(case, owner), _claim_snapshot(case)) == before


@pytest.mark.parametrize("kind", _KINDS)
@pytest.mark.parametrize("committed", (False, True))
def test_terminal_selection_allows_original_receiving_but_not_a_new_catalog_batch(
    tmp_path, monkeypatch, kind, committed
):
    with _original(tmp_path, monkeypatch, kind) as (case, claim, owner):
        with _lose_commit_reply(monkeypatch, committed=committed):
            with pytest.raises(OSError):
                owner.advance()
        ArchiveWriteAdmission(case.factory).select_terminal(
            case.collection_id,
            kind="cancel",
            request={"fixture": "original-catalog-turn"},
            principal_id="uploader",
            grants=case.service._config.archive_write,
            guard=lambda session: None,
            validate_selection=lambda *args: None,
        )
        before = _cursor(case, owner), _claim_snapshot(case)
        if committed:
            assert owner.advance() is True and not owner.has_pending
        else:
            with pytest.raises(Conflict, match="admission is closed"):
                owner.advance()
            assert owner.has_pending
        assert (_cursor(case, owner), _claim_snapshot(case)) == before


def test_terminal_poll_is_responsive_while_the_original_catalog_turn_holds_its_lock(
    tmp_path, monkeypatch
):
    with _original(tmp_path, monkeypatch, "artifact-tree") as (case, claim, owner):
        entered, release = threading.Event(), threading.Event()
        actual = owner._advance

        def advance(session, upload):
            entered.set()
            assert release.wait(10)
            return actual(session, upload)

        owner._advance = advance
        try:
            with ThreadPoolExecutor(max_workers=1) as pool:
                active = pool.submit(owner.advance)
                assert entered.wait(5) and owner.has_pending
                assert owner.drain_terminal("not-yet-selected") == "busy"
                release.set()
                assert active.result(timeout=5) is True
        finally:
            release.set()
        assert not owner.has_pending


@pytest.mark.parametrize("kind", _KINDS)
def test_changed_pending_cursor_is_not_a_fresh_batch(tmp_path, monkeypatch, kind):
    with _original(tmp_path, monkeypatch, kind) as (case, claim, owner):
        with _lose_commit_reply(monkeypatch, committed=False):
            with pytest.raises(OSError):
                owner.advance()
        with session_scope(case.factory) as session:
            upload = session.get(Upload, case.collection_id)
            setattr(upload, owner._fields[0], "f" * 64)
        before = _cursor(case, owner), _claim_snapshot(case)
        with pytest.raises(Conflict, match="original catalog finalization cursor changed"):
            owner.advance()
        assert owner.has_pending
        assert (_cursor(case, owner), _claim_snapshot(case)) == before


@contextmanager
def _lose_feedback_reply(monkeypatch, *, committed):
    actual = service_module.session_scope

    @contextmanager
    def lose_reply(factory):
        with actual(factory) as session:
            yield session
            if not committed:
                raise OSError("synthetic workflow acknowledgement")
        raise OSError("synthetic workflow acknowledgement")

    with monkeypatch.context() as patch:
        patch.setattr(service_module, "session_scope", lose_reply)
        yield


@pytest.mark.parametrize("kind", _KINDS)
@pytest.mark.parametrize("committed", (False, True))
def test_registered_catalog_result_survives_original_workflow_feedback_loss(
    tmp_path, monkeypatch, kind, committed
):
    from tests.unit.test_archive_finalizer_registry import _receive

    with _original(tmp_path, monkeypatch, kind) as (case, claim, initial):
        created, advances, feedback = [], [], []
        selection = f"catalog:{kind}:{claim.claim_id}"
        service = case.service

        def create():
            owner = service._catalog_finalization_owner(claim, kind=kind)
            created.append(owner)
            return owner

        def advance(owner):
            advances.append(1)
            return owner.advance()

        def receive(progressed):
            feedback.append(progressed)
            assert progressed is True
            service._requeue_finalization_step(claim)

        def completed():
            (result,) = _receive(service._archive_finalizers, service._archive_execution)
            return result.result()

        with _lose_feedback_reply(monkeypatch, committed=committed):
            assert (
                service._archive_finalizer_work._submit(
                    case.collection_id, selection, create, advance, receive
                )
                == "submitted"
            )
            with pytest.raises(OSError, match="workflow acknowledgement"):
                completed()
        context = service._archive_finalizers._contexts[case.collection_id]
        retained = context.owner
        assert retained._original is created[0]
        assert retained.has_pending and not created[0].has_pending
        after = _cursor(case, initial)
        assert after == created[0]._after
        assert len(created) == len(advances) == len(feedback) == 1
        assert service._archive_execution.status(context.key) == "untracked"
        assert not service._archive_finalizers.retire_idle(
            context.key, target_incarnation=context.target
        )

        def fresh(*args):
            pytest.fail("pending feedback must retain its original receiver and result")

        assert (
            service._archive_finalizer_work._submit(
                case.collection_id, "fresh-catalog-selection", fresh, fresh, fresh
            )
            == "submitted"
        )
        turn = completed()
        assert turn.selection == selection and turn.progressed
        assert context.owner is retained and not retained.has_pending
        assert _cursor(case, initial) == after
        assert len(created) == len(advances) == 1 and feedback == [True, True]
        with session_scope(case.factory) as session:
            upload = session.get(Upload, case.collection_id)
            assert upload.archive_phase == "finalization_queued"
            assert upload.archive_claim_id is None
        assert service._archive_finalizers.retire_idle(
            context.key, target_incarnation=context.target
        )
