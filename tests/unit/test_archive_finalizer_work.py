from __future__ import annotations

import json
import threading
from contextlib import contextmanager
from dataclasses import replace

import pytest
from riverhog_core.catalog_archive_write_models import ArchiveWriteControlRecord as Control
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.services import archive_finalizer_work as work_module
from riverhog_core.services import collection_uploads as service_module
from riverhog_core.services.collection_uploads import SqlAlchemyCollectionUploadService
from riverhog_protocol.errors import Conflict, NotFound

from tests.unit.test_archive_finalizer_registry import _receive, _scope
from tests.unit.test_archive_finalizer_terminal_drain import _original, _terminal
from tests.unit.test_archive_provenance_journal_checkpoint import _JOURNAL
from tests.unit.test_archive_provenance_journal_writer import _lose_ack
from tests.unit.test_archive_volume_metadata_receiving import _count
from tests.unit.test_finalization_claim_readiness import _current_claim

_KINDS = (
    "archive-volume",
    "archive-terminal",
    "description",
    "tag-node",
    "provenance-authority",
    "root",
    "recovery",
    "structure",
    "journal",
    "volume-bindings",
    "volume-journal",
    "volume-metadata",
)
_SELECTIONS = {
    "archive-volume": "archive-volumes",
    "archive-terminal": "archive-volumes",
    "description": "initial-description",
    "tag-node": "initial-tags",
    "provenance-authority": "provenance-authority",
    "root": "archive-authority",
    "recovery": "archive-authority",
    "structure": "provenance-structure",
    "journal": "provenance-journal",
    "volume-bindings": "provenance-volumes",
    "volume-journal": "provenance-volumes",
    "volume-metadata": "provenance-volumes",
}


@contextmanager
def _service(original, **tuning):
    config = original.owner._config
    if tuning:
        config = replace(config, archive_execution=replace(config.archive_execution, **tuning))
    service = SqlAlchemyCollectionUploadService(
        config,
        original.owner._archive_stores,
        session_factory=original.case.factory,
    )
    try:
        yield service
    finally:
        service.close()


def _submit(service, original, *, receive=lambda progressed: None):
    work = service._archive_finalizer_work
    collection_id = original.case.collection_id
    if original.kind == "structure":
        return work.submit_structure(
            collection_id,
            forward_guard=original.fixture.forward_guard,
            selected_object_id=original.fixture.source.object_id,
            receive=receive,
        )
    if original.kind == "journal":
        return work.submit_journal(
            collection_id,
            forward_guard=original.fixture.forward_guard,
            journal_id=_JOURNAL,
            offset=8,
            size=8,
            receive=receive,
        )
    return work.submit_metadata(collection_id, kind=_SELECTIONS[original.kind], receive=receive)


def _completed(service):
    (result,) = _receive(service._archive_finalizers, service._archive_execution)
    return result.result()


@pytest.mark.parametrize("kind", _KINDS)
@pytest.mark.parametrize("mode", ("direct", "http"))
def test_service_submits_actual_retained_finalizer_owners(tmp_path, monkeypatch, kind, mode):
    with _original(tmp_path, monkeypatch, kind, mode) as original, _service(original) as service:
        assert _submit(service, original) == "submitted"
        turn = _completed(service)
        assert turn.selection == _SELECTIONS[kind] and turn.progressed
        owner = service._archive_finalizers._contexts[original.case.collection_id].owner
        assert owner._original is not original.owner
        assert type(owner._original) is type(original.owner)
        assert not owner.has_pending and _count(original.case) == 0


@pytest.mark.parametrize(
    "kind",
    (
        "archive-volume",
        "description",
        "tag-node",
        "provenance-authority",
        "root",
        "structure",
        "journal",
        "volume-bindings",
    ),
)
@pytest.mark.parametrize("committed", (False, True))
@pytest.mark.parametrize("mode", ("direct", "http"))
def test_service_resumes_original_before_a_different_kind_after_lost_report(
    tmp_path, monkeypatch, kind, committed, mode
):
    _actual_service_retry(tmp_path, monkeypatch, kind, committed, mode)


def _actual_service_retry(tmp_path, monkeypatch, kind, committed, mode, *, database_url=None):
    with (
        _original(tmp_path, monkeypatch, kind, mode, database_url=database_url) as original,
        _service(original) as service,
    ):
        with _lose_ack(monkeypatch, "report", committed):
            assert _submit(service, original) == "submitted"
            with pytest.raises(OSError, match="injected journal writer"):
                _completed(service)
        context = service._archive_finalizers._contexts[original.case.collection_id]
        retained = context.owner
        owner, actor = retained._original, retained._original._actor
        assert owner.has_pending and actor._response_json is not None
        prior_response = actor._response_json
        # New service inputs cannot replace original receiving, including when
        # the new selected work belongs to another finalizer kind.
        service._archive_finalizer_work._config = None
        service._archive_finalizer_work._archive_stores = None
        owner._config = owner._archive_stores = None
        different = "archive-authority" if kind == "description" else "initial-description"
        assert (
            service._archive_finalizer_work.submit_metadata(
                original.case.collection_id, kind=different, receive=lambda progressed: None
            )
            == "submitted"
        )
        turn = _completed(service)
        assert turn.selection == _SELECTIONS[kind] and turn.progressed
        assert context.owner is retained and not retained.has_pending and not owner.has_pending
        assert actor._response_json == prior_response and _count(original.case) == 0


def test_submission_never_runs_constructor_or_key_provider_work_on_coordinator(
    tmp_path, monkeypatch
):
    with _original(tmp_path, monkeypatch, "description") as original:
        entered, release = threading.Event(), threading.Event()
        actual = work_module._METADATA["initial-description"]
        coordinator = threading.get_ident()

        def held(*args, **kwargs):
            assert threading.get_ident() != coordinator
            entered.set()
            assert release.wait(10)
            return actual(*args, **kwargs)

        monkeypatch.setitem(work_module._METADATA, "initial-description", held)
        try:
            with _service(original) as service:
                assert _submit(service, original) == "submitted"
                assert entered.wait(5)
                key, target = _scope(original.case)
                assert service._archive_execution.status(key) == "running"
                assert service._archive_finalizers._contexts[key.collection_id].owner is None
                operation = _terminal(original)
                assert (
                    service._archive_finalizers.fence_terminal(
                        key, target_incarnation=target, operation_id=operation
                    )
                    == "fenced"
                )
                release.set()
                with pytest.raises(Conflict, match="fenced during construction"):
                    _completed(service)
                assert _count(original.case) == 0
                assert (
                    service._archive_finalizers.submit_terminal(
                        key, target_incarnation=target, operation_id=operation
                    )
                    == "submitted"
                )
                assert _completed(service) == "closed"
        finally:
            release.set()


def test_bad_selection_and_missing_construction_start_no_workers(tmp_path, monkeypatch):
    with _original(tmp_path, monkeypatch, "description") as original, _service(original) as service:
        with pytest.raises(ValueError, match="unknown archive metadata"):
            service._archive_finalizer_work.submit_metadata(
                original.case.collection_id, kind="bad", receive=lambda progressed: None
            )
        with pytest.raises(NotFound, match="construction is unavailable"):
            service._archive_finalizer_work.submit_metadata(
                original.case.collection_id + 1,
                kind="initial-description",
                receive=lambda progressed: None,
            )
        assert not service._archive_execution._slots
        assert not service._archive_finalizers._contexts


def test_closed_admission_is_not_replaced_by_submission_labels(tmp_path, monkeypatch):
    with _original(tmp_path, monkeypatch, "description") as original, _service(original) as service:
        _terminal(original)
        with pytest.raises(Conflict, match="original archive finalizer is unavailable"):
            _submit(service, original)
        assert _count(original.case) == 0
        assert not service._archive_finalizers._contexts
        assert not service._archive_execution._slots


def test_changed_construction_cannot_adopt_pending_original(tmp_path, monkeypatch):
    with _original(tmp_path, monkeypatch, "description") as original, _service(original) as service:
        with _lose_ack(monkeypatch, "report", True):
            assert _submit(service, original) == "submitted"
            with pytest.raises(OSError):
                _completed(service)
        context = service._archive_finalizers._contexts[original.case.collection_id]
        retained = context.owner
        owner, response = retained._original, retained._original._actor._response_json
        with session_scope(original.case.factory) as session:
            control = session.get(Control, original.case.collection_id)
            changed = json.loads(control.construction_json)
            changed["archive_generation"] = "changed"
            control.construction_json = json.dumps(changed, sort_keys=True, separators=(",", ":"))
        with pytest.raises(Conflict, match="another original construction"):
            _submit(service, original)
        assert context.owner is retained and retained.has_pending and owner.has_pending
        assert owner._actor._response_json == response
        assert not service._archive_execution._slots


@contextmanager
def _lost_queue_feedback(monkeypatch, service, claim, committed):
    actual = service_module.session_scope
    state = {"active": False, "fired": False}
    calls = []

    @contextmanager
    def transaction(factory):
        selected = state["active"] and not state["fired"]
        with actual(factory) as session:
            yield session
            if selected and not committed:
                state["fired"] = True
                raise OSError("injected queue feedback rollback")
        if selected and committed:
            state["fired"] = True
            raise OSError("injected queue feedback commit reply")

    def receive(progressed):
        calls.append(progressed)
        state["active"] = True
        try:
            if claim is None:
                # Pre-finalization structure/journal owners acknowledge their
                # own workflow without inventing a finalization queue claim.
                with service_module.session_scope(service._session_factory):
                    pass
            else:
                service._requeue_finalization_step(claim)
        finally:
            state["active"] = False

    with monkeypatch.context() as patch:
        patch.setattr(service_module, "session_scope", transaction)
        yield receive, calls


def _queue_snapshot(case):
    with session_scope(case.factory) as session:
        upload = session.get(Upload, case.collection_id)
        return (
            upload.state,
            upload.archive_phase,
            upload.archive_next_attempt_at,
            upload.archive_failure,
            upload.archive_claim_id,
        )


@pytest.mark.parametrize(
    "kind",
    (
        "archive-volume",
        "description",
        "tag-node",
        "provenance-authority",
        "root",
        "structure",
        "journal",
        "volume-bindings",
    ),
)
@pytest.mark.parametrize("mode", ("direct", "http"))
@pytest.mark.parametrize("committed", (False, True))
def test_actual_finalizer_receives_original_queue_feedback_before_fresh_publication(
    tmp_path, monkeypatch, kind, mode, committed
):
    _actual_feedback(tmp_path, monkeypatch, kind, mode, committed)


@pytest.mark.parametrize("mode", ("direct", "http"))
@pytest.mark.parametrize("committed", (False, True))
def test_terminal_fence_preserves_unacknowledged_workflow_feedback(
    tmp_path, monkeypatch, mode, committed
):
    _actual_feedback(tmp_path, monkeypatch, "description", mode, committed, terminal=True)


def _actual_feedback(
    tmp_path, monkeypatch, kind, mode, committed, *, terminal=False, database_url=None
):
    with (
        _original(tmp_path, monkeypatch, kind, mode, database_url=database_url) as original,
        _service(original) as service,
    ):
        initial = _queue_snapshot(original.case)
        finalizing = initial[:2] == ("finalizing", "finalizing")
        claim = _current_claim(original.case) if finalizing else None
        with _lost_queue_feedback(monkeypatch, service, claim, committed) as (
            receive,
            calls,
        ):
            assert _submit(service, original, receive=receive) == "submitted"
            with pytest.raises(OSError, match="injected queue feedback"):
                _completed(service)
        context = service._archive_finalizers._contexts[original.case.collection_id]
        retained, child = context.owner, context.owner._original
        assert retained.has_pending and retained._result is True
        assert not child.has_pending and _count(original.case) == 0
        assert calls == [True]
        before = _queue_snapshot(original.case)
        if finalizing and committed:
            assert before[1] == "finalization_queued"
        else:
            assert before == initial

        def forbidden(*args, **kwargs):
            pytest.fail("saved feedback must precede new execution, receiver or inputs")

        with monkeypatch.context() as patch:
            patch.setattr(type(child), "advance", forbidden)
            service._archive_finalizer_work._config = None
            service._archive_finalizer_work._archive_stores = None
            if terminal:
                operation = _terminal(original)
                key, target = _scope(original.case)
                assert (
                    service._archive_finalizers.submit_terminal(
                        key, target_incarnation=target, operation_id=operation
                    )
                    == "submitted"
                )
                assert _completed(service) == "closed"
                assert retained._result is True and retained.has_pending
                assert context.owner is retained and calls == [True]
                assert _queue_snapshot(original.case) == before
                assert not service._archive_finalizers.retire_idle(key, target_incarnation=target)
                with pytest.raises(Conflict, match="stopped for its terminal"):
                    retained.advance()
                assert (
                    service._archive_finalizers.submit_terminal(
                        key, target_incarnation=target, operation_id=operation
                    )
                    == "drained"
                )
            else:
                different = "archive-authority" if kind == "description" else "initial-description"
                assert (
                    service._archive_finalizer_work.submit_metadata(
                        original.case.collection_id, kind=different, receive=forbidden
                    )
                    == "submitted"
                )
                turn = _completed(service)
                assert turn.selection == _SELECTIONS[kind] and turn.progressed
                assert context.owner is retained and not retained.has_pending
                assert retained._result is None and calls == [True, True]
                if finalizing:
                    assert _queue_snapshot(original.case)[1] == "finalization_queued"
                    if committed:
                        assert _queue_snapshot(original.case) == before
                else:
                    # Structure/journal feedback does not invent readiness or
                    # move a pre-finalization construction into the queue.
                    assert _queue_snapshot(original.case) == initial
            assert _count(original.case) == 0


@pytest.mark.parametrize("mode", ("direct", "http"))
def test_terminal_selected_during_feedback_prevents_a_late_requeue(tmp_path, monkeypatch, mode):
    _actual_gate_closed_feedback(tmp_path, monkeypatch, mode)


def _actual_gate_closed_feedback(tmp_path, monkeypatch, mode, *, database_url=None):
    with (
        _original(
            tmp_path, monkeypatch, "description", mode, database_url=database_url
        ) as original,
        _service(original) as service,
    ):
        entered, release = threading.Event(), threading.Event()
        calls = []
        claim = _current_claim(original.case)

        def receive(progressed):
            calls.append(progressed)
            entered.set()
            assert release.wait(10)
            service._requeue_finalization_step(claim)

        try:
            assert _submit(service, original, receive=receive) == "submitted"
            assert entered.wait(5)
            before = _queue_snapshot(original.case)
            operation = _terminal(original)
            key, target = _scope(original.case)
            assert (
                service._archive_finalizers.fence_terminal(
                    key, target_incarnation=target, operation_id=operation
                )
                == "fenced"
            )
            release.set()
            assert _completed(service).progressed
            assert calls == [True] and _queue_snapshot(original.case) == before
            assert (
                service._archive_finalizers.submit_terminal(
                    key, target_incarnation=target, operation_id=operation
                )
                == "submitted"
            )
            assert _completed(service) == "closed"
        finally:
            release.set()


@pytest.mark.parametrize(
    "kind",
    (
        "archive-volume",
        "description",
        "tag-node",
        "provenance-authority",
        "root",
        "volume-bindings",
    ),
)
@pytest.mark.parametrize("mode", ("direct", "http"))
@pytest.mark.parametrize("committed", (False, True))
def test_original_feedback_acknowledgement_cannot_requeue_a_newer_claim(
    tmp_path, monkeypatch, kind, mode, committed
):
    _actual_claim_feedback(tmp_path, monkeypatch, kind, mode, committed)


def _actual_claim_feedback(tmp_path, monkeypatch, kind, mode, committed, *, database_url=None):
    from tests.unit.test_finalization_claim_readiness import _fresh_coordinator

    with (
        _original(tmp_path, monkeypatch, kind, mode, database_url=database_url) as original,
        _service(original) as service,
    ):
        claim = _current_claim(original.case)
        with _lost_queue_feedback(monkeypatch, service, claim, committed) as (receive, calls):
            assert _submit(service, original, receive=receive) == "submitted"
            with pytest.raises(OSError, match="injected queue feedback"):
                _completed(service)
        context = service._archive_finalizers._contexts[original.case.collection_id]
        retained, child = context.owner, context.owner._original
        assert retained.has_pending and retained._result is True
        assert not child.has_pending and _count(original.case) == 0
        current = _fresh_coordinator(original.case)
        if not committed:
            assert current.requeue_interrupted_finalizations_for_startup() == 1
        replacement = current._claim_due_finalization()
        assert replacement.collection_id == claim.collection_id
        assert replacement.claim_id != claim.claim_id
        before = _queue_snapshot(original.case)

        def forbidden(*args, **kwargs):
            pytest.fail(
                "original feedback retry must not repeat publication or replace its receiver"
            )

        with monkeypatch.context() as patch:
            patch.setattr(type(child), "advance", forbidden)
            assert (
                service._archive_finalizer_work.submit_metadata(
                    original.case.collection_id, kind="initial-description", receive=forbidden
                )
                == "submitted"
            )
            turn = _completed(service)
        assert turn.selection == _SELECTIONS[kind] and turn.progressed
        assert context.owner is retained and retained._result is None and not retained.has_pending
        assert calls == [True, True] and _queue_snapshot(original.case) == before
        assert _count(original.case) == 0
