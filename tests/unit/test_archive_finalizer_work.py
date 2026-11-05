from __future__ import annotations

import json
import threading
from contextlib import contextmanager
from dataclasses import replace

import pytest
from riverhog_core.catalog_archive_write_models import ArchiveWriteControlRecord as Control
from riverhog_core.catalog_db import session_scope
from riverhog_core.services import archive_finalizer_work as work_module
from riverhog_core.services.collection_uploads import SqlAlchemyCollectionUploadService
from riverhog_protocol.errors import Conflict, NotFound

from tests.unit.test_archive_finalizer_registry import _receive, _scope
from tests.unit.test_archive_finalizer_terminal_drain import _original, _terminal
from tests.unit.test_archive_provenance_journal_checkpoint import _JOURNAL
from tests.unit.test_archive_provenance_journal_writer import _lose_ack
from tests.unit.test_archive_volume_metadata_receiving import _count

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


def _submit(service, original):
    work = service._archive_finalizer_work
    collection_id = original.case.collection_id
    if original.kind == "structure":
        return work.submit_structure(
            collection_id,
            forward_guard=original.fixture.forward_guard,
            selected_object_id=original.fixture.source.object_id,
        )
    if original.kind == "journal":
        return work.submit_journal(
            collection_id,
            forward_guard=original.fixture.forward_guard,
            journal_id=_JOURNAL,
            offset=8,
            size=8,
        )
    return work.submit_metadata(collection_id, kind=_SELECTIONS[original.kind])


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
        assert owner is not original.owner and type(owner) is type(original.owner)
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
        owner, actor = context.owner, context.owner._actor
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
                original.case.collection_id, kind=different
            )
            == "submitted"
        )
        turn = _completed(service)
        assert turn.selection == _SELECTIONS[kind] and turn.progressed
        assert context.owner is owner and not owner.has_pending
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
            service._archive_finalizer_work.submit_metadata(original.case.collection_id, kind="bad")
        with pytest.raises(NotFound, match="construction is unavailable"):
            service._archive_finalizer_work.submit_metadata(
                original.case.collection_id + 1, kind="initial-description"
            )
        assert not service._archive_execution._slots
        assert not service._archive_finalizers._contexts


def test_closed_admission_is_not_replaced_by_submission_labels(tmp_path, monkeypatch):
    with _original(tmp_path, monkeypatch, "description") as original, _service(original) as service:
        _terminal(original)
        assert _submit(service, original) == "submitted"
        with pytest.raises(Conflict):
            _completed(service)
        assert _count(original.case) == 0
        assert not service._archive_finalizers._contexts[
            original.case.collection_id
        ].owner.has_pending


def test_changed_construction_cannot_adopt_pending_original(tmp_path, monkeypatch):
    with _original(tmp_path, monkeypatch, "description") as original, _service(original) as service:
        with _lose_ack(monkeypatch, "report", True):
            assert _submit(service, original) == "submitted"
            with pytest.raises(OSError):
                _completed(service)
        context = service._archive_finalizers._contexts[original.case.collection_id]
        owner, response = context.owner, context.owner._actor._response_json
        with session_scope(original.case.factory) as session:
            control = session.get(Control, original.case.collection_id)
            changed = json.loads(control.construction_json)
            changed["archive_generation"] = "changed"
            control.construction_json = json.dumps(changed, sort_keys=True, separators=(",", ":"))
        with pytest.raises(Conflict, match="another original construction"):
            _submit(service, original)
        assert context.owner is owner and owner.has_pending
        assert owner._actor._response_json == response
        assert not service._archive_execution._slots
