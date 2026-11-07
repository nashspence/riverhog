from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadRecord as Upload
from riverhog_core.services.archive_finalization_pipeline import ArchiveFinalizationPipeline
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission
from riverhog_core.services.immutable_archive_receiving import OriginalImmutableArchiveReport
from riverhog_protocol.errors import Conflict

from tests.unit.test_archive_catalog_finalizer import (
    _cursor,
    _lose_commit_reply,
)
from tests.unit.test_archive_catalog_finalizer import (
    _original as _catalog_case,
)
from tests.unit.test_archive_finalizer_registry import _receive
from tests.unit.test_archive_immutable_source_receiving import _original_report
from tests.unit.test_archive_volume_metadata_receiving import _count
from tests.unit.test_finalization_claim_readiness import _claim_snapshot
from tests.unit.test_original_immutable_archive_report import _lose_reply


def _pipeline(case, *stages):
    return ArchiveFinalizationPipeline(
        case.factory, collection_id=case.collection_id, stages=stages
    )


def _forbidden(*args, **kwargs):
    pytest.fail("a retained pipeline cannot select fresh work")


def _terminal(case):
    return ArchiveWriteAdmission(case.factory).select_terminal(
        case.collection_id,
        kind="cancel",
        request={"fixture": "original-pipeline"},
        principal_id="uploader",
        grants=case.service._config.archive_write,
        guard=lambda session: None,
        validate_selection=lambda *args: None,
    )


@pytest.mark.parametrize("progressed", (False, True))
def test_real_pipeline_skips_closed_false_stages_and_caches_one_original_turn(
    tmp_path, monkeypatch, progressed
):
    with _catalog_case(tmp_path, monkeypatch, "artifact-tree") as (case, claim, catalog):
        if not progressed:
            with session_scope(case.factory) as session:
                session.get(Upload, case.collection_id).archive_tree_sha256 = "a" * 64
        report = OriginalImmutableArchiveReport(case.factory, case.collection_id)
        created = []

        def stage(name, owner):
            def create():
                created.append(name)
                return owner

            return create

        owner = _pipeline(case, stage("reports", report), stage("tree", catalog))
        assert created == [] and not owner.has_pending
        assert owner.advance() is progressed and not owner.has_pending
        cursor = _cursor(case, catalog)
        assert created == ["reports", "tree"] and report._result is False
        assert owner.advance() is progressed and _cursor(case, catalog) == cursor
        assert created == ["reports", "tree"]


@pytest.mark.parametrize("phase", ("receive", "retire"))
@pytest.mark.parametrize("committed", (False, True))
def test_pipeline_keeps_the_original_closed_report_before_any_next_stage(
    tmp_path, monkeypatch, phase, committed
):
    with _original_report(tmp_path, monkeypatch, "volume", receive=False) as (
        fixture,
        source,
        admission,
        admitted,
        receipt,
    ):
        case = fixture.case
        report = OriginalImmutableArchiveReport(case.factory, case.collection_id)
        owner = _pipeline(case, lambda: report, _forbidden)
        with _lose_reply(monkeypatch, report, phase, committed):
            with pytest.raises(OSError):
                owner.advance()
        selected = report._selected
        assert owner.has_pending and owner._original is report and owner._index == 0
        monkeypatch.setattr(report._admission, "load_next_collection_report", _forbidden)
        assert owner.advance() is True and not owner.has_pending
        assert report._selected is selected and _count(case) == 0
        assert owner.advance() is True


@pytest.mark.parametrize("committed", (False, True))
def test_pipeline_retries_the_same_catalog_cursor_after_commit_reply_loss(
    tmp_path, monkeypatch, committed
):
    with _catalog_case(tmp_path, monkeypatch, "artifact-tree") as (case, claim, catalog):
        report = OriginalImmutableArchiveReport(case.factory, case.collection_id)
        owner = _pipeline(case, lambda: report, lambda: catalog, _forbidden)
        with _lose_commit_reply(monkeypatch, committed=committed):
            with pytest.raises(OSError):
                owner.advance()
        assert owner.has_pending and owner._original is catalog and owner._index == 1
        assert report._result is False and catalog.has_pending
        assert owner.advance() is True and not owner.has_pending
        after = _cursor(case, catalog)
        assert owner.advance() is True and _cursor(case, catalog) == after


@pytest.mark.parametrize("returned", (False, True))
def test_unconfirmed_pipeline_constructor_stays_charged_and_is_never_replaced(
    tmp_path, monkeypatch, returned
):
    with _catalog_case(tmp_path, monkeypatch, "artifact-tree") as (case, claim, catalog):
        calls = []

        def create():
            calls.append(catalog)
            if returned:
                return None
            raise OSError("synthetic unconfirmed constructor")

        owner = _pipeline(case, create, _forbidden)
        with pytest.raises(ValueError if returned else OSError):
            owner.advance()
        assert owner.has_pending and owner._construction_failure
        with pytest.raises(Conflict, match="constructor remains unresolved"):
            owner.advance()
        assert calls == [catalog]
        operation = _terminal(case)
        assert owner.drain_terminal(operation) == "pending"
        assert owner.drain_terminal(operation) == "pending" and owner.has_pending
        with pytest.raises(Conflict, match="stopped for its terminal"):
            owner.advance()


def test_pipeline_terminal_drain_preserves_selected_report_without_advancing_plan(
    tmp_path, monkeypatch
):
    with _original_report(tmp_path, monkeypatch, "volume", receive=False) as (
        fixture,
        source,
        admission,
        admitted,
        receipt,
    ):
        case = fixture.case
        report = OriginalImmutableArchiveReport(case.factory, case.collection_id)
        owner = _pipeline(case, lambda: report, _forbidden)
        with _lose_reply(monkeypatch, report, "receive", False):
            with pytest.raises(OSError):
                owner.advance()
        selected = report._selected
        operation = _terminal(case)
        monkeypatch.setattr(report._admission, "load_next_collection_report", _forbidden)
        assert owner.drain_terminal(operation) == "closed"
        assert owner._original is report and report._selected is selected and owner._index == 0
        assert owner.has_pending and not report.has_pending and _count(case) == 0
        with pytest.raises(Conflict, match="stopped for its terminal"):
            owner.advance()


def test_pipeline_terminal_poll_does_not_wait_for_its_original_catalog_transaction(
    tmp_path, monkeypatch
):
    with _catalog_case(tmp_path, monkeypatch, "artifact-tree") as (case, claim, catalog):
        entered, release = threading.Event(), threading.Event()
        actual = catalog._advance

        def held(session, upload):
            entered.set()
            assert release.wait(10)
            return actual(session, upload)

        catalog._advance = held
        owner = _pipeline(case, lambda: catalog, _forbidden)
        try:
            with ThreadPoolExecutor(max_workers=1) as pool:
                active = pool.submit(owner.advance)
                assert entered.wait(5) and owner.has_pending
                assert owner.drain_terminal("not-selected") == "busy"
                release.set()
                assert active.result(timeout=5) is True
        finally:
            release.set()
        assert not owner.has_pending


def test_registry_retries_original_pipeline_feedback_without_advancing_a_new_claim(
    tmp_path, monkeypatch
):
    with _catalog_case(tmp_path, monkeypatch, "artifact-tree") as (case, claim, catalog):
        service = case.service
        owner = _pipeline(case, lambda: catalog, _forbidden)
        received = []

        def receive(progressed):
            service._requeue_finalization_step(claim)
            received.append(progressed)
            if len(received) == 1:
                raise OSError("synthetic pipeline feedback acknowledgement")

        def completed():
            (result,) = _receive(service._archive_finalizers, service._archive_execution)
            return result.result()

        assert (
            service._archive_finalizer_work._submit(
                case.collection_id,
                "pipeline",
                lambda: owner,
                lambda current: current.advance(),
                receive,
            )
            == "submitted"
        )
        with pytest.raises(OSError, match="feedback acknowledgement"):
            completed()
        context = service._archive_finalizers._contexts[case.collection_id]
        retained = context.owner
        assert retained._original is owner and retained.has_pending and not owner.has_pending
        cursor = _cursor(case, catalog)
        replacement = service._claim_due_finalization()
        assert replacement is not None and replacement.claim_id != claim.claim_id
        before = _claim_snapshot(case)
        assert (
            service._archive_finalizer_work._submit(
                case.collection_id,
                "new-selection",
                _forbidden,
                _forbidden,
                _forbidden,
            )
            == "submitted"
        )
        turn = completed()
        assert turn.selection == "pipeline" and turn.progressed and received == [True, True]
        assert context.owner is retained and not retained.has_pending
        assert _cursor(case, catalog) == cursor and _claim_snapshot(case) == before
        assert service._archive_finalizers.retire_idle(
            context.key, target_incarnation=context.target
        )
