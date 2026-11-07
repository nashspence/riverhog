from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor

import pytest
from riverhog_core.catalog_db import session_scope
from riverhog_core.services import immutable_archive_admission as admission_module
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission
from riverhog_core.services.immutable_archive_receiving import OriginalImmutableArchiveReport
from riverhog_protocol.errors import Conflict

from tests.unit.test_archive_finalizer_registry import _receive
from tests.unit.test_archive_immutable_source_receiving import _original_report
from tests.unit.test_archive_volume_metadata_receiving import _count
from tests.unit.test_finalization_claim_readiness import _current_claim
from tests.unit.test_original_immutable_archive_report import _lose_reply


@pytest.mark.parametrize("phase,committed", (("receive", False), ("retire", True)))
def test_selected_terminal_drains_only_the_retained_original_report(
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
        owner = OriginalImmutableArchiveReport(case.factory, case.collection_id)
        with _lose_reply(monkeypatch, owner, phase, committed):
            with pytest.raises(OSError):
                owner.advance()
        selected = owner._selected
        assert owner.has_pending
        operation = ArchiveWriteAdmission(case.factory).select_terminal(
            case.collection_id,
            kind="cancel",
            request={"fixture": "retained-original-report"},
            principal_id="uploader",
            grants=case.service._config.archive_write,
            guard=lambda session: None,
            validate_selection=lambda *args: None,
        )

        def fresh(*args):
            pytest.fail("terminal drainage cannot select another report")

        monkeypatch.setattr(owner._admission, "load_next_collection_report", fresh)
        assert owner.drain_terminal(operation) == "closed"
        assert owner._selected is selected and not owner.has_pending and _count(case) == 0
        assert owner.drain_terminal(operation) == "closed"
        with pytest.raises(Conflict, match="stopped for its terminal"):
            owner.advance()
        with session_scope(case.factory) as session:
            source.verify_prepared_receipt(session, admitted.prepared, receipt)


def test_original_report_terminal_poll_does_not_wait_on_receiving(tmp_path, monkeypatch):
    with _original_report(tmp_path, monkeypatch, "volume", receive=False) as (
        fixture,
        source,
        admission,
        admitted,
        receipt,
    ):
        case = fixture.case
        owner = OriginalImmutableArchiveReport(case.factory, case.collection_id)
        entered, release = threading.Event(), threading.Event()
        actual = owner._admission.receive_settled

        def held(*args, **kwargs):
            entered.set()
            assert release.wait(10)
            return actual(*args, **kwargs)

        monkeypatch.setattr(owner._admission, "receive_settled", held)
        try:
            with ThreadPoolExecutor(max_workers=1) as pool:
                active = pool.submit(owner.advance)
                assert entered.wait(5) and owner.has_pending
                assert owner.drain_terminal("not-yet-selected") == "busy"
                release.set()
                assert active.result(timeout=5) is True
        finally:
            release.set()
        assert not owner.has_pending and _count(case) == 0


def test_service_registry_retains_report_before_a_different_finalizer_selection(
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
        service = case.service
        claim = _current_claim(case)
        created, received, calls = [], [], []
        actual = admission_module.ImmutableArchiveAdmission.receive_settled

        def lose_once(receiver, *args, **kwargs):
            result = actual(receiver, *args, **kwargs)
            calls.append(1)
            if len(calls) == 1:
                raise OSError("synthetic original receiver reply")
            return result

        monkeypatch.setattr(
            admission_module.ImmutableArchiveAdmission, "receive_settled", lose_once
        )

        def create():
            owner = OriginalImmutableArchiveReport(case.factory, case.collection_id)
            created.append(owner)
            return owner

        def receive(progressed):
            received.append(progressed)
            service._requeue_finalization_step(claim)

        def completed():
            (result,) = _receive(service._archive_finalizers, service._archive_execution)
            return result.result()

        assert (
            service._archive_finalizer_work._submit(
                case.collection_id,
                "original-report",
                create,
                lambda owner: owner.advance(),
                receive,
            )
            == "submitted"
        )
        with pytest.raises(OSError, match="original receiver reply"):
            completed()
        context = service._archive_finalizers._contexts[case.collection_id]
        retained = context.owner
        original = created[0]
        assert retained._original is original and retained.has_pending and original.has_pending
        assert received == [] and _count(case) == 1

        def fresh(*args):
            pytest.fail("a pending original must precede a new factory or receiver")

        assert (
            service._archive_finalizer_work._submit(
                case.collection_id, "another-kind", fresh, fresh, fresh
            )
            == "submitted"
        )
        turn = completed()
        assert turn.selection == "original-report" and turn.progressed
        assert len(created) == 1 and received == [True] and calls == [1, 1]
        assert context.owner is retained and not retained.has_pending and _count(case) == 0
        assert service._archive_finalizers.retire_idle(
            context.key, target_incarnation=context.target
        )
        with session_scope(case.factory) as session:
            source.verify_prepared_receipt(session, admitted.prepared, receipt)
