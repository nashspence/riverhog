from __future__ import annotations

import threading

import pytest
from riverhog_core.services import physical_execution as execution_module
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission
from riverhog_protocol.errors import Conflict

from tests.unit.finalization_claim_fixtures import _different_claim
from tests.unit.test_archive_finalizer_registry import (
    Finalizer,
    _key,
    _receive,
)
from tests.unit.test_archive_finalizer_registry import owned as owned
from tests.unit.test_collection_finalization_plan import _metadata_case
from tests.unit.test_collection_finalization_work import _completed
from tests.unit.test_finalization_claim_readiness import _claim_snapshot


def _forbidden(*args, **kwargs):
    pytest.fail("a fresh workflow turn cannot replace or adopt an original")


def _fresh(registry, key=None, *, target="target-a", create=Finalizer):
    return registry.submit(
        key or _key(),
        target_incarnation=target,
        selection="fresh",
        create=create,
        advance=lambda owner: owner.advance(),
        resume_original=False,
    )


def test_fresh_turn_defers_pending_original_without_losing_independent_progress(owned):
    registry, pool = owned.make()
    original = Finalizer()
    attempts = []

    def old_step(owner):
        attempts.append(owner)
        owner.pending = len(attempts) == 1
        if owner.pending:
            raise OSError("original acknowledgement lost")
        return True

    assert (
        registry.submit(
            _key(),
            target_incarnation="target-a",
            selection="original",
            create=lambda: original,
            advance=old_step,
        )
        == "submitted"
    )
    (failed,) = _receive(registry, pool)
    with pytest.raises(OSError):
        failed.result()
    assert _fresh(registry, create=_forbidden) == "original-pending"
    assert registry._contexts[1].owner is original and attempts == [original]
    assert pool.status(_key()) == "untracked"
    assert _fresh(registry, _key(2), target="target-b") == "submitted"
    (independent,) = _receive(registry, pool)
    assert independent.key == _key(2) and independent.result().selection == "fresh"
    assert registry.resume(_key(), target_incarnation="target-a") == "submitted"
    (completed,) = _receive(registry, pool)
    assert completed.result().selection == "original" and attempts == [original, original]
    replacement = Finalizer()
    assert _fresh(registry, create=lambda: replacement) == "submitted"
    (fresh,) = _receive(registry, pool)
    assert fresh.result().selection == "fresh" and registry._contexts[1].owner is replacement


def test_fresh_turn_cannot_replace_an_unknown_constructor(owned):
    registry, pool = owned.make()
    calls = []

    def construct():
        calls.append(True)
        raise OSError("constructor outcome missing")

    assert _fresh(registry, create=construct) == "submitted"
    (failed,) = _receive(registry, pool)
    with pytest.raises(OSError):
        failed.result()
    assert _fresh(registry, create=_forbidden) == "original-pending"
    assert calls == [True] and not registry.retire_idle(_key(), target_incarnation="target-a")
    assert _fresh(registry, _key(2), target="target-b") == "submitted"
    assert _receive(registry, pool)[0].result().progressed


def test_fresh_turn_waits_for_actual_join_even_after_the_result_is_published(owned, monkeypatch):
    published, release = threading.Event(), owned.event()

    class HeldExit(threading.Thread):
        def run(self):
            super().run()
            published.set()
            assert release.wait(10)

    monkeypatch.setattr(execution_module, "Thread", HeldExit)
    registry, pool = owned.make()
    assert _fresh(registry) == "submitted"
    assert published.wait(5)
    assert pool._slots[_key(1)].future.done()
    assert _fresh(registry, create=_forbidden) == "busy"
    assert registry.collect(limit=1) == ()
    release.set()
    assert _receive(registry, pool)[0].result().progressed
    assert _fresh(registry) == "submitted"
    assert _receive(registry, pool)[0].result().selection == "fresh"


def test_fresh_turn_cannot_replace_a_running_constructor(owned):
    registry, pool = owned.make()
    entered, release = threading.Event(), owned.event()

    def construct():
        entered.set()
        assert release.wait(10)
        return Finalizer()

    assert _fresh(registry, create=construct) == "submitted"
    assert entered.wait(5)
    assert registry._contexts[1].owner is None
    assert _fresh(registry, create=_forbidden) == "busy"
    assert _fresh(registry, _key(2), target="target-b") == "submitted"
    pool._slots[_key(2)].worker.join(5)
    assert not pool._slots[_key(2)].worker.is_alive()
    # Each poll inspects one slot and rotates the still-running original.
    assert registry.collect(limit=1) == ()
    (completed,) = registry.collect(limit=1)
    assert completed.key == _key(2)
    release.set()


def test_fresh_turn_defers_registry_and_original_lock_contention(owned):
    registry, pool = owned.make()
    with registry._lock:
        assert _fresh(registry, create=_forbidden) == "busy"
    assert registry._contexts == {}
    assert _fresh(registry) == "submitted"
    _receive(registry, pool)
    with registry._contexts[1].lock:
        assert _fresh(registry, create=_forbidden) == "busy"
    assert _fresh(registry) == "submitted"
    _receive(registry, pool)


def test_fresh_turn_preserves_target_and_global_retention_charges(owned):
    registry, pool = owned.make(retained_constructions=2, retained_constructions_per_target=1)
    assert _fresh(registry) == "submitted"
    _receive(registry, pool)
    assert _fresh(registry, _key(2), create=_forbidden) == "target-retention-capacity"
    assert _fresh(registry, _key(2), target="target-b") == "submitted"
    _receive(registry, pool)
    assert _fresh(registry, _key(3), target="target-c", create=_forbidden) == "retention-capacity"
    assert set(registry._contexts) == {1, 2}


@pytest.mark.parametrize("phase", ("before-fresh-check", "during-submit"))
def test_fresh_turn_does_not_adopt_a_racing_foreign_execution(owned, monkeypatch, phase):
    registry, pool = owned.make()
    actual = pool.try_submit
    raced = []

    def submit(key, **arguments):
        if not raced:
            raced.append(True)
            assert (
                actual(
                    key,
                    target_incarnation="target-a",
                    terminal=False,
                    work=lambda: "foreign",
                )
                == "submitted"
            )
        return actual(key, **arguments)

    if phase == "during-submit":
        monkeypatch.setattr(pool, "try_submit", submit)
    else:
        status = pool.status
        probes = []

        def probe(key):
            probes.append(key)
            if len(probes) == 2:
                assert (
                    actual(
                        key,
                        target_incarnation="target-a",
                        terminal=False,
                        work=lambda: "foreign",
                    )
                    == "submitted"
                )
            return status(key)

        monkeypatch.setattr(pool, "status", probe)

    assert _fresh(registry, create=_forbidden) == "busy"
    assert registry._contexts == {} and _receive(registry, pool) == ()
    (foreign,) = pool.collect(limit=1, owner=None)
    assert foreign.owner is None and foreign.result() == "foreign"


def test_fresh_work_submission_preserves_a_new_claim_while_old_feedback_resumes(
    tmp_path, monkeypatch
):
    with _metadata_case(tmp_path, monkeypatch, count=1) as (case, claim):
        service = case.service
        receive = service._requeue_finalization_step
        received = []

        def lost(selected):
            received.append(selected)
            receive(selected)
            if len(received) == 1:
                raise OSError("original workflow reply lost")

        monkeypatch.setattr(service, "_requeue_finalization_step", lost)
        assert service._submit_finalization(claim) == "submitted"
        with pytest.raises(OSError):
            _completed(case)
        context = service._archive_finalizers._contexts[case.collection_id]
        original = context.owner
        fresh_claim = service._claim_due_finalization()
        assert fresh_claim is not None and fresh_claim.claim_id != claim.claim_id
        before = _claim_snapshot(case)
        assert (
            service._archive_finalizer_work._submit(
                case.collection_id,
                "fresh-claim",
                _forbidden,
                _forbidden,
                _forbidden,
                resume_original=False,
                claim_guard=fresh_claim.guard,
            )
            == "original-pending"
        )
        assert _claim_snapshot(case) == before and context.owner is original
        assert service._archive_execution.status(context.key) == "untracked"
        assert (
            service._archive_finalizers.resume(context.key, target_incarnation=context.target)
            == "submitted"
        )
        assert _completed(case).progressed
        assert received == [claim, claim] and not original.has_pending
        assert _claim_snapshot(case) == before
        assert service._submit_finalization(fresh_claim) == "submitted"
        assert _completed(case).progressed


def test_fresh_work_rechecks_current_claim_before_local_registration(tmp_path, monkeypatch):
    from riverhog_core.services.collection_uploads import _FinalizationClaim

    with _metadata_case(tmp_path, monkeypatch, count=1) as (case, claim):
        stale = _FinalizationClaim(case.collection_id, _different_claim(claim))
        before = _claim_snapshot(case)
        with pytest.raises(Conflict, match="claim is unavailable"):
            case.service._archive_finalizer_work._submit(
                case.collection_id,
                "stale-claim",
                _forbidden,
                _forbidden,
                _forbidden,
                resume_original=False,
                claim_guard=stale.guard,
            )
        assert case.service._archive_finalizers._contexts == {}
        assert _claim_snapshot(case) == before and case.calls == []


def test_closed_gate_rejects_fresh_registration_without_resuming_original_feedback(
    tmp_path, monkeypatch
):
    with _metadata_case(tmp_path, monkeypatch, count=1) as (case, claim):
        service = case.service
        receive = service._requeue_finalization_step
        received = []

        def lost(selected):
            received.append(selected)
            receive(selected)
            if len(received) == 1:
                raise OSError("original workflow reply lost")

        monkeypatch.setattr(service, "_requeue_finalization_step", lost)
        assert service._submit_finalization(claim) == "submitted"
        with pytest.raises(OSError):
            _completed(case)
        context = service._archive_finalizers._contexts[case.collection_id]
        original = context.owner
        ArchiveWriteAdmission(case.factory).select_terminal(
            case.collection_id,
            kind="cancel",
            request={"fixture": "fresh-claim"},
            principal_id="uploader",
            key_id=None,
            grants=service._config.archive_write,
            guard=lambda session: None,
            validate_selection=lambda *args: None,
        )
        assert (
            service._archive_finalizer_work._submit(
                case.collection_id,
                "fresh-claim",
                _forbidden,
                _forbidden,
                _forbidden,
                resume_original=False,
            )
            == "closed"
        )
        assert original.has_pending and received == [claim]
        assert service._archive_execution.status(context.key) == "untracked"
        assert service._submit_finalization(claim) == "submitted"
        assert _completed(case).progressed and received == [claim, claim]
