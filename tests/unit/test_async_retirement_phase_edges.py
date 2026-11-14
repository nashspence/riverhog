"""Decisive maintained-owner asynchronous boundaries; never a model executor."""

from __future__ import annotations

import os
import threading
from datetime import timedelta

import pytest
from riverhog_core.services import physical_execution as physical
from riverhog_core.services import workspace_retirement_registry as shared

from tests.support.async_retirement_growth import REGISTRY, SCHEDULE, PhaseEvidence
from tests.support.journal_retirement import cleanups as cleanups
from tests.unit.test_archive_upload_execution import _joined
from tests.unit.test_archive_upload_execution import owned as owned
from tests.unit.test_journal_retirement_registry import native_case
from tests.unit.test_journal_validation_retirement import ONE, TWO, selected

pytestmark = pytest.mark.skipif(os.name != "posix", reason="actual POSIX retirement receiving")


def test_completed_original_counts_receipt_withdrawal_and_next_submission(
    tmp_path, cleanups, record_property
):
    with native_case(tmp_path) as case:
        f = cleanups(case)
        with PhaseEvidence(case, f).installed() as e:
            assert f.cleanup.advance(limit=1, octet_budget=3) == 1
            original = f.registry._contexts[ONE]
            e.drain()
            assert original.actor.received and f.ledger._counts[False] == 1
            assert f.cleanup.advance(limit=1, octet_budget=3) == 3
            replacement = f.registry._contexts[ONE]
            assert replacement is not original
            assert replacement.reservation is not original.reservation
            assert case.files[ONE].read_bytes() == b"x" * 14
            e.drain()
            f.cleanup.close()
            assert f.cleanup.advance(limit=1, octet_budget=3) == 2
        assert f.ledger._counts == {False: 0, True: 0}
        assert case.files[ONE].read_bytes() == b"x" * 11
        assert [s["metrics"]["bookkeeping_events"] for op, s, _ in e.roots if op is SCHEDULE] == [
            1,
            3,
            2,
        ]
        assert len(e.links) == 2
        from tests.support.retirement_transcripts import negative_controls, verify

        rows = e.transcript_rows()
        assert verify(rows)["observed_attempts"] == 2
        assert set(negative_controls(rows)) == {
            "missing-original-join",
            "duplicate-worker-root",
            "cross-original-join",
            "false-ordinary-retention",
            "missing-worker-sample",
        }
        e.publish(record_property)


def test_phase_envelope_reaches_four_without_counting_file_progress(
    tmp_path, cleanups, monkeypatch, record_property
):
    with native_case(tmp_path, identities=(ONE, TWO)) as case:
        f = cleanups(case)
        clock = [1.0]
        monkeypatch.setattr(shared, "monotonic", lambda: clock[0])
        create = f.registry._create
        refusing = [True]

        def created(job):
            actor = create(job)
            if job.workspace_id == TWO:
                receive = actor._files.close

                def close():
                    if refusing[0]:
                        raise RuntimeError("known original close refusal")
                    return receive()

                actor._files.close = close
            return actor

        monkeypatch.setattr(f.registry, "_create", created)
        assert (
            f.registry.submit(
                selected(case.factory, TWO),
                octet_budget=3,
                metadata_budget=1024 * 1024,
                retry_after=timedelta(seconds=1),
            )
            == 1
        )
        _joined(f.pool)
        assert f.registry.collect(limit=1) == 1
        unresolved = f.registry._contexts[TWO]
        assert unresolved.closing and f.ledger._counts[False] == 1
        monkeypatch.delattr(f.registry, "_create")
        assert (
            f.registry.submit(
                selected(case.factory, ONE),
                octet_budget=3,
                metadata_budget=1024 * 1024,
                retry_after=timedelta(seconds=1),
            )
            == 1
        )
        _joined(f.pool)
        # Legitimate non-ready rotation makes the already-received unknown
        # follow the completion which can now retire. No fabricated receipt.
        assert f.registry.resume_ready(limit=1, retry_after=timedelta(seconds=1)) == 0
        assert list(f.registry._contexts) == [ONE, TWO]
        clock[0] = 3.0
        refusing[0] = False
        with PhaseEvidence(case, f).installed() as e:
            assert f.cleanup.advance(limit=1, octet_budget=3) == 4
            assert f.registry._contexts[TWO] is unresolved
            e.drain()
            f.cleanup.close()
            assert f.cleanup.advance(limit=2, octet_budget=3) == 4
        assert f.ledger._counts == {False: 0, True: 0}
        close_roots = [
            s for op, s, _ in e.roots if op is REGISTRY["_run"] and s["delegates"]["close"]
        ]
        assert len(close_roots) == 1 and not close_roots[0]["delegates"]["work"]
        assert close_roots[0]["delegates"]["frames"]
        e.publish(record_property)


def test_frame_receiving_runs_on_original_close_worker_not_coordinator(
    tmp_path, cleanups, monkeypatch, owned
):
    with native_case(tmp_path, identities=(ONE, TWO)) as case:
        f = cleanups(case)
        actual = f.cleanup._advance
        refusing = [True]

        def fault(job, budget, *, turn):
            if job.workspace_id == ONE and refusing[0]:
                raise RuntimeError("original failed unit")
            return actual(job, budget, turn=turn)

        monkeypatch.setattr(f.cleanup, "_advance", fault)
        assert f.cleanup.advance(limit=1, octet_budget=3) == 1
        context = f.registry._contexts[ONE]
        _joined(f.pool)
        assert f.registry.collect(limit=1) == 1
        release = owned.event()
        entered = threading.Event()
        receiving = []
        frames = physical._PhysicalExecutionResult.release_exception_frames

        def held(outcome):
            receiving.append(threading.get_ident())
            entered.set()
            assert release.wait(10)
            return frames(outcome)

        monkeypatch.setattr(physical._PhysicalExecutionResult, "release_exception_frames", held)
        context.resume_after = 0
        refusing[0] = False
        try:
            assert f.registry.resume_ready(limit=1, retry_after=timedelta(microseconds=1)) == 1
            assert entered.wait(5)
            assert receiving == [f.pool._slots[context.reservation.key].worker.ident]
            assert f.registry.collect(limit=1) == 0 and f.ledger._counts[False] == 1
            assert f.cleanup.advance(limit=1, octet_budget=3) == 1
            second = f.registry._contexts[TWO]
            second_worker = f.pool._slots[second.reservation.key].worker
            second_worker.join(timeout=10)
            assert not second_worker.is_alive()
            assert f.registry.collect(limit=2) == 2
            assert not f.registry.retained(TWO) and f.registry.retained(ONE)
        finally:
            release.set()
            _joined(f.pool)
        assert f.registry.collect(limit=2) == 2
        assert f.ledger._counts == {False: 0, True: 0}


def test_invalid_controls_do_not_touch_registry_or_execute(tmp_path, cleanups, monkeypatch):
    with native_case(tmp_path) as case:
        f = cleanups(case)
        monkeypatch.setattr(
            f.registry, "collect", lambda **kw: pytest.fail("invalid coordinator touched receiving")
        )
        for limit, budget in ((True, 1), (1, False), (1, 0), (1, -1)):
            with pytest.raises(ValueError):
                f.cleanup.advance(limit=limit, octet_budget=budget)
        assert f.cleanup.advance(limit=0, octet_budget=1) == 0
        assert case.files[ONE].read_bytes() == b"x" * 17
        # Restore before fixture's actual shutdown receiving.
        monkeypatch.undo()


def test_repeated_close_failures_keep_original_without_coordinator_frame_release(
    tmp_path, cleanups, monkeypatch, record_property
):
    with native_case(tmp_path) as case:
        f = cleanups(case)
        create = f.registry._create
        refusals = [3]

        def created(job):
            actor = create(job)
            actual = actor._files.close

            def receive():
                if refusals[0]:
                    refusals[0] -= 1
                    raise RuntimeError("original receiving refused")
                return actual()

            actor._files.close = receive
            return actor

        monkeypatch.setattr(f.registry, "_create", created)
        assert f.cleanup.advance(octet_budget=3) == 1
        context = f.registry._contexts[ONE]
        _joined(f.pool)
        assert f.registry.collect(limit=1) == 1
        original = context.actor
        first_error = context.unknown
        monkeypatch.delattr(f.registry, "_create")
        with PhaseEvidence(case, f).installed() as e:
            for remaining in (1, 0):
                context.resume_after = 0
                assert f.registry.resume_ready(limit=1, retry_after=timedelta(microseconds=1)) == 1
                e.drain()
                assert f.registry.collect(limit=1) == 1
                assert context.actor is original and context.unknown is first_error
                assert not context.closed and f.ledger._counts[False] == 1
                assert refusals[0] == remaining
            context.resume_after = 0
            assert f.registry.resume_ready(limit=1, retry_after=timedelta(microseconds=1)) == 1
            e.drain()
            assert f.registry.collect(limit=1) == 2
        assert f.ledger._counts == {False: 0, True: 0}
        assert case.files[ONE].read_bytes() == b"x" * 14
        assert (
            sum(len(s["delegates"]["diagnostic"]) for op, s, _ in e.roots if op is REGISTRY["_run"])
            == 2
        )
        e.publish(record_property)


def test_unstarted_reservation_withdrawal_does_not_fabricate_actor_receiving(
    tmp_path, cleanups, owned
):
    with native_case(tmp_path) as case:
        f = cleanups(case, terminal_workers=1)
        release = owned.event()
        entered = threading.Event()

        def occupied():
            entered.set()
            assert release.wait(10)
            return True

        key = physical.PhysicalWorkspaceExecutionKey(TWO, "b" * 64)
        other = object()
        assert (
            f.pool.try_submit_workspace(
                key,
                target_incarnation="other-original",
                owner=other,
                identity=other,
                work=occupied,
            )
            == "submitted"
        )
        assert entered.wait(5)
        try:
            with PhaseEvidence(case, f).installed() as evidence:
                assert f.cleanup.advance(limit=1, octet_budget=3) == 0
                context = f.registry._contexts[ONE]
                assert context.received and context.closed
                assert not context.entered and context.actor is None
                assert f.ledger._counts[False] == 1
                f.cleanup.close()
                assert f.cleanup.advance(limit=1, octet_budget=3) == 1
                assert f.ledger._counts[False] == 0
                assert not evidence.links
                # The unrelated occupied worker is not this registry's original.
                release.set()
                _joined(f.pool)
        finally:
            release.set()
            _joined(f.pool)
        assert case.files[ONE].read_bytes() == b"x" * 17


@pytest.mark.parametrize("operation", ["filesystem", "sql"])
def test_coordination_guard_rejects_unscoped_work(tmp_path, cleanups, monkeypatch, operation):
    from sqlalchemy import text

    with native_case(tmp_path) as case:
        f = cleanups(case)

        def retained(_identity):
            if operation == "filesystem":
                os.stat(case.root)
            else:
                with case.factory() as session:
                    session.execute(text("SELECT 1"))
            return False

        monkeypatch.setattr(f.registry, "retained", retained)
        with pytest.raises(AssertionError, match="coordination performed unscoped"):
            with PhaseEvidence(case, f).installed():
                f.cleanup.advance(limit=1, octet_budget=3)
        assert not f.registry._contexts and f.ledger._counts[False] == 0
        assert case.files[ONE].read_bytes() == b"x" * 17


def test_shutdown_frame_refusal_keeps_original_charge_until_receiving(
    tmp_path, cleanups, monkeypatch
):
    with native_case(tmp_path) as case:
        f = cleanups(case)
        actual = f.cleanup._advance

        def failed(_job, _budget, *, turn):
            raise RuntimeError("original retirement execution failed")

        monkeypatch.setattr(f.cleanup, "_advance", failed)
        assert f.cleanup.advance(limit=1, octet_budget=3) == 1
        _joined(f.pool)
        assert f.registry.collect(limit=1) == 1
        context = f.registry._contexts[ONE]
        actor, outcome = context.actor, context.unknown
        assert actor is not None and outcome is not None and not actor.received
        monkeypatch.setattr(f.cleanup, "_advance", actual)
        f.cleanup.close()
        f.pool.close()

        def refused(original):
            assert original is outcome
            raise RuntimeError("original diagnostic receiving refused")

        try:
            with monkeypatch.context() as fault:
                fault.setattr(
                    physical._PhysicalExecutionResult, "release_exception_frames", refused
                )
                with pytest.raises(RuntimeError, match="diagnostic receiving refused"):
                    f.cleanup.receive_stopped()
                assert actor.received and context.unknown is outcome
                assert not context.closed
                assert f.registry.collect(limit=1) == 0
                assert f.registry._contexts[ONE] is context
                assert f.ledger._counts[False] == 1
        finally:
            assert f.cleanup.receive_stopped()
        assert f.ledger._counts == {False: 0, True: 0}
        assert case.files[ONE].read_bytes() == b"x" * 17
