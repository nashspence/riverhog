from __future__ import annotations

import os
import threading
from datetime import timedelta

import pytest
from riverhog_core.services import physical_execution as physical

from tests.support.journal_retirement import cleanups as cleanups
from tests.support.journal_retirement_async import Observation
from tests.support.retained_resource import fault_native as fault_native
from tests.unit.test_archive_upload_execution import _joined
from tests.unit.test_archive_upload_execution import owned as owned
from tests.unit.test_journal_retirement_registry import native_case
from tests.unit.test_journal_validation_retirement import ONE, TWO, selected

pytestmark = pytest.mark.skipif(os.name != "posix", reason="Native POSIX retirement filesystem")
RETRY = timedelta(microseconds=1)
THREE = "c" * 32


def test_one_then_three_events_preserve_worker_and_charge_identity(tmp_path, cleanups):
    with native_case(tmp_path) as case:
        original = cleanups(case)
        evidence = Observation(case, original)
        with evidence.installed():
            assert evidence.schedule(limit=1, budget=3) == 1
            first = original.registry._contexts[ONE]
            evidence.finish_workers()
            assert first.actor.received
            assert original.ledger._counts[False] == 1
            assert case.files[ONE].read_bytes() == b"x" * 14
            assert evidence.schedule(limit=1, budget=3) == 3
            events = evidence.schedules[-1]["metrics"]
            assert (
                events["collected_outcomes"],
                events["withdrawn_reservations"],
                events["resumed_closes"],
                events["new_submissions"],
            ) == (1, 1, 0, 1)
            second = original.registry._contexts[ONE]
            assert second is not first and second.reservation is not first.reservation
            assert case.files[ONE].read_bytes() == b"x" * 14
            evidence.finish_workers()
            assert case.files[ONE].read_bytes() == b"x" * 11
            original.cleanup.close()
            assert evidence.schedule(limit=1, budget=3) == 2
            assert original.ledger._counts == {False: 0, True: 0}
            assert all(row["ordinary_still_retained"] for row in evidence.links)


def test_four_events_bound_is_constructively_reachable(tmp_path, cleanups, monkeypatch):
    """A prior failed original, a joined success and a new admission are distinct."""
    with native_case(tmp_path, identities=(ONE, TWO, THREE)) as case:
        original = cleanups(case)
        cleanup, registry, pool = original.cleanup, original.registry, original.pool
        initial = cleanup._advance

        def failed(*_args, **_kwargs):
            raise RuntimeError("isolated original execution refusal")

        monkeypatch.setattr(cleanup, "_advance", failed)
        assert (
            registry.submit(
                selected(case.factory, TWO),
                octet_budget=3,
                metadata_budget=1024 * 1024,
                retry_after=RETRY,
            )
            == 1
        )
        _joined(pool)
        assert registry.collect(limit=1) == 1
        failed_context = registry._contexts[TWO]
        assert failed_context.closing and failed_context.unknown is not None
        assert original.ledger._counts[False] == 1
        monkeypatch.setattr(cleanup, "_advance", initial)
        entered, release = threading.Event(), threading.Event()

        def blocked(job, *args, **kwargs):
            entered.set()
            assert release.wait(10)
            return initial(job, *args, **kwargs)

        monkeypatch.setattr(cleanup, "_advance", blocked)
        try:
            assert (
                registry.submit(
                    selected(case.factory, ONE),
                    octet_budget=3,
                    metadata_budget=1024 * 1024,
                    retry_after=RETRY,
                )
                == 1
            )
            assert entered.wait(5)
            # This ordinary poll rotates the failed context behind the live one.
            assert registry.collect(limit=1) == 0
        finally:
            release.set()
            _joined(pool)
        monkeypatch.setattr(cleanup, "_advance", initial)
        phases = {}
        actual_collect, actual_resume = registry.collect, registry.resume_ready

        def collect(**kwargs):
            phases["collect"] = actual_collect(**kwargs)
            return phases["collect"]

        def resume(**kwargs):
            phases["resume"] = actual_resume(**kwargs)
            return phases["resume"]

        monkeypatch.setattr(registry, "collect", collect)
        monkeypatch.setattr(registry, "resume_ready", resume)
        assert cleanup.advance(limit=1, octet_budget=3) == 4
        assert phases == {"collect": 2, "resume": 1}
        _joined(pool)
        cleanup.close()
        pool.close()
        assert cleanup.receive_stopped()
        assert original.ledger._counts == {False: 0, True: 0}
        # The failed original did not become a new file quantum during close.
        assert case.files[TWO].read_bytes() == b"x" * 17


def test_three_retained_candidates_bound_is_measured_without_inline_work(tmp_path, cleanups):
    with native_case(tmp_path, identities=(ONE, TWO, THREE)) as case:
        original = cleanups(case, terminal_workers=3)
        observed = Observation(case, original)
        with observed.installed():
            assert observed.schedule(limit=3, budget=3) == 3
            assert all(path.read_bytes() == b"x" * 17 for path in case.files.values())
            assert observed.schedule(limit=1, budget=3) == 0
            metrics = observed.schedules[-1]["metrics"]
            assert metrics["candidate_iterations"] == 3
            assert metrics["exclusion_references"] == 3
            assert metrics["inline_worker_calls"] == metrics["inline_file_calls"] == 0
            observed.finish_workers()
            original.cleanup.close()
            assert observed.schedule(limit=3, budget=3) == 6
            assert all(path.read_bytes() == b"x" * 14 for path in case.files.values())
            assert not original.registry._contexts


def test_done_result_does_not_charge_worker_effects_to_later_schedule(
    tmp_path, cleanups, monkeypatch
):
    with native_case(tmp_path) as case:
        original = cleanups(case)
        release, returned = threading.Event(), threading.Event()
        actual_thread = physical.Thread

        class LateExit(actual_thread):
            def run(self):
                super().run()
                returned.set()
                assert release.wait(10)

        monkeypatch.setattr(physical, "Thread", LateExit)
        try:
            assert original.cleanup.advance(limit=1, octet_budget=3) == 1
            assert returned.wait(5)
            context = original.registry._contexts[ONE]
            assert context.actor.received
            slot = original.pool._slots[context.reservation.key]
            assert slot.future.done() and slot.worker.is_alive()
            assert original.cleanup.advance(limit=1, octet_budget=3) == 0
            assert original.ledger._counts[False] == 1
            assert case.files[ONE].read_bytes() == b"x" * 14
        finally:
            release.set()
            _joined(original.pool)
        original.cleanup.close()
        assert original.cleanup.advance(limit=1, octet_budget=3) == 2
        assert original.ledger._counts[False] == 0


@pytest.mark.parametrize("fault", ["unknown_close_open", "unknown_close_closed"])
def test_unknown_original_stays_charged_while_independent_work_receives(
    tmp_path, fault_native, fault
):
    import json
    import subprocess
    import sys

    _, extension = fault_native
    code = (
        "import sys,json;sys.path[:]=json.loads(sys.argv[1]);"
        "from tests.support.journal_retirement_unknown import run;"
        "run(sys.argv[2],sys.argv[3],sys.argv[4])"
    )
    result = subprocess.run(
        [sys.executable, "-c", code, json.dumps(sys.path), str(extension), str(tmp_path), fault],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    receipt = json.loads(result.stdout)
    assert receipt["ordinary_charge"] == 1 and receipt["physical_workers"] == 0
    assert receipt["independent_received"] and receipt["original_retained"]
    assert receipt["repeat_close_dispatches"] == 0 and not receipt["forced_receiving"]


def test_original_close_and_exception_receiving_have_separate_samples(
    tmp_path, cleanups, monkeypatch, record_property
):
    with native_case(tmp_path) as case:
        original = cleanups(case)
        cleanup, registry = original.cleanup, original.registry
        initial = cleanup._advance

        def refuse(*_args, **_kwargs):
            raise RuntimeError("retained original execution failure")

        monkeypatch.setattr(cleanup, "_advance", refuse)
        assert (
            registry.submit(
                selected(case.factory, ONE),
                octet_budget=3,
                metadata_budget=1024 * 1024,
                retry_after=RETRY,
            )
            == 1
        )
        _joined(original.pool)
        assert registry.collect(limit=1) == 1
        context = registry._contexts[ONE]
        actor = context.actor
        assert context.unknown is not None and not actor.received
        monkeypatch.setattr(cleanup, "_advance", initial)
        observed = Observation(case, original)
        with observed.installed():
            assert observed.schedule(limit=1, budget=3) == 1
            assert observed.schedules[-1]["metrics"]["resumed_closes"] == 1
            observed.finish_workers()
            assert context.actor is actor and actor.received
            assert original.ledger._counts[False] == 1
            cleanup.close()
            assert observed.schedule(limit=1, budget=3) == 2
            received = observed.schedules[-1]["delegates"]["exception_receive"]
            assert len(received) == 2
            assert sum(row["dimensions"]["exception_nodes"] for row in received) >= 1
            assert observed.workers[0]["delegates"]["turn"] == []
            assert len(observed.workers[0]["delegates"]["close"]) == 1
            assert context.unknown is None and original.ledger._counts[False] == 0
            assert case.files[ONE].read_bytes() == b"x" * 17
        record_property("growth_samples", observed.schedules + observed.workers)
        record_property("original_invocations", observed.links)
