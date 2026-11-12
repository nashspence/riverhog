from __future__ import annotations

import os
from contextlib import ExitStack

import pytest
from riverhog_core.journal_validation_workspace import LostJournalValidationWorkspace
from riverhog_provenance import JournalValidationCapacity

from tests.support.journal_retirement import cleanups as cleanups
from tests.support.journal_retirement_async import Observation
from tests.unit import test_upload_journal_validation_retirement_catalog_growth as catalog
from tests.unit import test_upload_journal_validation_retirement_growth as units
from tests.unit.test_archive_upload_execution import owned as owned
from tests.unit.test_journal_validation_retirement_custody_growth import held
from tests.unit.test_journal_validation_workspace_growth import private_file, setup

pytestmark = pytest.mark.skipif(os.name != "posix", reason="Native POSIX retirement filesystem")
CAPACITY = "retired journal reclamation lacks capacity"
UNKNOWN = "unclassified files prevent directory retirement"
LOST = "retired workspace custody is unavailable"


def loop_samples(root, schedules, workers, links, cleanups, *, large=True):
    with catalog.build_cases(root / "catalogs", large=large) as all_cases:
        cases = [
            case for index, case in enumerate(all_cases) if not large or index in {0, 1, 2, 3, 6, 7}
        ]
        for index, case in enumerate(cases):
            case.base = [row for row in case.base if row["collection_id"] is not None]
            for cold in (False, True):
                scenarios = [
                    (kind, limit, 7)
                    for kind in (
                        "empty",
                        "linked",
                        "active",
                        "files",
                        "busy-writer",
                        "busy-reader",
                        "busy-active",
                        "three-busy",
                        "capacity",
                        "repeated-blocker",
                        "unclassified",
                        "missing-coordinator",
                    )
                    for limit in ((0, 1, 4) if index == 0 else (4,))
                ]
                scenarios.extend((kind, 4, 65536) for kind in ("files", "capacity"))
                for kind, limit, budget in scenarios:
                    state = (
                        "directory"
                        if kind == "unclassified"
                        else "active"
                        if kind in {"active", "linked"}
                        else "retiring"
                    )
                    selected = {**case.selected, "state": state}
                    if kind == "linked":
                        selected.update(collection_id=1, journal_id=catalog.JOURNAL)
                    if kind == "repeated-blocker":
                        selected["failure"] = CAPACITY
                    rows = [] if kind == "empty" else [selected]
                    if kind in {"busy-active", "three-busy"}:
                        rows.append(
                            {
                                **case.selected,
                                "workspace_id": catalog.TWO,
                                "state": "active" if kind == "busy-active" else "retiring",
                            }
                        )
                    if kind == "three-busy":
                        rows.append(
                            {**case.selected, "workspace_id": catalog.THREE, "state": "retiring"}
                        )
                    base, case.base = (
                        case.base,
                        [
                            row
                            for row in case.base
                            if kind != "linked" or row["journal_id"] != catalog.JOURNAL
                        ],
                    )
                    catalog.reset(case, *rows)
                    case.base = base
                    layout = setup(
                        root / "files" / f"{index}-{cold}-{kind}-{limit}-{budget}",
                        selected=kind != "empty",
                        coordinator=kind != "missing-coordinator",
                        depth=16 if index == 2 else 1,
                    )
                    for row in rows[1:]:
                        directory = layout.root / row["workspace_id"]
                        directory.mkdir(mode=0o700)
                        private_file(directory / ".lease")
                    directory = layout.root / catalog.ONE
                    if kind == "files":
                        for name, size in zip(units.DATA, (0, 17, 65537, 131073), strict=True):
                            private_file(directory / name, b"p" * size)
                    if kind in {"capacity", "repeated-blocker"}:
                        private_file(directory / units.DATA[-1], b"p" * (2 * budget + 3))
                    if kind == "unclassified":
                        private_file(directory / "unclassified", b"preserve unknown bytes")
                    failure_text = (
                        CAPACITY
                        if kind in {"capacity", "repeated-blocker"}
                        else UNKNOWN
                        if kind == "unclassified"
                        else LOST
                        if kind == "missing-coordinator"
                        else None
                    )
                    expected_error = (
                        JournalValidationCapacity
                        if kind in {"capacity", "repeated-blocker"}
                        else LostJournalValidationWorkspace
                        if failure_text is not None
                        else None
                    )
                    fault = (
                        ("reclaim", "fsync", 1)
                        if kind in {"capacity", "repeated-blocker"}
                        else None
                    )
                    case.root = layout.root
                    original = cleanups(case)
                    with ExitStack() as resources:
                        if kind in {"busy-writer", "busy-reader", "busy-active", "three-busy"}:
                            for row in rows if kind == "three-busy" else rows[:1]:
                                resources.enter_context(
                                    held(
                                        layout.root / row["workspace_id"] / ".lease",
                                        shared=kind != "busy-writer",
                                    )
                                )
                        observed = Observation(
                            case, original, cold=cold, expected_error=expected_error, fault=fault
                        )
                        with observed.installed():
                            result = observed.schedule(limit=limit, budget=budget)
                            # Busy ownership is discovered by the worker, not by admission.
                            if limit == 0 or kind in {"empty", "linked"}:
                                assert result == 0
                            else:
                                assert 1 <= result <= min(limit, len(rows), 2)
                            observed.finish_workers()
                            retained_count = len(original.registry._contexts)
                            for context in original.registry._contexts.values():
                                if context.actor is None:
                                    assert (
                                        not context.entered and context.received and context.closed
                                    )
                                else:
                                    assert context.actor.received
                                assert id(context.reservation) in original.ledger._reservations
                            # Stop new admissions to measure collection independently.
                            original.cleanup.close()
                            received = observed.schedule(limit=max(1, limit), budget=budget)
                            assert received == result + retained_count
                            assert not original.registry._contexts
                            assert original.ledger._counts == {False: 0, True: 0}
                        schedules.extend(observed.schedules)
                        workers.extend(observed.workers)
                        links.extend(observed.links)
                    if kind == "repeated-blocker":
                        assert catalog.read(case).version == selected["version"]
                    if kind == "unclassified":
                        assert (
                            directory / "unclassified"
                        ).read_bytes() == b"preserve unknown bytes"
                    if kind in {"capacity", "repeated-blocker"}:
                        removed = 0 if limit == 0 else budget
                        assert (directory / units.DATA[-1]).read_bytes() == b"p" * (
                            2 * budget + 3 - removed
                        )
                    if result and kind in {
                        "busy-writer",
                        "busy-reader",
                        "three-busy",
                        "repeated-blocker",
                    }:
                        assert all(
                            row["delegates"]["turn"][0]["metrics"]["progress_returns"] == 0
                            for row in observed.workers
                        )
                    assert case.engine.pool.checkedout() == 0


@pytest.fixture(scope="module")
def async_evidence(tmp_path_factory):
    # The real owner helpers retain and join all originals. Sharing these exact
    # observations between two declaration roots does not execute work twice.
    from types import SimpleNamespace

    from tests.support.journal_retirement import cleanups as cleanups_fixture
    from tests.unit.test_archive_upload_execution import owned as owned_fixture

    physical = owned_fixture.__wrapped__()
    owned_value = next(physical)
    cleanup_generator = cleanups_fixture.__wrapped__(owned_value)
    cleanup_value = next(cleanup_generator)
    schedules, workers, links = [], [], []
    try:
        loop_samples(
            tmp_path_factory.mktemp("async-retirement-matrix"),
            schedules,
            workers,
            links,
            cleanup_value,
        )
        yield SimpleNamespace(schedules=schedules, workers=workers, links=links)
    finally:
        try:
            next(cleanup_generator)
        except StopIteration:
            pass
        try:
            next(physical)
        except StopIteration:
            pass


@pytest.mark.growth_witness(
    "riverhog-validation-retirement-loop-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_journal_validation_retirement.UploadJournalValidationRetirement.advance",
)
def test_actual_retirement_loop_bounds_candidates_and_preserves_real_blockers(
    async_evidence, record_property
):
    record_property("growth_samples", async_evidence.schedules)
    record_property("original_invocations", async_evidence.links)
    assert async_evidence.schedules and async_evidence.workers


@pytest.mark.growth_witness(
    "riverhog-validation-retirement-original-workers/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.journal_retirement_registry.JournalRetirementRegistry._run",
)
def test_actual_original_workers_preserve_separate_growth_scope(async_evidence, record_property):
    record_property("growth_samples", async_evidence.workers)
    record_property("original_invocations", async_evidence.links)
    assert all(
        row["ordinary_still_retained"] and row["actor_received"] for row in async_evidence.links
    )
