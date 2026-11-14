from __future__ import annotations

import os
from contextlib import ExitStack

import pytest
from riverhog_core.journal_validation_workspace import LostJournalValidationWorkspace
from riverhog_provenance import JournalValidationCapacity

from tests.support.async_retirement_growth import REGISTRY, SCHEDULE, PhaseEvidence
from tests.support.journal_retirement import cleanups as cleanups
from tests.unit import test_upload_journal_validation_retirement_catalog_growth as catalog
from tests.unit import test_upload_journal_validation_retirement_growth as units
from tests.unit.test_archive_upload_execution import owned as owned
from tests.unit.test_journal_validation_retirement_custody_growth import held
from tests.unit.test_journal_validation_workspace_growth import private_file, setup

pytestmark = pytest.mark.skipif(os.name != "posix", reason="POSIX Native retirement composition")
ROOT = SCHEDULE
CAPACITY = "retired journal reclamation lacks capacity"
UNKNOWN = "unclassified files prevent directory retirement"
LOST = "retired workspace custody is unavailable"


def contexts_for(original, evidence):
    return [context for context, _, _ in evidence.gates]


def measure(case, current, *, limit, budget, cold, evidence):
    before = len(evidence.roots)
    result = evidence.wrapper(SCHEDULE)(current, limit=limit, octet_budget=budget)
    return result, next(row for op, row, _ in evidence.roots[before:] if op is SCHEDULE)


def loop_samples(root, samples, cleanups, *, large=True):
    worker_samples = []
    transcripts = []
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
                    original = cleanups(case, terminal_workers=3 if kind == "three-busy" else 2)
                    current = original.cleanup
                    with ExitStack() as resources:
                        if kind in {"busy-writer", "busy-reader", "busy-active", "three-busy"}:
                            for row in rows if kind == "three-busy" else rows[:1]:
                                resources.enter_context(
                                    held(
                                        layout.root / row["workspace_id"] / ".lease",
                                        shared=kind != "busy-writer",
                                    )
                                )
                        evidence = PhaseEvidence(
                            case,
                            original,
                            cold=cold,
                            error=expected_error,
                            text=failure_text,
                            fault=fault,
                        )
                        with evidence.installed():
                            result = current.advance(limit=limit, octet_budget=budget)
                            schedule_sample = next(
                                row for op, row, _ in evidence.roots if op is SCHEDULE
                            )
                            # Workers are held at an event before entering the
                            # original callback. No submitted file work is inline.
                            if kind in {"capacity", "repeated-blocker"}:
                                assert (directory / units.DATA[-1]).stat().st_size == 2 * budget + 3
                            evidence.drain()
                            # The observation of a positive actor receipt does
                            # not itself withdraw ordinary retention.
                            assert all(
                                context.reservation is not None
                                for context in original.registry._contexts.values()
                            )
                            # Receiving is another scheduling turn. Do not admit
                            # further work while this witness checks one wave.
                            current.close()
                            later = current.advance(limit=max(1, limit), octet_budget=budget)
                            assert later <= 2 * max(1, limit)
                        assert result <= 4 * max(0, limit)

                        actual_workers = [
                            row for op, row, _ in evidence.roots if op is REGISTRY["_run"]
                        ]
                        transcripts.append(
                            {
                                "case": index,
                                "cold": cold,
                                "kind": kind,
                                "limit": limit,
                                "budget": budget,
                                **evidence.transcript(),
                            }
                        )
                        if limit == 0 or kind in {"empty", "linked"}:
                            assert result == 0 and not actual_workers
                        else:
                            # Busy filesystem custody is observed by workers,
                            # not guessed by scheduling from their submission.
                            assert result >= 1 and actual_workers
                        if kind == "three-busy" and limit:
                            assert schedule_sample["metrics"]["candidate_iterations"] <= 3 * limit
                            assert all(
                                row["metrics"]["work_or_close_entries"] == 1
                                for row in actual_workers
                            )
                        samples.extend(row for op, row, _ in evidence.roots if op is SCHEDULE)
                        worker_samples.extend(
                            row for op, row, _ in evidence.roots if op is REGISTRY["_run"]
                        )
                        progress_values = [
                            context.progress for context in contexts_for(original, evidence)
                        ]
                        if kind in {"busy-writer", "busy-reader", "three-busy", "repeated-blocker"}:
                            assert all(value is False for value in progress_values)
                        if kind in {"capacity", "unclassified", "missing-coordinator"} and limit:
                            assert any(value is True for value in progress_values)
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
                    current.close()
                    original.pool.close()
                    assert current.receive_stopped()
                    assert original.ledger._counts == {False: 0, True: 0}
                    assert case.engine.pool.checkedout() == 0

    return worker_samples, transcripts


@pytest.mark.growth_witness(
    "riverhog-validation-retirement-loop-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_journal_validation_retirement.UploadJournalValidationRetirement.advance",
)
def test_actual_retirement_loop_bounds_candidates_and_preserves_real_blockers(
    tmp_path, record_property, cleanups
):
    samples = []
    workers, transcripts = loop_samples(tmp_path, samples, cleanups)
    record_property("growth_samples", samples)
    record_property("separate_original_worker_samples", workers)
    record_property("retirement_transcripts", transcripts)
    assert workers
