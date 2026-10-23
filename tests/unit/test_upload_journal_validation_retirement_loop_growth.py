from __future__ import annotations

import builtins
import inspect
import io
import os
import sqlite3
from collections import defaultdict
from contextlib import ExitStack, contextmanager

import pytest
from riverhog_core.journal_validation_workspace import LostJournalValidationWorkspace
from riverhog_core.services import upload_journal_validation_retirement as owner
from riverhog_growth_contracts import growth_binding
from riverhog_provenance import JournalValidationCapacity
from sqlalchemy import event

from tests.unit import test_upload_journal_validation_retirement_catalog_growth as catalog
from tests.unit import test_upload_journal_validation_retirement_growth as units
from tests.unit.test_journal_validation_retirement_custody_growth import held
from tests.unit.test_journal_validation_workspace_growth import private_file, setup

ROOT = owner.UploadJournalValidationRetirement.advance
CHILDREN = dict(owner._retirement_loop_children())
CAPACITY = "retired journal reclamation lacks capacity"
UNKNOWN = "unclassified files prevent directory retirement"
LOST = "retired workspace custody is unavailable"


def measure(
    case, current, *, limit, budget, cold, expected_error=None, failure_text=None, fault=None
):
    children = {tag: [] for tag in CHILDREN}
    own = defaultdict(int)
    active, depth, ranges = False, 0, 0

    @contextmanager
    def child():
        nonlocal depth
        depth += 1
        try:
            yield
        finally:
            depth -= 1

    def select(selected, excluding):
        own["excluded_jobs_per_call"] = max(own["excluded_jobs_per_call"], len(excluding))
        own["exclusion_copy_references"] += len(excluding)
        with child(), pytest.MonkeyPatch.context() as patch:
            patch.setattr(type(selected), "_select", CHILDREN["select"])
            result, observed = catalog.measure(case, selected, "_select", excluding, cold=cold)
        children["select"].append(observed)
        return result

    def unit(selected, job, admission):
        own["native_admission_per_call"] = max(own["native_admission_per_call"], admission)
        errors = []
        with child():
            result, observed = units.measure(
                case,
                selected,
                job,
                admission,
                cold=cold,
                error=expected_error,
                fault=fault,
                observed_error=errors,
            )
        children["unit"].append(observed)
        if errors[0] is not None:
            raise errors[0]
        return result

    def blocked(selected, job, failure):
        with child():
            persisted = catalog.read(case, job.workspace_id)
        changed = int(
            persisted == job and persisted.collection_id is None and persisted.failure != failure
        )
        with child(), pytest.MonkeyPatch.context() as patch:
            patch.setattr(type(selected), "_blocked", CHILDREN["blocked"])
            result, observed = catalog.measure(
                case, selected, "_blocked", job, proposed=failure, cold=cold, changed=changed
            )
        children["blocked"].append(observed)
        assert result is bool(changed)
        return result

    def ranges_observed(*args):
        nonlocal ranges
        caller = inspect.currentframe().f_back
        if not active or caller.f_code is not ROOT.__code__:
            return builtins.range(*args)
        metric = "outer_iterations" if ranges == 0 else "candidate_iterations"
        ranges += 1

        def iterate():
            for value in builtins.range(*args):
                own[metric] += 1
                yield value

        return iterate()

    def format_error(value):
        assert isinstance(value, (JournalValidationCapacity, LostJournalValidationWorkspace))
        result = str(value)
        assert result == failure_text
        own["error_format_calls"] += 1
        own["error_formatted_octets"] += len(result.encode())
        return result

    def catalog_guard(*_args):
        if active and depth == 0:
            raise units.EvidenceFailure("retirement coordination issued unscoped SQL")

    event.listen(case.engine, "before_cursor_execute", catalog_guard)
    try:
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(owner.UploadJournalValidationRetirement, "_select", select)
            patch.setattr(owner.UploadJournalValidationRetirement, "_advance", unit)
            patch.setattr(owner.UploadJournalValidationRetirement, "_blocked", blocked)
            patch.setattr(owner, "range", ranges_observed, raising=False)
            patch.setattr(owner, "str", format_error, raising=False)
            for module, name in (
                *(
                    (os, name)
                    for name in (
                        "open",
                        "close",
                        "stat",
                        "fstat",
                        "fsync",
                        "unlink",
                        "rmdir",
                        "ftruncate",
                        "read",
                        "write",
                        "listdir",
                        "scandir",
                    )
                ),
                (builtins, "open"),
                (io, "open"),
                (sqlite3, "connect"),
            ):
                actual = getattr(module, name)

                def guarded(*args, _actual=actual, **kwargs):
                    if active and depth == 0:
                        raise units.EvidenceFailure(
                            "retirement coordination performed unscoped filesystem work"
                        )
                    return _actual(*args, **kwargs)

                patch.setattr(module, name, guarded)
            active = True
            try:
                result = ROOT(current, limit=limit, octet_budget=budget)
            finally:
                active = False
    finally:
        event.remove(case.engine, "before_cursor_execute", catalog_guard)
    own["progressed_units"] = result
    dimensions = {
        "unit_limit": max(0, limit),
        "octet_budget": budget,
        "error_control_octets": 0 if failure_text is None else len(failure_text.encode()),
    }
    metrics = dict(own)
    for tag, operation in CHILDREN.items():
        binding = growth_binding(operation)
        dimensions[tag + "_calls"] = len(children[tag])
        metrics[tag + "_calls"] = metrics[tag + "_calls_limit"] = len(children[tag])
        for item in binding.declaration.dimensions:
            dimensions[tag + "_" + item.name] = max(
                (row["dimensions"][item.name] for row in children[tag]), default=0
            )
        for bound in binding.declaration.bounds:
            value = sum(row["metrics"][bound.metric] for row in children[tag])
            metrics[tag + "_" + bound.metric] = value
            metrics[bound.metric] = metrics.get(bound.metric, 0) + value
    assert own["outer_iterations"] <= max(0, limit)
    assert own["candidate_iterations"] <= 3 * max(0, limit)
    return result, units.sample(ROOT, dimensions, metrics, children)


def loop_samples(root, samples, *, large=True):
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
                    current = owner.UploadJournalValidationRetirement(case.factory, layout.root)
                    with ExitStack() as resources:
                        if kind in {"busy-writer", "busy-reader", "busy-active", "three-busy"}:
                            for row in rows if kind == "three-busy" else rows[:1]:
                                resources.enter_context(
                                    held(
                                        layout.root / row["workspace_id"] / ".lease",
                                        shared=kind != "busy-writer",
                                    )
                                )
                        result, observed = measure(
                            case,
                            current,
                            limit=limit,
                            budget=budget,
                            cold=cold,
                            expected_error=expected_error,
                            failure_text=failure_text,
                            fault=fault,
                        )
                    if limit == 0 or kind in {
                        "empty",
                        "linked",
                        "busy-writer",
                        "busy-reader",
                        "three-busy",
                        "repeated-blocker",
                    }:
                        assert result == 0
                    elif kind in {"capacity", "unclassified", "missing-coordinator"}:
                        assert result == 1
                    elif kind in {"active", "busy-active"}:
                        assert result == min(limit, 3)
                    else:
                        assert result == limit
                    if kind == "three-busy" and limit:
                        assert observed["metrics"]["candidate_iterations"] == 3
                        assert observed["metrics"]["excluded_jobs_per_call"] == 2
                    if kind == "repeated-blocker":
                        assert catalog.read(case).version == selected["version"]
                    if kind == "unclassified":
                        assert (
                            directory / "unclassified"
                        ).read_bytes() == b"preserve unknown bytes"
                    if kind in {"capacity", "repeated-blocker"}:
                        removed = (
                            0
                            if limit == 0
                            else budget * (2 if kind == "capacity" and limit > 1 else 1)
                        )
                        assert (directory / units.DATA[-1]).read_bytes() == b"p" * (
                            2 * budget + 3 - removed
                        )
                    assert case.engine.pool.checkedout() == 0
                    samples.append(observed)


@pytest.mark.growth_witness(
    "riverhog-validation-retirement-loop-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.services.upload_journal_validation_retirement.UploadJournalValidationRetirement.advance",
)
def test_actual_retirement_loop_bounds_candidates_and_preserves_real_blockers(
    tmp_path, record_property
):
    samples = []
    record_property("growth_samples", samples)
    loop_samples(tmp_path, samples)
