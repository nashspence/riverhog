from __future__ import annotations

import hashlib
from contextlib import contextmanager

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core import canonical_discovery_archive_framing as framing
from riverhog_growth_contracts import (
    evaluate_bound,
    growth_binding,
    project_bindings,
    validate_execution_samples,
)

from tests.support.catalog_work import octets
from tests.unit.test_canonical_discovery_archive_framing import (
    apply,
    begin,
    framing_case,
    prepare,
    progress,
)
from tests.unit.test_canonical_discovery_selection import _observe

_OPERATIONS = {
    "begin": framing.begin_archive_framing,
    "progress": framing.archive_framing_progress,
    "prepare": framing.prepare_archive_framing,
    "apply": framing.apply_archive_framing,
    "position": framing.archive_journal_frame,
    "page": framing.archive_framing_page,
}


@contextmanager
def observe(case, work, monkeypatch):
    db = case.metadata._db
    with _observe(db, monkeypatch, work.source.reader) as recorder:
        with monkeypatch.context() as patch:
            children = [
                (work.source, "verify_current", "source_current_calls"),
                (work.source, "read_window", "source_window_calls"),
                (framing, "advance_journal_framing", "native_framing_calls"),
            ]
            for owner, name, metric in children:
                actual = getattr(owner, name)

                def call(*args, _actual=actual, _metric=metric, **kwargs):
                    # Observe the actual invocation; the child's own maintained
                    # witness measures its body, rather than charging it twice.
                    before = dict(recorder.metrics)
                    try:
                        return _actual(*args, **kwargs)
                    finally:
                        recorder.metrics.update(before)
                        recorder.metrics[_metric] = before.get(_metric, 0) + 1

                patch.setattr(owner, name, call)
            for name, metric in (
                ("format_scalar", "scalar_encoded_octets"),
                ("parse_scalar", "scalar_decoded_octets"),
            ):
                actual = getattr(framing, name)

                def scalar(domain, value, _actual=actual, _metric=metric):
                    result = _actual(domain, value)
                    recorder.metrics[_metric] = recorder.metrics.get(_metric, 0) + len(
                        str(result if _metric == "scalar_encoded_octets" else value).encode()
                    )
                    return result

                patch.setattr(framing, name, scalar)
            actual_capture = framing._capture_bytes

            def capture(tail, content, consumed):
                result = actual_capture(tail, content, consumed)
                recorder.metrics["copied_octets"] = (
                    recorder.metrics.get("copied_octets", 0) + consumed + len(result)
                )
                return result

            patch.setattr(framing, "_capture_bytes", capture)
            yield recorder
    for sql, parameters in recorder.statements:
        if sql.lstrip().upper().startswith("SELECT"):
            plan = " ".join(
                str(row[3]).upper() for row in db.execute("EXPLAIN QUERY PLAN " + sql, parameters)
            )
            assert "SCAN " not in plan and "TEMP B-TREE" not in plan, plan


def dimensions(case, work, position_budget):
    db = case.metadata._db
    retained, width = 0, octets((case.metadata.key, work.source.identity, position_budget))
    for (table,) in db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'discovery_%'"
    ).fetchall():
        for row in db.execute(f"SELECT * FROM {table}"):
            retained += 1
            width = max(
                width, octets(tuple(value for value in row if not isinstance(value, bytes)))
            )
    return {
        "registered_inputs": retained,
        "metadata_octets": width,
        "position_budget": position_budget,
    }


def sample(mode, dims, metrics):
    binding = growth_binding(_OPERATIONS[mode])
    projection = project_bindings((binding,))
    result = {
        "declaration": binding.declaration.id,
        "declaration_sha256": hashlib.sha256(
            canonical_json_bytes(projection["declarations"][binding.declaration.id])
        ).hexdigest(),
        "dimensions": dims,
        "metrics": {
            bound.metric: metrics.get(bound.metric, 0) for bound in binding.declaration.bounds
        },
        "delegates": {},
    }
    try:
        validate_execution_samples(projection, binding.declaration.id, [result])
    except ValueError as error:
        raise AssertionError(
            [
                {
                    "metric": bound.metric,
                    "actual": result["metrics"][bound.metric],
                    "bound": evaluate_bound(binding, bound.metric, dims),
                }
                for bound in binding.declaration.bounds
                if result["metrics"][bound.metric] > evaluate_bound(binding, bound.metric, dims)
            ]
        ) from error
    return result


def exercise(tmp_path, monkeypatch, record_property, mode):
    samples = []
    for size, retained in ((1, 1), (17, 17), (1025, 1025), (17, 1025), (1025, 1)):
        raw = b"\x1e{}\n" * size
        with framing_case(tmp_path / f"{size}-{retained}", raw, retained=retained) as (case, work):
            begin(case, work)

            def measure(cold):
                before = progress(case, work)
                held = 0 if work._prepared is None else len(work._prepared.advance.positions)
                budget = max(held, 3 if mode == "prepare" else 1 if mode == "position" else 0)
                page = None
                if mode == "apply":
                    page = (
                        prepare(case, work, byte_budget=31, frame_budget=3)
                        if before.phase == "scanning"
                        else work._prepared
                    )
                    assert page is not None
                    budget = max(budget, len(page.advance.positions))
                dims = dimensions(case, work, budget)
                if cold:
                    case.metadata._db.execute("PRAGMA shrink_memory").close()
                transaction = mode in {"begin", "apply"}
                context = case.metadata.transaction if transaction else case.metadata.read
                with context() as (_, key):
                    with observe(case, work, monkeypatch) as recorder:
                        if mode == "begin":
                            output = framing.begin_archive_framing(recorder, key, work)
                        elif mode == "progress":
                            output = framing.archive_framing_progress(recorder, key, work)
                        elif mode == "prepare":
                            output = framing.prepare_archive_framing(
                                recorder, key, work, byte_budget=31, frame_budget=3
                            )
                        elif mode == "apply":
                            output = framing.apply_archive_framing(recorder, key, work, page)
                        elif mode == "page":
                            output = framing.archive_framing_page(
                                recorder,
                                key,
                                work,
                                max(
                                    0,
                                    (before.cursor.byte_offset - 1)
                                    // framing.ARCHIVE_CAPTURE_PAGE_OCTETS,
                                ),
                            )
                        else:
                            output = framing.archive_journal_frame(
                                recorder, key, work, max(0, before.cursor.frame_count - 1)
                            )
                    returned = (
                        len(output.advance.positions)
                        if mode == "prepare" and output is not None
                        else int(mode == "position" and output is not None)
                    )
                    recorder.metrics["returned_positions"] = returned
                    samples.append(sample(mode, dims, recorder.metrics))
                if before.phase != "complete" and mode != "apply":
                    page = (
                        output
                        if mode == "prepare"
                        else prepare(case, work, byte_budget=31, frame_budget=3)
                    )
                    assert page is not None and apply(case, work, page)

            measure(True)
            measure(False)
            # Advance the same original retained directory to a late active
            # boundary. Setup uses larger explicit quanta, without reset/replay.
            while size - progress(case, work).cursor.frame_count > 6:
                remaining = size - progress(case, work).cursor.frame_count
                page = prepare(case, work, frame_budget=min(128, remaining - 6))
                assert page is not None and apply(case, work, page)
            measure(True)
            measure(False)
            terminal = progress(case, work)
            assert terminal.phase == "complete" and terminal.cursor.frame_count == size
    if mode in {"prepare", "apply", "page"}:
        samples.extend(full_page_samples(tmp_path, monkeypatch, mode))
    record_property("growth_samples", samples)
    record_property(
        "framing_conditions",
        "Actual owned Core/custody and original authenticated archives; independent frame and "
        "unrelated-row counts 1/17/1025, fixed measured budgets, early/late retained cursors, "
        "cold/warm access, indexed SQL and complete scalar/row widths. Source and public Native "
        "bodies retain their own witnesses.",
    )


def full_page_samples(tmp_path, monkeypatch, mode):
    samples = []
    extent = framing.ARCHIVE_CAPTURE_PAGE_OCTETS
    raw = b"\x1e" + b"x" * extent + b"\n"
    for retained in (1, 1025):
        with framing_case(tmp_path / f"full-page-{retained}", raw, retained=retained) as (
            case,
            work,
        ):
            begin(case, work)
            first = prepare(case, work, byte_budget=extent - 1, frame_budget=3)
            assert apply(case, work, first)
            assert progress(case, work).cursor.byte_offset == extent - 1
            page = None
            if mode in {"apply", "page"}:
                page = prepare(case, work, byte_budget=1, frame_budget=3)
                if mode == "page":
                    assert apply(case, work, page)
            for cold in (True, False):
                dims = dimensions(case, work, 3)
                if cold:
                    case.metadata._db.execute("PRAGMA shrink_memory").close()
                context = case.metadata.transaction if mode == "apply" else case.metadata.read
                with context() as (_, key):
                    with observe(case, work, monkeypatch) as recorder:
                        if mode == "prepare":
                            result = framing.prepare_archive_framing(
                                recorder, key, work, byte_budget=1, frame_budget=3
                            )
                            assert result.captured.content == raw[:extent]
                        elif mode == "apply":
                            assert framing.apply_archive_framing(recorder, key, work, page) == cold
                        else:
                            assert (
                                framing.archive_framing_page(recorder, key, work, 0).content
                                == raw[:extent]
                            )
                    samples.append(sample(mode, dims, recorder.metrics))
    return samples


@pytest.mark.growth_witness(
    "canonical-discovery-archive-framing-begin/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_archive_framing.begin_archive_framing",
)
def test_begin_work(tmp_path, monkeypatch, record_property):
    exercise(tmp_path, monkeypatch, record_property, "begin")


@pytest.mark.growth_witness(
    "canonical-discovery-archive-framing-progress/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_archive_framing.archive_framing_progress",
)
def test_progress_work(tmp_path, monkeypatch, record_property):
    exercise(tmp_path, monkeypatch, record_property, "progress")


@pytest.mark.growth_witness(
    "canonical-discovery-archive-framing-prepare/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_archive_framing.prepare_archive_framing",
)
def test_prepare_work(tmp_path, monkeypatch, record_property):
    exercise(tmp_path, monkeypatch, record_property, "prepare")


@pytest.mark.growth_witness(
    "canonical-discovery-archive-framing-apply/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_archive_framing.apply_archive_framing",
)
def test_apply_work(tmp_path, monkeypatch, record_property):
    exercise(tmp_path, monkeypatch, record_property, "apply")


@pytest.mark.growth_witness(
    "canonical-discovery-archive-framing-position/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_archive_framing.archive_journal_frame",
)
def test_position_work(tmp_path, monkeypatch, record_property):
    exercise(tmp_path, monkeypatch, record_property, "position")


@pytest.mark.growth_witness(
    "canonical-discovery-archive-framing-page/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_archive_framing.archive_framing_page",
)
def test_page_work(tmp_path, monkeypatch, record_property):
    exercise(tmp_path, monkeypatch, record_property, "page")
