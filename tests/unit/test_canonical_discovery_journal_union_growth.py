from __future__ import annotations

from contextlib import contextmanager
from dataclasses import fields, is_dataclass

import pytest
from riverhog_canonical_json import format_scalar
from riverhog_core import canonical_discovery_journal_epochs as epochs
from riverhog_core import canonical_discovery_journal_topology as topology
from riverhog_core import canonical_discovery_journal_union as owner
from riverhog_growth_contracts import growth_binding

from tests.unit.test_canonical_discovery_archive_framing_growth import (
    dimensions as framing_dimensions,
)
from tests.unit.test_canonical_discovery_archive_framing_growth import observe as observe_framing
from tests.unit.test_canonical_discovery_journal_source_growth import sample
from tests.unit.test_canonical_discovery_journal_union import (
    apply,
    begin,
    edge,
    frame,
    prepare,
    progress,
    union_case,
)

OPERATIONS = {
    "progress": owner.discovery_journal_union_progress,
    "begin": owner.begin_discovery_journal_union,
    "prepare": owner.prepare_discovery_journal_union,
    "apply": owner.apply_discovery_journal_union,
}


def width(value):
    if value is None:
        return 4
    if type(value) is bool:
        return 5
    if type(value) is int:
        return len(format_scalar("nonnegative", value))
    if isinstance(value, str):
        return len(value.encode())
    if isinstance(value, bytes):
        return len(value)
    if is_dataclass(value):
        return sum(width(getattr(value, f.name)) for f in fields(value))
    if isinstance(value, dict):
        return sum(width(k) + width(v) for k, v in value.items())
    if isinstance(value, (tuple, list, set, frozenset)):
        return sum(map(width, value))
    raise AssertionError(type(value))


@contextmanager
def observe(case, task, operation, monkeypatch):
    with observe_framing(case, task.topology.epochs.framing, monkeypatch) as recorder:
        for bound in growth_binding(operation).declaration.bounds:
            recorder.metrics.setdefault(bound.metric, 0)
        with monkeypatch.context() as patch:
            children = [
                (topology, "discovery_journal_topology_progress", "topology_progress_calls"),
                (topology, "discovery_journal_topology_page", "topology_page_calls"),
                (epochs, "discovery_journal_epoch_frame", "epoch_frame_calls"),
                (owner, "plan_epoch_union_additions", "native_plan_calls"),
                (owner.TopologyOccurrence, "checked", "native_occurrence_checks"),
            ]
            if operation is not OPERATIONS["progress"]:
                children.append((owner, "discovery_journal_union_progress", "progress_calls"))
            for module, name, metric in children:
                actual = getattr(module, name)

                def call(*args, _actual=actual, _metric=metric, **kwargs):
                    before = dict(recorder.metrics)
                    try:
                        return _actual(*args, **kwargs)
                    finally:
                        recorder.metrics.update(before)
                        recorder.metrics[_metric] = before[_metric] + 1
                        if _metric == "native_occurrence_checks":
                            recorder.metrics["native_occurrence_input_octets"] += width(args[0])
                        if _metric == "native_plan_calls":
                            recorder.metrics["native_plan_input_octets"] += width((args, kwargs))

                patch.setattr(module, name, call)
            for name, metric in (
                ("parse_scalar", "scalar_decoded_octets"),
                ("format_scalar", "scalar_encoded_octets"),
            ):
                actual = getattr(owner, name)

                def scalar(domain, value, _actual=actual, _metric=metric):
                    if _metric == "scalar_decoded_octets" and isinstance(value, str):
                        recorder.metrics[_metric] += len(value.encode())
                    result = _actual(domain, value)
                    if _metric == "scalar_encoded_octets":
                        recorder.metrics[_metric] += len(result.encode())
                    return result

                patch.setattr(owner, name, scalar)
            actual_identity = owner.require_canonical_uuid_urn

            def identity(value):
                recorder.metrics["identity_checks"] += 1
                if isinstance(value, str):
                    recorder.metrics["identity_input_octets"] += len(value.encode())
                return actual_identity(value)

            patch.setattr(owner, "require_canonical_uuid_urn", identity)
            yield recorder
    for sql, parameters in recorder.statements:
        if "FROM discovery_journal_active_edges" in sql and "ORDER BY" in sql:
            plan = " ".join(
                row[3] for row in case.metadata._db.execute("EXPLAIN QUERY PLAN " + sql, parameters)
            )
            assert "discovery_journal_active_seed" in plan


def dimensions(case, task):
    base = framing_dimensions(case, task.topology.epochs.framing, 1)
    return {
        "retained_rows": base["registered_inputs"],
        "metadata_octets": max(
            base["metadata_octets"],
            width((case.metadata.key, task.binding_json, task._prepared)),
        ),
    }


def advance_until(case, task, predicate):
    while not predicate(progress(case.metadata, task)):
        page = prepare(case.metadata, task)
        assert page is not None
        assert apply(case.metadata, task, page)


def exercise(tmp_path, monkeypatch, record_property, mode):
    samples = []
    operation = OPERATIONS[mode]
    for active, retired in ((1, 1), (17, 17), (1025, 1025), (1, 1025), (1025, 1)):
        raw = frame([edge(n, 0, n + 1) for n in range(active + retired)], kind="journal_init")
        raw += frame(kind="correction", retracts=range(retired))
        raw += frame(kind="correction")
        with union_case(tmp_path / f"{active}-{retired}", raw) as (case, task):
            if mode != "begin":
                begin(case.metadata, task)
            stages = (
                (None,)
                if mode == "begin"
                else (
                    (0, "frame"),
                    (0, "add"),
                    (1, "retracts"),
                    (1, "seed"),
                    (2, "seed"),
                )
            )
            for target in stages:
                if target is not None:
                    epoch, phase = target
                    advance_until(
                        case,
                        task,
                        lambda p, e=epoch, f=phase: (
                            p.epoch == e and p.phase == f and p.state == "building"
                        ),
                    )
                page = prepare(case.metadata, task) if mode == "apply" else None
                for retry in range(3):
                    case.metadata._db.execute("PRAGMA shrink_memory").close()
                    values = dimensions(case, task)
                    context = (
                        case.metadata.transaction
                        if mode in {"begin", "apply"}
                        else case.metadata.read
                    )
                    with context() as (_, key):
                        with observe(case, task, operation, monkeypatch) as recorder:
                            before = case.metadata._db.total_changes
                            if mode == "apply" and retry:
                                with pytest.raises(owner.StaleArchiveTask, match="owned prepared"):
                                    operation(recorder, key, task, page)
                            elif mode == "apply":
                                assert operation(recorder, key, task, page)
                            else:
                                result = operation(recorder, key, task)
                                if mode == "prepare" and retry == 0 and target[1] == "seed":
                                    assert result is not None and result.delta is not None
                                    assert recorder.metrics["returned_rows"] >= min(active, 64) + 2
                            recorder.metrics["changed_rows"] = (
                                case.metadata._db.total_changes - before
                            )
                            metrics = dict(recorder.metrics)
                    samples.append(sample(operation, values, metrics))
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "riverhog-discovery-journal-union-progress/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_journal_union.discovery_journal_union_progress",
)
def test_actual_original_union_cursor_and_markers(tmp_path, monkeypatch, record_property):
    exercise(tmp_path, monkeypatch, record_property, "progress")


@pytest.mark.growth_witness(
    "riverhog-discovery-journal-union-begin/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_journal_union.begin_discovery_journal_union",
)
def test_actual_union_creation_and_retry(tmp_path, monkeypatch, record_property):
    exercise(tmp_path, monkeypatch, record_property, "begin")


@pytest.mark.growth_witness(
    "riverhog-discovery-journal-union-prepare/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_journal_union.prepare_discovery_journal_union",
)
def test_actual_native_points_retirement_and_indexed_active_carry(
    tmp_path, monkeypatch, record_property
):
    exercise(tmp_path, monkeypatch, record_property, "prepare")


@pytest.mark.growth_witness(
    "riverhog-discovery-journal-union-apply/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_journal_union.apply_discovery_journal_union",
)
def test_actual_atomic_union_deltas_and_duplicate_rejection(tmp_path, monkeypatch, record_property):
    exercise(tmp_path, monkeypatch, record_property, "apply")
