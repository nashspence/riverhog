from __future__ import annotations

from contextlib import contextmanager

import pytest
from riverhog_core import canonical_discovery_journal_rank as owner
from riverhog_core import canonical_discovery_journal_union as union
from riverhog_growth_contracts import growth_binding

from tests.unit.test_canonical_discovery_archive_framing_growth import (
    dimensions as framing_dimensions,
)
from tests.unit.test_canonical_discovery_archive_framing_growth import observe as observe_framing
from tests.unit.test_canonical_discovery_journal_rank import (
    apply,
    begin,
    edge,
    frame,
    prepare,
    progress,
    rank_case,
)
from tests.unit.test_canonical_discovery_journal_source_growth import sample
from tests.unit.test_canonical_discovery_journal_union_growth import width

OPERATIONS = {
    "progress": owner.discovery_journal_rank_progress,
    "begin": owner.begin_discovery_journal_rank,
    "prepare": owner.prepare_discovery_journal_rank,
    "apply": owner.apply_discovery_journal_rank,
}


@contextmanager
def observe(case, task, operation, monkeypatch):
    with observe_framing(case, task.union.topology.epochs.framing, monkeypatch) as recorder:
        for bound in growth_binding(operation).declaration.bounds:
            recorder.metrics.setdefault(bound.metric, 0)
        with monkeypatch.context() as patch:
            children = [
                (union, "discovery_journal_union_progress", "union_progress_calls"),
                (owner, "decrement_rank_degrees", "native_plan_calls"),
            ]
            if operation is not OPERATIONS["progress"]:
                children.append((owner, "discovery_journal_rank_progress", "progress_calls"))
            for module, name, metric in children:
                actual = getattr(module, name)

                def call(*args, _actual=actual, _metric=metric, **kwargs):
                    before = dict(recorder.metrics)
                    try:
                        return _actual(*args, **kwargs)
                    finally:
                        recorder.metrics.update(before)
                        recorder.metrics[_metric] = before[_metric] + 1
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
        if "FROM discovery_journal_rank_vertices" in sql and "ORDER BY" in sql:
            plan = " ".join(
                row[3] for row in case.metadata._db.execute("EXPLAIN QUERY PLAN " + sql, parameters)
            )
            assert "discovery_journal_rank_ready" in plan


def dimensions(case, task):
    base = framing_dimensions(case, task.union.topology.epochs.framing, 1)
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
    regimes = [
        (1, 1, False),
        (17, 17, False),
        (1025, 1025, False),
        (1, 1025, False),
        (1025, 1, False),
        (1025, 1, True),
    ]
    for size, retained, cyclic in regimes:
        rows = (
            [edge(n, n, (n + 1) % size) for n in range(size)]
            if cyclic
            else [edge(n, 0, n + 1) for n in range(size)]
        )
        raw = frame(rows, kind="journal_init")
        with rank_case(tmp_path / f"{size}-{retained}-{cyclic}", raw, retained=retained) as (
            case,
            task,
        ):
            if mode != "begin":
                begin(case.metadata, task)
            stages = [None] if mode == "begin" else ["graph", "seed", "pop"]
            if mode != "begin":
                stages += ["second", "complete"] if cyclic else ["edges"]
                if not cyclic and size > 64:
                    stages.append("late-edges")
                if mode == "apply" and cyclic:
                    stages.remove("complete")
            for stage in stages:
                if stage is not None:

                    def target(p, stage=stage):
                        if stage == "complete":
                            return p.state == "complete"
                        if stage == "second":
                            return p.kind == "specialization" and p.phase == "graph"
                        return (
                            p.kind == "derivation"
                            and p.state == "building"
                            and p.phase == ("edges" if stage == "late-edges" else stage)
                            and (
                                p.edge_after is not None
                                if stage == "late-edges"
                                else p.edge_after is None
                            )
                        )

                    advance_until(case, task, target)
                page = prepare(case.metadata, task) if mode == "apply" else None
                for retry in range(3):
                    case.metadata._db.execute("PRAGMA shrink_memory").close()
                    values = dimensions(case, task)
                    context = (
                        case.metadata.transaction
                        if mode in {"begin", "apply"}
                        else (case.metadata.read)
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
                                if mode == "prepare" and retry == 0 and stage == "seed":
                                    assert result is not None
                                    assert recorder.metrics["returned_rows"] >= min(size + 1, 64)
                                if mode == "prepare" and retry == 0 and stage == "late-edges":
                                    assert result is not None and result.changes
                                    assert recorder.metrics["native_plan_calls"] == 1
                                    assert recorder.metrics["returned_rows"] >= 2 * len(
                                        result.changes
                                    )
                            recorder.metrics["changed_rows"] = (
                                case.metadata._db.total_changes - before
                            )
                            metrics = dict(recorder.metrics)
                    samples.append(sample(operation, values, metrics))
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "riverhog-discovery-journal-rank-progress/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_journal_rank.discovery_journal_rank_progress",
)
def test_actual_original_rank_cursor_and_markers(tmp_path, monkeypatch, record_property):
    exercise(tmp_path, monkeypatch, record_property, "progress")


@pytest.mark.growth_witness(
    "riverhog-discovery-journal-rank-begin/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_journal_rank.begin_discovery_journal_rank",
)
def test_actual_rank_creation_and_retry(tmp_path, monkeypatch, record_property):
    exercise(tmp_path, monkeypatch, record_property, "begin")


@pytest.mark.growth_witness(
    "riverhog-discovery-journal-rank-prepare/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_journal_rank.prepare_discovery_journal_rank",
)
def test_actual_seed_ready_high_degree_and_cycle_preparation(
    tmp_path,
    monkeypatch,
    record_property,
):
    exercise(tmp_path, monkeypatch, record_property, "prepare")


@pytest.mark.growth_witness(
    "riverhog-discovery-journal-rank-apply/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_journal_rank.apply_discovery_journal_rank",
)
def test_actual_atomic_rank_deltas_and_rejected_retries(tmp_path, monkeypatch, record_property):
    exercise(tmp_path, monkeypatch, record_property, "apply")
