from __future__ import annotations

from contextlib import contextmanager

import pytest
from riverhog_core import canonical_discovery_journal_preparation as owner
from riverhog_growth_contracts import growth_binding

from tests.unit.test_canonical_discovery_archive_framing_growth import (
    dimensions as framing_dimensions,
)
from tests.unit.test_canonical_discovery_archive_framing_growth import observe as observe_framing
from tests.unit.test_canonical_discovery_journal_preparation import (
    advance_until,
    begin,
    edge,
    frame,
    preparation_case,
    prepare,
)
from tests.unit.test_canonical_discovery_journal_source_growth import sample
from tests.unit.test_canonical_discovery_journal_union_growth import width

OPERATIONS = {
    "progress": owner.discovery_journal_preparation_progress,
    "begin": owner.begin_discovery_journal_preparation,
    "prepare": owner.prepare_discovery_journal_preparation,
    "apply": owner.apply_discovery_journal_preparation,
}


@contextmanager
def observe(case, task, operation, monkeypatch):
    with observe_framing(case, task.epochs.framing, monkeypatch) as recorder:
        for bound in growth_binding(operation).declaration.bounds:
            recorder.metrics.setdefault(bound.metric, 0)
        recorder.delegated_changes = 0
        with monkeypatch.context() as patch:
            children = []
            for name in owner._PHASES[:-1]:
                stage = task.stage(name)
                for action, metric in (
                    ("progress", "child_progress_calls"),
                    ("begin", "child_begin_calls"),
                    ("prepare", "child_prepare_calls"),
                    ("apply", "child_apply_calls"),
                ):
                    target = (
                        stage.stem + "_progress"
                        if action == "progress"
                        else (
                            action
                            + "_"
                            + stage.stem
                            + ("s" if action == "begin" and name == "epochs" else "")
                        )
                    )
                    children.append((stage.module, target, metric))
            if operation is not OPERATIONS["progress"]:
                children.append(
                    (
                        owner,
                        "discovery_journal_preparation_progress",
                        "progress_calls",
                    )
                )
            for module, name, metric in children:
                actual = getattr(module, name)

                def call(*args, _actual=actual, _metric=metric, **kwargs):
                    before = dict(recorder.metrics)
                    changes = case.metadata._db.total_changes
                    delegated = recorder.delegated_changes
                    try:
                        return _actual(*args, **kwargs)
                    finally:
                        recorder.metrics.update(before)
                        recorder.metrics[_metric] = before[_metric] + 1
                        recorder.delegated_changes = (
                            delegated + case.metadata._db.total_changes - changes
                        )

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
            yield recorder


def dimensions(case, task):
    base = framing_dimensions(case, task.epochs.framing, 1)
    return {
        "retained_rows": base["registered_inputs"],
        "metadata_octets": max(
            base["metadata_octets"],
            width((case.metadata.key, task.binding_json)),
        ),
    }


def exercise(tmp_path, monkeypatch, record_property, mode):
    samples = []
    operation = OPERATIONS[mode]
    for size, retained in ((1, 1), (17, 17), (1025, 1), (1, 1025), (1025, 1025)):
        raw = frame([edge(n, n, (n + 1) % size) for n in range(size)], kind="journal_init")
        with preparation_case(tmp_path / f"{size}-{retained}", raw, retained=retained) as (
            case,
            task,
        ):
            if mode != "begin":
                begin(case.metadata, task)
            targets = (
                [(None, False)]
                if mode == "begin"
                else [
                    (phase, terminal) for phase in owner._PHASES[:-1] for terminal in (False, True)
                ]
            )
            if mode in {"progress", "prepare"}:
                targets.append(("complete", True))
            for phase, terminal in targets:
                if phase is not None:
                    advance_until(
                        case.metadata,
                        task,
                        lambda p, phase=phase, terminal=terminal: (
                            p.phase == phase and (p.child_complete or not terminal)
                        ),
                    )
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
                                with pytest.raises(owner.StaleArchiveTask, match="owned"):
                                    operation(recorder, key, task, page)
                            elif mode == "apply":
                                assert operation(recorder, key, task, page)
                            else:
                                operation(recorder, key, task)
                            recorder.metrics["changed_rows"] = (
                                case.metadata._db.total_changes
                                - before
                                - recorder.delegated_changes
                            )
                            metrics = dict(recorder.metrics)
                    samples.append(sample(operation, values, metrics))
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "riverhog-discovery-journal-preparation-progress/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation=(
        "riverhog_core.canonical_discovery_journal_preparation."
        "discovery_journal_preparation_progress"
    ),
)
def test_actual_initialized_stage_and_child_currentness(tmp_path, monkeypatch, record_property):
    exercise(tmp_path, monkeypatch, record_property, "progress")


@pytest.mark.growth_witness(
    "riverhog-discovery-journal-preparation-begin/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation=(
        "riverhog_core.canonical_discovery_journal_preparation.begin_discovery_journal_preparation"
    ),
)
def test_actual_first_initialization_and_owned_retry(tmp_path, monkeypatch, record_property):
    exercise(tmp_path, monkeypatch, record_property, "begin")


@pytest.mark.growth_witness(
    "riverhog-discovery-journal-preparation-prepare/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation=(
        "riverhog_core.canonical_discovery_journal_preparation."
        "prepare_discovery_journal_preparation"
    ),
)
def test_actual_one_child_or_transition_preparation(tmp_path, monkeypatch, record_property):
    exercise(tmp_path, monkeypatch, record_property, "prepare")


@pytest.mark.growth_witness(
    "riverhog-discovery-journal-preparation-apply/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation=(
        "riverhog_core.canonical_discovery_journal_preparation.apply_discovery_journal_preparation"
    ),
)
def test_actual_atomic_child_parent_publication_and_rejected_retry(
    tmp_path,
    monkeypatch,
    record_property,
):
    exercise(tmp_path, monkeypatch, record_property, "apply")
