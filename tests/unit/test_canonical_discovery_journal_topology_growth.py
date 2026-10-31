from __future__ import annotations

from contextlib import contextmanager

import pytest
from riverhog_core import canonical_discovery_journal_epochs as epochs
from riverhog_core import canonical_discovery_journal_topology as owner
from riverhog_growth_contracts import growth_binding

from tests.support.catalog_work import octets
from tests.unit.test_canonical_discovery_archive_framing_growth import (
    dimensions as framing_dimensions,
)
from tests.unit.test_canonical_discovery_archive_framing_growth import observe as observe_framing
from tests.unit.test_canonical_discovery_journal_source_growth import sample
from tests.unit.test_canonical_discovery_journal_topology import (
    apply,
    begin,
    encoded,
    prepare,
    progress,
    topology_case,
)

OPERATIONS = {
    "progress": owner.discovery_journal_topology_progress,
    "page": owner.discovery_journal_topology_page,
    "begin": owner.begin_discovery_journal_topology,
    "prepare": owner.prepare_discovery_journal_topology,
    "apply": owner.apply_discovery_journal_topology,
}


@contextmanager
def observe(case, task, operation, monkeypatch):
    with observe_framing(case, task.epochs.framing, monkeypatch) as recorder:
        for bound in growth_binding(operation).declaration.bounds:
            recorder.metrics.setdefault(bound.metric, 0)
        with monkeypatch.context() as patch:
            children = [
                (epochs, "discovery_journal_epoch_progress", "epoch_progress_calls", None),
                (epochs, "discovery_journal_epoch_frame", "epoch_frame_calls", None),
                (task.epochs.source, "read_exact", "source_exact_calls", "source_exact_octets"),
                (task.epochs.source, "read_window", "source_window_calls", "source_window_octets"),
                (owner, "plan_topology_frame", "native_plan_calls", "native_plan_octets"),
                (owner, "advance_topology_frame", "native_advance_calls", None),
                (owner.TopologyCaptureCheckpoint, "from_json", "checkpoint_decodes", None),
                (owner.TopologyCaptureCheckpoint, "to_json", "checkpoint_encodes", None),
            ]
            if operation is not OPERATIONS["progress"]:
                children.append(
                    (owner, "discovery_journal_topology_progress", "progress_calls", None)
                )
            for module, name, metric, width in children:
                actual = getattr(module, name)

                def call(*args, _actual=actual, _metric=metric, _width=width, **kwargs):
                    before = dict(recorder.metrics)
                    result = None
                    try:
                        result = _actual(*args, **kwargs)
                        return result
                    finally:
                        callbacks = (
                            {
                                key: recorder.metrics[key]
                                for key in ("source_window_calls", "source_window_octets")
                            }
                            if _metric == "native_advance_calls"
                            else {}
                        )
                        recorder.metrics.update(before)
                        recorder.metrics.update(callbacks)
                        recorder.metrics[_metric] = before[_metric] + 1
                        if _width:
                            size = (
                                len(args[0])
                                if _metric == "native_plan_calls"
                                else len(result or b"")
                            )
                            recorder.metrics[_width] = before[_width] + size

                if name == "from_json":
                    patch.setattr(module, name, staticmethod(call))
                else:
                    patch.setattr(module, name, call)
            for name, metric in (
                ("format_scalar", "scalar_encoded_octets"),
                ("parse_scalar", "scalar_decoded_octets"),
            ):
                actual = getattr(owner, name)

                def scalar(domain, value, _actual=actual, _metric=metric):
                    result = _actual(domain, value)
                    width = result if _metric == "scalar_encoded_octets" else value
                    recorder.metrics[_metric] += len(width.encode())
                    return result

                patch.setattr(owner, name, scalar)
            yield recorder


def seed(case, task, retained):
    with case.metadata.transaction() as (db, _):
        row = db.execute("SELECT * FROM discovery_journal_topology").fetchone()
        for index in range(retained):
            identity = f"urn:uuid:{index:08x}-7777-4777-8777-777777777777"
            values = (*row[:2], identity, *row[3:])
            db.execute(
                "INSERT INTO discovery_journal_topology VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", values
            ).close()


def dimensions(case, task, raw):
    base = framing_dimensions(case, task.epochs.framing, 1)
    return {
        "retained_rows": base["registered_inputs"],
        "metadata_octets": max(
            base["metadata_octets"], octets((case.metadata.key, task.binding_json, task._prepared))
        ),
        "frame_octets": len(raw),
        "frame_pages": (len(raw) + 65535) // 65536,
    }


def exercise(tmp_path, monkeypatch, record_property, mode):
    samples = []
    operation = OPERATIONS[mode]
    for items, retained, padding in (
        (1, 1, 0),
        (17, 17, 0),
        (1025, 1025, 0),
        (1, 1025, 0),
        (1025, 1, 0),
        (1, 17, 65537),
    ):
        raw = encoded(items, padding=padding)
        with topology_case(
            tmp_path / f"{items}-{retained}-{padding}", b"\x1e" + raw + b"\n", retained=1
        ) as (
            case,
            task,
        ):
            begin(case.metadata, task)
            seed(case, task, retained)
            if mode == "begin":
                with case.metadata.transaction() as (db, _):
                    db.execute(
                        "DELETE FROM discovery_journal_topology WHERE journal_id=?",
                        (task.epochs.source.binding.journal_id,),
                    ).close()
            if mode in {"progress", "page"}:
                # Real selected late cursor and fact rows, independent from unrelated rows.
                target = max(1, items - 2)
                while progress(case.metadata, task).checkpoint.facts < target:
                    assert apply(case.metadata, task, prepare(case.metadata, task))
            page = prepare(case.metadata, task) if mode == "apply" else None
            for retry in range(6 if mode == "prepare" else 3):
                if mode == "prepare" and retry == 3:
                    while progress(case.metadata, task).checkpoint.facts < max(1, items - 2):
                        assert apply(case.metadata, task, prepare(case.metadata, task))
                case.metadata._db.execute("PRAGMA shrink_memory").close()
                values = dimensions(case, task, raw)
                context = (
                    case.metadata.transaction if mode in {"begin", "apply"} else case.metadata.read
                )
                with context() as (_, key):
                    with observe(case, task, operation, monkeypatch) as recorder:
                        before = case.metadata._db.total_changes
                        if mode == "apply" and retry:
                            with pytest.raises(owner.StaleArchiveTask, match="owned prepared"):
                                operation(recorder, key, task, page)
                        elif mode == "page":
                            result = operation(recorder, key, task, 0, fact_budget=64)
                            assert len(result) <= 64
                        elif mode == "apply":
                            assert operation(recorder, key, task, page)
                        else:
                            operation(recorder, key, task)
                        recorder.metrics["changed_rows"] = case.metadata._db.total_changes - before
                        recorder.metrics["published_facts"] = sum(
                            sql.startswith("INSERT INTO discovery_journal_topology_facts")
                            for sql, _ in recorder.statements
                        )
                        metrics = dict(recorder.metrics)
                samples.append(sample(operation, values, metrics))
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "riverhog-discovery-journal-topology-progress/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_journal_topology.discovery_journal_topology_progress",
)
def test_actual_active_and_last_fact_point_checks(tmp_path, monkeypatch, record_property):
    exercise(tmp_path, monkeypatch, record_property, "progress")


@pytest.mark.growth_witness(
    "riverhog-discovery-journal-topology-begin/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_journal_topology.begin_discovery_journal_topology",
)
def test_actual_creation_and_idempotent_retry(tmp_path, monkeypatch, record_property):
    exercise(tmp_path, monkeypatch, record_property, "begin")


@pytest.mark.growth_witness(
    "riverhog-discovery-journal-topology-prepare/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_journal_topology.prepare_discovery_journal_topology",
)
def test_actual_native_and_source_callback_ports(tmp_path, monkeypatch, record_property):
    exercise(tmp_path, monkeypatch, record_property, "prepare")


@pytest.mark.growth_witness(
    "riverhog-discovery-journal-topology-apply/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_journal_topology.apply_discovery_journal_topology",
)
def test_actual_bounded_publication_and_rejected_duplicate(tmp_path, monkeypatch, record_property):
    exercise(tmp_path, monkeypatch, record_property, "apply")


@pytest.mark.growth_witness(
    "riverhog-discovery-journal-topology-page/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_journal_topology.discovery_journal_topology_page",
)
def test_actual_contiguous_original_fact_page(tmp_path, monkeypatch, record_property):
    exercise(tmp_path, monkeypatch, record_property, "page")
