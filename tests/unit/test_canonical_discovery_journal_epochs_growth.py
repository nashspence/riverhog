from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace

import pytest
from riverhog_core import canonical_discovery_journal_epochs as owner
from riverhog_core import canonical_discovery_journal_registration as registration
from riverhog_growth_contracts import growth_binding

from tests.support.catalog_work import octets
from tests.unit.test_canonical_discovery_archive_framing_growth import (
    dimensions as framing_dimensions,
)
from tests.unit.test_canonical_discovery_archive_framing_growth import observe as observe_framing
from tests.unit.test_canonical_discovery_journal_epochs import apply, begin, prepare
from tests.unit.test_canonical_discovery_journal_source import source_case
from tests.unit.test_canonical_discovery_journal_source_growth import sample

OPERATIONS = {
    "progress": owner.discovery_journal_epoch_progress,
    "frame": owner.discovery_journal_epoch_frame,
    "begin": owner.begin_discovery_journal_epochs,
    "prepare": owner.prepare_discovery_journal_epoch,
    "apply": owner.apply_discovery_journal_epoch,
}


@contextmanager
def observe(case, task, operation, monkeypatch):
    with observe_framing(case, task.framing, monkeypatch) as recorder:
        for bound in growth_binding(operation).declaration.bounds:
            recorder.metrics.setdefault(bound.metric, 0)
        with monkeypatch.context() as patch:
            children = [
                (registration, "find_discovery_journal", "registration_calls", None),
                (owner.framing, "archive_journal_frame", "frame_position_calls", None),
                (owner.framing, "archive_framing_progress", "framing_progress_calls", None),
                (task.source, "read_exact", "source_exact_calls", "source_read_octets"),
                (
                    owner,
                    "classify_epoch_boundary",
                    "native_classification_calls",
                    "native_classification_octets",
                ),
            ]
            if operation is not OPERATIONS["progress"]:
                children.append((owner, "discovery_journal_epoch_progress", "progress_calls", None))
            for module, name, metric, width in children:
                actual = getattr(module, name)

                def invoke(*args, _actual=actual, _metric=metric, _width=width, **kwargs):
                    before = dict(recorder.metrics)
                    result = None
                    try:
                        result = _actual(*args, **kwargs)
                        return result
                    finally:
                        recorder.metrics.update(before)
                        recorder.metrics[_metric] = before.get(_metric, 0) + 1
                        if _width:
                            size = (
                                len(args[0])
                                if _metric == "native_classification_calls"
                                else len(result or b"")
                            )
                            recorder.metrics[_width] = before.get(_width, 0) + size

                patch.setattr(module, name, invoke)
            for name, metric in (
                ("format_scalar", "scalar_encoded_octets"),
                ("parse_scalar", "scalar_decoded_octets"),
            ):
                actual = getattr(owner, name)

                def scalar(domain, value, _actual=actual, _metric=metric):
                    result = _actual(domain, value)
                    recorder.metrics[_metric] += len(
                        str(result if _metric == "scalar_encoded_octets" else value).encode()
                    )
                    return result

                patch.setattr(owner, name, scalar)

            yield recorder


def seed(case, task, retained):
    with case.metadata.transaction() as (db, _):
        row = db.execute("SELECT * FROM discovery_journal_epochs").fetchone()
        for index in range(retained):
            identity = f"urn:uuid:{index:08x}-7777-4777-8777-777777777777"
            other = replace(task.source.binding, journal_id=identity)
            values = (*row[:2], identity, *row[3:6], other.to_json(), *row[7:])
            db.execute(
                "INSERT INTO discovery_journal_epochs VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", values
            ).close()
            db.execute(
                "INSERT INTO discovery_journal_epoch_frames "
                "VALUES(?,?,?,'0','0','26','0','assertion',?)",
                (*row[:2], identity, "a" * 64),
            ).close()


def dimensions(case, task, frame_octets):
    base = framing_dimensions(case, task.framing, 1)
    return {
        "retained_rows": base["registered_inputs"],
        "metadata_octets": max(
            base["metadata_octets"],
            octets((case.metadata.key, task.source.binding, task.binding_json, task._prepared)),
        ),
        "frame_octets": frame_octets,
    }


def exercise(tmp_path, monkeypatch, record_property, mode):
    samples = []
    operation = OPERATIONS[mode]
    raw_frame = b'\x1e{"entry_kind":"assertion"}\n'
    for frames, retained in ((1, 1), (17, 17), (1025, 1025), (1, 1025), (1025, 1)):
        with source_case(
            tmp_path / f"{frames}-{retained}", raw_frame * frames, segments=1, retained=1
        ) as (case, framing, source):
            task = owner.DiscoveryJournalEpochWork(framing, source)
            with case.metadata.transaction() as (db, key):
                registration.register_discovery_journal(db, key, framing, source.binding)
            if mode == "begin":
                # Seed unrelated rows, then remove only the selected empty row
                # so both creation and idempotent retry use this actual directory.
                begin(case.metadata, task)
                seed(case, task, retained)
                with case.metadata.transaction() as (db, key):
                    db.execute(
                        "DELETE FROM discovery_journal_epochs WHERE journal_id=?",
                        (source.binding.journal_id,),
                    ).close()
            else:
                begin(case.metadata, task)
                seed(case, task, retained)
            if mode in {"progress", "frame"}:
                assert apply(case.metadata, task, prepare(case.metadata, task))
            page = prepare(case.metadata, task) if mode == "apply" else None
            for retry in range(3):
                case.metadata._db.execute("PRAGMA shrink_memory").close()
                values = dimensions(case, task, len(raw_frame))
                context = (
                    case.metadata.transaction if mode in {"begin", "apply"} else case.metadata.read
                )
                with (
                    context() as (_db, key),
                    observe(case, task, operation, monkeypatch) as recorder,
                ):
                    before = case.metadata._db.total_changes
                    if mode == "apply" and retry:
                        with pytest.raises(owner.StaleArchiveTask, match="owned prepared"):
                            operation(recorder, key, task, page)
                    elif mode == "frame":
                        assert operation(recorder, key, task, 0)[0].ordinal == 0
                    elif mode == "apply":
                        assert operation(recorder, key, task, page)
                    else:
                        result = operation(recorder, key, task)
                        if mode == "prepare":
                            assert result is not None
                    recorder.metrics["changed_rows"] = case.metadata._db.total_changes - before
                    metrics = dict(recorder.metrics)
                samples.append(sample(operation, values, metrics))
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "riverhog-discovery-journal-epochs-progress/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_journal_epochs.discovery_journal_epoch_progress",
)
def test_actual_epoch_checkpoint_and_last_fact_remain_indexed(
    tmp_path, monkeypatch, record_property
):
    exercise(tmp_path, monkeypatch, record_property, "progress")


@pytest.mark.growth_witness(
    "riverhog-discovery-journal-epochs-begin/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_journal_epochs.begin_discovery_journal_epochs",
)
def test_actual_epoch_creation_and_retry_keep_unrelated_rows_independent(
    tmp_path, monkeypatch, record_property
):
    exercise(tmp_path, monkeypatch, record_property, "begin")


@pytest.mark.growth_witness(
    "riverhog-discovery-journal-epochs-prepare/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_journal_epochs.prepare_discovery_journal_epoch",
)
def test_actual_one_frame_preparation_and_cached_retry_ports(
    tmp_path, monkeypatch, record_property
):
    exercise(tmp_path, monkeypatch, record_property, "prepare")


@pytest.mark.growth_witness(
    "riverhog-discovery-journal-epochs-apply/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_journal_epochs.apply_discovery_journal_epoch",
)
def test_actual_fact_cursor_publication_and_duplicate_rejection_are_bounded(
    tmp_path, monkeypatch, record_property
):
    exercise(tmp_path, monkeypatch, record_property, "apply")


@pytest.mark.growth_witness(
    "riverhog-discovery-journal-epochs-frame/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_journal_epochs.discovery_journal_epoch_frame",
)
def test_actual_registered_epoch_frame_point_lookup(tmp_path, monkeypatch, record_property):
    exercise(tmp_path, monkeypatch, record_property, "frame")
