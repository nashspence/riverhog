from __future__ import annotations

import json
import sqlite3
import sys
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from riverhog_canonical_json import format_scalar, parse_scalar
from riverhog_core import canonical_discovery_journal_selector_work as journal_work
from riverhog_core import canonical_discovery_selector_work as selectors
from riverhog_core.canonical_discovery_archive_work import admit_archive_journal
from riverhog_core.canonical_discovery_endpoint_index import EndpointIndexWork
from riverhog_core.canonical_discovery_selector_work import SelectorCapacity
from riverhog_provenance import JournalFrame

from tests.support.canonical_discovery_work import observe_metadata, write
from tests.unit.test_canonical_discovery_activity_selectors import (
    APPLY,
    PREPARE,
    compiler,
    current,
    graph,
    seed,
)
from tests.unit.test_canonical_discovery_activity_selectors import (
    activity_originals as activity_originals,
)
from tests.unit.test_canonical_discovery_history_selector_growth import dimensions
from tests.unit.test_canonical_discovery_selector_growth import retain
from tests.unit.test_journal_validation_workspace_reference_growth import sample


@contextmanager
def observed_activity(case, monkeypatch):
    loads = json.loads
    same_source, same_coordinates = journal_work._same_source, selectors._equal_coordinates
    changed, depth = case.db.total_changes, 0
    with observe_metadata(case.db, monkeypatch) as recorder:
        recorder.metrics.update(
            decoded_octets=0,
            compared_octets=0,
            source_payload_octets=0,
            source_checks=0,
            native_anchor_calls=0,
            native_reference_calls=0,
        )

        def decoded(value, *args, **kwargs):
            recorder.metrics["decoded_octets"] += len(
                value if isinstance(value, bytes) else value.encode()
            )
            return loads(value, *args, **kwargs)

        def encoded_scalar(kind, value):
            result = format_scalar(kind, value)
            recorder.metrics["encoded_octets"] += len(result.encode())
            return result

        def decoded_scalar(kind, value):
            recorder.metrics["decoded_octets"] += len(value.encode())
            return parse_scalar(kind, value)

        def compared(left, right, original):
            recorder.metrics["compared_octets"] += len(left.encode()) + len(right.encode())
            return original(left, right)

        def child(function, metric):
            def measured(*args, **kwargs):
                nonlocal depth
                if depth:
                    return function(*args, **kwargs)
                before = dict(recorder.metrics)
                depth += 1
                try:
                    return function(*args, **kwargs)
                finally:
                    depth -= 1
                    recorder.metrics.update(before)
                    recorder.metrics[metric] += 1

            return measured

        def forbidden(*args, **kwargs):
            pytest.fail("activity preparation reconstructed source entry or assertion payload")

        with monkeypatch.context() as patch:
            patch.setattr(json, "loads", decoded)
            patch.setattr(
                journal_work,
                "_same_source",
                lambda left, right: compared(left, right, same_source),
            )
            patch.setattr(
                selectors,
                "_equal_coordinates",
                lambda left, right: compared(left, right, same_coordinates),
            )
            for source in (case.source, *getattr(case, "additional_sources", ())):
                for name, metric in (
                    ("check_open", "source_checks"),
                    ("anchor_at", "native_anchor_calls"),
                    ("reference_for_assertion", "native_reference_calls"),
                    ("reference_anchor", "native_reference_calls"),
                ):
                    patch.setattr(source, name, child(getattr(source, name), metric))
                patch.setattr(source, "entry", forbidden)
                patch.setattr(source, "assertion", forbidden)
            patch.setattr(JournalFrame, "document", property(forbidden))
            for module in tuple(sys.modules.values()):
                if module is not None and getattr(module, "__name__", "").startswith(
                    ("riverhog_core.canonical_discovery", "riverhog_canonical_json")
                ):
                    if getattr(module, "format_scalar", None) is format_scalar:
                        patch.setattr(module, "format_scalar", encoded_scalar)
                    if getattr(module, "parse_scalar", None) is parse_scalar:
                        patch.setattr(module, "parse_scalar", decoded_scalar)
            yield recorder
        recorder.metrics["changed_rows"] = case.db.total_changes - changed
    for sql, parameters in recorder.statements:
        if sql.lstrip().startswith("SELECT"):
            plan = case.db.execute("EXPLAIN QUERY PLAN " + sql, parameters).fetchall()
            assert not any("SCAN " in row[3] or "TEMP B-TREE" in row[3] for row in plan), plan


def exercise_activity(original, tmp_path, monkeypatch, record_property, operation, *, degree=None):
    samples, skipped, terminal = [], False, False
    for backlog in (0,) if degree is not None else (1, 17, 1025, 8193):
        path = tmp_path / f"activity-{backlog}.sqlite3"
        db = sqlite3.connect(path, isolation_level=None)
        db.execute("PRAGMA foreign_keys=ON").close()
        original.db.backup(db)
        case = SimpleNamespace(**vars(original))
        case.db, case.path = db, path
        case.source = admit_archive_journal(db, case.archive_key, case.admission, case.native)
        case.work = EndpointIndexWork(case.source, case.anchor)
        try:
            retain(SimpleNamespace(db=db, key=case.archive_key), backlog)
            for sequence in (0,) if degree is not None else (0, 1, 2):
                spec = seed(case, sequence)
                work = compiler(case)
                while not (before := current(case, spec)).complete:
                    measuring = degree is None or before.version in {
                        0,
                        1,
                        2,
                        degree // 2,
                        degree,
                        degree + 1,
                        degree + 2,
                        degree + 3,
                    }
                    inputs = dimensions(case, before, work._binding_json) if measuring else {}
                    if measuring and operation is PREPARE:
                        if before.version == 0:
                            with observed_activity(case, monkeypatch) as recorder:
                                with pytest.raises(SelectorCapacity):
                                    PREPARE(recorder, case.archive_key, compiler(case, 32), before)
                            samples.append(sample(operation, inputs, recorder.metrics))
                        with observed_activity(case, monkeypatch) as recorder:
                            prepared = PREPARE(recorder, case.archive_key, work, before)
                        inputs["metadata_octets"] = max(
                            inputs["metadata_octets"],
                            *(len(row.exact_json.encode()) for row in prepared.sources),
                        )
                        samples.append(sample(operation, inputs, recorder.metrics))
                    else:
                        prepared = PREPARE(db, case.archive_key, work, before)
                    if measuring and operation is APPLY:
                        inputs.update(
                            source_controls=len(prepared.sources),
                            control_octets=max(
                                len(row.exact_json.encode()) for row in prepared.sources
                            ),
                        )
                        db.execute("BEGIN IMMEDIATE").close()
                        with observed_activity(case, monkeypatch) as recorder:
                            APPLY(recorder, case.archive_key, work, prepared)
                        samples.append(sample(operation, inputs, recorder.metrics))
                        db.rollback()
                        assert current(case, spec) == before
                    for _retry in range(2 if measuring else 1):
                        with write(db):
                            if measuring and operation is APPLY:
                                with observed_activity(case, monkeypatch) as recorder:
                                    APPLY(recorder, case.archive_key, work, prepared)
                            else:
                                APPLY(db, case.archive_key, work, prepared)
                        if measuring and operation is APPLY:
                            samples.append(sample(operation, inputs, recorder.metrics))
                    skipped |= not prepared.page.result.complete and prepared.page.output is None
                    terminal |= prepared.page.result.complete
                    if measuring:
                        db.close()
                        db = case.db = sqlite3.connect(path, isolation_level=None)
                        db.execute("PRAGMA foreign_keys=ON").close()
                        case.work = EndpointIndexWork(case.source, case.anchor)
                        work = compiler(case)
                nodes, edges = graph(case)
                actual = {target for parent, target in edges if parent == spec}
                if degree is not None:
                    assert len(actual) == 2 and {node.kind for node in actual} == {"own", "direct"}
                    assert len(nodes) == backlog + 3
                else:
                    assert len(actual) == (3 if sequence < 2 else 0)
        finally:
            case.source.close()
            case.db.close()
    assert skipped and terminal
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "riverhog-activity-selector-original-prepare/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_activity_selectors.prepare_activity_selector",
)
def test_activity_original_controls_stay_bounded_across_retained_rows_and_native_prefixes(
    activity_originals, tmp_path, monkeypatch, record_property
):
    exercise_activity(activity_originals, tmp_path, monkeypatch, record_property, PREPARE)


@pytest.mark.growth_witness(
    "riverhog-journal-selector-exact-replay/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_journal_selector_work.apply_journal_selector",
)
def test_activity_original_replay_rollback_retry_and_cold_cursor_have_bounded_cost(
    activity_originals, tmp_path, monkeypatch, record_property
):
    exercise_activity(activity_originals, tmp_path, monkeypatch, record_property, APPLY)
