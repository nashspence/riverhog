from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from riverhog_core import canonical_discovery_endpoint_matches as matches
from riverhog_core.canonical_discovery_archive_work import admit_archive_journal
from riverhog_core.canonical_discovery_endpoint_index import EndpointIndexWork
from riverhog_core.canonical_discovery_selector_work import SelectorCapacity

from tests.support.canonical_discovery_work import write
from tests.unit.test_canonical_discovery_activity_selector_growth import observed_activity
from tests.unit.test_canonical_discovery_documentary_selectors import (
    APPLY,
    PREPARE,
    atom_count,
    compiler,
    current,
    expected,
    graph,
    reopen,
    seed,
)
from tests.unit.test_canonical_discovery_documentary_selectors import (
    documentary_originals as documentary_originals,
)
from tests.unit.test_canonical_discovery_history_selector_growth import dimensions
from tests.unit.test_canonical_discovery_selector_growth import retain
from tests.unit.test_journal_validation_workspace_reference_growth import sample


@contextmanager
def observed(case, monkeypatch):
    equal = matches._equal_bytes
    with observed_activity(case, monkeypatch) as recorder:

        def compared(left, right):
            recorder.metrics["compared_octets"] += len(left) + len(right)
            return equal(left, right)

        with monkeypatch.context() as patch:
            patch.setattr(matches, "_equal_bytes", compared)
            yield recorder


def exercise(original, tmp_path, monkeypatch, record_property, operation):
    samples, skipped, empty, kinds = [], False, False, set()
    for backlog in (1, 17, 1025, 8193):
        path = tmp_path / f"documentary-{backlog}.sqlite3"
        db = sqlite3.connect(path, isolation_level=None)
        db.execute("PRAGMA foreign_keys=ON").close()
        original.db.backup(db)
        case = SimpleNamespace(**vars(original))
        case.db, case.path = db, path
        case.source = admit_archive_journal(db, case.archive_key, case.admission, case.native)
        case.work = EndpointIndexWork(case.source, case.anchor)
        try:
            retain(SimpleNamespace(db=db, key=case.archive_key), backlog)
            for sequence in (0, 1, 2):
                choices = [
                    ("observed", case.state),
                    ("own", case.observation["id"]),
                    ("own", case.reported["id"]),
                    ("own", case.plain["id"]),
                    ("own", case.artifact),
                    ("own", case.future["id"]),
                ]
                for kind, identity in choices:
                    spec = seed(case, kind, sequence, identity=identity)
                    kinds.add(kind)
                    work, atoms = compiler(case), 0
                    while not (before := current(case, spec)).complete:
                        values = dimensions(case, before, work._binding_json)
                        if operation is PREPARE:
                            if before.version == 0:
                                with observed(case, monkeypatch) as recorder:
                                    with pytest.raises(SelectorCapacity):
                                        PREPARE(
                                            recorder, case.archive_key, compiler(case, 32), before
                                        )
                                samples.append(sample(operation, values, recorder.metrics))
                            with observed(case, monkeypatch) as recorder:
                                prepared = PREPARE(recorder, case.archive_key, work, before)
                            values["metadata_octets"] = max(
                                values["metadata_octets"],
                                *(len(row.exact_json.encode()) for row in prepared.sources),
                            )
                            samples.append(sample(operation, values, recorder.metrics))
                        else:
                            prepared = PREPARE(db, case.archive_key, work, before)
                            values.update(
                                source_controls=len(prepared.sources),
                                control_octets=max(
                                    len(row.exact_json.encode()) for row in prepared.sources
                                ),
                            )
                            db.execute("BEGIN IMMEDIATE").close()
                            with observed(case, monkeypatch) as recorder:
                                APPLY(recorder, case.archive_key, work, prepared)
                            samples.append(sample(operation, values, recorder.metrics))
                            db.rollback()
                            assert current(case, spec) == before
                        for _retry in range(2):
                            with write(db):
                                if operation is APPLY:
                                    with observed(case, monkeypatch) as recorder:
                                        APPLY(recorder, case.archive_key, work, prepared)
                                else:
                                    APPLY(db, case.archive_key, work, prepared)
                            if operation is APPLY:
                                samples.append(sample(operation, values, recorder.metrics))
                        skipped |= (
                            not prepared.page.result.complete and prepared.page.output is None
                        )
                        empty |= (
                            prepared.page.result.complete
                            and before.version == 0
                            and prepared.page.output is None
                        )
                        atoms += 1
                        reopen(case)
                        db = case.db
                        work = compiler(case)
                    assert atoms == atom_count(case, spec)
                    assert {
                        child for parent, child in graph(case)[1] if parent == spec
                    } == expected(case, spec)
        finally:
            case.source.close()
            case.db.close()
    assert kinds == {"own", "observed"} and skipped and empty
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "riverhog-documentary-dependencies-original-prepare/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_documentary_selectors.prepare_documentary_dependencies",
)
def test_documentary_dependency_atoms_bound_original_fields_and_total_monotone_work(
    documentary_originals, tmp_path, monkeypatch, record_property
):
    exercise(documentary_originals, tmp_path, monkeypatch, record_property, PREPARE)


@pytest.mark.growth_witness(
    "riverhog-documentary-dependencies-exact-replay/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_journal_selector_work.apply_journal_selector",
)
def test_documentary_dependency_replay_rollback_retry_and_cold_resume_keep_bounds(
    documentary_originals, tmp_path, monkeypatch, record_property
):
    exercise(documentary_originals, tmp_path, monkeypatch, record_property, APPLY)
