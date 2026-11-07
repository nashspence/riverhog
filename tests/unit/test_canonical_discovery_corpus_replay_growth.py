from __future__ import annotations

import hashlib
from contextlib import contextmanager, nullcontext

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core import canonical_discovery_corpus_replay as replay
from riverhog_core.canonical_discovery_archive_work import StaleArchiveTask
from riverhog_core.services.discovery_validation_owner import DiscoveryValidationOwner
from riverhog_growth_contracts import (
    evaluate_bound,
    growth_binding,
    project_bindings,
    validate_execution_samples,
)
from riverhog_provenance import JournalSummary

from tests.support.catalog_work import octets
from tests.unit.test_canonical_discovery_corpus_replay import open_replay, prepare, progress
from tests.unit.test_canonical_discovery_corpus_source import case as case
from tests.unit.test_canonical_discovery_corpus_source import fixed_inputs as fixed_inputs
from tests.unit.test_canonical_discovery_corpus_source import original as original
from tests.unit.test_canonical_discovery_corpus_source import qualified as qualified
from tests.unit.test_canonical_discovery_corpus_source import seed
from tests.unit.test_canonical_discovery_selection import _observe
from tests.unit.test_discovery_validation_owner import unit

_OPERATIONS = {
    "begin": replay.begin_corpus_replay,
    "progress": replay.corpus_replay_progress,
    "next": replay.next_corpus_replay_source,
    "prepare": replay.prepare_corpus_replay,
    "apply": replay.apply_corpus_replay,
}


@contextmanager
def observe(metadata, owner, monkeypatch):
    db = metadata._db
    with _observe(db, monkeypatch, owner.selection.history.reader) as recorder:
        recorder.metrics["native_phase_calls"] = 0
        anchor = JournalSummary.anchor_at

        def original_anchor(summary, sequence):
            recorder.metrics["native_anchor_calls"] += 1
            return anchor(summary, sequence)

        with monkeypatch.context() as patch:
            patch.setattr(JournalSummary, "anchor_at", original_anchor)
            for name in (
                "advance_source",
                "select_source",
                "advance_semantics",
                "advance_dependencies",
                "advance_order",
            ):
                actual = getattr(owner.corpus, name)

                def native(*args, _actual=actual, **kwargs):
                    # This declaration covers direct Core work. Public Native
                    # declarations own the actual body and its source callbacks.
                    before = dict(recorder.metrics)
                    try:
                        return _actual(*args, **kwargs)
                    finally:
                        recorder.metrics.update(before)
                        recorder.metrics["native_phase_calls"] += 1

                patch.setattr(owner.corpus, name, native)
            yield recorder
    for sql, parameters in recorder.statements:
        if sql.lstrip().upper().startswith("SELECT"):
            plan = " ".join(
                str(row[3]).upper() for row in db.execute("EXPLAIN QUERY PLAN " + sql, parameters)
            )
            assert "SCAN " not in plan and "TEMP B-TREE" not in plan, plan


def dimensions(metadata, owner):
    db = metadata._db
    retained, width = 0, octets((metadata.key, owner.manifest, metadata.admission.binding_json))
    for (table,) in db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'discovery_%'"
    ).fetchall():
        for row in db.execute(f"SELECT * FROM {table}"):
            retained += 1
            width = max(width, octets(row))
    return {"registered_inputs": retained, "metadata_octets": width}


def sample(mode, dims, metrics):
    binding = growth_binding(_OPERATIONS[mode])
    projection = project_bindings((binding,))
    result = {
        "declaration": binding.declaration.id,
        "declaration_sha256": hashlib.sha256(
            canonical_json_bytes(projection["declarations"][binding.declaration.id])
        ).hexdigest(),
        "dimensions": dims,
        "metrics": {bound.metric: metrics[bound.metric] for bound in binding.declaration.bounds},
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


def exercise(case, qualified, original, monkeypatch, record_property, mode):
    samples, phases = [], set()
    app = DiscoveryValidationOwner(case.factory, case.root)
    try:
        with unit(app, case) as work:
            selection, _ = seed(work.metadata, qualified)
            owner = open_replay(work, selection)
            metadata, db = work.metadata, work.metadata._db
            retained = 0
            for size in (1, 17, 1025):
                with metadata.transaction() as (current, _):
                    for index in range(retained, size):
                        current.execute(
                            "INSERT INTO discovery_corpus_prefix_inputs VALUES(?,?,?,?,?)",
                            (
                                "unrelated-" + str(index),
                                metadata.key.admission_sha256,
                                "f" * 64,
                                f"{index:064x}",
                                "unselected original metadata",
                            ),
                        ).close()
                retained = size
                # Keep the accepted Native source and start another current
                # metadata observation at its persisted phase. Never reset it.
                while True:
                    before = progress(metadata, owner)
                    phases.add(before.phase)
                    dims = dimensions(metadata, owner)
                    with metadata.read() as (current, key):
                        target = replay.next_corpus_replay_source(current, key, owner)
                    source = nullcontext(None) if target is None else original[1].borrow()
                    with source as summary:
                        page = None
                        if mode == "apply":
                            page = prepare(metadata, owner, original)
                        db.execute("PRAGMA shrink_memory").close()
                        with observe(metadata, owner, monkeypatch) as recorder:
                            if mode in ("begin", "apply"):
                                with metadata.transaction() as (_, key):
                                    if mode == "begin":
                                        replay.begin_corpus_replay(recorder, key, owner)
                                    else:
                                        replay.apply_corpus_replay(recorder, key, owner, page)
                            else:
                                with metadata.read() as (_, key):
                                    if mode == "progress":
                                        replay.corpus_replay_progress(recorder, key, owner)
                                    elif mode == "next":
                                        replay.next_corpus_replay_source(recorder, key, owner)
                                    else:
                                        page = replay.prepare_corpus_replay(
                                            recorder,
                                            key,
                                            owner,
                                            summary=summary,
                                            row_budget=1,
                                            byte_budget=65536,
                                        )
                        samples.append(sample(mode, dims, recorder.metrics))
                    if before.phase == "complete":
                        # A retained corpus completion remains a point read or
                        # idempotent dispatch under independent index growth.
                        break
                    if mode != "apply":
                        if page is None:
                            page = prepare(metadata, owner, original)
                        with metadata.transaction() as (current, key):
                            replay.apply_corpus_replay(current, key, owner, page)
                with metadata.transaction() as (current, _):
                    current.execute(
                        "UPDATE discovery_corpus_replay SET worker_id='stale' "
                        "WHERE build_id=? AND admission_sha256=?",
                        (metadata.key.build_id, metadata.key.admission_sha256),
                    ).close()
                try:
                    with observe(metadata, owner, monkeypatch) as recorder:
                        with metadata.read() as (_, key):
                            with pytest.raises(StaleArchiveTask):
                                _OPERATIONS["progress"](recorder, key, owner)
                    if mode == "progress":
                        samples.append(sample(mode, dims, recorder.metrics))
                finally:
                    with metadata.transaction() as (current, _):
                        current.execute(
                            "UPDATE discovery_corpus_replay SET worker_id=? "
                            "WHERE build_id=? AND admission_sha256=?",
                            (
                                metadata.key.worker_id,
                                metadata.key.build_id,
                                metadata.key.admission_sha256,
                            ),
                        ).close()
    finally:
        app.close()
    assert phases == {
        "sources",
        "selections",
        "semantics",
        "dependencies",
        "derivation",
        "specialization",
        "complete",
    }
    record_property("growth_samples", samples)
    record_property(
        "replay_conditions",
        "Actual retained Native execution under original Core/catalog/kernel custody. "
        "Direct Core SQL, complete returned rows and parameters, codecs, hash and SHA "
        "block work, VM and Native dispatch count are observed. Every SELECT requires "
        "an indexed plan. Source and selection barriers and all global Native phases "
        "are covered, then retained unrelated original rows grow 1/17/1025 without reset. "
        "Native body/callback costs retain their separately qualified public declarations; "
        "these samples are not total corpus, causal relevance or publication evidence.",
    )


@pytest.mark.growth_witness(
    "canonical-discovery-corpus-replay-begin/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_corpus_replay.begin_corpus_replay",
)
def test_begin_work(case, qualified, original, monkeypatch, record_property):
    exercise(case, qualified, original, monkeypatch, record_property, "begin")


@pytest.mark.growth_witness(
    "canonical-discovery-corpus-replay-progress/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_corpus_replay.corpus_replay_progress",
)
def test_progress_work(case, qualified, original, monkeypatch, record_property):
    exercise(case, qualified, original, monkeypatch, record_property, "progress")


@pytest.mark.growth_witness(
    "canonical-discovery-corpus-replay-next/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_corpus_replay.next_corpus_replay_source",
)
def test_next_source_work(case, qualified, original, monkeypatch, record_property):
    exercise(case, qualified, original, monkeypatch, record_property, "next")


@pytest.mark.growth_witness(
    "canonical-discovery-corpus-replay-prepare/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_corpus_replay.prepare_corpus_replay",
)
def test_prepare_work(case, qualified, original, monkeypatch, record_property):
    exercise(case, qualified, original, monkeypatch, record_property, "prepare")


@pytest.mark.growth_witness(
    "canonical-discovery-corpus-replay-apply/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_corpus_replay.apply_corpus_replay",
)
def test_apply_work(case, qualified, original, monkeypatch, record_property):
    exercise(case, qualified, original, monkeypatch, record_property, "apply")
