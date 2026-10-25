from __future__ import annotations

import hashlib
import sqlite3
from contextlib import closing
from dataclasses import replace

import pytest
from riverhog_archive_contracts import HistoryJournalAnchor
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core import canonical_discovery_corpus_inputs as sources
from riverhog_core import canonical_discovery_journal_work as journal_work
from riverhog_core import canonical_discovery_prefix_inputs as owner
from riverhog_core.canonical_discovery_archive_work import (
    StaleArchiveTask,
    admit_archive_journal,
    initialize_archive_work,
)
from riverhog_core.canonical_discovery_journal_work import StaleJournalTask
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples
from riverhog_provenance import JournalSummary, validate_journal

from tests.support.canonical_discovery_work import (
    MeasuredConnection,
    corrected_journal_fixture,
    metadata_octets,
    write,
)
from tests.unit.test_canonical_discovery_corpus_inputs import commit_input, register_archive


@pytest.fixture(scope="module")
def native():
    fixture, raw, _ = corrected_journal_fixture()
    with validate_journal(raw, catalog=fixture.catalog) as summary:
        yield raw, summary


@pytest.fixture
def owned(tmp_path, native):
    raw, summary = native
    db = sqlite3.connect(tmp_path / "prefix.sqlite", isolation_level=None)
    initialize_archive_work(db)
    owner.initialize_corpus_prefix_inputs(db)
    key, admission = register_archive(db, raw, summary)
    with admit_archive_journal(db, key, admission, summary) as source:
        original = sources.CorpusSourceInputWork(admission, source)
        commit_input(db, key, original)
        with write(db):
            manifest = sources.complete_corpus_source_inputs(db, key, admission)
            owner.begin_corpus_prefix_inputs(db, key, admission, manifest)
        work = owner.CorpusPrefixWork(admission, manifest, source)
        yield db, key, admission, manifest, work
    db.close()


def request(db, key, work, sequence):
    anchor = HistoryJournalAnchor.from_mapping(work.original.source.anchor_at(sequence))
    page = owner.prepare_corpus_prefix(db, key, work, anchor)
    with write(db):
        result = owner.apply_corpus_prefix(db, key, work, page)
    return page, result


@pytest.mark.parametrize("order", [(0, 2, 1), (2, 1, 0)])
def test_exact_prefixes_share_one_monotone_original_source_maximum(owned, order):
    db, key, admission, manifest, work = owned
    expected = -1
    for count, sequence in enumerate(order, 1):
        page, progress = request(db, key, work, sequence)
        expected = max(expected, sequence)
        assert (progress.prefixes, progress.journals) == (count, 1)
        maximum = owner.corpus_prefix_maximum(
            db, key, admission, manifest, work.original.source_key
        )
        assert maximum.anchor == HistoryJournalAnchor.from_mapping(
            work.original.source.anchor_at(expected)
        )
        with write(db):
            assert owner.apply_corpus_prefix(db, key, work, page) == progress
        assert owner.corpus_prefix_progress(db, key, admission, manifest) == progress
    assert db.execute("SELECT count(*) FROM discovery_corpus_prefix_inputs").fetchone() == (3,)
    assert db.execute("SELECT count(*) FROM discovery_corpus_prefix_maxima").fetchone() == (1,)
    assert owner.corpus_prefix_maximum(db, key, admission, manifest, "0" * 64) is None


def test_registry_and_exact_maximum_survive_reopen(owned):
    db, key, admission, manifest, work = owned
    _, progress = request(db, key, work, 2)
    expected = owner.corpus_prefix_maximum(db, key, admission, manifest, work.original.source_key)
    path = db.execute("PRAGMA database_list").fetchone()[2]
    with closing(sqlite3.connect(path, isolation_level=None)) as reopened:
        owner.initialize_corpus_prefix_inputs(reopened)
        with write(reopened):
            assert owner.begin_corpus_prefix_inputs(reopened, key, admission, manifest) == progress
        assert (
            owner.corpus_prefix_maximum(
                reopened, key, admission, manifest, work.original.source_key
            )
            == expected
        )


@pytest.mark.parametrize(
    "field,value",
    [
        ("journal_id", "urn:uuid:11111111-1111-4111-8111-111111111111"),
        ("through_entry_id", "urn:uuid:22222222-2222-4222-8222-222222222222"),
        ("through_sequence", 1),
        ("through_json_sha256", "0" * 64),
        ("prefix_sha256", "0" * 64),
        ("prefix_bytes", 1),
    ],
)
def test_every_original_prefix_coordinate_must_match_native(owned, field, value):
    db, key, _, _, work = owned
    actual = HistoryJournalAnchor.from_mapping(work.original.source.anchor_at(0))
    with pytest.raises(StaleArchiveTask, match="exact original"):
        owner.prepare_corpus_prefix(db, key, work, replace(actual, **{field: value}))
    assert db.execute("SELECT count(*) FROM discovery_corpus_prefix_inputs").fetchone() == (0,)


@pytest.mark.parametrize("field", ["build_id", "worker_id"])
def test_current_archive_worker_and_fixed_source_manifest_are_required(owned, field):
    db, key, admission, manifest, work = owned
    with pytest.raises(StaleArchiveTask):
        owner.corpus_prefix_progress(db, replace(key, **{field: "other"}), admission, manifest)
    altered = replace(manifest, manifest_sha256="0" * 64)
    with pytest.raises(StaleArchiveTask, match="exact completed"):
        owner.corpus_prefix_progress(db, key, admission, altered)


def test_stale_preparation_cannot_add_an_unseen_prefix(owned):
    db, key, admission, manifest, work = owned
    other = owner.CorpusPrefixWork(admission, manifest, work.original.source)
    first = HistoryJournalAnchor.from_mapping(work.original.source.anchor_at(0))
    page = owner.prepare_corpus_prefix(db, key, other, first)
    request(db, key, work, 2)
    with write(db), pytest.raises(StaleArchiveTask, match="stale registry"):
        owner.apply_corpus_prefix(db, key, other, page)
    with write(db), pytest.raises(StaleArchiveTask, match="exact preparation"):
        owner.apply_corpus_prefix(db, key, other, replace(page))


def test_prefix_effects_and_shared_maximum_roll_back_with_the_header(owned):
    db, key, admission, manifest, work = owned
    request(db, key, work, 0)
    next_anchor = HistoryJournalAnchor.from_mapping(work.original.source.anchor_at(2))
    page = owner.prepare_corpus_prefix(db, key, work, next_anchor)
    before = tuple(db.iterdump())

    def deny(action, table, _column, _database, _trigger):
        if action == sqlite3.SQLITE_UPDATE and table == "discovery_corpus_prefix_headers":
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    db.set_authorizer(deny)
    try:
        with write(db), pytest.raises(sqlite3.DatabaseError):
            owner.apply_corpus_prefix(db, key, work, page)
    finally:
        db.set_authorizer(None)
    assert tuple(db.iterdump()) == before
    with write(db):
        assert owner.apply_corpus_prefix(db, key, work, page).prefixes == 2
    assert (
        owner.corpus_prefix_maximum(db, key, admission, manifest, work.original.source_key).anchor
        == next_anchor
    )


def test_same_native_prefix_keeps_independent_archive_admission(owned, native):
    db, key, admission, manifest, work = owned
    page, _ = request(db, key, work, 0)
    raw, summary = native
    other, other_admission = register_archive(db, raw, summary, total_bytes=8)
    with admit_archive_journal(db, other, other_admission, summary) as source:
        original = sources.CorpusSourceInputWork(other_admission, source)
        commit_input(db, other, original)
        with write(db):
            other_manifest = sources.complete_corpus_source_inputs(db, other, other_admission)
            owner.begin_corpus_prefix_inputs(db, other, other_admission, other_manifest)
        other_work = owner.CorpusPrefixWork(other_admission, other_manifest, source)
        other_page, _ = request(db, other, other_work, 0)
        assert page.anchor == other_page.anchor
        assert page.source_key != other_page.source_key
    assert db.execute("SELECT count(*) FROM discovery_corpus_prefix_inputs").fetchone() == (2,)
    assert db.execute("SELECT count(*) FROM discovery_corpus_prefix_maxima").fetchone() == (2,)


def test_closed_source_cannot_commit_an_original_prefix(owned):
    db, key, _, _, work = owned
    anchor = HistoryJournalAnchor.from_mapping(work.original.source.anchor_at(0))
    page = owner.prepare_corpus_prefix(db, key, work, anchor)
    work.original.source.close()
    with write(db), pytest.raises(StaleJournalTask, match="closed"):
        owner.apply_corpus_prefix(db, key, work, page)


@pytest.mark.parametrize(
    "field,value", [("prefixes", "01"), ("journals", "2"), ("version", "-1"), ("version", "100")]
)
def test_noncanonical_or_inconsistent_registry_counters_fail_closed(owned, field, value):
    db, key, admission, manifest, _ = owned
    allowed = {"prefixes", "journals", "version"}
    assert field in allowed
    db.execute("UPDATE discovery_corpus_prefix_headers SET " + field + "=?", (value,))
    with pytest.raises(StaleArchiveTask):
        owner.corpus_prefix_progress(db, key, admission, manifest)


def _measure(owned, native, tmp_path, monkeypatch, record_property, mode):
    db, key, admission, manifest, work = owned
    operations = {
        "begin": owner.begin_corpus_prefix_inputs,
        "progress": owner.corpus_prefix_progress,
        "prepare": owner.prepare_corpus_prefix,
        "apply": owner.apply_corpus_prefix,
        "maximum": owner.corpus_prefix_maximum,
    }
    operation = operations[mode]
    binding = growth_binding(operation)
    projection = project_bindings((binding,))
    digest = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    actual_anchor = JournalSummary.anchor_at
    actual_encode = owner.canonical_json_bytes
    actual_hash = sources.sha256
    samples = []
    anchor = HistoryJournalAnchor.from_mapping(work.original.source.anchor_at(0))
    for retained in (1, 17, 1025):
        with write(db):
            for index in range(retained):
                unrelated = f"unrelated-{retained}-{index}"
                db.execute(
                    "INSERT INTO discovery_corpus_prefix_headers VALUES (?,?,?,?,'0','0','0','0')",
                    (unrelated, key.admission_sha256, "worker", "f" * 64),
                )
                db.execute(
                    "INSERT INTO discovery_corpus_prefix_maxima VALUES (?,?,?,?,?,'0',0)",
                    (
                        unrelated,
                        key.admission_sha256,
                        "f" * 64,
                        "unrelated" * 513,
                        "damaged unrelated anchor" * 129,
                    ),
                )
        for cache in ("warm", "reopened"):
            if mode == "begin" and cache == "warm":
                db.execute(
                    "DELETE FROM discovery_corpus_prefix_headers WHERE build_id=?", (key.build_id,)
                )
            if cache == "reopened":
                path = db.execute("PRAGMA database_list").fetchone()[2]
                db = sqlite3.connect(path, isolation_level=None)
            page = owner.prepare_corpus_prefix(db, key, work, anchor) if mode == "apply" else None
            tables = (
                "discovery_archive_tasks",
                "discovery_archive_journals",
                "discovery_corpus_source_manifests",
                "discovery_corpus_source_inputs",
                "discovery_corpus_prefix_headers",
                "discovery_corpus_prefix_inputs",
                "discovery_corpus_prefix_maxima",
                "discovery_corpus_dependency_cursors",
            )
            rows = []
            for table in tables:
                rows += db.execute(
                    "SELECT * FROM " + table + " WHERE build_id=?", (key.build_id,)
                ).fetchall()
            widths = [sum(map(metadata_octets, row)) for row in rows]
            widths.append(
                sum(
                    map(
                        metadata_octets,
                        (
                            key.build_id,
                            key.admission_sha256,
                            key.worker_id,
                            admission.root_json,
                            admission.binding_json,
                            work.original.source_binding_json,
                            canonical_json_bytes(anchor.to_mapping()),
                        ),
                    )
                )
            )
            dimensions = {
                "registered_inputs": sum(
                    db.execute("SELECT count(*) FROM " + table).fetchone()[0] for table in tables
                ),
                "metadata_octets": max(widths),
            }
            recorder = MeasuredConnection(db)
            paused = [False]

            def native_anchor(summary, sequence, recorder=recorder, paused=paused):
                recorder.metrics["native_anchor_calls"] += 1
                paused[0] = True
                try:
                    return actual_anchor(summary, sequence)
                finally:
                    paused[0] = False

            def encode(value, recorder=recorder, paused=paused):
                encoded = actual_encode(value)
                if not paused[0]:
                    recorder.metrics["encoded_octets"] += len(encoded)
                return encoded

            def hashed(value=b"", recorder=recorder):
                recorder.metrics["hashed_octets"] += len(value)
                return actual_hash(value)

            def tick(recorder=recorder):
                recorder.metrics["sqlite_vm_instructions"] += 1
                return 0

            original_execute = recorder.execute

            def execute(sql, parameters=(), recorder=recorder, original_execute=original_execute):
                result = original_execute(sql, parameters)
                if sql.startswith(("INSERT", "UPDATE")):
                    recorder.metrics["written_octets"] += sum(map(metadata_octets, parameters))
                return result

            recorder.execute = execute
            db.set_progress_handler(tick, 1)
            try:
                with monkeypatch.context() as patch:
                    patch.setattr(JournalSummary, "anchor_at", native_anchor)
                    patch.setattr(owner, "canonical_json_bytes", encode)
                    patch.setattr(sources, "canonical_json_bytes", encode)
                    patch.setattr(journal_work, "canonical_json_bytes", encode)
                    patch.setitem(
                        owner.require_canonical_json.__globals__, "canonical_json_bytes", encode
                    )
                    patch.setattr(sources, "sha256", hashed)
                    if mode == "begin":
                        with write(db):
                            result = operation(recorder, key, admission, manifest)
                    elif mode == "progress":
                        result = operation(recorder, key, admission, manifest)
                    elif mode == "prepare":
                        result = operation(recorder, key, work, anchor)
                    elif mode == "apply":
                        with write(db):
                            result = operation(recorder, key, work, page)
                    else:
                        result = operation(
                            recorder, key, admission, manifest, work.original.source_key
                        )
            finally:
                db.set_progress_handler(None, 0)
            assert result is not None or mode == "maximum"
            sample = {
                "declaration": binding.declaration.id,
                "declaration_sha256": digest,
                "dimensions": dimensions,
                "metrics": {
                    bound.metric: recorder.metrics[bound.metric]
                    for bound in binding.declaration.bounds
                },
                "delegates": {},
            }
            validate_execution_samples(projection, binding.declaration.id, [sample])
            samples.append(sample)
            if cache == "reopened":
                db.close()
                db = owned[0]
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "canonical-discovery-prefix-begin/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_prefix_inputs.begin_corpus_prefix_inputs",
)
def test_measured_prefix_begin(owned, native, tmp_path, monkeypatch, record_property):
    _measure(owned, native, tmp_path, monkeypatch, record_property, "begin")


@pytest.mark.growth_witness(
    "canonical-discovery-prefix-progress/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_prefix_inputs.corpus_prefix_progress",
)
def test_measured_prefix_progress(owned, native, tmp_path, monkeypatch, record_property):
    _measure(owned, native, tmp_path, monkeypatch, record_property, "progress")


@pytest.mark.growth_witness(
    "canonical-discovery-prefix-prepare/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_prefix_inputs.prepare_corpus_prefix",
)
def test_measured_prefix_prepare(owned, native, tmp_path, monkeypatch, record_property):
    _measure(owned, native, tmp_path, monkeypatch, record_property, "prepare")


@pytest.mark.growth_witness(
    "canonical-discovery-prefix-apply/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_prefix_inputs.apply_corpus_prefix",
)
def test_measured_prefix_apply(owned, native, tmp_path, monkeypatch, record_property):
    _measure(owned, native, tmp_path, monkeypatch, record_property, "apply")


@pytest.mark.growth_witness(
    "canonical-discovery-prefix-maximum/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_prefix_inputs.corpus_prefix_maximum",
)
def test_measured_prefix_maximum(owned, native, tmp_path, monkeypatch, record_property):
    request(owned[0], owned[1], owned[4], 0)
    _measure(owned, native, tmp_path, monkeypatch, record_property, "maximum")


@pytest.mark.parametrize("fault", ["source", "anchor", "version", "missing"])
def test_shared_maximum_preserves_full_original_metadata(owned, fault):
    db, key, admission, manifest, work = owned
    page, _ = request(db, key, work, 0)
    if fault == "source":
        db.execute("UPDATE discovery_corpus_prefix_maxima SET binding_json='different'")
    elif fault == "anchor":
        changed = page.anchor.to_mapping()
        changed["through"]["entry_id"] = "urn:uuid:33333333-3333-4333-8333-333333333333"
        db.execute(
            "UPDATE discovery_corpus_prefix_maxima SET anchor_json=?",
            (canonical_json_bytes(changed).decode(),),
        )
    elif fault == "version":
        db.execute("UPDATE discovery_corpus_prefix_maxima SET version='01'")
    else:
        db.execute("DELETE FROM discovery_corpus_prefix_maxima")
    with pytest.raises(StaleArchiveTask):
        owner.corpus_prefix_maximum(db, key, admission, manifest, work.original.source_key)
