from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import replace

import pytest
from riverhog_archive_contracts import BOUND_HISTORY_EXTENT, RETAINED_HISTORY_EXTENT
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core.canonical_discovery_archive_work import (
    ArchiveTaskKey,
    StaleArchiveTask,
    apply_archive_descriptor,
    archive_metadata_progress,
    begin_archive_metadata,
    initialize_archive_work,
    prepare_archive_descriptor,
)
from riverhog_core.canonical_discovery_corpus_members import (
    CorpusMemberRegistration,
    apply_corpus_member,
    begin_corpus_members,
    initialize_corpus_members,
    prepare_corpus_member,
)
from riverhog_core.canonical_discovery_history_inputs import (
    CorpusImportWork,
    _register_corpus_history,
    apply_corpus_import,
    begin_corpus_history_inputs,
    corpus_history_progress,
    next_corpus_history_input,
    next_corpus_history_structure,
    prepare_corpus_import,
)
from riverhog_core.canonical_discovery_history_work import (
    HistorySetWork,
    apply_history_import,
    apply_history_record,
    history_set_progress,
    prepare_history_record,
)
from riverhog_core.canonical_discovery_member_work import archive_member_progress

from tests.support.canonical_discovery_work import metadata_octets, observe_metadata, write
from tests.unit.test_canonical_discovery_history_work import fixture


def setup(path, *, members=3, empty=False, distinct=False):
    reader, objects, bindings, histories, admission, reads = fixture(
        count=3,
        members=members,
        inherited=not empty,
        import_targets=(0, 1),
        distinct_import_sets=distinct,
        empty_imports=empty,
        empty_source_imports=True,
    )
    db = sqlite3.connect(path, isolation_level=None)
    initialize_archive_work(db)
    initialize_corpus_members(db)
    key = ArchiveTaskKey("build", admission.identity, "worker")
    with write(db):
        begin_archive_metadata(db, key, admission)
    while not archive_metadata_progress(db, key).complete:
        page = prepare_archive_descriptor(db, key, admission, reader)
        with write(db):
            apply_archive_descriptor(db, key, admission, page)
    with write(db):
        begin_corpus_members(db, key, admission)
    member = CorpusMemberRegistration(admission, reader)
    while not archive_member_progress(db, key).complete:
        page = prepare_corpus_member(db, key, member)
        with write(db):
            apply_corpus_member(db, key, member, page)
    return db, key, admission, reader, histories, objects, reads


def structures(db, key, admission, reader):
    while (authority := next_corpus_history_structure(db, key, admission)) is not None:
        work = HistorySetWork(admission, reader, authority)
        while not history_set_progress(db, key, work).complete:
            page = prepare_history_record(db, key, work)
            with write(db):
                apply_history_record(db, key, work, page)


def advance(db, key, owner):
    page = prepare_corpus_import(db, key, owner)
    with write(db):
        return apply_corpus_import(db, key, owner, page)


def test_shared_original_import_closure_resumes_and_keeps_recorded_extents(tmp_path):
    path = tmp_path / "history.sqlite"
    db, key, admission, reader, histories, _, reads = setup(path, members=17)
    try:
        initial = corpus_history_progress(db, key, admission)
        assert (initial.root_sets, initial.import_sets) == (1, 1)
        structures(db, key, admission, reader)
        task = next_corpus_history_input(db, key, admission, kind="imports")
        work = CorpusImportWork(HistorySetWork(admission, reader, task.authority))
        result = advance(db, key, work)
        assert result.records == 1 and result.pending
        db.close()
        db = sqlite3.connect(path, isolation_level=None)
        work = CorpusImportWork(HistorySetWork(admission, reader, task.authority))
        reads.clear()
        advance(db, key, work)
        advance(db, key, work)
        assert db.execute("SELECT count(*) FROM discovery_history_import_links").fetchone() == (2,)
        assert db.execute("SELECT count(*) FROM discovery_history_import_proofs").fetchone() == (2,)
        # The original roots are retained; imported-only roots remain bound.
        root_extents = db.execute(
            "SELECT required_extent FROM discovery_corpus_history_inputs "
            "WHERE kind='roots' ORDER BY required_extent"
        ).fetchall()
        assert root_extents == [(BOUND_HISTORY_EXTENT,), (RETAINED_HISTORY_EXTENT,)]
        assert db.execute(
            "SELECT records FROM discovery_corpus_history_inputs WHERE kind='imports' AND pending=0"
        ).fetchone() == ("2",)
        # The leaf histories share a verified empty import set with its own terminal.
        structures(db, key, admission, reader)
        empty = next_corpus_history_input(db, key, admission, kind="imports")
        assert empty.authority.record_count == 0
        advance(db, key, CorpusImportWork(HistorySetWork(admission, reader, empty.authority)))
        assert next_corpus_history_input(db, key, admission, kind="imports") is None
        progress = corpus_history_progress(db, key, admission)
        assert progress.pending_imports == 0 and progress.pending_roots == 2
        assert not any(path.endswith(histories[0].identity + ".json.age") for path in reads)
    finally:
        db.close()


@pytest.mark.parametrize(
    "table",
    [
        "discovery_history_import_links",
        "discovery_corpus_history_inputs",
        "discovery_corpus_history_headers",
    ],
)
def test_import_child_requirements_and_parent_cursor_roll_back_together(tmp_path, table):
    db, key, admission, reader, _, _, _ = setup(tmp_path / "history.sqlite")
    try:
        structures(db, key, admission, reader)
        task = next_corpus_history_input(db, key, admission, kind="imports")
        work = CorpusImportWork(HistorySetWork(admission, reader, task.authority))
        page = prepare_corpus_import(db, key, work)
        before = corpus_history_progress(db, key, admission)
        event = "UPDATE" if table == "discovery_corpus_history_headers" else "INSERT"
        db.execute(
            f"CREATE TRIGGER fail_seed BEFORE {event} ON {table} "
            "BEGIN SELECT RAISE(ABORT,'injected seed failure'); END"
        )
        with write(db), pytest.raises(sqlite3.IntegrityError, match="injected"):
            apply_corpus_import(db, key, work, page)
        assert corpus_history_progress(db, key, admission) == before
        assert db.execute("SELECT count(*) FROM discovery_history_import_links").fetchone() == (0,)
        assert db.execute("SELECT count(*) FROM discovery_history_import_proofs").fetchone() == (0,)
        current = next_corpus_history_input(db, key, admission, kind="imports")
        assert current.records == 0 and current.after_key is None
        db.execute("DROP TRIGGER fail_seed")
        with write(db):
            result = apply_corpus_import(db, key, work, page)
        assert result.records == 1
        with write(db), pytest.raises(StaleArchiveTask, match="continuation changed"):
            apply_corpus_import(db, key, work, page)
        assert advance(db, key, work).records == 2
    finally:
        db.close()


def test_empty_original_set_has_explicit_terminal_and_exact_source_fences(tmp_path):
    db, key, admission, reader, _, _, _ = setup(tmp_path / "history.sqlite", empty=True)
    try:
        task = next_corpus_history_input(db, key, admission, kind="imports")
        work = CorpusImportWork(HistorySetWork(admission, reader, task.authority))
        with pytest.raises(StaleArchiveTask, match="not completely verified"):
            prepare_corpus_import(db, key, work)
        structures(db, key, admission, reader)
        page = prepare_corpus_import(db, key, work)
        assert page.child is None
        with write(db), pytest.raises(StaleArchiveTask, match="exact detached"):
            apply_corpus_import(db, key, work, replace(page))
        with write(db):
            result = apply_corpus_import(db, key, work, page)
        assert result.records == 0 and not result.pending
        assert corpus_history_progress(db, key, admission).pending_imports == 0
        assert db.execute("SELECT count(*) FROM discovery_history_import_links").fetchone() == (0,)
        with pytest.raises(StaleArchiveTask, match="source/build/worker"):
            next_corpus_history_input(db, replace(key, worker_id="other"), admission, kind="roots")
    finally:
        db.close()


@pytest.mark.parametrize("damage", ["missing", "header", "worker", "cursor", "structural"])
def test_corrupt_original_queues_and_continuations_fail_closed(tmp_path, damage):
    db, key, admission, reader, _, _, _ = setup(tmp_path / "history.sqlite")
    try:
        structures(db, key, admission, reader)
        task = next_corpus_history_input(db, key, admission, kind="imports")
        work = CorpusImportWork(HistorySetWork(admission, reader, task.authority))
        if damage == "missing":
            db.execute("DELETE FROM discovery_corpus_history_inputs WHERE kind='imports'")
        elif damage == "header":
            db.execute("UPDATE discovery_corpus_history_headers SET pending_imports='0'")
        elif damage == "worker":
            db.execute(
                "UPDATE discovery_corpus_history_inputs SET worker_id='other' WHERE kind='imports'"
            )
        elif damage == "cursor":
            advance(db, key, work)
            db.execute(
                "UPDATE discovery_corpus_history_inputs SET after_key=? WHERE kind='imports'",
                ("f" * 64,),
            )
        else:
            db.execute(
                "UPDATE discovery_history_sets SET complete=0 WHERE records_sha256=?",
                (task.authority.records_sha256,),
            )
        with pytest.raises(StaleArchiveTask):
            next_corpus_history_input(db, key, admission, kind="imports")
            prepare_corpus_import(db, key, work)
    finally:
        db.close()


def test_shared_imports_keep_each_original_set_incidence_and_upgrade_root_extent(tmp_path):
    db, key, admission, reader, _, _, _ = setup(tmp_path / "history.sqlite", distinct=True)
    try:
        while True:
            structures(db, key, admission, reader)
            task = next_corpus_history_input(db, key, admission, kind="imports")
            if task is None:
                break
            work = CorpusImportWork(HistorySetWork(admission, reader, task.authority))
            while task.pending:
                task = advance(db, key, work)
        assert db.execute("SELECT count(*) FROM discovery_history_import_proofs").fetchone() == (2,)
        assert db.execute("SELECT count(*) FROM discovery_history_import_links").fetchone() == (4,)
        assert db.execute(
            "SELECT DISTINCT required_extent FROM discovery_corpus_history_inputs "
            "WHERE kind='roots'"
        ).fetchall() == [(RETAINED_HISTORY_EXTENT,)]
        assert corpus_history_progress(db, key, admission).pending_imports == 0
    finally:
        db.close()


def test_missing_shared_header_cannot_recreate_empty_work_after_member_advancement(tmp_path):
    db, key, admission, _, _, _, _ = setup(tmp_path / "history.sqlite")
    try:
        db.execute("DELETE FROM discovery_corpus_history_headers")
        with write(db), pytest.raises(StaleArchiveTask, match="advanced member source"):
            begin_corpus_history_inputs(db, key, admission)
        assert db.execute("SELECT count(*) FROM discovery_corpus_history_headers").fetchone() == (
            0,
        )
    finally:
        db.close()


def _measured(mode, tmp_path, monkeypatch, record_property):
    from riverhog_growth_contracts import (
        growth_binding,
        project_bindings,
        validate_execution_samples,
    )

    operation = {
        "begin": begin_corpus_history_inputs,
        "progress": corpus_history_progress,
        "register": _register_corpus_history,
        "next": next_corpus_history_input,
        "structure": next_corpus_history_structure,
        "prepare": prepare_corpus_import,
        "apply": apply_corpus_import,
    }[mode]
    binding = growth_binding(operation)
    projection = project_bindings((binding,))
    digest = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    base, key, admission, reader, _, objects, _ = setup(tmp_path / "base.sqlite")
    if mode != "structure":
        structures(base, key, admission, reader)
    original = next_corpus_history_input(base, key, admission, kind="imports")
    samples = []
    phases = (
        ("insert", "retry", "upgrade")
        if mode == "register"
        else ("first", "last", "terminal", "empty")
        if mode in ("prepare", "apply")
        else ("active", "empty")
        if mode == "structure"
        else ("ordinary",)
    )
    try:
        for width in (1, 17, 1025):
            for reopen in (False, True):
                for phase in phases:
                    path = tmp_path / f"{mode}-{width}-{reopen}-{phase}.sqlite"
                    db = sqlite3.connect(path, isolation_level=None)
                    base.backup(db)
                    work = CorpusImportWork(HistorySetWork(admission, reader, original.authority))
                    descriptor = None
                    page = None
                    if phase in ("last", "terminal", "empty") and mode in ("prepare", "apply"):
                        advance(db, key, work)
                    if phase in ("terminal", "empty") and mode in ("prepare", "apply"):
                        advance(db, key, work)
                    if phase == "empty" and mode in ("prepare", "apply"):
                        advance(db, key, work)
                        structures(db, key, admission, reader)
                        task = next_corpus_history_input(db, key, admission, kind="imports")
                        assert task.authority.record_count == 0
                        work = CorpusImportWork(HistorySetWork(admission, reader, task.authority))
                    if mode == "structure" and phase == "empty":
                        structures(db, key, admission, reader)
                    if mode == "register":
                        page = prepare_corpus_import(db, key, work)
                        descriptor = page.descriptor
                        with write(db):
                            if phase == "insert":
                                apply_history_import(db, key, work.history, page.child)
                            else:
                                apply_corpus_import(db, key, work, page)
                    if mode == "apply":
                        page = prepare_corpus_import(db, key, work)
                    # Dense completed controls are deliberately unreadable metadata.
                    # None is selected or decoded by a pending work selector.
                    with write(db):
                        for index in range(width):
                            identity = hashlib.sha256(f"completed-{index}".encode()).hexdigest()
                            db.execute(
                                "INSERT INTO discovery_corpus_history_inputs "
                                "VALUES (?,?,?,?,?,?,'all','0',0,NULL,'0')",
                                (
                                    key.build_id,
                                    key.admission_sha256,
                                    "imports",
                                    identity,
                                    key.worker_id,
                                    "unreadable completed authority " * 30,
                                ),
                            )
                            db.execute(
                                "INSERT INTO discovery_history_sets "
                                "VALUES (?,?,?,?,?,'0','0',0,?,1,'0')",
                                (
                                    key.build_id,
                                    key.admission_sha256,
                                    identity,
                                    key.worker_id,
                                    "unreadable completed authority",
                                    "unreadable completed checkpoint",
                                ),
                            )
                        db.execute(
                            "UPDATE discovery_corpus_history_headers SET import_sets="
                            "CAST(CAST(import_sets AS INTEGER)+? AS TEXT),version="
                            "CAST(CAST(version AS INTEGER)+? AS TEXT)",
                            (width, width),
                        )
                    if reopen:
                        db.close()
                        db = sqlite3.connect(path, isolation_level=None)
                        if mode == "prepare":
                            work = CorpusImportWork(
                                HistorySetWork(admission, reader, work.history.authority)
                            )
                    tables = [
                        row[0]
                        for row in db.execute(
                            "SELECT name FROM sqlite_master WHERE type='table' "
                            "AND name LIKE 'discovery_%'"
                        ).fetchall()
                    ]
                    registered = sum(
                        db.execute("SELECT count(*) FROM " + table).fetchone()[0]
                        for table in tables
                    )
                    metadata = max(
                        sum(map(metadata_octets, row))
                        for table in tables
                        for row in db.execute("SELECT * FROM " + table).fetchall()
                    )
                    if page is not None and page.child is not None:
                        metadata = max(
                            metadata,
                            len(page.child.proof_json.encode()),
                            len(page.child.descriptor_json.encode()),
                        )
                    transactional = mode in ("begin", "register", "apply")
                    if transactional:
                        db.execute("BEGIN IMMEDIATE")
                    try:
                        with observe_metadata(db, monkeypatch, reader=reader) as recorder:
                            if mode in ("begin", "progress"):
                                result = operation(recorder, key, admission)
                                assert result.pending_roots >= 1
                            elif mode == "register":
                                result = operation(
                                    recorder,
                                    key,
                                    admission,
                                    descriptor,
                                    extent=(
                                        RETAINED_HISTORY_EXTENT
                                        if phase == "upgrade"
                                        else BOUND_HISTORY_EXTENT
                                    ),
                                )
                                assert result.pending_roots == 2
                            elif mode == "next":
                                result = operation(recorder, key, admission, kind="imports")
                                assert result.authority == original.authority
                            elif mode == "structure":
                                result = operation(recorder, key, admission)
                                assert (result is None) == (phase == "empty")
                            elif mode == "prepare":
                                result = operation(recorder, key, work)
                                assert (result.child is None) == (phase in ("terminal", "empty"))
                            else:
                                result = operation(recorder, key, work, page)
                                assert result.pending == (phase in ("first", "last"))
                    finally:
                        if transactional:
                            db.commit()
                    source_octets = max((len(objects[p]) for p in recorder.source_paths), default=0)
                    sample = {
                        "declaration": binding.declaration.id,
                        "declaration_sha256": digest,
                        "dimensions": {
                            "registered_inputs": registered,
                            "metadata_octets": metadata,
                            "source_octets": source_octets,
                        },
                        "metrics": {
                            bound.metric: recorder.metrics[bound.metric]
                            for bound in binding.declaration.bounds
                        },
                        "delegates": {},
                    }
                    validate_execution_samples(projection, binding.declaration.id, [sample])
                    samples.append(sample)
                    db.close()
    finally:
        base.close()
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "canonical-discovery-history-input-begin/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_history_inputs.begin_corpus_history_inputs",
)
def test_measured_history_input_begin(tmp_path, monkeypatch, record_property):
    _measured("begin", tmp_path, monkeypatch, record_property)


@pytest.mark.growth_witness(
    "canonical-discovery-history-input-progress/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_history_inputs.corpus_history_progress",
)
def test_measured_history_input_progress(tmp_path, monkeypatch, record_property):
    _measured("progress", tmp_path, monkeypatch, record_property)


@pytest.mark.growth_witness(
    "canonical-discovery-history-input-register/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_history_inputs._register_corpus_history",
)
def test_measured_history_input_register(tmp_path, monkeypatch, record_property):
    _measured("register", tmp_path, monkeypatch, record_property)


@pytest.mark.growth_witness(
    "canonical-discovery-history-input-next/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_history_inputs.next_corpus_history_input",
)
def test_measured_history_input_next(tmp_path, monkeypatch, record_property):
    _measured("next", tmp_path, monkeypatch, record_property)


@pytest.mark.growth_witness(
    "canonical-discovery-history-input-structure/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_history_inputs.next_corpus_history_structure",
)
def test_measured_history_input_structure(tmp_path, monkeypatch, record_property):
    _measured("structure", tmp_path, monkeypatch, record_property)


@pytest.mark.growth_witness(
    "canonical-discovery-history-input-prepare/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_history_inputs.prepare_corpus_import",
)
def test_measured_history_input_prepare(tmp_path, monkeypatch, record_property):
    _measured("prepare", tmp_path, monkeypatch, record_property)


@pytest.mark.growth_witness(
    "canonical-discovery-history-input-apply/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_history_inputs.apply_corpus_import",
)
def test_measured_history_input_apply(tmp_path, monkeypatch, record_property):
    _measured("apply", tmp_path, monkeypatch, record_property)


@pytest.mark.parametrize("damage", ["missing", "header"])
def test_root_requirement_queue_cannot_lose_its_independent_pending_count(tmp_path, damage):
    db, key, admission, _, _, _, _ = setup(tmp_path / "history.sqlite")
    try:
        if damage == "missing":
            db.execute("DELETE FROM discovery_corpus_history_inputs WHERE kind='roots'")
        else:
            db.execute("UPDATE discovery_corpus_history_headers SET pending_roots='0'")
        with pytest.raises(StaleArchiveTask, match="pending queue"):
            next_corpus_history_input(db, key, admission, kind="roots")
    finally:
        db.close()
