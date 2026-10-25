from __future__ import annotations

import json
import sqlite3
from contextlib import ExitStack, closing
from dataclasses import replace

import pytest
from riverhog_archive_contracts import (
    BOUND_HISTORY_EXTENT,
    MEMBER_HISTORY_IMPORTS_SCHEMA,
    MEMBER_HISTORY_ROOTS_SCHEMA,
    RETAINED_HISTORY_EXTENT,
    HistoryJournalAnchor,
    MemberHistoryDocument,
    MemberHistoryPrimary,
    MemberHistoryRoot,
    ProvenanceRootDocument,
)
from riverhog_core import canonical_discovery_corpus_inputs as inputs
from riverhog_core import canonical_discovery_prefix_inputs as prefixes
from riverhog_core import canonical_discovery_root_work as owner
from riverhog_core.canonical_discovery_archive_work import (
    ArchiveTaskKey,
    StaleArchiveTask,
    admit_archive_journal,
    apply_archive_descriptor,
    archive_metadata_progress,
    begin_archive_metadata,
    initialize_archive_work,
    prepare_archive_descriptor,
)
from riverhog_core.canonical_discovery_corpus_members import (
    begin_corpus_members,
    initialize_corpus_members,
)
from riverhog_core.canonical_discovery_history_inputs import (
    _register_corpus_history,
    corpus_history_progress,
)
from riverhog_core.canonical_discovery_history_work import (
    ArchiveHistoryWork,
    HistorySetWork,
    apply_history_descriptor,
    prepare_history_descriptor,
)
from riverhog_core.canonical_discovery_journal_work import StaleJournalTask
from riverhog_core.canonical_discovery_member_work import (
    ArchiveMemberRegistration,
    apply_archive_member,
    archive_member_progress,
    prepare_archive_member,
)
from riverhog_core.canonical_discovery_snapshot_work import (
    apply_history_snapshot,
)
from riverhog_provenance import validate_journal

from tests.support.canonical_discovery_work import corrected_journal_fixture, write
from tests.support.provenance_archive import archive_reader_fixture, discovery_archive_admission
from tests.unit.test_canonical_discovery_corpus_inputs import commit_input
from tests.unit.test_canonical_discovery_history_inputs import structures
from tests.unit.test_canonical_discovery_history_work import record_set


@pytest.fixture(scope="module")
def original():
    fixture, raw, _ = corrected_journal_fixture()
    with validate_journal(raw, catalog=fixture.catalog) as summary:
        yield raw, summary


def setup(path, original, *, extent=RETAINED_HISTORY_EXTENT, missing=False, selected_roots=None):
    raw, native = original
    anchors = tuple(HistoryJournalAnchor.from_mapping(native.anchor_at(i)) for i in range(3))
    selected = [
        MemberHistoryRoot(value, "bound" if i == 0 else "retained")
        for i, value in enumerate(anchors)
    ]
    if missing:
        selected[-1] = replace(
            selected[-1],
            journal=replace(
                selected[-1].journal,
                journal_id="urn:uuid:11111111-1111-4111-8111-111111111111",
            ),
        )
    if selected_roots is not None:
        selected = list(selected_roots)
    objects = {}
    roots = record_set(
        objects,
        MEMBER_HISTORY_ROOTS_SCHEMA,
        [{"key": value.key, "value": value.to_mapping()} for value in selected],
    )
    imports = record_set(objects, MEMBER_HISTORY_IMPORTS_SCHEMA, [])
    # This owner fixture authenticates exact original Native roots; synthetic member
    # identities do not claim complete member-State or Native corpus semantics.
    histories = tuple(
        MemberHistoryDocument(
            f"{i:064x}",
            7,
            "a" * 64,
            MemberHistoryPrimary(
                next(value.journal for value in selected if value.inclusion == "bound"),
                "urn:uuid:33333333-3333-4333-8333-333333333333",
            ),
            roots,
            imports,
        )
        for i in range(2)
    )
    reader, archived, _, _ = archive_reader_fixture(
        raw, native.journal_id, histories=histories, history_objects=objects
    )
    admission = discovery_archive_admission(
        ProvenanceRootDocument.from_json_bytes(archived["provenance/root.json.age"]),
        total_bytes=14,
    )
    db = sqlite3.connect(path, isolation_level=None)
    initialize_archive_work(db)
    initialize_corpus_members(db)
    owner.initialize_corpus_root_work(db)
    key = ArchiveTaskKey("build", admission.identity, "worker")
    with write(db):
        begin_archive_metadata(db, key, admission)
    while not archive_metadata_progress(db, key).complete:
        page = prepare_archive_descriptor(db, key, admission, reader)
        with write(db):
            apply_archive_descriptor(db, key, admission, page)
    with write(db):
        begin_corpus_members(db, key, admission)
    member = ArchiveMemberRegistration(admission, reader)
    while not archive_member_progress(db, key).complete:
        page = prepare_archive_member(db, key, member)
        with write(db):
            apply_archive_member(db, key, member, page)
    descriptors = ArchiveHistoryWork(admission, reader)
    for history in histories:
        page = prepare_history_descriptor(db, key, descriptors, history.artifact_id)
        with write(db):
            apply_history_descriptor(db, key, descriptors, page)
            _register_corpus_history(db, key, admission, history, extent=extent)
    structures(db, key, admission, reader)
    with write(db):
        inputs.begin_corpus_source_inputs(db, key, admission)
    with admit_archive_journal(db, key, admission, native) as source:
        commit_input(db, key, inputs.CorpusSourceInputWork(admission, source))
    with write(db):
        manifest = inputs.complete_corpus_source_inputs(db, key, admission)
        prefixes.begin_corpus_prefix_inputs(db, key, admission, manifest)
    db.close()
    return key, admission, reader, manifest, histories, anchors


@pytest.fixture(scope="module")
def qualified(tmp_path_factory, original):
    path = tmp_path_factory.mktemp("root-originals") / "qualified.sqlite"
    return path, setup(path, original)


@pytest.fixture
def owned(tmp_path, qualified, original):
    initial, values = qualified
    key, admission, reader, manifest, histories, anchors = values
    path = tmp_path / "root.sqlite"
    db = sqlite3.connect(path, isolation_level=None)
    with closing(sqlite3.connect(initial)) as source:
        source.backup(db)
    with ExitStack() as leases:
        source = leases.enter_context(admit_archive_journal(db, key, admission, original[1]))
        prefix = prefixes.CorpusPrefixWork(admission, manifest, source)
        work = owner.CorpusRootWork(HistorySetWork(admission, reader, histories[0].roots), manifest)
        try:
            yield db, key, work, {original[1].journal_id: prefix}, path, histories, anchors, leases
        finally:
            db.close()


def prepare(db, key, work, works):
    return owner.prepare_corpus_root(db, key, work, resolve=works.__getitem__)


def advance(db, key, work, works):
    page = prepare(db, key, work, works)
    with write(db):
        result = owner.apply_corpus_root(db, key, work, page)
    return page, result


def finish(db, key, work, works):
    actions = []
    while owner.corpus_root_progress(db, key, work).requirement.pending:
        page, _ = advance(db, key, work, works)
        actions.append(page.action)
    return actions


def test_original_shared_root_selection_commits_exact_native_prefixes_once(owned):
    db, key, work, works, _, _, anchors, _ = owned
    assert corpus_history_progress(db, key, work.history.admission).root_sets == 1
    assert finish(db, key, work, works) == [
        "bound",
        "bound-done",
        "retained",
        "retained",
        "retained-done",
    ]
    saved = owner.corpus_root_progress(db, key, work)
    assert (saved.bound_records, saved.retained_records, saved.version) == (1, 2, 5)
    assert saved.bound_complete and saved.retained_complete
    assert not saved.requirement.pending
    assert corpus_history_progress(db, key, work.history.admission).pending_roots == 0
    assert db.execute("SELECT count(*) FROM discovery_history_snapshot_links").fetchone() == (3,)
    assert db.execute("SELECT count(*) FROM discovery_corpus_prefix_inputs").fetchone() == (3,)
    assert (
        prefixes.corpus_prefix_progress(db, key, work.history.admission, work.manifest).journals
        == 1
    )
    source = next(iter(works.values()))
    assert (
        prefixes.corpus_prefix_maximum(
            db, key, work.history.admission, work.manifest, source.original.source_key
        ).anchor
        == anchors[2]
    )
    with pytest.raises(StaleArchiveTask, match="already complete"):
        prepare(db, key, work, works)


def test_restart_keeps_original_positions_and_last_child_receipt(owned):
    db, key, work, works, path, _, _, _ = owned
    advance(db, key, work, works)
    advance(db, key, work, works)
    advance(db, key, work, works)
    before = owner.corpus_root_progress(db, key, work)
    with closing(sqlite3.connect(path, isolation_level=None)) as reopened:
        fresh = owner.CorpusRootWork(
            HistorySetWork(work.history.admission, work.history.reader, work.history.authority),
            work.manifest,
        )
        assert owner.corpus_root_progress(reopened, key, fresh) == before
        assert finish(reopened, key, fresh, works) == ["retained", "retained-done"]
        assert reopened.execute(
            "SELECT count(*) FROM discovery_history_snapshot_links"
        ).fetchone() == (3,)


@pytest.mark.parametrize(
    "table,event",
    [
        ("discovery_history_snapshot_links", "INSERT"),
        ("discovery_history_snapshot_coverage", "INSERT"),
        ("discovery_corpus_prefix_inputs", "INSERT"),
        ("discovery_corpus_prefix_headers", "UPDATE"),
        ("discovery_corpus_root_cursors", "INSERT"),
        ("discovery_corpus_history_inputs", "UPDATE"),
    ],
)
def test_child_registration_and_original_cursor_rollback_together(owned, table, event):
    db, key, work, works, _, _, _, _ = owned
    page = prepare(db, key, work, works)
    before = owner.corpus_root_progress(db, key, work)
    db.execute(
        f"CREATE TRIGGER fail_root BEFORE {event} ON {table} "
        "BEGIN SELECT RAISE(ABORT,'injected root failure'); END"
    )
    with write(db), pytest.raises(sqlite3.IntegrityError, match="injected root"):
        owner.apply_corpus_root(db, key, work, page)
    assert owner.corpus_root_progress(db, key, work) == before
    assert db.execute("SELECT count(*) FROM discovery_history_snapshot_links").fetchone() == (0,)
    assert db.execute("SELECT count(*) FROM discovery_corpus_prefix_inputs").fetchone() == (0,)
    db.execute("DROP TRIGGER fail_root")
    with write(db):
        result = owner.apply_corpus_root(db, key, work, page)
    assert result.bound_records == 1
    with write(db), pytest.raises(StaleArchiveTask, match="continuation"):
        owner.apply_corpus_root(db, key, work, page)


def test_changed_prefix_header_rolls_back_preceding_snapshot_child(owned):
    db, key, work, works, _, _, anchors, _ = owned
    page = prepare(db, key, work, works)
    source = next(iter(works.values()))
    independent = prefixes.CorpusPrefixWork(
        work.history.admission, work.manifest, source.original.source
    )
    other = prefixes.prepare_corpus_prefix(db, key, independent, anchors[2])
    with write(db):
        prefixes.apply_corpus_prefix(db, key, independent, other)
    with write(db), pytest.raises(StaleArchiveTask, match="stale registry version"):
        owner.apply_corpus_root(db, key, work, page)
    assert owner.corpus_root_progress(db, key, work).bound_records == 0
    assert db.execute("SELECT count(*) FROM discovery_history_snapshot_links").fetchone() == (0,)


def test_uncomposed_snapshot_registration_cannot_supply_original_root_completeness(owned):
    db, key, work, works, _, _, _, _ = owned
    page = prepare(db, key, work, works)
    snapshot = work._snapshot
    with write(db):
        apply_history_snapshot(db, key, snapshot, page.snapshot)
    with pytest.raises(StaleArchiveTask, match="coverage disagree"):
        prepare(db, key, work, works)


@pytest.mark.parametrize(
    "damage",
    [
        "version",
        "count",
        "position",
        "receipt",
        "receipt-type",
        "snapshot",
        "prefix",
        "source",
        "worker",
        "authority",
        "maximum",
        "prefix-header",
    ],
)
def test_original_cursor_and_child_receipt_corruption_fail_closed(owned, damage):
    db, key, work, works, _, _, _, _ = owned
    advance(db, key, work, works)
    if damage == "version":
        db.execute("UPDATE discovery_corpus_root_cursors SET version='2'")
    elif damage == "count":
        db.execute("UPDATE discovery_corpus_root_cursors SET bound_records='0'")
    elif damage == "position":
        db.execute("UPDATE discovery_corpus_root_cursors SET bound_after='wrong'")
    elif damage == "receipt":
        db.execute("UPDATE discovery_corpus_root_cursors SET receipt_json=NULL")
    elif damage == "receipt-type":
        receipt = json.loads(
            db.execute("SELECT receipt_json FROM discovery_corpus_root_cursors").fetchone()[0]
        )
        receipt["source_key"] = 42
        db.execute(
            "UPDATE discovery_corpus_root_cursors SET receipt_json=?",
            (json.dumps(receipt, sort_keys=True, separators=(",", ":")),),
        )
    elif damage == "snapshot":
        db.execute("DELETE FROM discovery_history_snapshot_links")
    elif damage == "prefix":
        db.execute("DELETE FROM discovery_corpus_prefix_inputs")
    elif damage == "maximum":
        db.execute("DELETE FROM discovery_corpus_prefix_maxima")
    elif damage == "prefix-header":
        db.execute("DELETE FROM discovery_corpus_prefix_headers")
    elif damage == "source":
        db.execute("UPDATE discovery_corpus_source_inputs SET source_binding_json='{}'")
    elif damage == "worker":
        db.execute(
            "UPDATE discovery_corpus_history_inputs SET worker_id='other' WHERE kind='roots'"
        )
    else:
        db.execute("UPDATE discovery_history_sets SET complete=0")
    with pytest.raises(StaleArchiveTask):
        owner.corpus_root_progress(db, key, work)


def test_bound_terminal_upgrade_requeues_only_retained_work(tmp_path, original):
    path = tmp_path / "bound.sqlite"
    key, admission, reader, manifest, histories, _ = setup(
        path, original, extent=BOUND_HISTORY_EXTENT
    )
    with closing(sqlite3.connect(path, isolation_level=None)) as db:
        with admit_archive_journal(db, key, admission, original[1]) as source:
            works = {original[1].journal_id: prefixes.CorpusPrefixWork(admission, manifest, source)}
            work = owner.CorpusRootWork(
                HistorySetWork(admission, reader, histories[0].roots), manifest
            )
            assert finish(db, key, work, works) == ["bound", "bound-done"]
            before = owner.corpus_root_progress(db, key, work)
            assert before.bound_complete and not before.retained_complete
            with write(db):
                _register_corpus_history(
                    db, key, admission, histories[0], extent=RETAINED_HISTORY_EXTENT
                )
            resumed = owner.corpus_root_progress(db, key, work)
            assert resumed.bound_after == before.bound_after
            assert resumed.bound_records == before.bound_records
            assert corpus_history_progress(db, key, admission).pending_roots == 1
            assert finish(db, key, work, works) == ["retained", "retained", "retained-done"]
            assert db.execute(
                "SELECT count(*) FROM discovery_history_snapshot_links"
            ).fetchone() == (3,)


def test_bound_selection_omits_unarchived_retained_only_source_until_required(tmp_path, original):
    path = tmp_path / "missing.sqlite"
    key, admission, reader, manifest, histories, _ = setup(
        path, original, extent=BOUND_HISTORY_EXTENT, missing=True
    )
    with closing(sqlite3.connect(path, isolation_level=None)) as db:
        with admit_archive_journal(db, key, admission, original[1]) as source:
            works = {original[1].journal_id: prefixes.CorpusPrefixWork(admission, manifest, source)}
            work = owner.CorpusRootWork(
                HistorySetWork(admission, reader, histories[0].roots), manifest
            )
            assert finish(db, key, work, works) == ["bound", "bound-done"]
            with write(db):
                _register_corpus_history(
                    db, key, admission, histories[0], extent=RETAINED_HISTORY_EXTENT
                )
            with pytest.raises(KeyError):
                finish(db, key, work, works)
            assert owner.corpus_root_progress(db, key, work).requirement.pending
            assert corpus_history_progress(db, key, admission).pending_roots == 1


def test_exact_preparation_worker_manifest_extent_and_lease_fences(owned):
    db, key, work, works, _, histories, _, leases = owned
    page = prepare(db, key, work, works)
    with write(db), pytest.raises(StaleArchiveTask, match="exact detached"):
        owner.apply_corpus_root(db, key, work, replace(page))
    with pytest.raises(StaleArchiveTask):
        prepare(db, replace(key, worker_id="other"), work, works)
    changed = owner.CorpusRootWork(work.history, replace(work.manifest, manifest_sha256="f" * 64))
    with pytest.raises(StaleArchiveTask):
        prepare(db, key, changed, works)
    with write(db), pytest.raises(ValueError, match="no active"):
        prepare(db, key, work, works)
    leases.close()
    with write(db), pytest.raises(StaleJournalTask, match="lease is closed"):
        owner.apply_corpus_root(db, key, work, page)
    assert db.execute("SELECT count(*) FROM discovery_history_snapshot_links").fetchone() == (0,)


def test_terminal_pending_count_failure_rolls_back_original_extent_terminal(owned):
    db, key, work, works, _, _, _, _ = owned
    for _ in range(4):
        advance(db, key, work, works)
    page = prepare(db, key, work, works)
    assert page.action == "retained-done"
    before = owner.corpus_root_progress(db, key, work)
    db.execute(
        "CREATE TRIGGER fail_terminal BEFORE UPDATE ON discovery_corpus_history_headers "
        "BEGIN SELECT RAISE(ABORT,'injected terminal failure'); END"
    )
    with write(db), pytest.raises(sqlite3.IntegrityError, match="injected terminal"):
        owner.apply_corpus_root(db, key, work, page)
    assert owner.corpus_root_progress(db, key, work) == before
    assert corpus_history_progress(db, key, work.history.admission).pending_roots == 1
    db.execute("DROP TRIGGER fail_terminal")
    with write(db):
        result = owner.apply_corpus_root(db, key, work, page)
    assert not result.requirement.pending
    assert corpus_history_progress(db, key, work.history.admission).pending_roots == 0


def test_extent_upgrade_invalidates_detached_bound_terminal(tmp_path, original):
    path = tmp_path / "upgrade.sqlite"
    key, admission, reader, manifest, histories, _ = setup(
        path, original, extent=BOUND_HISTORY_EXTENT
    )
    with closing(sqlite3.connect(path, isolation_level=None)) as db:
        with admit_archive_journal(db, key, admission, original[1]) as source:
            works = {original[1].journal_id: prefixes.CorpusPrefixWork(admission, manifest, source)}
            work = owner.CorpusRootWork(
                HistorySetWork(admission, reader, histories[0].roots), manifest
            )
            advance(db, key, work, works)
            page = prepare(db, key, work, works)
            assert page.action == "bound-done"
            with write(db):
                _register_corpus_history(
                    db, key, admission, histories[0], extent=RETAINED_HISTORY_EXTENT
                )
            with write(db), pytest.raises(StaleArchiveTask, match="required extent changed"):
                owner.apply_corpus_root(db, key, work, page)
            assert finish(db, key, work, works) == [
                "bound-done",
                "retained",
                "retained",
                "retained-done",
            ]


def _projection(operation):
    import hashlib

    from riverhog_canonical_json import canonical_json_bytes
    from riverhog_growth_contracts import growth_binding, project_bindings

    binding = growth_binding(operation)
    projection = project_bindings((binding,))
    digest = hashlib.sha256(
        canonical_json_bytes(projection["declarations"][binding.declaration.id])
    ).hexdigest()
    return binding, projection, digest


def _sample(db, binding, digest, metrics, *, prepared=None):
    from tests.support.canonical_discovery_work import metadata_octets

    tables = [
        row[0]
        for row in db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'discovery_%'"
        ).fetchall()
    ]
    rows = [row for table in tables for row in db.execute("SELECT * FROM " + table).fetchall()]
    metadata = max((sum(map(metadata_octets, row)) for row in rows), default=0)
    if prepared is not None:
        metadata = max(metadata, len(str(prepared).encode()))
    return {
        "declaration": binding.declaration.id,
        "declaration_sha256": digest,
        "dimensions": {"registered_inputs": len(rows), "metadata_octets": metadata},
        "metrics": {bound.metric: metrics[bound.metric] for bound in binding.declaration.bounds},
        "delegates": {},
    }


def _measured(mode, tmp_path, qualified, original, monkeypatch, record_property):
    from riverhog_growth_contracts import validate_execution_samples

    from tests.support.canonical_discovery_work import observe_metadata

    operation = {
        "progress": owner.corpus_root_progress,
        "prepare": owner.prepare_corpus_root,
        "apply": owner.apply_corpus_root,
    }[mode]
    binding, projection, digest = _projection(operation)
    initial, values = qualified
    key, admission, reader, manifest, histories, _ = values
    samples, regimes = [], []
    steps = range(6 if mode == "progress" else 5)
    for width in (1, 17, 1025):
        for reopen in (False, True):
            for step in steps:
                path = tmp_path / f"{mode}-{width}-{reopen}-{step}.sqlite"
                db = sqlite3.connect(path, isolation_level=None)
                try:
                    with closing(sqlite3.connect(initial)) as base:
                        base.backup(db)
                    # Other build cursors share the physical indexes, never this input's authority.
                    with write(db):
                        for index in range(width):
                            db.execute(
                                "INSERT INTO discovery_history_set_records "
                                "VALUES (?,?,?,?,?,'0',0,NULL)",
                                (
                                    "other-" + str(index),
                                    key.admission_sha256,
                                    histories[0].roots.records_sha256,
                                    f"{index:064x}",
                                    "unselected original metadata " * 30,
                                ),
                            )
                    with admit_archive_journal(db, key, admission, original[1]) as source:
                        works = {
                            original[1].journal_id: prefixes.CorpusPrefixWork(
                                admission, manifest, source
                            )
                        }
                        work = owner.CorpusRootWork(
                            HistorySetWork(admission, reader, histories[0].roots), manifest
                        )
                        for _ in range(step):
                            advance(db, key, work, works)
                        if reopen:
                            db.close()
                            db = sqlite3.connect(path, isolation_level=None)
                            work = owner.CorpusRootWork(
                                HistorySetWork(admission, reader, histories[0].roots), manifest
                            )
                        page = prepare(db, key, work, works) if mode == "apply" else None
                        native_anchor = type(original[1]).anchor_at
                        if mode == "apply":
                            db.execute("BEGIN IMMEDIATE")
                        try:
                            with observe_metadata(db, monkeypatch, reader=reader) as recorder:
                                with monkeypatch.context() as patch:

                                    def point(
                                        *args, _native=native_anchor, _recorder=recorder, **kwargs
                                    ):
                                        _recorder.metrics["native_anchor_calls"] += 1
                                        return _native(*args, **kwargs)

                                    patch.setattr(type(original[1]), "anchor_at", point)
                                    if mode == "progress":
                                        result = operation(recorder, key, work)
                                        assert result.version == step
                                    elif mode == "prepare":
                                        result = operation(
                                            recorder, key, work, resolve=works.__getitem__
                                        )
                                        assert (
                                            result.action
                                            == (
                                                "bound",
                                                "bound-done",
                                                "retained",
                                                "retained",
                                                "retained-done",
                                            )[step]
                                        )
                                    else:
                                        result = operation(recorder, key, work, page)
                                        assert result.version == step + 1
                        finally:
                            if mode == "apply":
                                db.commit()
                        sample = _sample(db, binding, digest, recorder.metrics, prepared=page)
                        validate_execution_samples(projection, binding.declaration.id, [sample])
                        samples.append(sample)
                        regimes.append(
                            {
                                "width": width,
                                "reopened": reopen,
                                "step": step,
                                "sqlite_vm_instructions": recorder.metrics[
                                    "sqlite_vm_instructions"
                                ],
                            }
                        )
                finally:
                    db.close()
    record_property("growth_samples", samples)
    record_property("root_regimes", regimes)


@pytest.mark.growth_witness(
    "canonical-discovery-corpus-root-progress/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_root_work.corpus_root_progress",
)
def test_measured_original_root_progress(
    tmp_path, qualified, original, monkeypatch, record_property
):
    _measured("progress", tmp_path, qualified, original, monkeypatch, record_property)


@pytest.mark.growth_witness(
    "canonical-discovery-corpus-root-prepare/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_root_work.prepare_corpus_root",
)
def test_measured_original_root_prepare(
    tmp_path, qualified, original, monkeypatch, record_property
):
    _measured("prepare", tmp_path, qualified, original, monkeypatch, record_property)


@pytest.mark.growth_witness(
    "canonical-discovery-corpus-root-apply/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_root_work.apply_corpus_root",
)
def test_measured_original_root_apply(tmp_path, qualified, original, monkeypatch, record_property):
    _measured("apply", tmp_path, qualified, original, monkeypatch, record_property)


@pytest.mark.growth_witness(
    "canonical-discovery-corpus-root-next/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.canonical_discovery_root_work._next_record",
)
def test_measured_original_root_partial_index_seeks(
    tmp_path, original, monkeypatch, record_property
):
    from riverhog_growth_contracts import validate_execution_samples

    from tests.support.canonical_discovery_work import observe_metadata
    from tests.unit.test_canonical_discovery_history_work import anchor

    binding, projection, digest = _projection(owner._next_record)
    samples, regimes = [], []
    for width in (1, 17, 1025):
        originals = [MemberHistoryRoot(anchor(i + 10000), "bound") for i in range(width + 1)]
        last_key = max(value.key for value in originals)
        for dense in ("bound", "retained"):
            selected = tuple(
                replace(
                    value,
                    inclusion=(
                        "retained" if (value.key == last_key) == (dense == "bound") else "bound"
                    ),
                )
                for value in originals
            )
            path = tmp_path / f"source-only-{dense}-{width}.sqlite"
            key, admission, reader, manifest, histories, _ = setup(
                path, original, selected_roots=selected
            )
            db = sqlite3.connect(path, isolation_level=None)
            try:
                work = owner.CorpusRootWork(
                    HistorySetWork(admission, reader, histories[0].roots), manifest
                )
                requirement = owner.corpus_root_progress(db, key, work).requirement
                # Source-only positions derive from authenticated original records.
                # Native resolution and preceding child registration are outside this seek.
                for reopen in (False, True):
                    if reopen:
                        db.close()
                        db = sqlite3.connect(path, isolation_level=None)
                    for action in ("bound", "retained"):
                        order = "bound_journal" if action == "bound" else "record_key"
                        condition = "IS NOT NULL" if action == "bound" else "IS NULL"
                        roots = db.execute(
                            f"SELECT record_key,{order} FROM discovery_history_set_records "
                            "WHERE build_id=? AND admission_sha256=? AND records_sha256=? "
                            f"AND bound_journal {condition} ORDER BY {order}",
                            (
                                key.build_id,
                                key.admission_sha256,
                                work.history.authority.records_sha256,
                            ),
                        ).fetchall()
                        for offset in sorted({0, len(roots) // 2, len(roots) - 1, len(roots)}):
                            position = None if offset == 0 else roots[offset - 1][1]
                            progress = owner.CorpusRootProgress(
                                requirement,
                                0,
                                position if action == "bound" else None,
                                position if action == "retained" else None,
                                0,
                                0,
                                False,
                                False,
                                None,
                            )
                            with observe_metadata(db, monkeypatch) as recorder:
                                result = owner._next_record(recorder, key, work, progress, action)
                            assert (result is None) == (offset == len(roots))
                            if result is not None:
                                assert result[0] == roots[offset][0]
                            sample = _sample(db, binding, digest, recorder.metrics)
                            validate_execution_samples(projection, binding.declaration.id, [sample])
                            samples.append(sample)
                            regimes.append(
                                {
                                    "width": width,
                                    "dense": dense,
                                    "reopened": reopen,
                                    "action": action,
                                    "offset": offset,
                                    "sqlite_vm_instructions": recorder.metrics[
                                        "sqlite_vm_instructions"
                                    ],
                                }
                            )
            finally:
                db.close()
    record_property("growth_samples", samples)
    record_property("root_regimes", regimes)
