from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import subprocess
import sys
from contextlib import closing
from dataclasses import replace
from pathlib import Path

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_provenance import parse_journal
from riverhog_provenance._journal_prefix import JournalPrefix
from riverhog_provenance._journal_prefix import install as install_prefix
from riverhog_provenance._online_dag import OnlineDag, StaleDagWork
from riverhog_provenance._online_dag import install as install_graph
from riverhog_provenance._session_checkpoint import (
    StaleValidationCheckpoint,
    ValidationCheckpoint,
    ValidationLedger,
    install,
)
from riverhog_provenance_contracts import ContractCatalog
from test_semantic_owner import assert_uncommitted_hidden, snapshot

ATTEMPT = "a" * 32
INPUTS = b"exact-owner-source-catalog-and-mode-binding"


def connect(path):
    db = sqlite3.connect(path)
    db.execute("PRAGMA synchronous=FULL").close()
    db.execute("PRAGMA cache_size=-64").close()
    return db


def prefix(db):
    return JournalPrefix(
        db,
        attempt_id=ATTEMPT,
        source_binding="exact-owner-source",
        catalog=ContractCatalog(),
        require_profiles=True,
    )


def ledger(db, *, epoch=2, **options):
    return ValidationLedger(db, attempt_id=ATTEMPT, binding=INPUTS, worker_epoch=epoch, **options)


def create(path):
    with closing(connect(path)) as db:
        install_prefix(db)
        install(db)
        install_graph(db)
        prefix(db).initialize()
        ledger(db).initialize()


def publish(db, frame, *, fail_at=None):
    parent, native = ledger(db), prefix(db)
    before = parent.load()
    prepared = native.prepare_publication(native.prepare(frame, ContractCatalog()))
    after = replace(
        before.checkpoint,
        version=before.checkpoint.version + 1,
        frame_ordinal=before.checkpoint.frame_ordinal + 1,
    )

    def seam(name):
        if fail_at == name:
            os._exit(81)

    db.execute("BEGIN IMMEDIATE").close()
    try:
        seam("begin")
        parent._fence(before)
        seam("fence")
        accepted = native._apply(prepared)
        seam("prefix")
        parent._publish(before, after, prefix=(ATTEMPT, native.binding, accepted.to_json()))
        seam("parent")
        if fail_at == "commit-start":
            db.set_trace_callback(
                lambda statement: seam("commit-start") if statement == "COMMIT" else None
            )
        db.commit()
        seam("committed")
    except BaseException:
        db.rollback()
        raise


@pytest.mark.parametrize("phase", ["frame", "terminal", "done", "failed"])
def test_empty_parent_codec_and_wide_scalars(phase):
    value = ValidationCheckpoint(version=2**80, frame_ordinal=2**70, phase=phase)
    assert ValidationCheckpoint.from_json(value.to_json()) == value


@pytest.mark.parametrize(
    "change",
    [
        {"version": "00"},
        {"version": -1},
        {"phase": "unknown"},
        {"phase": "entry"},
        {"frame_sha256": "0" * 64},
        {"row_key": "invalid"},
        {"child_id": "0" * 32},
        {"extra": "unowned"},
        {"row_sha256": "0" * 64},
    ],
)
def test_corrupt_parent_metadata_is_not_a_checkpoint(change):
    value = json.loads(ValidationCheckpoint().to_json())
    value.update(change)
    with pytest.raises(RuntimeError, match="corrupt journal validation checkpoint"):
        ValidationCheckpoint.from_json(canonical_json_bytes(value).decode())


def test_active_row_and_child_codec():
    value = ValidationCheckpoint(
        version=7,
        phase="generic",
        frame_ordinal=3,
        frame_sha256="1" * 64,
        frame_octets=100,
        row_key="urn:uuid:00000000-0000-4000-8000-000000000001",
        row_sha256="2" * 64,
        child_id="3" * 32,
        child_sha256="4" * 64,
    )
    assert ValidationCheckpoint.from_json(value.to_json()) == value
    with pytest.raises(RuntimeError, match="checkpoint"):
        ValidationCheckpoint.from_json(replace(value, child_sha256=None).to_json())


def test_prefix_and_parent_share_exact_commit_and_rollback(tmp_path, journal):
    path = tmp_path / "parent.sqlite3"
    create(path)
    frame = parse_journal(journal)[0]
    with closing(connect(path)) as db:
        parent, native = ledger(db), prefix(db)
        before = parent.load()
        batch = native.prepare_publication(native.prepare(frame, ContractCatalog()))
        after = replace(before.checkpoint, version=1, frame_ordinal=1)
        old = snapshot(db)
        for rollback in (True, False):
            db.execute("BEGIN IMMEDIATE").close()
            parent._fence(before)
            accepted = native._apply(batch)
            target = parent._publish(
                before, after, prefix=(ATTEMPT, native.binding, accepted.to_json())
            )
            assert_uncommitted_hidden(path, old)
            if rollback:
                db.rollback()
                assert snapshot(db) == old
            else:
                db.commit()
                assert parent.load() == target
                assert native.load().hexdigest == hashlib.sha256(journal).hexdigest()

        def authorize(action, table, column, *_rest):
            return (
                sqlite3.SQLITE_DENY
                if action == sqlite3.SQLITE_READ and table == "frames" and column == "value"
                else sqlite3.SQLITE_OK
            )

        db.set_authorizer(authorize)
        try:
            assert parent.load() == target
        finally:
            db.set_authorizer(None)
    with closing(connect(path)) as db:
        assert ledger(db).load() == target


@pytest.mark.parametrize("change", ["worker", "version", "prefix"])
def test_stale_parent_cannot_publish_a_prepared_prefix(tmp_path, journal, change):
    path = tmp_path / "parent.sqlite3"
    create(path)
    with closing(connect(path)) as db:
        parent, native = ledger(db), prefix(db)
        before = parent.load()
        batch = native.prepare_publication(
            native.prepare(parse_journal(journal)[0], ContractCatalog())
        )
        if change == "worker":
            current = parent.replace_worker(2**70)
            assert current.load().checkpoint.version == 1
        elif change == "version":
            db.execute("BEGIN IMMEDIATE").close()
            parent._fence(before)
            parent._publish(before, replace(before.checkpoint, version=1))
            db.commit()
        else:
            db.execute("UPDATE journal_prefix SET binding=?", (b"replacement",)).close()
            db.commit()
        old = snapshot(db)
        db.execute("BEGIN IMMEDIATE").close()
        try:
            with pytest.raises(StaleValidationCheckpoint):
                parent._fence(before)
                native._apply(batch)
        finally:
            db.rollback()
        assert snapshot(db) == old


@pytest.mark.parametrize(
    "column,value",
    [("attempt_id", "b" * 32), ("binding", b"different-source"), ("algorithm", "different")],
)
def test_foreign_private_state_is_not_adopted(tmp_path, column, value):
    path = tmp_path / "parent.sqlite3"
    create(path)
    with closing(connect(path)) as db:
        db.execute("UPDATE journal_validation SET " + column + "=?", (value,)).close()
        db.commit()
        with pytest.raises(StaleValidationCheckpoint):
            ledger(db).load()


@pytest.mark.parametrize("remove", [False, True])
def test_restore_checks_the_exact_predecessor_without_history_payloads(tmp_path, journal, remove):
    path = tmp_path / "parent.sqlite3"
    create(path)
    with closing(connect(path)) as db:
        publish(db, parse_journal(journal)[0])
        if remove:
            db.execute("DELETE FROM frames").close()
        else:
            db.execute("UPDATE frames SET reference=?", (b"changed",)).close()
        db.commit()
        with pytest.raises(StaleValidationCheckpoint, match="predecessor"):
            ledger(db).load()


@pytest.mark.parametrize(
    "fail_at", ["begin", "fence", "prefix", "parent", "commit-start", "committed"]
)
def test_process_death_exposes_only_the_complete_old_or_new_prefix(tmp_path, journal, fail_at):
    path = tmp_path / "parent.sqlite3"
    expected_path = tmp_path / "expected.sqlite3"
    raw = tmp_path / "source.jsonseq"
    raw.write_bytes(journal)
    create(path)
    with closing(connect(path)) as db, closing(connect(expected_path)) as expected:
        before = snapshot(db)
        db.backup(expected)
        publish(expected, parse_journal(journal)[0])
        after = snapshot(expected)
    code = """import sys
sys.path.insert(0,sys.argv[1])
from test_validation_checkpoint import connect,publish
from pathlib import Path
from riverhog_provenance import parse_journal
db=connect(sys.argv[2])
publish(db,parse_journal(Path(sys.argv[3]).read_bytes())[0],fail_at=sys.argv[4])
"""
    result = subprocess.run(
        [sys.executable, "-c", code, str(Path(__file__).parent), str(path), str(raw), fail_at],
        capture_output=True,
    )
    assert result.returncode == 81, result.stderr.decode()
    with closing(connect(path)) as db:
        assert snapshot(db) == (after if fail_at == "committed" else before)
        assert ledger(db).load().checkpoint.frame_ordinal == (1 if fail_at == "committed" else 0)


def test_graph_replacement_and_each_retirement_page_share_the_parent(tmp_path):
    path = tmp_path / "parent.sqlite3"
    create(path)
    with closing(connect(path)) as db:
        parent = ledger(db)
        graph = OnlineDag(db, "derivation", "old")
        graph.bind()
        vertices = [f"urn:uuid:00000000-0000-4000-8000-{n:012x}" for n in (1, 2, 3)]
        graph.add(vertices[0], vertices[1])
        graph.add(vertices[1], vertices[2])
        before = parent.load()
        batch = graph._prepare_replacement("new")
        old = snapshot(db)
        for rollback in (True, False):
            db.execute("BEGIN IMMEDIATE").close()
            parent._fence(before)
            replacement = graph._apply_replacement(batch)
            parent._publish(
                before, replace(before.checkpoint, version=before.checkpoint.version + 1)
            )
            if rollback:
                db.rollback()
                assert snapshot(db) == old
            else:
                db.commit()
                graph = replacement
        pages = 0
        while (batch := graph._prepare_retirement(1)) is not None:
            before = parent.load()
            old = snapshot(db)
            for rollback in (True, False):
                db.execute("BEGIN IMMEDIATE").close()
                parent._fence(before)
                graph._apply_retirement(batch)
                parent._publish(
                    before, replace(before.checkpoint, version=before.checkpoint.version + 1)
                )
                if rollback:
                    db.rollback()
                    assert snapshot(db) == old
                else:
                    db.commit()
            pages += 1
            assert pages < 100
        assert pages > 1
        assert db.execute("SELECT 1 FROM dag_retired LIMIT 1").fetchone() is None


def test_graph_composition_refuses_foreign_runs_and_closed_transactions(tmp_path):
    path = tmp_path / "parent.sqlite3"
    create(path)
    with closing(connect(path)) as db:
        graph = OnlineDag(db, "derivation", "old")
        graph.bind()
        batch = graph._prepare_replacement("new")
        with pytest.raises(RuntimeError, match="owner transaction"):
            graph._apply_replacement(batch)
        db.execute("BEGIN IMMEDIATE").close()
        try:
            with pytest.raises(StaleDagWork, match="another run"):
                OnlineDag(db, "specialization", "other")._apply_replacement(batch)
        finally:
            db.rollback()
