from __future__ import annotations

import sqlite3
from dataclasses import replace

import pytest
from riverhog_provenance import (
    CorpusReplayBinding,
    RetainedCorpusValidator,
    StaleCorpusWorkspace,
)
from riverhog_provenance import corpus_workspace as owner
from test_corpus_order_growth import edges_for, insert
from test_retained_corpus_workspace import directory, finish, open_workspace


class EmptySource:
    def __init__(self, catalog):
        self.binding = CorpusReplayBinding(
            "a" * 32,
            "owned-empty-corpus",
            "b" * 64,
            0,
            "c" * 64,
            0,
            "d" * 64,
            catalog.checkpoint_sha256(),
        )

    def verify_current(self):
        return True

    def admit_source(self, anchor):
        return False

    def admit_selection(self, anchor):
        return False


@pytest.mark.parametrize("fault", ["format", "marker", "checkpoint", "order_table", "file", "mode"])
def test_corrupt_or_missing_workspace_is_rejected_without_repair(tmp_path, catalog, fault):
    path = directory(tmp_path)
    source = EmptySource(catalog)
    with open_workspace(path, source, create=True) as workspace:
        if fault == "format":
            workspace._db.execute("UPDATE corpus_workspace SET format='foreign'")
        elif fault == "marker":
            workspace._db.execute("DELETE FROM corpus_workspace")
        elif fault == "checkpoint":
            workspace._db.execute("UPDATE corpus_workspace SET checkpoint=x'7b7d'")
        elif fault == "order_table":
            workspace._db.execute("DROP TABLE corpus_order_derivation_nodes")
        workspace._db.commit()
    file = path / "corpus.sqlite3"
    if fault == "file":
        file.unlink()
    elif fault == "mode":
        with sqlite3.connect(file) as db:
            assert db.execute("PRAGMA journal_mode=WAL").fetchone() == ("wal",)
    before = file.read_bytes() if file.exists() else None
    with pytest.raises(StaleCorpusWorkspace):
        open_workspace(path, source, epoch=1)
    assert (file.read_bytes() if file.exists() else None) == before


def test_order_mutations_and_parent_checkpoint_rollback_together(tmp_path, catalog, monkeypatch):
    path = directory(tmp_path)
    source = EmptySource(catalog)
    with open_workspace(path, source, create=True) as workspace:
        # The Native ordering owner also qualifies arbitrary fixed edge indexes.
        with workspace._operation():
            insert(workspace._corpus, edges_for("chain", 17), "derivation")
        while not workspace.advance_semantics().complete:
            pass
        while not workspace.advance_dependencies().complete:
            pass
        assert workspace.advance_order("derivation", row_budget=7).source_edges == 7
        before = (path / "corpus.sqlite3").read_bytes()
        write = owner._write

        def interrupted(db, query, parameters=()):
            if query.startswith("UPDATE corpus_workspace SET checkpoint="):
                raise InterruptedError("parent checkpoint publication interrupted")
            return write(db, query, parameters)

        with monkeypatch.context() as patch:
            patch.setattr(owner, "_write", interrupted)
            with pytest.raises(InterruptedError):
                workspace.advance_order("derivation", row_budget=3)
        assert (path / "corpus.sqlite3").read_bytes() == before
        assert workspace.advance_order("derivation", row_budget=3).source_edges == 10
    with open_workspace(path, source, epoch=1) as resumed:
        assert finish(resumed)[0].source_edges == 17


class LostCommit:
    def __init__(self, db):
        self.db = db

    def __getattr__(self, name):
        return getattr(self.db, name)

    def commit(self):
        self.db.commit()
        raise InterruptedError("successful commit return was lost")


def test_lost_commit_reply_reloads_actual_parent_and_order_progress(tmp_path, catalog):
    path = directory(tmp_path)
    source = EmptySource(catalog)
    with open_workspace(path, source, create=True) as workspace:
        with workspace._operation():
            insert(workspace._corpus, edges_for("chain", 17), "derivation")
        while not workspace.advance_semantics().complete:
            pass
        while not workspace.advance_dependencies().complete:
            pass
        db = workspace._db
        workspace._db = LostCommit(db)
        with pytest.raises(InterruptedError):
            workspace.advance_order("derivation", row_budget=7)
        workspace._db = db
        assert workspace.advance_order("derivation", row_budget=3).source_edges == 10
        assert finish(workspace)[0].source_edges == 17


@pytest.mark.parametrize("epoch", [True, -1, "1", 1.0])
def test_invalid_epoch_never_creates_storage(tmp_path, catalog, epoch):
    path = directory(tmp_path)
    with pytest.raises(ValueError):
        open_workspace(path, EmptySource(catalog), epoch=epoch, create=True)
    assert not (path / "corpus.sqlite3").exists()


@pytest.mark.parametrize(
    "field,value",
    [
        ("source_count", True),
        ("selection_count", -1),
        ("policy_sha256", "opaque"),
        ("attempt_id", "x"),
    ],
)
def test_binding_rejects_ambiguous_or_noncanonical_expectations(catalog, field, value):
    with pytest.raises(ValueError):
        replace(EmptySource(catalog).binding, **{field: value})


def test_release_callback_runs_after_actual_database_close(tmp_path, catalog):
    path = directory(tmp_path)
    source = EmptySource(catalog)
    calls = []

    def release():
        with pytest.raises(sqlite3.ProgrammingError):
            workspace._db.execute("SELECT 1")
        calls.append("released")

    workspace = RetainedCorpusValidator.open_retained(
        path, source, worker_epoch=0, create=True, on_storage_close=release
    )
    assert finish(workspace)[0].vertices == 0
    workspace.close()
    workspace.close()
    assert calls == ["released"]


def test_replacement_cannot_publish_metadata_that_its_budget_cannot_read(tmp_path, catalog):
    path = directory(tmp_path)
    source = EmptySource(catalog)
    with open_workspace(path, source, create=True):
        pass
    before = (path / "corpus.sqlite3").read_bytes()
    from riverhog_provenance import JournalValidationCapacity

    with pytest.raises(JournalValidationCapacity):
        open_workspace(path, source, epoch=10**1024, metadata_byte_budget=1024)
    assert (path / "corpus.sqlite3").read_bytes() == before
    with open_workspace(path, source, epoch=1) as resumed:
        assert finish(resumed)[0].complete


def test_reopening_never_reads_native_corpus_payload_or_ordering_rows(
    tmp_path, catalog, monkeypatch
):
    path = directory(tmp_path)
    source = EmptySource(catalog)
    with open_workspace(path, source, create=True) as workspace:
        with workspace._operation():
            insert(workspace._corpus, edges_for("chain", 129), "derivation")
        finish(workspace)
    connect = owner.sqlite3.connect

    def metadata_only(*args, **kwargs):
        db = connect(*args, **kwargs)

        def authorize(action, table, column, *_rest):
            if action == sqlite3.SQLITE_READ and table != "corpus_workspace":
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        db.set_authorizer(authorize)
        return db

    with monkeypatch.context() as patch:
        patch.setattr(owner.sqlite3, "connect", metadata_only)
        with open_workspace(path, source, epoch=1):
            pass


class UncertainClose:
    def __init__(self, db, *, lost_reply):
        self.db, self.lost_reply = db, lost_reply
        self.attempted = False

    def close(self):
        if not self.attempted:
            self.attempted = True
            if self.lost_reply:
                self.db.close()
            raise OSError("storage close outcome is unconfirmed")
        self.db.close()


@pytest.mark.parametrize("lost_reply", [False, True])
def test_uncertain_storage_close_can_be_confirmed_without_reopening_work(
    tmp_path, catalog, lost_reply
):
    path = directory(tmp_path)
    source = EmptySource(catalog)
    releases = []
    workspace = open_workspace(
        path, source, create=True, on_storage_close=lambda: releases.append("released")
    )
    db = workspace._db

    workspace._corpus._db = UncertainClose(db, lost_reply=lost_reply)
    try:
        with pytest.raises(OSError, match="unconfirmed"):
            workspace.close()
        assert releases == []
        with pytest.raises(RuntimeError, match="closed"):
            workspace.advance_semantics()
        if lost_reply:
            with pytest.raises(sqlite3.ProgrammingError):
                db.execute("SELECT 1")
        else:
            assert db.execute("SELECT 1").fetchone() == (1,)
        workspace.close()
        with pytest.raises(sqlite3.ProgrammingError):
            db.execute("SELECT 1")
        assert releases == ["released"]
        workspace.close()
        assert releases == ["released"]
    finally:
        db.close()


@pytest.mark.parametrize("damage", ["unlink", "replace", "symlink", "permissions", "hardlink"])
def test_live_handle_rejects_lost_or_replaced_database_before_work(tmp_path, catalog, damage):
    path = directory(tmp_path)
    source = EmptySource(catalog)
    workspace = open_workspace(path, source, create=True)
    file = path / "corpus.sqlite3"
    try:
        if damage == "unlink":
            file.unlink()
        elif damage in {"replace", "symlink"}:
            file.rename(path / "detached.sqlite3")
            if damage == "replace":
                file.write_bytes(b"unrelated")
                file.chmod(0o600)
            else:
                file.symlink_to(path / "detached.sqlite3")
        elif damage == "permissions":
            file.chmod(0o644)
        else:
            import os

            os.link(file, path / "duplicate.sqlite3")
        with pytest.raises(StaleCorpusWorkspace, match="file"):
            workspace.advance_semantics()
    finally:
        workspace.close()


def test_serialized_native_work_and_close_can_move_between_worker_threads(tmp_path, catalog):
    from concurrent.futures import ThreadPoolExecutor

    path = directory(tmp_path)
    source = EmptySource(catalog)
    workspace = open_workspace(path, source, create=True)
    try:
        with ThreadPoolExecutor(max_workers=1) as worker:
            assert worker.submit(workspace.advance_semantics).result(timeout=10).complete
            while not worker.submit(workspace.advance_dependencies).result(timeout=10).complete:
                pass
            while (
                not worker.submit(workspace.advance_order, "derivation").result(timeout=10).complete
            ):
                pass
            worker.submit(workspace.close).result(timeout=10)
    finally:
        workspace.close()


@pytest.mark.parametrize("lost_reply", [False, True])
def test_uncertain_open_close_leaves_the_actual_handle_reachable(
    tmp_path, catalog, monkeypatch, lost_reply
):
    path = directory(tmp_path)
    source = EmptySource(catalog)
    retained, released = [], []
    initialize = owner.RetainedCorpusValidator._initialize

    def interrupted(workspace):
        initialize(workspace)
        workspace._corpus._db = UncertainClose(workspace._db, lost_reply=lost_reply)
        raise InterruptedError("initialization return lost")

    with monkeypatch.context() as patch:
        patch.setattr(owner.RetainedCorpusValidator, "_initialize", interrupted)
        with pytest.raises(OSError, match="unconfirmed"):
            open_workspace(
                path,
                source,
                create=True,
                on_storage_open=retained.append,
                on_storage_close=lambda: released.append(True),
            )
    assert len(retained) == 1
    workspace = retained[0]
    assert not workspace._storage_closed
    assert workspace._descriptor is not None
    assert released == []
    with pytest.raises(RuntimeError, match="closed"):
        workspace.advance_semantics()
    workspace.close()
    assert workspace._descriptor is None
    assert released == [True]
    workspace.close()
    assert released == [True]
