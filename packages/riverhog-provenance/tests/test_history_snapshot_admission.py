from __future__ import annotations

import gc
import shutil
from dataclasses import replace
from pathlib import Path

import pytest
from riverhog_archive_contracts import HistoryJournalAnchor, MemberHistoryStore
from riverhog_provenance import (
    JournalStore,
    MemberHistoryClosure,
    ProvenanceValidationError,
    journal_store,
    validate_journal,
)
from riverhog_provenance import history as owner

from tests.support.sqlite_admission import Capacity, Population


@pytest.fixture
def material(journal, catalog):
    with validate_journal(journal, catalog=catalog) as summary:
        return journal, catalog, HistoryJournalAnchor.from_mapping(summary.anchor)


@pytest.fixture
def paths(tmp_path, monkeypatch):
    make, paths = journal_store.mkdtemp, []

    def created(**options):
        path = Path(make(dir=tmp_path, **options))
        paths.append(path)
        return str(path)

    monkeypatch.setattr(journal_store, "mkdtemp", created)
    monkeypatch.setattr(owner, "mkdtemp", created)
    return paths


def closure(material, population, handles, parents, *, read=None, retained=None):
    raw, catalog, _anchor = material
    return MemberHistoryClosure(
        MemberHistoryStore(lambda _path: ()),
        read or (lambda _identity, end: (raw[:end],)),
        member_role="member",
        catalog=catalog,
        connection_factory=population,
        on_storage_open=parents.append,
        journal_connection_factory=population,
        on_journal_storage_open=retained or handles.append,
    )


def forbid_default(monkeypatch):
    def denied(*_args, **_options):
        pytest.fail("history snapshot bypassed the selected connection factory")

    monkeypatch.setattr(journal_store.sqlite3, "connect", denied)


def test_history_snapshot_admits_and_retains_each_actual_reader_without_reopening(
    material, paths, monkeypatch
):
    population, handles, parents = Population(limit=2), [], []
    forbid_default(monkeypatch)
    owner = closure(material, population, handles, parents)
    first = owner.summary_at(material[2])
    second = owner.summary_at(material[2])
    assert first.anchor == second.anchor == material[2].to_mapping()
    assert len(population.connections) == 2 and population.peak == population.active == 2
    assert len(handles) == 4 and len({id(handle) for handle in handles}) == 4
    assert first._store is handles[2] and second._store is handles[3]
    assert population.attempts[1][1] == {"check_same_thread": False, "cached_statements": 0}
    owner.close()
    assert parents == [owner] and population.active == 1
    assert first.anchor == second.anchor and paths[1].exists() and not paths[0].exists()
    first.close()
    assert population.active == 1
    second.close()
    assert population.active == 0 and not any(path.exists() for path in paths)
    assert all(connection.close_calls == 1 for connection in population.connections)


@pytest.mark.parametrize("unreturned", [False, True])
def test_history_snapshot_refusal_precedes_source_iteration_and_preserves_factory_files(
    material, paths, monkeypatch, unreturned
):
    population, handles, parents, reads = Population(limit=2), [], [], []

    def read(_identity, end):
        reads.append(end)
        yield material[0][:end]

    owner = closure(material, population, handles, parents, read=read)
    population.refuse, population.unreturned = not unreturned, unreturned
    forbid_default(monkeypatch)
    with pytest.raises(OSError if unreturned else Capacity):
        with owner:
            owner.summary_at(material[2])
    gc.collect()
    assert not reads and not handles and population.active == int(unreturned)
    assert not paths[0].exists() and paths[1].exists()
    if unreturned:
        assert population.connections[-1].close_calls == 0
        population.connections[-1].close()
    shutil.rmtree(paths[1])
    assert population.active == 0


@pytest.mark.parametrize("stage", ["schema", "constructor", "result", "borrow"])
def test_history_snapshot_initialization_and_handoff_failure_release_confirmed_storage(
    material, paths, monkeypatch, stage
):
    population, handles, parents = Population(limit=2), [], []

    def retained(handle):
        handles.append(handle)
        if len(handles) == {"constructor": 1, "result": 2, "borrow": 3}.get(stage):
            raise InterruptedError("reader retention interrupted")

    owner = closure(material, population, handles, parents, retained=retained)
    population.fail_schema = stage == "schema"
    forbid_default(monkeypatch)
    with pytest.raises(InterruptedError):
        with owner:
            owner.summary_at(material[2])
    assert population.active == 0 and not any(path.exists() for path in paths)
    assert all(connection.close_calls == 1 for connection in population.connections)


@pytest.mark.parametrize("after", [False, True])
def test_history_snapshot_final_cache_close_retains_real_connection_for_explicit_retry(
    material, paths, after
):
    population, handles, parents = Population(limit=2), [], []
    owner = closure(material, population, handles, parents)
    with owner.summary_at(material[2]) as selected:
        assert selected.anchor == material[2].to_mapping()
    db = population.connections[1]
    db.close_failures, db.close_after = 1, after
    with pytest.raises(OSError, match="close unconfirmed"):
        with owner:
            owner.close()
    gc.collect()
    assert db.close_calls == 1 and population.active == 2 and all(path.exists() for path in paths)
    with pytest.raises(RuntimeError, match="closed"):
        owner.summary_at(material[2])
    owner.close()
    assert population.active == 0 and db.close_calls == 2
    assert not any(path.exists() for path in paths)


@pytest.mark.parametrize("after", [False, True])
def test_history_snapshot_borrow_handoff_retains_uncertain_reader_close(
    material, paths, monkeypatch, after
):
    population, handles, parents = Population(limit=2), [], []
    original, closes = JournalStore.close, []

    def retained(handle):
        handles.append(handle)
        if len(handles) == 3:
            raise InterruptedError("borrow retention interrupted")

    def interrupted(handle):
        if len(handles) == 3 and handle is handles[2]:
            closes.append(handle)
            if len(closes) == 1:
                if after:
                    original(handle)
                raise OSError("borrow close unconfirmed")
        original(handle)

    monkeypatch.setattr(JournalStore, "close", interrupted)
    owner = closure(material, population, handles, parents, retained=retained)
    with pytest.raises(OSError, match="borrow close unconfirmed"):
        with owner:
            owner.summary_at(material[2])
    assert len(closes) == 1 and parents == [owner] and population.active == 2
    assert owner._pending_summary._store is handles[2]
    owner.close()
    assert len(closes) == 2 and population.active == 0
    assert not any(path.exists() for path in paths)


@pytest.mark.parametrize("warm", [False, True])
@pytest.mark.parametrize("change", ["bytes", "short", "anchor", "fence"])
def test_history_snapshot_admission_preserves_exact_identity_and_live_source_fences(
    material, paths, monkeypatch, warm, change
):
    population, handles, parents, changed = Population(limit=3), [], [], False

    def read(_identity, end):
        try:
            raw = material[0]
            if changed and change == "bytes":
                raw = b"\x1f" + raw[1:]
            elif changed and change == "short":
                raw = raw[:-1]
            yield raw[:end]
        finally:
            if changed and change == "fence":
                raise PermissionError("source fence revoked")

    forbid_default(monkeypatch)
    with closure(material, population, handles, parents, read=read) as owner:
        if warm:
            with owner.summary_at(material[2]):
                pass
        changed = True
        anchor = replace(material[2], prefix_sha256="0" * 64) if change == "anchor" else material[2]
        with pytest.raises(PermissionError if change == "fence" else ProvenanceValidationError):
            owner.summary_at(anchor)
    assert population.active == 0 and not any(path.exists() for path in paths)
