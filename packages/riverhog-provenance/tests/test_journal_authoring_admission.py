from __future__ import annotations

import gc
import hashlib
import shutil
from io import BytesIO

import pytest
from riverhog_provenance import (
    append_assertion_batches,
    append_assertions,
    append_checkpoint,
    append_correction,
    assertion,
    assertion_reference,
    create_journal,
    journal_store,
    reference,
    validate_journal,
    write_assertion_batches,
)
from riverhog_provenance import (
    journal as owner,
)
from test_disposable_sqlite_admission import directories_under
from test_sqlite_connection_admission import no_default_connection

from tests.support.sqlite_admission import Capacity, Population

KINDS = ("create", "append", "correction", "batches", "spool", "checkpoint")
CONNECTIONS = dict(zip(KINDS, (1, 2, 2, 2, 2, 3), strict=True))
PEAK = dict(zip(KINDS, (1, 2, 2, 1, 1, 3), strict=True))
ENTRY = "urn:uuid:00000000-0000-4000-8000-000000000009"
ASSERTION = "urn:uuid:00000000-0000-4000-8000-000000000008"


@pytest.fixture
def material(journal, who, catalog, monkeypatch):
    with validate_journal(journal, catalog=catalog) as summary:
        graph, journal_id = summary.graph, summary.journal_id
        old_agent = dict(graph["agents"][0])
        target = assertion_reference(summary, old_agent["assertion_id"])
        artifact = graph["artifacts"][0]["id"]
    changed = {**old_agent, "assertion_id": ASSERTION, "name": "Corrected label"}
    row = assertion(
        "extension",
        who,
        subject=reference(artifact, "artifact"),
        property="urn:test:admission",
        value={"type": "text", "value": "exact authored value"},
    )
    row["assertion_id"] = ASSERTION
    monkeypatch.setattr(owner, "new_id", lambda: ENTRY)
    monkeypatch.setattr(owner, "utc_now", lambda: "2026-10-30T00:00:00Z")
    return {
        "raw": journal,
        "who": who,
        "catalog": catalog,
        "graph": graph,
        "journal_id": journal_id,
        "target": target,
        "changed": changed,
        "extra": {"extensions": [row]},
    }


def author(kind, material, *, destination=None, batches=None, **ports):
    common = {"recorded_by_agent_id": material["who"], "catalog": material["catalog"], **ports}
    raw = material["raw"]
    if kind == "create":
        return create_journal(
            material["graph"], journal_id=material["journal_id"], entry_id=ENTRY, **common
        )
    if kind == "append":
        return append_assertions(raw, material["extra"], **common)
    if kind == "correction":
        return append_correction(
            raw,
            [material["target"]],
            reason="label",
            assertions={"agents": [material["changed"]]},
            **common,
        )
    if kind == "checkpoint":
        return append_checkpoint(raw, purpose="urn:test:admitted-checkpoint", **common)
    if batches is None:
        batches = (material["extra"],)
    if kind == "batches":
        return append_assertion_batches(raw, batches, entry_ids=(ENTRY,), **common)
    assert kind == "spool"
    destination = BytesIO() if destination is None else destination
    with write_assertion_batches(destination, raw, batches, entry_ids=(ENTRY,), **common):
        return destination.getvalue()


@pytest.mark.parametrize("kind", KINDS)
def test_authors_use_selected_real_connections_and_preserve_exact_bytes(
    tmp_path, monkeypatch, material, kind
):
    expected = author(kind, material)
    paths = directories_under(tmp_path, monkeypatch)
    population, handles = Population(limit=PEAK[kind]), []
    with monkeypatch.context() as patch:
        patch.setattr(journal_store.sqlite3, "connect", no_default_connection)
        actual = author(
            kind, material, connection_factory=population, on_storage_open=handles.append
        )
    assert (
        actual == expected and hashlib.sha256(actual).digest() == hashlib.sha256(expected).digest()
    )
    if kind != "create":
        assert actual.startswith(material["raw"])
    assert len(population.connections) == CONNECTIONS[kind] and population.peak == PEAK[kind]
    assert population.active == 0 and all(db.close_calls == 1 for db in population.connections)
    assert len(handles) == 2 * CONNECTIONS[kind]
    for constructor, reader in zip(handles[::2], handles[1::2], strict=True):
        assert constructor is not reader and constructor._storage is reader._storage
        assert constructor._lease.closed and reader._lease.closed
    assert all(not path.exists() for path in paths)


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("after", [False, True])
def test_authored_result_close_retains_actual_reader_for_explicit_retry(
    tmp_path, monkeypatch, material, kind, after
):
    paths = directories_under(tmp_path, monkeypatch)
    population, handles = Population(), []

    def retain(store):
        handles.append(store)
        if len(handles) == 2 * CONNECTIONS[kind]:
            store._storage.connection.close_failures = 1
            store._storage.connection.close_after = after

    with pytest.raises(OSError, match="close unconfirmed"):
        author(kind, material, connection_factory=population, on_storage_open=retain)
    gc.collect()
    assert population.active == 1 and paths[-1].is_dir()
    assert not handles[-1]._lease.closed and handles[-2]._lease.closed
    assert all(not path.exists() for path in paths[:-1])
    with pytest.raises(RuntimeError, match="closed"):
        _ = handles[-1].db
    handles[-1].close()
    handles[-1].close()
    assert population.active == 0 and population.connections[-1].close_calls == 2
    assert all(not path.exists() for path in paths)


@pytest.mark.parametrize("kind", KINDS)
def test_author_refusal_retains_factory_directory_and_never_reports_completion(
    tmp_path, monkeypatch, material, kind
):
    paths = directories_under(tmp_path, monkeypatch)
    population, handles = Population(), []
    population.refuse = True

    class UnreadBatches:
        def __iter__(self):
            raise AssertionError("batches consumed before initial connection admission")

    destination = BytesIO()
    with pytest.raises(Capacity):
        author(
            kind,
            material,
            destination=destination,
            batches=UnreadBatches(),
            connection_factory=population,
            on_storage_open=handles.append,
        )
    assert len(paths) == len(population.attempts) == 1 and paths[0].is_dir()
    assert not population.connections and not handles and population.active == 0
    assert destination.getvalue() == b"" and not list(paths[0].iterdir())
    shutil.rmtree(paths[0])


@pytest.mark.parametrize("kind,limit", [("append", 1), ("correction", 1), ("checkpoint", 2)])
def test_nested_author_refusal_releases_returned_readers_without_mutating_input(
    tmp_path, monkeypatch, material, kind, limit
):
    paths = directories_under(tmp_path, monkeypatch)
    population, handles = Population(limit=limit), []
    before = bytes(material["raw"])
    with pytest.raises(Capacity):
        author(kind, material, connection_factory=population, on_storage_open=handles.append)
    assert population.active == 0 and population.peak == limit
    assert len(population.connections) == limit and len(population.attempts) == limit + 1
    assert all(db.close_calls == 1 for db in population.connections)
    assert all(handle._lease.closed for handle in handles)
    assert material["raw"] == before and all(not path.exists() for path in paths[:-1])
    assert paths[-1].is_dir() and not list(paths[-1].iterdir())
    shutil.rmtree(paths[-1])


@pytest.mark.parametrize("kind", ["create", "spool"])
@pytest.mark.parametrize("after", [False, True])
def test_internal_result_handoff_failure_keeps_its_uncertain_close_reachable(
    tmp_path, monkeypatch, material, kind, after
):
    paths = directories_under(tmp_path, monkeypatch)
    population, handles = Population(), []

    def retain(store):
        handles.append(store)
        if len(handles) == 2 * CONNECTIONS[kind]:
            store._storage.connection.close_failures = 1
            store._storage.connection.close_after = after
            raise InterruptedError("result handoff interrupted")

    with pytest.raises(OSError, match="close unconfirmed"):
        author(kind, material, connection_factory=population, on_storage_open=retain)
    gc.collect()
    assert population.active == 1 and paths[-1].is_dir()
    pending = [handle for handle in handles if not handle._lease.closed]
    assert pending == [handles[-2]]
    pending[0].close()
    assert population.active == 0 and all(not path.exists() for path in paths)


@pytest.mark.parametrize("after", [False, True])
def test_spool_final_rewind_failure_preserves_retryable_reader_and_original_output(
    tmp_path, monkeypatch, material, after
):
    expected = author("spool", material)
    paths = directories_under(tmp_path, monkeypatch)
    population, handles = Population(), []

    class Spool(BytesIO):
        def seek(self, offset, whence=0):
            if len(handles) == 4 and offset == whence == 0:
                raise OSError("final rewind failed")
            return super().seek(offset, whence)

    def retain(store):
        handles.append(store)
        if len(handles) == 4:
            store._storage.connection.close_failures = 1
            store._storage.connection.close_after = after

    destination = Spool()
    with pytest.raises(OSError, match="close unconfirmed"):
        author(
            "spool",
            material,
            destination=destination,
            connection_factory=population,
            on_storage_open=retain,
        )
    assert destination.getvalue() == expected and population.active == 1
    handles[-1].close()
    assert population.active == 0 and all(not path.exists() for path in paths)


@pytest.mark.parametrize("kind", ["append", "correction", "batches", "spool", "checkpoint"])
def test_author_admission_does_not_accept_an_invalid_existing_journal(
    tmp_path, monkeypatch, material, kind
):
    paths = directories_under(tmp_path, monkeypatch)
    population, handles = Population(), []
    material = {**material, "raw": material["raw"] + b"\x1e{}\n"}
    with pytest.raises(ValueError):
        author(kind, material, connection_factory=population, on_storage_open=handles.append)
    assert population.active == 0 and len(population.connections) == 1
    assert len(handles) == 1 and population.connections[0].close_calls == 1
    assert all(not path.exists() for path in paths)


def test_streaming_author_returns_one_live_reader_without_holding_its_predecessor(
    tmp_path, monkeypatch, material
):
    paths = directories_under(tmp_path, monkeypatch)
    population, handles = Population(limit=1), []
    destination = BytesIO()
    summary = write_assertion_batches(
        destination,
        material["raw"],
        (material["extra"],),
        recorded_by_agent_id=material["who"],
        catalog=material["catalog"],
        entry_ids=(ENTRY,),
        connection_factory=population,
        on_storage_open=handles.append,
    )
    assert destination.tell() == 0 and population.active == population.peak == 1
    assert len(population.connections) == 2 and len(handles) == 4
    assert handles[0]._lease.closed and handles[1]._lease.closed and handles[2]._lease.closed
    assert not handles[3]._lease.closed and summary._store is handles[3]
    assert summary.journal_bytes == len(destination.getvalue())
    assert not paths[0].exists() and paths[1].is_dir()
    summary.close()
    assert population.active == 0 and all(not path.exists() for path in paths)


@pytest.mark.parametrize("after", [False, True])
def test_public_validation_retains_final_reader_after_caller_loses_summary(
    tmp_path, monkeypatch, material, after
):
    paths = directories_under(tmp_path, monkeypatch)
    population, handles = Population(limit=1), []
    summary = validate_journal(
        material["raw"],
        catalog=material["catalog"],
        connection_factory=population,
        on_storage_open=handles.append,
    )
    assert len(handles) == 2 and handles[1] is summary._store
    assert handles[0]._lease.closed and not handles[1]._lease.closed
    population.connections[0].close_failures = 1
    population.connections[0].close_after = after
    with pytest.raises(OSError, match="close unconfirmed"):
        summary.close()
    del summary
    gc.collect()
    assert population.active == 1 and paths[0].is_dir() and not handles[1]._lease.closed
    handles[1].close()
    assert population.active == 0 and population.connections[0].close_calls == 2
    assert not paths[0].exists()
