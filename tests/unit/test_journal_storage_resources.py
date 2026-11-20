"""Real application process admission and original retained Native tickets."""

from __future__ import annotations

import os

import pytest
from riverhog_core.journal_storage_grants import JournalStorageGrants
from riverhog_core.runtime_config import RuntimeConfig
from riverhog_core.runtime_document import JournalStorageDocument
from riverhog_core.services import journal_storage_resources as owner
from riverhog_provenance import JournalStore
from sqlite_admission import SqliteCapacity, SqliteCleanupPending


@pytest.mark.parametrize(
    "name", ["connections", "cursors", "operations", "file_records", "corpus_file_records"]
)
@pytest.mark.parametrize("value", [0, -1, True])
def test_grants_reject_nonpositive_or_boolean_values(name, value):
    with pytest.raises(ValueError):
        JournalStorageGrants(**{name: value})


def test_operator_document_and_runtime_keep_explicit_native_grants():
    document = JournalStorageDocument(connections=7, cursors=29, operations=3, file_records=4)
    config = RuntimeConfig.for_testing(journal_storage=document.grants())
    assert config.journal_storage == JournalStorageGrants(7, 29, 3, 4)
    with pytest.raises(ValueError):
        JournalStorageDocument(file_records=1)
    with pytest.raises(ValueError):
        JournalStorageDocument(corpus_file_records=2)
    with pytest.raises(ValueError):
        JournalStorageGrants(corpus_file_records=2)


@pytest.mark.parametrize("refusal", ["pid", "engine", "closing"])
def test_process_refusal_precedes_file_effects_and_keeps_original_receivable(
    tmp_path, monkeypatch, refusal
):
    directory = tmp_path / "journal"
    directory.mkdir(mode=0o700)
    resources = owner.JournalStorageResources(JournalStorageGrants())
    original = resources.journal()
    assert resources.status.connections == 0
    if refusal == "pid":
        pid = os.getpid()
        monkeypatch.setattr(owner.os, "getpid", lambda: pid + 1)
    elif refusal == "engine":
        monkeypatch.setattr(owner.sqlite3, "sqlite_version", "changed-engine")
    else:
        resources.close()
    with pytest.raises((RuntimeError, SqliteCleanupPending)):
        JournalStore.open_retained(directory, construction=original, create=True)
    assert not list(directory.iterdir())
    assert original.close_step() and resources.status.connections == 0


def test_shared_connection_grant_cannot_release_borrowed_reader_for_new_work(tmp_path):
    resources = owner.JournalStorageResources(JournalStorageGrants(connections=1))
    one, two = tmp_path / "one", tmp_path / "two"
    one.mkdir(mode=0o700)
    two.mkdir(mode=0o700)
    original = resources.journal()
    store = JournalStore.open_retained(one, construction=original, create=True)
    store.freeze()
    borrowed = store.borrow()
    assert not original.close_step()
    rejected = resources.journal()
    with pytest.raises(SqliteCapacity):
        JournalStore.open_retained(two, construction=rejected, create=True)
    assert not list(two.iterdir()) and rejected.close_step()
    with pytest.raises(SqliteCleanupPending):
        resources.close()
    assert resources.status.connections == 1
    borrowed.close()
    assert original.close_step() and resources.status.connections == 0
    resources.close()
