from __future__ import annotations

import hashlib
from contextlib import closing

import pytest
from riverhog_provenance import validate_journal_chunks
from riverhog_provenance.journal_store import JournalStore
from sqlite_admission import SqliteConnectionAdmission, SqliteConnectionSettings


def selected(temporary):
    return SqliteConnectionAdmission(
        connection_limit=1,
        cursor_limit=4,
        operation_limit=1,
        verify_process=lambda: None,
        connection_settings=SqliteConnectionSettings(cache_kib=19, temporary_storage=temporary),
    )


def row(db, sql):
    with closing(db.execute(sql)) as cursor:
        return cursor.fetchone()


@pytest.mark.parametrize("temporary", ["FILE", "MEMORY"])
def test_journal_profile_precedes_schema_and_survives_cold_retained_reopen(tmp_path, temporary):
    admission, retained = selected(temporary), []
    directory = tmp_path / "retained"
    directory.mkdir()

    def retain(store):
        retained.append(store)
        assert admission.status.connections == 1
        assert row(store.db, "PRAGMA temp_store") == (0,)

    with closing(
        JournalStore.open_retained(
            directory, create=True, connection_factory=admission.connect, on_storage_open=retain
        )
    ) as store:
        assert row(store.db, "PRAGMA cache_size") == (-19,)
        assert row(store.db, "PRAGMA temp_store") == (1 if temporary == "FILE" else 2,)
        assert row(store.db, "PRAGMA journal_mode") == ("delete",)
        assert row(store.db, "PRAGMA synchronous") == (2,)
        assert row(store.db, "PRAGMA auto_vacuum") == (2,)
        store.db.execute("INSERT INTO findings VALUES('owned','exact retained value')").close()
        store.db.commit()
    before = hashlib.sha256((directory / "snapshot.sqlite3").read_bytes()).hexdigest()
    with closing(
        JournalStore.open_retained(
            directory, read_only=True, connection_factory=admission.connect, on_storage_open=retain
        )
    ) as store:
        assert row(store.db, "PRAGMA cache_size") == (-19,)
        assert row(store.db, "PRAGMA temp_store") == (1 if temporary == "FILE" else 2,)
        assert row(store.db, "SELECT value FROM findings WHERE key='owned'") == (
            "exact retained value",
        )
    assert hashlib.sha256((directory / "snapshot.sqlite3").read_bytes()).hexdigest() == before
    assert admission.status.connections == admission.status.cursors == 0
    assert len(retained) == 2 and all(store.cleanup_complete for store in retained)


@pytest.mark.parametrize("temporary", ["FILE", "MEMORY"])
def test_journal_helper_result_reader_keeps_selected_profile_and_exact_anchor(
    journal, catalog, temporary
):
    admission, retained = selected(temporary), []
    summary = validate_journal_chunks(
        (journal,),
        catalog=catalog,
        connection_factory=admission.connect,
        on_storage_open=retained.append,
    )
    try:
        assert len(retained) == 2
        reader = retained[1]
        assert row(reader.db, "PRAGMA cache_size") == (-19,)
        assert row(reader.db, "PRAGMA temp_store") == (1 if temporary == "FILE" else 2,)
        assert summary.journal_bytes == len(journal)
        assert summary.journal_sha256 == hashlib.sha256(journal).hexdigest()
    finally:
        summary.close()
    assert admission.status.connections == admission.status.cursors == 0
