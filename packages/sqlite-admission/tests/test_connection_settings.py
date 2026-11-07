from __future__ import annotations

import sqlite3
from contextlib import closing

import pytest
from sqlite_admission import (
    SqliteCapacity,
    SqliteConfigurationError,
    SqliteConnectionAdmission,
    SqliteConnectionSettings,
    configure_sqlite_connection,
)


def one(connection, statement):
    with closing(connection.execute(statement)) as cursor:
        return cursor.fetchone()


def pool(settings=None, *, cursors=2):
    return SqliteConnectionAdmission(
        connection_limit=1,
        cursor_limit=cursors,
        operation_limit=1,
        verify_process=lambda: None,
        connection_settings=settings,
    )


@pytest.mark.parametrize("temporary", ["FILE", "MEMORY"])
@pytest.mark.parametrize("cache", [None, 0, 17])
def test_selected_settings_override_owner_defaults_on_actual_connection(tmp_path, temporary, cache):
    admission = pool(SqliteConnectionSettings(cache_kib=cache, temporary_storage=temporary))
    db = admission.connect(tmp_path / "projection.sqlite")
    try:
        one(db, "PRAGMA cache_size=-29")
        one(db, "PRAGMA mmap_size=1048576")
        configure_sqlite_connection(db, defaults=SqliteConnectionSettings(cache_kib=512))
        assert one(db, "PRAGMA cache_size") == (-29 if cache is None else -cache,)
        assert one(db, "PRAGMA temp_store") == (1 if temporary == "FILE" else 2,)
        assert one(db, "PRAGMA mmap_size") in (None, (0,))
        assert admission.status.cursors == admission.status.operations == 0
        assert admission.status.connections == 1
    finally:
        db.close()
    assert admission.status.connections == 0


def test_owner_defaults_apply_without_an_application_profile(tmp_path):
    with closing(sqlite3.connect(tmp_path / "projection.sqlite")) as db:
        configure_sqlite_connection(db, defaults=SqliteConnectionSettings(cache_kib=31))
        assert one(db, "PRAGMA cache_size") == (-31,)
        assert one(db, "PRAGMA temp_store") == (1,)


@pytest.mark.parametrize("existing", ["temp", "attached", "transaction"])
def test_configuration_refuses_live_foreign_or_temporary_state_without_deleting_it(
    tmp_path, existing
):
    with closing(sqlite3.connect(tmp_path / "projection.sqlite")) as db:
        one(db, "PRAGMA cache_size=-29")
        one(db, "CREATE TABLE owned(value TEXT)")
        one(db, "INSERT INTO owned VALUES ('exact original')")
        db.commit()
        if existing == "temp":
            one(db, "CREATE TEMP TABLE retained(value TEXT)")
            one(db, "INSERT INTO retained VALUES ('temporary original')")
            db.commit()
        elif existing == "attached":
            one(db, "ATTACH ':memory:' AS retained")
        else:
            one(db, "BEGIN")
        before = one(db, "PRAGMA temp_store")
        with pytest.raises(SqliteConfigurationError, match="fresh|owned main"):
            configure_sqlite_connection(
                db, defaults=SqliteConnectionSettings(cache_kib=7, temporary_storage="MEMORY")
            )
        assert one(db, "SELECT value FROM owned") == ("exact original",)
        assert one(db, "PRAGMA temp_store") == before
        assert one(db, "PRAGMA cache_size") == (-29,)
        if existing == "temp":
            assert one(db, "SELECT value FROM retained") == ("temporary original",)
        if existing == "attached":
            assert any(row[1] == "retained" for row in db.execute("PRAGMA database_list"))
        if existing == "transaction":
            assert db.in_transaction
            db.rollback()


@pytest.mark.parametrize(
    "temporary,reported", [("MEMORY", "TEMP_STORE=0"), ("FILE", "TEMP_STORE=3")]
)
def test_reported_compile_time_temporary_override_is_refused(tmp_path, temporary, reported):
    with closing(sqlite3.connect(tmp_path / "projection.sqlite")) as db:
        db.create_function("sqlite_compileoption_used", 1, lambda name: int(name == reported))
        one(db, "PRAGMA cache_size=-29")
        before = one(db, "PRAGMA temp_store")
        with pytest.raises(SqliteConfigurationError, match="compile-time"):
            configure_sqlite_connection(
                db, defaults=SqliteConnectionSettings(cache_kib=7, temporary_storage=temporary)
            )
        assert one(db, "PRAGMA temp_store") == before
        assert one(db, "PRAGMA cache_size") == (-29,)


@pytest.mark.parametrize("setting", ["cache_size", "mmap_size", "temp_store"])
def test_effective_profile_is_checked_after_an_ignored_setting_write(tmp_path, setting):
    admission = pool(SqliteConnectionSettings(cache_kib=17, temporary_storage="MEMORY"))
    db = admission.connect(tmp_path / "projection.sqlite")
    try:
        one(db, "PRAGMA cache_size=-29")
        one(db, "PRAGMA temp_store=FILE")
        one(db, "PRAGMA mmap_size=1048576")

        def authorize(action, name, value, *rest):
            return (
                sqlite3.SQLITE_IGNORE
                if action == sqlite3.SQLITE_PRAGMA and name == setting and value is not None
                else sqlite3.SQLITE_OK
            )

        mapping_was_enabled = one(db, "PRAGMA mmap_size") not in (None, (0,))
        db.set_authorizer(authorize)
        if setting == "mmap_size" and not mapping_was_enabled:
            configure_sqlite_connection(db, defaults=SqliteConnectionSettings(cache_kib=512))
            assert one(db, "PRAGMA mmap_size") in (None, (0,))
        else:
            with pytest.raises(SqliteConfigurationError):
                configure_sqlite_connection(db, defaults=SqliteConnectionSettings(cache_kib=512))
        assert admission.status.cursors == admission.status.operations == 0
        assert admission.status.connections == 1
    finally:
        db.close()


def test_configuration_capacity_retry_keeps_original_cursor_and_selected_profile(tmp_path):
    selected = SqliteConnectionSettings(cache_kib=17, temporary_storage="MEMORY")
    admission = pool(selected, cursors=1)
    db = admission.connect(tmp_path / "projection.sqlite")
    cursor = db.execute("SELECT 1 UNION ALL SELECT 2")
    try:
        assert cursor.fetchone() == (1,)
        with pytest.raises(SqliteCapacity, match="cursor"):
            configure_sqlite_connection(db, defaults=SqliteConnectionSettings(cache_kib=512))
        assert admission.status.cursors == 1
        admission.increase(connection_limit=1, cursor_limit=2)
        configure_sqlite_connection(db, defaults=SqliteConnectionSettings(cache_kib=512))
        assert cursor.fetchone() == (2,) and cursor.fetchone() is None
        assert one(db, "PRAGMA cache_size") == (-17,)
        assert one(db, "PRAGMA temp_store") == (2,)
        assert admission.status.cursors == 1
    finally:
        cursor.close()
        db.close()


def test_false_valued_selected_profile_is_not_replaced_by_owner_defaults(tmp_path):
    class Selected(SqliteConnectionSettings):
        def __bool__(self):
            return False

    admission = pool(Selected(cache_kib=17, temporary_storage="MEMORY"))
    with closing(admission.connect(tmp_path / "projection.sqlite")) as db:
        configure_sqlite_connection(db, defaults=SqliteConnectionSettings(cache_kib=512))
        assert one(db, "PRAGMA cache_size") == (-17,)
        assert one(db, "PRAGMA temp_store") == (2,)
