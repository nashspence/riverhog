from __future__ import annotations

import pytest
from riverhog_core.catalog_db import create_catalog_engine
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker
from state_schema import read_snapshot, sqlite_engine


@pytest.mark.parametrize("engine_owner", ["catalog", "shared"])
def test_sqlite_read_snapshot_keeps_controls_and_selected_rows_in_one_snapshot(
    tmp_path, engine_owner
):
    path = tmp_path / "snapshot.sqlite3"
    engine = (
        create_catalog_engine(f"sqlite:///{path}")
        if engine_owner == "catalog"
        else sqlite_engine(path)
    )
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    try:
        with engine.connect() as connection:
            assert connection.exec_driver_sql("PRAGMA journal_mode=WAL").scalar_one() == "wal"
        with engine.begin() as connection:
            connection.exec_driver_sql("CREATE TABLE head (accepted INTEGER NOT NULL)")
            connection.exec_driver_sql("CREATE TABLE member (ordinal INTEGER PRIMARY KEY)")
            connection.exec_driver_sql("INSERT INTO head VALUES (0)")
            connection.exec_driver_sql("INSERT INTO member VALUES (0)")
        with read_snapshot(factory) as observer:
            assert observer.scalar(text("SELECT accepted FROM head")) == 0
            with engine.begin() as writer:
                writer.exec_driver_sql("UPDATE head SET accepted = 1")
            assert observer.scalar(text("SELECT accepted FROM head")) == 0
            assert (
                observer.scalar(
                    text("SELECT count(*) FROM member WHERE ordinal < (SELECT accepted FROM head)")
                )
                == 0
            )
        with read_snapshot(factory) as observer:
            assert observer.scalar(text("SELECT accepted FROM head")) == 1
            assert (
                observer.scalar(
                    text("SELECT count(*) FROM member WHERE ordinal < (SELECT accepted FROM head)")
                )
                == 1
            )
    finally:
        engine.dispose()


def test_sqlite_read_snapshot_releases_failed_reader_before_reuse(tmp_path):
    engine = create_catalog_engine(f"sqlite:///{tmp_path / 'failed-reader.sqlite3'}")
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql("CREATE TABLE head (accepted INTEGER NOT NULL)")
            connection.exec_driver_sql("INSERT INTO head VALUES (0)")
        with pytest.raises(RuntimeError, match="reader interrupted"):
            with read_snapshot(factory) as observer:
                assert observer.scalar(text("SELECT accepted FROM head")) == 0
                raise RuntimeError("reader interrupted")
        with engine.begin() as writer:
            writer.exec_driver_sql("UPDATE head SET accepted = 1")
        with read_snapshot(factory) as observer:
            assert observer.scalar(text("SELECT accepted FROM head")) == 1
    finally:
        engine.dispose()


def test_sqlite_immediate_writer_fences_before_reading_a_registration_cursor(tmp_path):
    engine = create_catalog_engine(f"sqlite:///{tmp_path / 'writer.sqlite3'}")
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql("CREATE TABLE head (accepted INTEGER NOT NULL)")
            connection.exec_driver_sql("INSERT INTO head VALUES (0)")
        with engine.connect() as first, engine.connect() as second:
            second.exec_driver_sql("PRAGMA busy_timeout=0")
            second.commit()
            with first.execution_options(sqlite_immediate_writer=True).begin():
                assert first.exec_driver_sql("SELECT accepted FROM head").scalar_one() == 0
                with pytest.raises(OperationalError) as blocked:
                    second.execution_options(sqlite_immediate_writer=True).begin()
                assert blocked.value.orig.sqlite_errorcode == 5
                second.rollback()
            with second.execution_options(sqlite_immediate_writer=True).begin():
                second.exec_driver_sql("UPDATE head SET accepted = 1")
        with read_snapshot(sessionmaker(bind=engine)) as observer:
            assert observer.scalar(text("SELECT accepted FROM head")) == 1
    finally:
        engine.dispose()


@pytest.mark.parametrize("contended", [False, True])
def test_sqlite_deferred_writer_restores_connection_timeout(tmp_path, contended):
    engine = create_catalog_engine(f"sqlite:///{tmp_path / 'deferred-writer.sqlite3'}")
    try:
        with engine.connect() as first, engine.connect() as second:
            second.exec_driver_sql("PRAGMA busy_timeout=321")
            second.commit()
            first.execution_options(sqlite_immediate_writer=True).begin()
            if contended:
                with pytest.raises(OperationalError):
                    second.execution_options(
                        sqlite_immediate_writer=True, sqlite_defer_if_busy=True
                    ).begin()
                second.rollback()
            else:
                first.rollback()
                second.execution_options(
                    sqlite_immediate_writer=True, sqlite_defer_if_busy=True
                ).begin()
                second.rollback()
            assert second.exec_driver_sql("PRAGMA busy_timeout").scalar_one() == 321
            first.rollback()
    finally:
        engine.dispose()
