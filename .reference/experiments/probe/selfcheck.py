"""Finite semantic checks; this never runs collection publication."""

import json
import sqlite3
import sys
import threading
from contextlib import closing
from pathlib import Path

import sqlite_admission
from riverhog_991_probe import Recorder, calibration, install_catalog, install_sqlite
from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError


def main():
    recorder = Recorder()
    saved = [
        (owner, name, getattr(owner, name))
        for owner, names in (
            (
                sqlite_admission._Cursor,
                (
                    "execute",
                    "executemany",
                    "executescript",
                    "fetchone",
                    "fetchmany",
                    "fetchall",
                    "__next__",
                ),
            ),
            (sqlite_admission._Connection, ("commit", "rollback")),
        )
        for name in names
    ]
    listeners = ()
    try:
        install_sqlite(recorder)
        listeners = install_catalog(recorder)
        pool = sqlite_admission.SqliteConnectionAdmission(
            connection_limit=1, cursor_limit=2, operation_limit=1, verify_process=lambda: None
        )
        with closing(pool.connect(":memory:")) as db:
            with closing(db.execute("CREATE TABLE example(value)")) as cursor:
                assert isinstance(cursor, sqlite_admission._Cursor)
                assert cursor.connection is db
            consumed = []

            def parameters():
                for value in (1, 2):
                    consumed.append(value)
                    yield (value,)

            with closing(db.executemany("INSERT INTO example VALUES(?)", parameters())):
                pass
            assert consumed == [1, 2]
            db.commit()
            with closing(db.execute("SELECT value FROM example ORDER BY value")) as cursor:
                assert list(cursor) == [(1,), (2,)]
            try:
                db.execute("SELECT missing_column FROM example")
            except sqlite3.OperationalError:
                pass
            else:
                raise AssertionError("actual SQL exception was lost")
            db.rollback()
            assert pool.status.operations == 0
        assert pool.status.connections == pool.status.cursors == pool.status.operations == 0
        engine = create_engine("sqlite://")
        try:
            with engine.connect() as connection:
                assert connection.exec_driver_sql("SELECT 7").scalar_one() == 7
                try:
                    connection.exec_driver_sql("SELECT missing_column")
                except OperationalError:
                    pass
                else:
                    raise AssertionError("SQLAlchemy error was lost")
        finally:
            engine.dispose()
        marker = object()
        assert recorder.wrap(lambda: marker, "synthetic.return")() is marker
        error = KeyboardInterrupt()

        def fail():
            raise error

        try:
            recorder.wrap(fail, "synthetic.error")()
        except KeyboardInterrupt as observed:
            assert observed is error
        else:
            raise AssertionError("BaseException was lost")

        def nested():
            with recorder.span("synthetic.parent"):
                with recorder.span("synthetic.child"):
                    sum(range(1000))

        worker = threading.Thread(target=nested, name="probe-selfcheck")
        worker.start()
        worker.join()
        nested()
        report = recorder.report()
        assert report["attribution_valid"]
        rows = [row for thread in report["threads"] for row in thread["buckets"]]
        iterations = next(
            row for row in rows if row["operation"] == "native.admitted.cursor.__next__"
        )
        assert iterations["calls"] == 3 and iterations["rows"] == 2
        assert iterations["exhausted"] == 1 and iterations["raised"] == 0
        catalog = next(row for row in rows if row["operation"] == "catalog.cursor.execute")
        assert catalog["calls"] == 2 and catalog["raised"] == 1
        assert all(0 <= row["exclusive_cpu_ns"] <= row["cpu_ns"] for row in rows)
        assert all(0 <= row["exclusive_wall_ns"] <= row["wall_ns"] for row in rows)
        assert len(report["threads"]) == 2
        output = Path(sys.argv[1])
        output.mkdir(parents=True, exist_ok=True)
        (output / "selfcheck.json").write_text(
            json.dumps(
                {
                    "passed": True,
                    "actual_admitted_grants_received": True,
                    "actual_sqlite_and_sqlalchemy_errors_preserved": True,
                    "return_and_baseexception_identity_preserved": True,
                    "original_thread_joined": True,
                    "generator_consumed_once": True,
                    "micro_calibration": calibration(),
                    "observation": report,
                },
                indent=2,
            )
            + "\n"
        )
        print(
            json.dumps(
                {
                    "passed": True,
                    "threads": len(report["threads"]),
                    "iterator_calls": iterations["calls"],
                    "catalog_calls": catalog["calls"],
                }
            )
        )
    finally:
        for owner, name, original in saved:
            setattr(owner, name, original)
        for name, callback in listeners:
            event.remove(Engine, name, callback)


if __name__ == "__main__":
    main()
