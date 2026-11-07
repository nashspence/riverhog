"""Observe actual catalog SQL, driver, metadata, codec and hash work."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from sqlalchemy import Row, event


def octets(value):
    if value is None:
        return 0
    if isinstance(value, (tuple, list, Row)):
        return sum(map(octets, value))
    if isinstance(value, dict):
        return sum(map(octets, value.values()))
    return len(value) if isinstance(value, bytes) else len(str(value).encode())


class CatalogRecorder:
    def __init__(self, owner):
        self.owner = owner
        self.active = False
        self.metrics = {
            name: 0
            for name in (
                "driver_statements",
                "sql_statements",
                "returned_rows",
                "returned_metadata_octets",
                "bound_parameter_octets",
                "sqlite_vm_instructions",
                "json_decodes",
                "json_encodes",
                "json_decoded_octets",
                "json_encoded_octets",
                "hashed_octets",
                "uuid_values",
            )
        }
        self.native, self.statements = [], []

    def row(self, value):
        if self.active and value is not None:
            self.metrics["returned_rows"] += 1
            assert not any(isinstance(item, bytes) for item in value)
            self.metrics["returned_metadata_octets"] += octets(value)
        return value

    def trace(self, _statement):
        if self.active:
            self.metrics["driver_statements"] += 1

    def tick(self):
        if self.active:
            self.metrics["sqlite_vm_instructions"] += 1
        return 0

    @contextmanager
    def observe(self, engine, *, cold):
        observer = self
        owner = self.owner
        real_uuid = owner.uuid4
        engine.dispose()
        dbapi, connect = engine.dialect.dbapi, engine.dialect.dbapi.connect
        real_loads, real_encode, real_hash = (
            getattr(getattr(owner, "json", None), "loads", None),
            getattr(owner, "canonical_json_bytes", None),
            owner.hashlib.sha256,
        )

        class Cursor(sqlite3.Cursor):
            def execute(self, sql, parameters=()):
                if observer.active:
                    observer.metrics["bound_parameter_octets"] += octets(parameters)
                return super().execute(sql, parameters)

            def fetchone(self):
                return observer.row(super().fetchone())

            def fetchall(self):
                return [observer.row(row) for row in super().fetchall()]

            def fetchmany(self, *args):
                return [observer.row(row) for row in super().fetchmany(*args)]

            def __next__(self):
                return observer.row(super().__next__())

        class Connection(sqlite3.Connection):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                observer.native.append(self)
                self.set_trace_callback(observer.trace)
                self.set_progress_handler(observer.tick, 1)
                self.set_authorizer(self.authorize)

            def cursor(self, *args, **kwargs):
                kwargs["factory"] = Cursor
                return super().cursor(*args, **kwargs)

            def authorize(self, action, _table, column, _database, _trigger):
                if (
                    observer.active
                    and action == sqlite3.SQLITE_READ
                    and column
                    in {
                        "content",
                        "canonical_json",
                        "exact_json",
                        "schema_json",
                    }
                ):
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK

        def observed_connect(*args, **kwargs):
            kwargs["factory"] = Connection
            return connect(*args, **kwargs)

        def sql(_connection, _cursor, statement, parameters, _context, many):
            assert not many
            if observer.active:
                observer.metrics["sql_statements"] += 1
                observer.statements.append((statement, parameters))

        def loads(value, *args, **kwargs):
            observer.metrics["json_decodes"] += 1
            observer.metrics["json_decoded_octets"] += octets(value)
            return real_loads(value, *args, **kwargs)

        def encode(*args, **kwargs):
            observer.metrics["json_encodes"] += 1
            value = real_encode(*args, **kwargs)
            observer.metrics["json_encoded_octets"] += len(value)
            return value

        class Hash:
            def __init__(self, actual):
                self.actual = actual

            def update(self, value):
                observer.metrics["hashed_octets"] += len(value)
                self.actual.update(value)

            def copy(self):
                return Hash(self.actual.copy())

            def __getattr__(self, name):
                return getattr(self.actual, name)

        def sha256(value=b"", *args, **kwargs):
            observer.metrics["hashed_octets"] += len(value)
            return Hash(real_hash(value, *args, **kwargs))

        def uuid4():
            observer.metrics["uuid_values"] += 1
            return real_uuid()

        event.listen(engine, "before_cursor_execute", sql)
        try:
            with pytest.MonkeyPatch.context() as patch:
                patch.setattr(dbapi, "connect", observed_connect)
                if not cold:
                    with engine.connect():
                        pass
                if real_loads is not None:
                    patch.setattr(owner, "json", SimpleNamespace(loads=loads))
                if real_encode is not None:
                    patch.setattr(owner, "canonical_json_bytes", encode)
                patch.setattr(owner, "uuid4", uuid4)
                patch.setattr(owner, "hashlib", SimpleNamespace(sha256=sha256))
                self.active = True
                try:
                    yield
                finally:
                    self.active = False
        finally:
            event.remove(engine, "before_cursor_execute", sql)
        self.metrics["connection_statements"] = (
            self.metrics["driver_statements"] - self.metrics["sql_statements"]
        )
        self.metrics["changed_rows"] = sum(db.total_changes for db in self.native)
        self.metrics["source_payload_octets"] = 0
        assert engine.pool.checkedout() == 0
        for statement, parameters in self.statements:
            with engine.connect() as connection:
                with connection.exec_driver_sql(
                    "EXPLAIN QUERY PLAN " + statement, parameters
                ) as result:
                    plan = " ".join(row[3] for row in result)
            assert "SCAN" not in plan and "USE TEMP" not in plan
            if statement.lstrip().upper().startswith(("SELECT", "UPDATE", "DELETE")):
                assert "SEARCH" in plan
