"""Measure real admitted connections and cursors without changing their grants."""

from contextlib import contextmanager
from functools import cache

import pytest
import sqlite_admission


class CursorObservation:
    def __init__(self, actual):
        self.actual = actual
        self.actual._row_observer = type(self)._row_observer
        self.closed = False

    @property
    def connection(self):
        return self.actual.connection

    def __getattr__(self, name):
        return getattr(self.actual, name)

    def execute(self, *args, **kwargs):
        self.actual.execute(*args, **kwargs)
        return self

    def executemany(self, *args, **kwargs):
        self.actual.executemany(*args, **kwargs)
        return self

    def executescript(self, *args, **kwargs):
        self.actual.executescript(*args, **kwargs)
        return self

    def fetchone(self):
        return self.record(self.actual.fetchone())

    def fetchall(self):
        return [self.record(row) for row in self.actual.fetchall()]

    def fetchmany(self, *args):
        return [self.record(row) for row in self.actual.fetchmany(*args)]

    def __iter__(self):
        return self

    def __next__(self):
        return self.record(next(self.actual))

    def close(self):
        if not self.closed:
            self.actual.close()
            self.closed = True


@cache
def cursor_observation(rows):
    return type("ObservedCursor", (CursorObservation, rows), {"_row_observer": rows})


def observed_connection_type(observation, original, *, initialize, recording):
    class ObservedConnection(observation, original):
        def __init__(self, *args, **kwargs):
            original.__init__(self, *args, **kwargs)
            initialize(self)

        def cursor(self, factory=None):
            selected = None if factory is None else cursor_observation(factory._row_observer)
            actual = original.cursor(self)
            return actual if selected is None else selected(actual)

        def execute(self, sql, parameters=()):
            # Raw observers may call sqlite3.Connection directly while idle.
            # Every retained statement still uses the actual original grant.
            if not recording(self):
                return original.execute(self, sql, parameters)
            return observation.execute(self, sql, parameters)

    return ObservedConnection


@contextmanager
def observe_admitted_scope(scope, observation, *, initialize, recording):
    driver = sqlite_admission._connect

    def connect(database, **options):
        original = options.get("factory")
        if getattr(original, "_reservation", None) is not scope._lease:
            return driver(database, **options)
        options["factory"] = observed_connection_type(
            observation, original, initialize=initialize, recording=recording
        )
        result = driver(database, **options)
        assert result is scope._lease.connection
        return result

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(sqlite_admission, "_connect", connect)
        yield
