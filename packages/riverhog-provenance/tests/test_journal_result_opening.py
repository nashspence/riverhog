from __future__ import annotations

from contextlib import contextmanager

import pytest
from riverhog_provenance import (
    InvalidJournalWorkspace,
    JournalResultOpening,
    open_retained_journal_result,
)
from riverhog_provenance import journal_result as owner
from riverhog_provenance.journal_store import JournalStore
from test_journal_result import completed


class CloseFault:
    def __init__(self, connection, *, after, failures=1):
        self.connection, self.after, self.failures = connection, after, failures
        self.attempts = 0

    def __getattr__(self, name):
        return getattr(self.connection, name)

    def close(self):
        self.attempts += 1
        if self.failures:
            self.failures -= 1
            if self.after:
                self.connection.close()
            raise OSError("Native result close unconfirmed")
        self.connection.close()


@contextmanager
def failed_open(path, source, confirmed, catalog, monkeypatch, *, boundary, after):
    openings, connections, releases = [], [], []
    restore = JournalStore._restore_workspace

    def fault(store):
        wrapped = CloseFault(store._storage.connection, after=after)
        connections.append(wrapped)
        store._storage.connection = wrapped
        raise InterruptedError("Native result admission interrupted")

    def restore_fault(store):
        restore(store)
        fault(store)

    def prefix_fault(*args, **kwargs):
        fault(openings[0]._store)

    with monkeypatch.context() as patch:
        if boundary == "restore":
            patch.setattr(JournalStore, "_restore_workspace", restore_fault)
        else:
            patch.setattr(owner, "JournalPrefix", prefix_fault)
        with pytest.raises(OSError, match="close unconfirmed"):
            open_retained_journal_result(
                path,
                source,
                confirmed=confirmed,
                catalog=catalog,
                on_storage_open=openings.append,
                on_storage_close=lambda: releases.append(True),
            )
    assert len(openings) == len(connections) == 1
    opening, connection = openings[0], connections[0]
    try:
        yield opening, connection, releases
    finally:
        opening.close()


@pytest.mark.parametrize("boundary", ["restore", "prefix"])
@pytest.mark.parametrize("after", [False, True])
def test_actual_partial_native_handle_survives_unconfirmed_constructor_cleanup(
    tmp_path, journal, catalog, monkeypatch, boundary, after
):
    path, source, confirmed, _, _expected = completed(tmp_path, journal, catalog)
    file = path / "snapshot.sqlite3"
    before, inode = file.read_bytes(), file.stat().st_ino
    with failed_open(
        path, source, confirmed, catalog, monkeypatch, boundary=boundary, after=after
    ) as (opening, connection, releases):
        assert isinstance(opening, JournalResultOpening) and not opening.closed
        store = opening._store
        assert store is not None and store._storage.connection is connection
        assert connection.attempts == 1 and not releases
        opening.close()
        assert opening.closed and opening._store is None
        assert connection.attempts == 2 and not releases
        opening.close()
        assert connection.attempts == 2
        assert store._lease.closed and not store._cleanup.alive
    assert not source.calls
    assert file.read_bytes() == before and file.stat().st_ino == inode


@pytest.mark.parametrize("interruption", ["raise", "close"])
def test_early_retention_precedes_any_database_acquisition(
    tmp_path, journal, catalog, monkeypatch, interruption
):
    path, source, confirmed, _, _expected = completed(tmp_path, journal, catalog)
    openings = []

    def retained(opening):
        openings.append(opening)
        assert opening._store is None and not opening.closed
        if interruption == "raise":
            raise InterruptedError("opening retained before acquisition")
        opening.close()

    def forbidden(*args, **kwargs):
        pytest.fail("interrupted opening acquired a database")

    with monkeypatch.context() as patch:
        patch.setattr(JournalStore, "open_retained", forbidden)
        error = InterruptedError if interruption == "raise" else InvalidJournalWorkspace
        with pytest.raises(error):
            open_retained_journal_result(
                path, source, confirmed=confirmed, catalog=catalog, on_storage_open=retained
            )
    assert len(openings) == 1 and openings[0]._store is None
    openings[0].close()
    assert openings[0].closed and not source.calls


def test_connection_failure_leaves_the_early_opening_reachable_without_storage(
    tmp_path, journal, catalog, monkeypatch
):
    path, source, confirmed, _, _expected = completed(tmp_path, journal, catalog)
    openings = []

    def unavailable(*args, **kwargs):
        assert len(openings) == 1 and openings[0]._store is None
        raise OSError("read-only connection unavailable")

    with monkeypatch.context() as patch:
        patch.setattr(owner.JournalStore, "open_retained", unavailable)
        with pytest.raises(OSError, match="connection unavailable"):
            open_retained_journal_result(
                path,
                source,
                confirmed=confirmed,
                catalog=catalog,
                on_storage_open=openings.append,
            )
    openings[0].close()
    assert openings[0].closed and not source.calls


def test_success_transfers_storage_and_last_borrower_alone_releases_callback(
    tmp_path, journal, catalog, monkeypatch
):
    path, source, confirmed, _, expected = completed(tmp_path, journal, catalog)
    file = path / "snapshot.sqlite3"
    before, inode = file.read_bytes(), file.stat().st_ino
    openings, releases, results = [], [], []
    connect = owner.JournalStore.open_retained

    def opened(*args, **kwargs):
        assert len(openings) == 1
        assert not openings[0].closed and openings[0]._store is None
        return connect(*args, **kwargs)

    try:
        with monkeypatch.context() as patch:
            patch.setattr(owner.JournalStore, "open_retained", opened)
            result = open_retained_journal_result(
                path,
                source,
                confirmed=confirmed,
                catalog=catalog,
                on_storage_open=openings.append,
                on_storage_close=lambda: releases.append(True),
            )
        results.append(result)
        last = result.borrow()
        results.append(last)
        opening = openings[0]
        assert opening.closed and opening._store is None
        opening.close()
        assert result.anchor == expected["journal"] and not releases
        result.close()
        assert last.anchor == expected["journal"] and not releases
        last.close()
        assert releases == [True]
        opening.close()
        assert releases == [True] and not source.calls
        assert file.read_bytes() == before and file.stat().st_ino == inode
    finally:
        for result in results:
            result.close()
        for opening in openings:
            opening.close()
