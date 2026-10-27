"""Physical observations and explicit maintenance preserve original primary authority."""

from __future__ import annotations

import os
import sqlite3

import pytest
from riverhog_client import PrimaryStateBusy, PrimaryStateError
from riverhog_client._prepared_primary_sqlite import SqlitePrimaryLimits, TransactionalPrimaryFiles

from tests.support.prepared_primary import SqlitePrimaryTestGuard
from tests.support.prepared_primary_space import witness

pytestmark = pytest.mark.skipif(os.name == "nt", reason="portable Unix-VFS physical-space witness")
LIMITS = SqlitePrimaryLimits(16 * 1024**2, 32 * 1024**2, 128 * 1024)


def _open(path, *, initialize):
    if initialize:
        path.touch(exist_ok=False)
    return TransactionalPrimaryFiles(
        SqlitePrimaryTestGuard(path),
        allocation_descriptor=b"physical maintenance allocation",
        limits=LIMITS,
        initialize=initialize,
    )


def test_alternating_growth_reuse_and_bounded_physical_release(tmp_path):
    path = tmp_path / "state.sqlite3"
    evidence = witness(lambda **kwargs: _open(path, **kwargs))
    assert len(evidence["cycles"]) == 3
    assert all(cycle["released_database_allocation"] > 0 for cycle in evidence["cycles"])


@pytest.mark.parametrize("chunks", [1, 65, 257])
def test_space_observation_is_read_only_and_does_not_access_payload_rows(tmp_path, chunks):
    with _open(tmp_path / "state.sqlite3", initialize=True) as files:
        files.put_bytes("journal", (b"x" * 4096 for _ in range(chunks)), byte_limit=chunks * 4096)
        changes = files._connection.total_changes

        def no_payload(action, table, *_rest):
            return (
                sqlite3.SQLITE_DENY
                if action == sqlite3.SQLITE_READ and table == "primary_chunks"
                else sqlite3.SQLITE_OK
            )

        files._connection.set_authorizer(no_payload)
        try:
            space = files.space_usage()
        finally:
            files._connection.set_authorizer(None)
        assert space.database.end_bytes == space.database_pages * space.page_bytes
        assert files._connection.total_changes == changes
        assert not files._connection.in_transaction


def test_maintenance_respects_live_readers_namespaces_and_interrupted_work(tmp_path):
    with _open(tmp_path / "state.sqlite3", initialize=True) as files:
        selected = files.put_bytes("journal", (b"x" * 9000,), byte_limit=9000)
        with files.verified(selected) as stream:
            with pytest.raises(PrimaryStateBusy, match="replay reader"):
                files.vacuum_step(keep_free_pages=0)
            assert stream.read(1) == b"x"
        with files.child("member", initialize=True) as member:
            with pytest.raises(PrimaryStateError, match="container root"):
                member.vacuum_step(keep_free_pages=0)
        rows = [("a", 1, "a" * 64), ("b", 2, "b" * 64)]
        ordering = files.ordered_rows(rows, byte_limit=128 * 1024)
        assert next(ordering) == rows[0]
        with pytest.raises(PrimaryStateBusy, match="ordering reader"):
            files.vacuum_step(keep_free_pages=0)
        ordering.close()
        assert not files.remove_step("journal")
        with pytest.raises(PrimaryStateBusy, match="cleanup"):
            files.vacuum_step(keep_free_pages=0)
        while not files.remove_step("journal"):
            pass
        assert files.space_usage().reusable_pages > 0


@pytest.mark.parametrize("floor", [-1, True, 1.5, None])
def test_maintenance_requires_an_explicit_nonnegative_floor(tmp_path, floor):
    with _open(tmp_path / "state.sqlite3", initialize=True) as files:
        with pytest.raises(ValueError, match="free-page floor"):
            files.vacuum_step(keep_free_pages=floor)
        files.sync()


def test_uncommitted_space_is_not_reported_as_a_physical_observation(tmp_path):
    with _open(tmp_path / "state.sqlite3", initialize=True) as files:
        with files._transaction():
            with pytest.raises(PrimaryStateBusy, match="settled transaction"):
                files.space_usage()
        assert files.space_usage().database_pages > 0


def test_failed_maintenance_requires_reopen_and_preserves_exact_primary(tmp_path):
    path = tmp_path / "state.sqlite3"
    with _open(path, initialize=True) as files:
        original = files.put_bytes("original", (b"original",), byte_limit=8)
        files.put_bytes("temporary", (b"x" * 9000,), byte_limit=9000)
        while not files.remove_step("temporary"):
            pass

        def denied(action, name, *_rest):
            return (
                sqlite3.SQLITE_DENY
                if action == sqlite3.SQLITE_PRAGMA and name == "incremental_vacuum"
                else sqlite3.SQLITE_OK
            )

        files._connection.set_authorizer(denied)
        with pytest.raises(PrimaryStateError, match="mutation failed"):
            files.vacuum_step(keep_free_pages=0)
        files._connection.set_authorizer(None)
        with pytest.raises(PrimaryStateError, match="uncertain write"):
            files.sync()
    with _open(path, initialize=False) as files:
        with files.verified(original) as stream:
            assert stream.read() == b"original"
        assert not files.vacuum_step(keep_free_pages=0).done


def test_reserved_controls_progress_at_zero_free_pages_through_cold_reopens(tmp_path):
    from tests.support.prepared_primary_controls import witness as control_witness

    path = tmp_path / "state.sqlite3"
    limits = SqlitePrimaryLimits(256 * 1024, 256 * 1024, 128 * 1024)

    def opened(*, initialize):
        if initialize:
            path.touch(exist_ok=False)
        return TransactionalPrimaryFiles(
            SqlitePrimaryTestGuard(path),
            allocation_descriptor=b"reserved controls at database saturation",
            limits=limits,
            initialize=initialize,
        )

    evidence = control_witness(opened, database_bytes=limits.database_bytes)
    assert len(evidence["cases"]) == 5
    assert evidence["removal_retains_capacity"] and evidence["original_bytes_preserved"]


@pytest.mark.parametrize("capacity", [65536, 262144])
def test_prepared_journal_headroom_survives_payload_saturation_and_cold_controls(
    tmp_path, capacity
):
    from tests.support.prepared_primary_journal import witness as journal_witness

    path = tmp_path / "state.sqlite3"
    limits = SqlitePrimaryLimits(2 * 1024**2, 2 * 1024**2, 128 * 1024)

    def opened(*, initialize):
        if initialize:
            path.touch(exist_ok=False)
        return TransactionalPrimaryFiles(
            SqlitePrimaryTestGuard(path),
            allocation_descriptor=b"prepared journal resource witness",
            limits=limits,
            initialize=initialize,
        )

    evidence = journal_witness(opened, capacity=capacity, database_bytes=limits.database_bytes)
    assert evidence["original_preserved"]
