"""Exact control selection and retry across an uncertain storage mutation."""

from __future__ import annotations

import sqlite3

from riverhog_client import PrimaryStateError

CAPACITY = 65536
BEFORE = bytes(range(256)) * 256
AFTER = bytes(reversed(range(256))) * 256
ORIGINAL = b"original sealed encoding" * 100
PAYLOAD = b"original retained journal" * 400


def _body(files):
    return files.read_control("changing", byte_limit=CAPACITY) if files.exists("changing") else None


def _extent(files):
    return files._one(
        "SELECT count(*),sum(length(body)) FROM primary_controls WHERE namespace=? "
        "AND name IN ('original','changing')",
        (files._namespace,),
    )


def _controls_only(action, table, *_rest):
    return (
        sqlite3.SQLITE_DENY
        if action == sqlite3.SQLITE_READ and table == "primary_chunks"
        else sqlite3.SQLITE_OK
    )


def witness(opened, *, change, inject):
    assert change in ("install", "replace", "remove")
    before = None if change == "install" else BEFORE
    after = None if change == "remove" else AFTER

    def mutate(files):
        if after is None:
            files.remove_control("changing", expected=before)
        else:
            files.put_control("changing", after, expected=before, byte_limit=CAPACITY)

    with opened(initialize=True) as files:
        files.prepare_controls(("original", "changing"), byte_limit=CAPACITY)
        files.put_control("original", ORIGINAL, byte_limit=CAPACITY)
        original = files.put_bytes("journal", (PAYLOAD,), byte_limit=len(PAYLOAD))
        if before is not None:
            files.put_control("changing", before, byte_limit=CAPACITY)
        capacity = _extent(files)
        assert capacity == (2, 2 * (CAPACITY + 9))
        files._connection.set_authorizer(_controls_only)
        inject(files)
        try:
            mutate(files)
        except PrimaryStateError as exc:
            assert isinstance(exc.__cause__, sqlite3.Error)
            error = getattr(exc.__cause__, "sqlite_errorcode", None)
        else:
            raise AssertionError("storage failure was reported as a successful control mutation")
        try:
            files.sync()
        except PrimaryStateError:
            pass
        else:
            raise AssertionError("an uncertain control mutation retained a usable owner")

    with opened(initialize=False) as files:
        files._connection.set_authorizer(_controls_only)
        assert files.read_control("original", byte_limit=CAPACITY) == ORIGINAL
        visible = _body(files)
        assert visible in (before, after)
        assert _extent(files) == capacity
        files.prepare_controls(("original", "changing"), byte_limit=CAPACITY)
        if visible != after:
            mutate(files)
        else:
            # A visible uncertain winner needs a successful storage barrier.
            files.sync()
        assert _body(files) == after
        assert files.read_control("original", byte_limit=CAPACITY) == ORIGINAL
        assert _extent(files) == capacity
        files._connection.set_authorizer(None)
        with files.verified(original) as stream:
            assert stream.read() == PAYLOAD

    with opened(initialize=False) as files:
        assert _body(files) == after
        assert _extent(files) == capacity
        assert files.read_control("original", byte_limit=CAPACITY) == ORIGINAL
        with files.verified(original) as stream:
            assert stream.read() == PAYLOAD
    return {
        "change": change,
        "visible_outcome": "after" if visible == after else "before",
        "sqlite_errorcode": error,
        "original_control_exact": True,
        "original_payload_exact": True,
        "capacity_retained": True,
        "retry_coldly_verified": True,
        "power_failure_certified": False,
    }
