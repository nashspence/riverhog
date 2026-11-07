"""Real storage progress at its page ceiling; journal headroom is observed separately."""

from __future__ import annotations

from dataclasses import asdict


def witness(opened, *, database_bytes: int):
    from riverhog_client import PrimaryStateBusy, PrimaryStateCapacity, PrimaryStateError

    capacity = 8192
    original_body = b"o" * 32768
    cases = []
    with opened(initialize=True) as files:
        files.prepare_controls(("receipt",), byte_limit=capacity)
        assert not files.exists("receipt")
        original = files.put_bytes("original", (original_body,), byte_limit=len(original_body))
        try:
            files.put_bytes("candidate", (b"x" * database_bytes,), byte_limit=database_bytes)
        except PrimaryStateCapacity:
            pass
        else:
            raise AssertionError("fixture did not exhaust the configured database page grant")
    previous = None
    for body in (b"", b"r" * 17, b"c" * capacity, b"d" * capacity, b"e"):
        with opened(initialize=False) as files:
            before = files.space_usage()
            assert before.database_pages * before.page_bytes == database_bytes
            assert before.reusable_pages == 0
            operation = files._one("SELECT generation,cursor,phase FROM primary_operation")
            changes = files._connection.total_changes
            files.prepare_controls(("receipt",), byte_limit=capacity)
            assert files._connection.total_changes == changes
            try:
                files.prepare_controls(("unadmitted",), byte_limit=capacity)
            except PrimaryStateBusy:
                pass
            else:
                raise AssertionError("a new control borrowed capacity during candidate cleanup")

            def no_payload(action, table, *_rest):
                import sqlite3

                return (
                    sqlite3.SQLITE_DENY
                    if action == sqlite3.SQLITE_READ and table == "primary_chunks"
                    else sqlite3.SQLITE_OK
                )

            files._connection.set_authorizer(no_payload)
            try:
                files.put_control("receipt", body, byte_limit=capacity, expected=previous)
                assert files.read_control("receipt", byte_limit=capacity) == body
                try:
                    files.put_control(
                        "receipt", b"wrong", byte_limit=capacity, expected=b"mismatch"
                    )
                except PrimaryStateError:
                    pass
                else:
                    raise AssertionError("a mismatched control comparison was accepted")
                assert files.read_control("receipt", byte_limit=capacity) == body
            finally:
                files._connection.set_authorizer(None)
            after = files.space_usage()
            assert after.database_pages == before.database_pages
            assert after.reusable_pages == before.reusable_pages
            assert after.database == before.database
            assert files._one("SELECT generation,cursor,phase FROM primary_operation") == operation
            with files.verified(original) as stream:
                assert stream.read() == original_body
            cases.append(
                {"body_bytes": len(body), "before": asdict(before), "after": asdict(after)}
            )
            previous = body
    with opened(initialize=False) as files:
        before = files.space_usage()
        files.remove_control("receipt", expected=previous)
        assert not files.exists("receipt")
        files.remove_control("receipt", expected=previous)
        files.put_control("receipt", b"new exact receipt", byte_limit=capacity)
        assert files.read_control("receipt", byte_limit=capacity) == b"new exact receipt"
        assert files.space_usage().database == before.database
        with files.verified(original) as stream:
            assert stream.read() == original_body
    return {
        "capacity_bytes": capacity,
        "database_ceiling_bytes": database_bytes,
        "cases": cases,
        "removal_retains_capacity": True,
        "original_bytes_preserved": True,
        "journal_preallocation_certified": False,
    }
