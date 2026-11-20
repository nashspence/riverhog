"""Journal preparation evidence, separate from physical-volume and machine-reset qualification."""

from __future__ import annotations

import sqlite3
from dataclasses import asdict


def witness(opened, *, capacity: int, database_bytes: int):
    from riverhog_client import PrimaryStateCapacity

    with opened(initialize=True) as files:
        files.prepare_controls(("receipt",), byte_limit=capacity)
        prepared = files.space_usage()
        assert prepared.journal_reserved_bytes > capacity
        assert prepared.journal.end_bytes >= prepared.journal_reserved_bytes
        assert prepared.journal.allocated_bytes >= prepared.journal_reserved_bytes
        original = files.put_bytes("original", (b"original" * 4096,), byte_limit=32768)
        try:
            files.put_bytes("candidate", (b"p" * database_bytes,), byte_limit=database_bytes)
        except PrimaryStateCapacity as exc:
            assert isinstance(exc.__cause__, sqlite3.Error)
            assert exc.__cause__.sqlite_errorcode == sqlite3.SQLITE_FULL
        else:
            raise AssertionError("fixture did not exhaust its database grant")
    with opened(initialize=False) as files:
        before = files.space_usage()
        assert before.database_pages * before.page_bytes <= database_bytes
        body = b"r" * capacity
        files.put_control("receipt", body, byte_limit=capacity)
        assert files.read_control("receipt", byte_limit=capacity) == body
        files.put_control("receipt", b"after", byte_limit=capacity, expected=body)
        files.remove_control("receipt", expected=b"after")
        after = files.space_usage()
        assert after.database == before.database
        assert after.journal == before.journal
        assert after.journal_reserved_bytes == prepared.journal_reserved_bytes
        with files.verified(original) as stream:
            assert stream.read() == b"original" * 4096
    return {
        "capacity_bytes": capacity,
        "database_grant_bytes": database_bytes,
        "payload_rejected_with_sqlite_full": True,
        "unallocated_grant_pages": database_bytes // before.page_bytes - before.database_pages,
        "prepared_before_payload": asdict(prepared),
        "cold_before_control": asdict(before),
        "after_control": asdict(after),
        "original_preserved": True,
        "volume_enospc_certified": False,
    }
