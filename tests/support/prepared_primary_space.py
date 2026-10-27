"""Common physical-space witness; control admission and machine-reset proofs are separate."""

from __future__ import annotations

from dataclasses import asdict


def witness(opened, *, io_bytes=None):
    """Measure repeated growth/reclaim and single-page maintenance without crediting free pages."""
    cycles = []
    with opened(initialize=True) as files:
        original = files.put_bytes("original", (b"retained original",), byte_limit=17)
        files.put_control("receipt", b"exact original control", byte_limit=64)
        for _cycle in range(3):
            before = files.space_usage()
            files.put_bytes("journal", (b"x" * 65536,), byte_limit=65536)
            written = files.space_usage()
            assert written.database.end_bytes > before.database.end_bytes
            steps = 0
            while not files.remove_step("journal"):
                steps += 1
                assert steps < 100  # This fixture contains exactly sixteen payload chunks.
            deleted = files.space_usage()
            assert deleted.database == written.database
            assert deleted.reusable_pages > before.reusable_pages
            floor = 4  # Fixture-owned floor, not a production reservation.
            maintenance = 0
            released_pages = 0
            released_allocation = 0
            maximum_database_write_bytes = 0
            maximum_journal_write_bytes = 0
            total_database_write_bytes = 0
            while True:
                io_before = io_bytes() if io_bytes else {}
                step = files.vacuum_step(keep_free_pages=floor)
                io_after = io_bytes() if io_bytes else {}
                if step.before != step.after:
                    maintenance += 1
                    assert step.after.reusable_pages == step.before.reusable_pages - 1
                    released_pages += step.before.database_pages - step.after.database_pages
                    released_allocation += (
                        step.before.database.allocated_bytes - step.after.database.allocated_bytes
                    )
                assert step.after.reusable_pages >= floor
                database_write = io_after.get("write:database", 0) - io_before.get(
                    "write:database", 0
                )
                journal_write = io_after.get("write:journal", 0) - io_before.get("write:journal", 0)
                maximum_database_write_bytes = max(maximum_database_write_bytes, database_write)
                maximum_journal_write_bytes = max(maximum_journal_write_bytes, journal_write)
                total_database_write_bytes += database_write
                assert maintenance < 100
                if step.done:
                    break
            maintained = files.space_usage()
            assert maintained.reusable_pages == floor
            assert maintained.database.end_bytes < deleted.database.end_bytes
            assert released_pages == deleted.database_pages - maintained.database_pages
            assert released_allocation == (
                deleted.database.allocated_bytes - maintained.database.allocated_bytes
            )
            assert released_allocation > 0
            assert maintained.journal.allocated_bytes > 0
            with files.verified(original) as stream:
                assert stream.read() == b"retained original"
            assert files.read_control("receipt", byte_limit=64) == b"exact original control"
            changes = files._connection.total_changes

            def read_only(action, *_arguments):
                import sqlite3

                return (
                    sqlite3.SQLITE_DENY
                    if action
                    in (
                        sqlite3.SQLITE_INSERT,
                        sqlite3.SQLITE_UPDATE,
                        sqlite3.SQLITE_DELETE,
                        sqlite3.SQLITE_TRANSACTION,
                    )
                    else sqlite3.SQLITE_OK
                )

            files._connection.set_authorizer(read_only)
            try:
                no_work = files.vacuum_step(keep_free_pages=floor)
                assert no_work.done and no_work.before == no_work.after == maintained
            finally:
                files._connection.set_authorizer(None)
            assert files._connection.total_changes == changes
            cycles.append(
                {
                    "before": asdict(before),
                    "written": asdict(written),
                    "deleted": asdict(deleted),
                    "maintained": asdict(maintained),
                    "cleanup_steps": steps + 1,
                    "maintenance_steps": maintenance,
                    "released_pages": released_pages,
                    "released_database_allocation": released_allocation,
                    "maximum_vacuum_database_write_bytes": maximum_database_write_bytes,
                    "maximum_vacuum_journal_write_bytes": maximum_journal_write_bytes,
                    "total_vacuum_database_write_bytes": total_database_write_bytes,
                }
            )
    with opened(initialize=False) as files:
        with files.verified(original) as stream:
            assert stream.read() == b"retained original"
        assert files.read_control("receipt", byte_limit=64) == b"exact original control"
        cold = files.space_usage()
    return {"cycles": cycles, "cold": asdict(cold)}
