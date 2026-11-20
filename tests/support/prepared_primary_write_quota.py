"""Actual SQLite xWrite refusal and automatic rollback; not a production VFS."""

from __future__ import annotations

import sqlite3

from riverhog_client import PrimaryStateCapacity, PrimaryStateError

from tests.support.sqlite_vfs_faults import WriteLimits

CAPACITY = 256 * 1024


class _ObservedConnection:
    """Snapshot the driver error before the storage owner's exception handler."""

    def __init__(self, connection, quota, *, read_report=None, callback_report=None):
        self.connection, self.quota = connection, quota
        self.read_report = read_report
        self.callback_report = callback_report
        self.commit_error = None
        self.explicit_rollbacks = 0

    def __getattr__(self, name):
        return getattr(self.connection, name)

    def execute(self, statement, *arguments):
        try:
            return self.connection.execute(statement, *arguments)
        except sqlite3.Error as exc:
            if statement == "COMMIT":
                self.commit_error = {
                    "sqlite_errorcode": exc.sqlite_errorcode,
                    "in_transaction": self.connection.in_transaction,
                    "quota": self.quota.report(),
                    "read_quota": self.read_report() if self.read_report else None,
                    "callback_quota": self.callback_report() if self.callback_report else None,
                }
            raise

    def rollback(self):
        self.explicit_rollbacks += 1
        return self.connection.rollback()


def witness(opened, shim, refusal, *, compiled=False):
    assert refusal in ("database", "journal")
    original_control = b"a" * CAPACITY
    changed_control = b"b" * CAPACITY
    survivor_control = b"unrelated original control"
    payload = b"unrelated original payload" * 500
    with opened(initialize=True) as files:
        files.prepare_controls(("changing", "survivor"), byte_limit=CAPACITY)
        files.put_control("changing", original_control, byte_limit=CAPACITY)
        files.put_control("survivor", survivor_control, byte_limit=CAPACITY)
        original_payload = files.put_bytes("journal", (payload,), byte_limit=len(payload))
        limits = files._limits
        forward = WriteLimits(
            4096,
            2 if refusal == "database" else 128,
            limits.database_bytes,
            8192 if refusal == "journal" else limits.journal_bytes,
            4 * (limits.database_bytes + limits.journal_bytes),
            65536,
        )
        recovery = WriteLimits(
            4096,
            1 + limits.journal_bytes // (4096 + 8),
            limits.database_bytes,
            limits.journal_bytes,
            4 * (limits.database_bytes + limits.journal_bytes),
            65536,
        )
        with shim.write_quota(forward=forward, recovery=recovery, compiled=compiled) as quota:
            observed = _ObservedConnection(
                files._connection,
                quota,
                read_report=getattr(shim, "read_report", None),
                callback_report=getattr(shim, "callback_report", None),
            )
            files._connection = observed
            try:
                files.put_control(
                    "changing", changed_control, expected=original_control, byte_limit=CAPACITY
                )
            except PrimaryStateCapacity as exc:
                assert isinstance(exc.__cause__, sqlite3.Error)
                assert exc.__cause__.sqlite_errorcode == sqlite3.SQLITE_FULL
            else:
                raise AssertionError("quota refusal was reported as successful control progress")
            report = quota.report()
            assert report["refusals"] == 1 and report["recovering"]
            assert report["first_refusal"]["file"] == refusal
            assert report["first_refusal"]["bank"] == "forward"
            if refusal == "database":
                assert report["forward"]["database_pages_attempted"] == 2
                assert observed.commit_error is not None
                automatic = observed.commit_error["quota"]["recovery"]["database_pages_attempted"]
                assert automatic > 2
                assert observed.commit_error["in_transaction"] is False
                assert observed.explicit_rollbacks == 0
                if observed.read_report:
                    assert observed.commit_error["read_quota"]["recovery"]["read_calls"] > 0
                    assert observed.commit_error["read_quota"]["recovery"]["read_bytes"] > 0
            else:
                automatic = 0
                assert report["forward"]["database_pages_attempted"] == 0
            assert files._root._poisoned
            try:
                files.sync()
            except PrimaryStateError:
                pass
            else:
                raise AssertionError("quota refusal left its uncertain owner usable")
            # Preserve the bank through close; clearing it is not a rollback mechanism.
            files.close()
            report = quota.report()
            reads = observed.read_report() if observed.read_report else None
            callbacks = observed.callback_report() if observed.callback_report else None
            heap = shim.heap_report() if hasattr(shim, "heap_report") else None
            if callbacks is not None:
                assert callbacks["recovery"]["callback_slots_charged"] > 0
                assert callbacks["reserved_recovery_closes"] == 0
    cold = WriteLimits(
        4096,
        256,
        limits.database_bytes,
        limits.journal_bytes,
        4 * (limits.database_bytes + limits.journal_bytes),
        65536,
    )
    with shim.write_quota(forward=cold, recovery=recovery, compiled=compiled):
        with opened(initialize=False) as files:
            assert files.read_control("changing", byte_limit=CAPACITY) == original_control
            assert files.read_control("survivor", byte_limit=CAPACITY) == survivor_control
            with files.verified(original_payload) as stream:
                assert stream.read() == payload
            assert files._one(
                "SELECT length(body) FROM primary_controls WHERE name='changing'"
            ) == (CAPACITY + 9,)
            files.put_control(
                "changing", changed_control, expected=original_control, byte_limit=CAPACITY
            )
    with shim.write_quota(forward=cold, recovery=recovery, compiled=compiled):
        with opened(initialize=False) as files:
            assert files.read_control("changing", byte_limit=CAPACITY) == changed_control
            assert files.read_control("survivor", byte_limit=CAPACITY) == survivor_control
            with files.verified(original_payload) as stream:
                assert stream.read() == payload
    return {
        "refusal": refusal,
        "bank_implementation": "compiled-native" if compiled else "python-fixture",
        "tracking_bytes": quota.tracking_bytes if compiled else None,
        "quota": report,
        "read_quota": reads,
        "callback_quota": callbacks,
        "sqlite_process_heap": heap,
        "automatic_rollback_pages_before_driver_error": automatic,
        "explicit_rollback_calls": observed.explicit_rollbacks,
        "original_control_exact": True,
        "original_payload_exact": True,
        "survivor_control_exact": True,
        "capacity_retained": True,
        "larger_grant_retry_coldly_verified": True,
        "production_resource_gate_certified": False,
        "all_allocating_file_methods_covered": False,
        "initialization_quota_covered": False,
    }
