"""Real normal-Win32 delegation through the compiled private write VFS."""

from __future__ import annotations

import hashlib
import sqlite3
from contextlib import contextmanager
from dataclasses import astuple
from pathlib import Path


class NativeWriteShim:
    def __init__(self, limits):
        from riverhog_client import _prepared_primary_native as native

        with sqlite3.connect(":memory:") as connection:
            expected = connection.execute("SELECT sqlite_source_id()").fetchone()[0]
        initial = (
            4096,
            1 + limits.database_bytes // 4096,
            limits.database_bytes,
            limits.journal_bytes,
            8 * (limits.database_bytes + limits.journal_bytes),
            65536,
        )
        self._initial = native.WriteQuota(initial, initial)
        read_limits = (64 * (limits.database_bytes + limits.journal_bytes), 65536)
        self._initial_reads = native.ReadQuota(self._initial, read_limits, read_limits)
        self._active_reads = self._initial_reads
        self._native = native.WriteVfs(expected, self._initial_reads)
        self.name = self._native.name
        try:
            identity = self._native.identity()
            self.identity = {
                "sqlite_source_id": identity["sqlite_source_id"],
                "sqlite_library_sha256": hashlib.sha256(
                    Path(identity["sqlite_library_path"]).read_bytes()
                ).hexdigest(),
                "base_vfs": identity["base_vfs"],
                "interposer": "compiled-write-vfs-version-1",
            }
        except BaseException:
            self._native.close()
            raise

    def __enter__(self):
        # Registration must be visible to stdlib, not merely a matching DLL build.
        try:
            connection = sqlite3.connect("file::memory:?vfs=" + self.name, uri=True)
            try:
                actual = connection.execute("SELECT sqlite_source_id()").fetchone()[0]
            finally:
                connection.close()
            assert actual == self.identity["sqlite_source_id"]
        except BaseException:
            self._native.close()
            raise
        return self

    def __exit__(self, *_args):
        self._native.close()

    def read_report(self):
        return self._active_reads.report()

    @contextmanager
    def write_quota(self, *, forward, recovery, compiled=False):
        from riverhog_client import _prepared_primary_native as native

        assert compiled
        quota = native.WriteQuota(astuple(forward), astuple(recovery))
        reads = native.ReadQuota(
            quota,
            (64 * (forward.database_end + forward.journal_end), forward.calls),
            (64 * (recovery.database_end + recovery.journal_end), recovery.calls),
        )
        self._native.arm(reads)
        self._active_reads = reads
        try:
            yield quota
        finally:
            self._native.arm(self._initial_reads)
            self._active_reads = self._initial_reads
