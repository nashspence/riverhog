"""Public application startup for the qualified Windows SQLite resource owner."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from pathlib import Path

from riverhog_client._prepared_primary_factory import WindowsPrimaryFactory
from riverhog_client._prepared_primary_runtime import _admit_process_budget
from riverhog_client._prepared_primary_sqlite import SqlitePrimaryLimits
from riverhog_client._prepared_primary_sqlite_operations import SqliteOperationGrants


class WindowsSqliteProcess:
    """Install one application-owned H and native population policy before work.

    Controlled application startup must exclude other SQLite configuration and
    install this owner before its database consumers. H covers SQLite-accounted
    heap in the qualified loaded engine, not Python, other libraries or RSS.
    The application separately owns connection/cursor and operation admission,
    priorities and per-consumer cache/temp settings.

    Pass verify to the shared connection admission's process check, and its
    connect method to primary_factory and every selected co-resident consumer.
    Closing a store does not reset this process policy or release unknown work.
    """

    def __init__(self, *, heap_bytes: int, native_limits: tuple[int, int]) -> None:
        self._process = _admit_process_budget(heap_bytes, native_limits=native_limits)

    def report(self) -> dict[str, int]:
        """Verify current ownership, engine accounting and the aggregate ceiling."""
        from riverhog_client import _prepared_primary_native as native

        if native.current_process_budget() is not self._process:
            raise RuntimeError("Windows SQLite process policy has a different owner")
        return self._process.report()

    def verify(self) -> None:
        """Verify the installed policy without opening a database or changing H."""
        self.report()

    def native_report(self) -> dict[str, int]:
        """Observe retained native owner/file obligations under the current H."""
        self.verify()
        return self._process.native_report()

    def increase_heap(self, heap_bytes: int) -> None:
        """Apply an explicit larger aggregate grant without resetting use."""
        self.verify()
        self._process.increase(heap_bytes)

    def increase_native(self, native_limits: tuple[int, int]) -> None:
        """Raise explicit native grants without releasing retained obligations."""
        self.verify()
        self._process.increase_native(native_limits)

    def primary_factory(
        self,
        *,
        limits: SqlitePrimaryLimits,
        operations: SqliteOperationGrants,
        retry_profile: tuple[int, int],
        connection_factory: Callable[..., sqlite3.Connection],
    ) -> WindowsPrimaryFactory:
        """Use the application's actual connection admission for every pager.

        This includes explicit schema initialization, cold schema verification
        and the post-reconciliation pager replacement. Physical VFS admission
        and its original forward/recovery grants remain with the byte owner.
        """
        if not callable(connection_factory):
            raise TypeError("application SQLite connection admission is required")
        self.verify()
        return WindowsPrimaryFactory(
            limits, operations, self._process, retry_profile, connection_factory
        )

    def primary_location(self, parent: Path, allocation_descriptor: bytes) -> Path:
        """Select the exact allocation's container under an admitted durable parent."""
        from riverhog_client._prepared_primary_windows_native import windows_container_path

        self.verify()
        return windows_container_path(parent, allocation_descriptor)
