"""Explicit physical storage construction from an already validated allocation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from riverhog_client._prepared_primary_storage import PrimaryStorage

if TYPE_CHECKING:
    from riverhog_client._prepared_primary_native import SqliteProcessBudget
    from riverhog_client._prepared_primary_sqlite import SqlitePrimaryLimits
    from riverhog_client._prepared_primary_sqlite_operations import SqliteOperationGrants


def posix_primary_files(
    location: Path, *, allocation_descriptor: bytes, initialize: bool
) -> PrimaryStorage:
    """Open a directory; the shared owner retains and checks its allocation control."""
    from riverhog_client._prepared_primary_posix import PrimaryFiles

    return PrimaryFiles(location, initialize=initialize)


@dataclass(frozen=True, slots=True)
class WindowsPrimaryFactory:
    """Compose native storage using explicit grants and admitted application resources."""

    limits: SqlitePrimaryLimits
    operations: SqliteOperationGrants
    process: SqliteProcessBudget
    retry_profile: tuple[int, int]

    def __call__(
        self, location: Path, *, allocation_descriptor: bytes, initialize: bool
    ) -> PrimaryStorage:
        from riverhog_client._prepared_primary_sqlite_native import open_native

        return open_native(
            location,
            allocation_descriptor=allocation_descriptor,
            limits=self.limits,
            operations=self.operations,
            process=self.process,
            retry_profile=self.retry_profile,
            initialize=initialize,
        )
