"""Explicit physical storage construction from an already validated allocation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from riverhog_client._prepared_primary_storage import PrimaryStorage

if TYPE_CHECKING:
    from riverhog_client._prepared_primary_sqlite import SqlitePrimaryLimits


def posix_primary_files(
    location: Path, *, allocation_descriptor: bytes, initialize: bool
) -> PrimaryStorage:
    """Open a directory; the shared owner retains and checks its allocation control."""
    from riverhog_client._prepared_primary_posix import PrimaryFiles

    return PrimaryFiles(location, initialize=initialize)


@dataclass(frozen=True, slots=True)
class WindowsPrimaryFactory:
    """Open an explicitly located database using separately supplied SQLite grants."""

    limits: SqlitePrimaryLimits

    def __call__(
        self, location: Path, *, allocation_descriptor: bytes, initialize: bool
    ) -> PrimaryStorage:
        from riverhog_client._prepared_primary_sqlite import TransactionalPrimaryFiles

        return TransactionalPrimaryFiles.windows(
            location,
            allocation_descriptor=allocation_descriptor,
            limits=self.limits,
            initialize=initialize,
        )
