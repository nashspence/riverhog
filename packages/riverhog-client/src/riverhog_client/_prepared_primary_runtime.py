"""Admit the qualified loaded Windows SQLite binary before global resource policy.

The binary identity supplies the compiled retry defaults. Controlled application
startup must still exclude foreign SQLite configuration and VFS mutation; reading
a source ID or querying retries after opening a file cannot establish that.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from riverhog_client._prepared_primary_native import SqliteProcessBudget

_SQLITE_SOURCE_ID = (
    "2024-01-30 16:01:20 e876e51a0ed5c5b3126f52e532044363a014bc594cfefa87ffb5b82257cc467a"
)
_SQLITE_LIBRARY_SHA256 = "6551fcb8a3d89a43f3f86196b653b8b04088103a50f1b8ebb5137ed34e8517d1"
_QUALIFIED_INITIAL_RETRIES = (10, 25)
_LIBRARY_BLOCK_BYTES = 64 * 1024
_MAX_LIBRARY_BYTES = 32 * 1024 * 1024


def _loaded_runtime_identity() -> dict[str, str]:
    # Importing stdlib loads its engine; this performs no container open.
    import sqlite3  # noqa: F401

    from riverhog_client import _prepared_primary_native as native

    return native.runtime_identity()


def _qualified_runtime() -> dict[str, str]:
    identity = _loaded_runtime_identity()
    if identity["sqlite_source_id"] != _SQLITE_SOURCE_ID:
        raise RuntimeError("the loaded SQLite source has no qualified resource profile")
    digest = hashlib.sha256()
    total = 0
    with Path(identity["sqlite_library_path"]).open("rb") as library:
        while block := library.read(_LIBRARY_BLOCK_BYTES):
            total += len(block)
            if total > _MAX_LIBRARY_BYTES:
                raise RuntimeError("the loaded SQLite binary exceeds its qualified image envelope")
            digest.update(block)
    if digest.hexdigest() != _SQLITE_LIBRARY_SHA256:
        raise RuntimeError("the loaded SQLite binary has no qualified resource profile")
    return {
        "sqlite_source_id": identity["sqlite_source_id"],
        "sqlite_library_sha256": digest.hexdigest(),
    }


def _admit_process_budget(heap_bytes: int) -> SqliteProcessBudget:
    identity = _qualified_runtime()
    from riverhog_client import _prepared_primary_native as native

    return native.SqliteProcessBudget(identity["sqlite_source_id"], heap_bytes)
