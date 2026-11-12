"""Operator-selected active Native journal storage resources."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class JournalStorageGrants:
    """Shared ordinary connection/cursor/call grants and per-attempt file records.

    These bound active resources through actual receiving, not total journal or
    source size. They do not install an aggregate SQLite heap ceiling, allocate
    filesystem scratch or replace independently reserved terminal/worker grants.
    Native retains FILE temporary storage and its existing durability profile.
    """

    connections: int = 8
    cursors: int = 32
    operations: int = 4
    file_records: int = 2
    corpus_file_records: int = 3

    def __post_init__(self) -> None:
        for name in ("connections", "cursors", "operations", "file_records", "corpus_file_records"):
            value = getattr(self, name)
            if type(value) is not int or value < 1:
                raise ValueError(f"journal storage {name} must be a positive integer")
        if self.file_records < 2:
            raise ValueError("journal storage requires creator and POSIX sync records")
        if self.corpus_file_records < 3:
            raise ValueError("corpus storage requires guard, borrower and sync records")
