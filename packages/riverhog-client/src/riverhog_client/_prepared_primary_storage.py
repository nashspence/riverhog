"""Logical retained-byte and control operations under a construction owner."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from contextlib import AbstractContextManager
from pathlib import Path
from typing import BinaryIO, Protocol, Self

from riverhog_client._prepared_primary_types import PrimaryByteFile


class PrimaryStorage(Protocol):
    """Physical candidate names, generations and ordering stay with the adapter."""

    def __enter__(self) -> Self: ...
    def __exit__(self, *_args: object) -> None: ...
    def close(self) -> None: ...
    def check_current(self) -> None: ...
    def sync(self) -> None: ...
    def child(self, name: str, *, initialize: bool) -> Self: ...

    def put_bytes(
        self,
        name: str,
        chunks: Iterable[bytes],
        *,
        byte_limit: int,
        expected: tuple[int, str] | None = None,
    ) -> PrimaryByteFile: ...

    def discard_candidates(self, names: tuple[str, ...]) -> bool:
        """Advance only unselected candidates for these logical names."""
        ...

    def prepare_controls(self, names: tuple[str, ...], *, byte_limit: int) -> None:
        """Prepare backend control roles before payload; SQLite allocates fixed extents."""
        ...

    def read_control(self, name: str, *, byte_limit: int) -> bytes: ...

    def put_control(
        self, name: str, body: bytes, *, byte_limit: int, expected: bytes | None = None
    ) -> None: ...

    def remove_control(self, name: str, *, expected: bytes) -> None: ...
    def exists(self, name: str) -> bool: ...
    def remove_step(self, name: str) -> bool: ...

    def extent_reader(self, name: str, *, byte_count: int) -> AbstractContextManager[BinaryIO]: ...

    def verified(self, file: PrimaryByteFile) -> AbstractContextManager[BinaryIO]: ...

    def ordering_reserved_bytes(self) -> int:
        """Retained scratch admission, without advancing or scanning its rows."""
        ...

    def recover_ordering_step(self) -> bool:
        """Recover interrupted scratch in one step, retaining a ready generation."""
        ...

    def ordering_cleanup_step(self) -> bool:
        """Advance bounded explicit execution cleanup, retaining admission until done."""
        ...

    def ordered_rows(
        self, rows: Iterable[tuple[str, int, str]], *, byte_limit: int
    ) -> Iterator[tuple[str, int, str]]: ...


class PrimaryStorageFactory(Protocol):
    """Open one explicit location using the shared owner's canonical allocation bytes."""

    def __call__(
        self, location: Path, *, allocation_descriptor: bytes, initialize: bool
    ) -> PrimaryStorage: ...
