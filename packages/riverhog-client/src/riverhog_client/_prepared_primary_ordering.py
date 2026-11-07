"""A scoped derived ordering view with independent point-read cursors."""

from __future__ import annotations

from collections.abc import Callable, Iterator

from riverhog_client._prepared_primary_storage import RetainedPrimaryOrdering
from riverhog_client._prepared_primary_types import PrimaryStateError

_Row = tuple[str, int, str]


class OrderedRows:
    """Retain only a count and a backend-owned point reader, never the rows."""

    def __init__(
        self,
        count: int,
        read: Callable[[str | None], _Row | None],
        retain: Callable[[], RetainedPrimaryOrdering],
    ) -> None:
        self._count = count
        self._read = read
        self._closed = False
        self._retain = retain

    @property
    def count(self) -> int:
        return self._count

    def close(self) -> None:
        self._closed = True

    def retain(self) -> RetainedPrimaryOrdering:
        """Retain this completed derivation explicitly; never extend the selected lease."""
        if self._closed:
            raise PrimaryStateError("prepared-primary ordering view is closed")
        return self._retain()

    def rows(self) -> Iterator[_Row]:
        after: str | None = None
        seen = 0
        while True:
            if self._closed:
                raise PrimaryStateError("prepared-primary ordering view is closed")
            row = self._read(after)
            if row is None:
                if seen != self.count:
                    raise PrimaryStateError("prepared-primary ordering row count changed")
                return
            key, _size, _digest = row
            if seen >= self.count or (after is not None and key <= after):
                raise PrimaryStateError("prepared-primary ordering rows changed")
            seen += 1
            after = key
            yield row
