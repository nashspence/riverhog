"""Retain the first uncertain close and every resource that still depends on it."""

from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path
from typing import Any, Literal

from retained_resource import Borrow, Resource


class RetainedWorkspaceResource:
    """One original native namespace owner and its explicit descriptor borrow."""

    def __init__(
        self, kind: Literal["file", "directory"], after_close: Callable[[], None] | None = None
    ) -> None:
        self.owner = Resource(kind)
        self.borrow = Borrow(self.owner)
        self._after_close = after_close

    def acquire(
        self,
        path: str | Path,
        *,
        parent: RetainedWorkspaceResource | None = None,
        writable: bool = False,
        create: bool = False,
        exclusive: bool = False,
    ) -> None:
        self.owner.acquire(
            os.fsencode(path),
            writable=writable,
            parent=None if parent is None else parent.owner,
            create=create,
            exclusive=exclusive,
        )
        self.borrow.acquire()

    def duplicate(self, source: RetainedWorkspaceResource) -> None:
        self.owner.duplicate(source.owner)
        self.borrow.acquire()

    def fileno(self) -> int:
        return self.borrow.fileno()

    def close(self) -> None:
        self.borrow.close()
        self.owner.close()
        if self._after_close is not None:
            self._after_close()


class RetainedWorkspaceHandles:
    """A caller retains this owner before registering any acquired handle.

    Callbacks run in reverse acquisition order. A callback is removed only after
    actual success; a refusal retains it and all earlier custody. This owner
    never suppresses an error, acquires storage or infers physical completion.
    """

    def __init__(self) -> None:
        self._callbacks: list[tuple[Callable[..., Any], tuple[Any, ...]]] = []
        self.uncertain = False

    def acquire(
        self,
        kind: Literal["file", "directory"],
        path: str | Path,
        *,
        parent: RetainedWorkspaceResource | None = None,
        writable: bool = False,
        create: bool = False,
        exclusive: bool = False,
        after_close: Callable[[], None] | None = None,
    ) -> RetainedWorkspaceResource:
        original = RetainedWorkspaceResource(kind, after_close)
        self.callback(original.close)
        original.acquire(path, parent=parent, writable=writable, create=create, exclusive=exclusive)
        return original

    def callback(self, operation: Callable[..., Any], *arguments: Any) -> None:
        self._callbacks.append((operation, arguments))

    def close(self) -> None:
        while self._callbacks:
            operation, arguments = self._callbacks[-1]
            try:
                operation(*arguments)
            except BaseException:
                self.uncertain = True
                raise
            self._callbacks.pop()
        self.uncertain = False

    def __enter__(self) -> RetainedWorkspaceHandles:
        if self._callbacks:
            raise RuntimeError("workspace handle receiving remains uncertain")
        return self

    def __exit__(self, *_exc: Any) -> None:
        self.close()
