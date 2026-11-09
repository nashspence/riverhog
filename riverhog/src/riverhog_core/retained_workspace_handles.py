"""Retain the first uncertain close and every resource that still depends on it."""

from __future__ import annotations

import os
from collections.abc import Callable
from typing import Any


def _close_descriptor(descriptor: int) -> None:
    os.close(descriptor)


class RetainedWorkspaceHandles:
    """A caller retains this owner before registering any acquired handle.

    Callbacks run in reverse acquisition order. A callback is removed only after
    actual success; a refusal retains it and all earlier custody. This owner
    never suppresses an error, acquires storage or infers physical completion.
    """

    def __init__(self) -> None:
        self._callbacks: list[tuple[Callable[..., Any], tuple[Any, ...]]] = []
        self.uncertain = False

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
