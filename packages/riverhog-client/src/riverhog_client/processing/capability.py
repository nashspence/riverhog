"""Refreshable in-memory Riverhog capability client."""

from __future__ import annotations

import threading
from collections.abc import Iterator
from contextlib import AbstractContextManager
from typing import Any, Self


class _CapabilityClientState:
    def __init__(self, client: Any, *, owned: bool) -> None:
        self.lock = threading.RLock()
        self.current = client
        self.current_owned = owned
        self.retired: dict[int, tuple[Any, bool]] = {}
        self.users: dict[int, int] = {}
        self.closed = False

    def snapshot(self) -> Any:
        with self.lock:
            if self.closed:
                raise RuntimeError("processing capability client is closed")
            return self.current

    def replace(self, client: Any, *, owned: bool) -> None:
        retired = None
        with self.lock:
            if self.closed:
                _close(client, owned=owned)
                raise RuntimeError("processing capability client is closed")
            if client is self.current:
                self.current_owned = self.current_owned or owned
                return
            prior = (self.current, self.current_owned)
            if self.users.get(id(self.current), 0):
                self.retired[id(self.current)] = prior
            else:
                retired = prior
            self.current = client
            self.current_owned = owned
        if retired is not None:
            _close(retired[0], owned=retired[1])

    def acquire(self) -> Any:
        with self.lock:
            client = self.snapshot()
            self.users[id(client)] = self.users.get(id(client), 0) + 1
            return client

    def release(self, client: Any) -> None:
        retired = None
        with self.lock:
            key = id(client)
            remaining = self.users[key] - 1
            if remaining:
                self.users[key] = remaining
            else:
                self.users.pop(key)
                retired = self.retired.pop(key, None)
        if retired is not None:
            _close(retired[0], owned=retired[1])

    def close(self) -> None:
        with self.lock:
            if self.closed:
                return
            self.closed = True
            values = [*self.retired.values(), (self.current, self.current_owned)]
            self.retired = {}
        for client, owned in values:
            _close(client, owned=owned)


class _LeasedResult:
    def __init__(self, state: _CapabilityClientState, client: Any, result: Any) -> None:
        self.state = state
        self.client = client
        self.result = result
        self.released = False

    def release(self) -> None:
        if not self.released:
            self.released = True
            self.state.release(self.client)

    def __del__(self) -> None:
        self.release()


class _LeasedContext(_LeasedResult, AbstractContextManager[Any]):
    def __enter__(self) -> Any:
        try:
            return self.result.__enter__()
        except BaseException:
            self.release()
            raise

    def __exit__(self, *args: Any) -> Any:
        try:
            return self.result.__exit__(*args)
        finally:
            self.release()


class _LeasedIterator(_LeasedResult, Iterator[Any]):
    def __next__(self) -> Any:
        if self.released:
            raise StopIteration
        try:
            return next(self.result)
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        if self.released:
            return
        try:
            close = getattr(self.result, "close", None)
            if callable(close):
                close()
        finally:
            self.release()


class CapabilityApiClient:
    """Stable API facade whose bearer client can be refreshed concurrently.

    Worker facades returned by :meth:`spawn` share the same in-memory state. Each
    HTTP operation resolves the current delegate when it begins, while delegates
    already serving an open stream remain alive until that stream finishes.
    Capability material is never persisted by this class.
    """

    def __init__(
        self,
        client: Any,
        *,
        owns_client: bool = False,
        _state: _CapabilityClientState | None = None,
        _root: bool = True,
    ) -> None:
        self._state = _state or _CapabilityClientState(client, owned=owns_client)
        self._root = _root

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    @property
    def current(self) -> Any:
        return self._state.snapshot()

    def replace(self, client: Any, *, owns_client: bool = True) -> None:
        if not self._root:
            raise RuntimeError("only the root capability client may replace its delegate")
        self._state.replace(client, owned=owns_client)

    def spawn(self) -> CapabilityApiClient:
        return CapabilityApiClient(
            self.current,
            _state=self._state,
            _root=False,
        )

    def close(self) -> None:
        if self._root:
            self._state.close()

    def __getattr__(self, name: str) -> Any:
        attribute = getattr(self.current, name)
        if not callable(attribute):
            return attribute

        def invoke(*args: Any, **kwargs: Any) -> Any:
            client = self._state.acquire()
            try:
                result = getattr(client, name)(*args, **kwargs)
            except BaseException:
                self._state.release(client)
                raise
            if isinstance(result, AbstractContextManager):
                return _LeasedContext(self._state, client, result)
            if isinstance(result, Iterator):
                return _LeasedIterator(self._state, client, result)
            self._state.release(client)
            return result

        return invoke


def _close(client: Any, *, owned: bool) -> None:
    if not owned:
        return
    close = getattr(client, "close", None)
    if callable(close):
        close()


__all__ = ["CapabilityApiClient"]
