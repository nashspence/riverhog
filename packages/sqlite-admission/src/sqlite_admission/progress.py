"""Owned SQLite progress dispatch for instruction observation and execution checks."""

from __future__ import annotations

import math
import operator
import sqlite3
import struct
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

_EXECUTION_INSTRUCTIONS = 1024
_PROGRESS_PERIOD_LIMIT = 1 << (struct.calcsize("i") * 8 - 1)


@dataclass(eq=False, slots=True)
class _Execution:
    check: Callable[[], None]
    pending: int = 0
    interrupted: BaseException | None = None


class _Progress:
    """One observational slot and one exact original execution registration.

    The connection owns dispatch from creation. There is no handler discovery,
    global callback registry or borrowing of an arbitrary driver's existing hook.
    Owners change registrations between statements on their private connection.
    """

    def __init__(self, connection: SqliteProgressConnection) -> None:
        self.connection = connection
        self.observer: Callable[[], int | None] | None = None
        self.observer_period = self.observer_pending = 0
        self.execution: _Execution | None = None
        self.quantum = 0
        self.installed: Callable[[], int | None] | None = None
        self.dispatch_callback = self.dispatch
        self.closed = False

    def install(self) -> None:
        period = self.observer_period
        quantum = (
            math.gcd(period, _EXECUTION_INSTRUCTIONS) if self.execution is not None else period
        )
        callback = self.dispatch_callback if self.execution is not None else self.observer
        if (quantum == self.quantum and callback is self.installed) or self.closed:
            return
        sqlite3.Connection.set_progress_handler(self.connection, callback, quantum)
        self.quantum, self.installed = quantum, callback

    def dispatch(self) -> int:
        observer = self.observer
        if observer is not None:
            self.observer_pending += self.quantum
            if self.observer_pending >= self.observer_period:
                self.observer_pending -= self.observer_period
                if observer():
                    return 1
        execution = self.execution
        if execution is not None:
            if execution.interrupted is not None:
                return 1
            execution.pending += self.quantum
            if execution.pending >= _EXECUTION_INSTRUCTIONS:
                execution.pending -= _EXECUTION_INSTRUCTIONS
                try:
                    _check(execution.check)
                except BaseException as error:
                    execution.interrupted = error
                    return 1
        return 0


def _check(check: Callable[[], object]) -> None:
    if check() is not None:
        raise TypeError("SQLite execution check must return None or raise its original cause")


class SqliteProgressConnection(sqlite3.Connection):
    """A connection with an owned progress dispatcher from original construction.

    The standard observation setter owns one slot. Scoped execution checking
    owns the other and samples at most 1,024 VM instructions apart. An observer
    with a finer period keeps those actual calls, including every instruction
    when its period is one. Without an execution check, SQLite invokes the
    observer directly. Removing an observer cannot remove execution checks.
    This is cooperative control, not a wall-clock bound on storage or callbacks.
    """

    def __init__(self, *args: Any, **options: Any) -> None:
        if hasattr(self, "_sqlite_progress"):
            raise RuntimeError("SQLite progress connection construction is single-attempt")
        self._sqlite_progress = _Progress(self)
        super().__init__(*args, **options)

    def set_progress_handler(
        self, progress_handler: Callable[[], int | None] | None, n: int
    ) -> None:
        state = self._sqlite_progress
        if state.closed:
            raise sqlite3.ProgrammingError("Cannot operate on a closed database.")
        period = operator.index(n)
        if not -_PROGRESS_PERIOD_LIMIT <= period < _PROGRESS_PERIOD_LIMIT:
            raise OverflowError("SQLite progress period does not fit its C int")
        if progress_handler is not None and not callable(progress_handler):
            raise TypeError("progress handler must be callable or None")
        observer = progress_handler if period > 0 else None
        period = period if observer is not None else 0
        previous = state.observer, state.observer_period, state.observer_pending
        state.observer, state.observer_period, state.observer_pending = observer, period, 0
        try:
            state.install()
        except BaseException:
            state.observer, state.observer_period, state.observer_pending = previous
            raise

    @contextmanager
    def execution_progress(self, check: Callable[[], None]) -> Iterator[None]:
        """Retain the exact check until the original scope exits, even on failure.

        Checks inspect bounded live cancellation/deadline state and raise the
        owner's original exception. They must not read sources, perform effects
        or wait. No caller can nest another execution registration on this same
        connection. A growth observer stays independently registered.
        The owner also checks between work units: short statements can finish
        before a VM sample. Exit this scope before rolling back an interrupted
        private transaction, so its cancellation check cannot interrupt cleanup.
        """
        state = self._sqlite_progress
        if state.closed:
            raise sqlite3.ProgrammingError("Cannot operate on a closed database.")
        if not callable(check):
            raise TypeError("SQLite execution requires a callable check")
        if state.execution is not None:
            raise RuntimeError("the original SQLite execution check is still registered")
        _check(check)
        execution = _Execution(check)
        state.execution = execution
        try:
            state.install()
            try:
                yield
            except sqlite3.OperationalError as error:
                if (
                    getattr(error, "sqlite_errorcode", None) == sqlite3.SQLITE_INTERRUPT
                    and execution.interrupted is not None
                ):
                    raise execution.interrupted from error
                raise
        finally:
            if state.execution is not execution:
                raise RuntimeError("original SQLite execution registration was replaced")
            state.execution = None
            state.install()

    def close(self) -> None:
        super().close()
        self._sqlite_progress.closed = True
        self._sqlite_progress.observer = None
        self._sqlite_progress.installed = None


@contextmanager
def observe_sqlite_execution(
    connection: sqlite3.Connection, check: Callable[[], None]
) -> Iterator[None]:
    """Observe through the original owned dispatcher, refusing an arbitrary hook."""
    if not isinstance(connection, SqliteProgressConnection):
        raise TypeError("SQLite execution requires a dispatcher owned from connection creation")
    with connection.execution_progress(check):
        yield
