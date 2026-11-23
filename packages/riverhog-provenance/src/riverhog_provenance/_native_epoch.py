"""Private transaction and live-control receiving for one Native executor."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Iterator
from contextlib import contextmanager, nullcontext
from functools import wraps
from threading import Lock
from typing import Concatenate, Protocol

from sqlite_admission import observe_sqlite_execution

from ._native_conductor import NativeConductor
from ._native_work import NativeWorkCapacity, WorkAmount, WorkQuantum, WorkReservation


class NativeCaptureBusy(BlockingIOError):
    """The original optional capture still excludes semantic execution."""


class EpochContext:
    """One original private writer, including its post-commit live updates.

    Preparation runs before admission. Application mutates only this private
    database; its return value is pending live state until the original commit
    returns successfully. The receiving callback installs that state and any
    related frame/graph cache updates before the executor becomes quiescent.

    An unsuccessful epoch permanently invalidates this disposable executor.
    In particular, neither rollback nor an empty SQLite transaction flag proves
    whether a failed commit published. Reconstruct from verified authority
    instead of guessing whether the live continuation matches the database.
    Resource closure remains with the original storage/construction owner.
    """

    def __init__(self, db: sqlite3.Connection) -> None:
        self.db = db
        self.conductor = NativeConductor()
        self._phase = "quiescent"
        self._failure: BaseException | None = None
        self._execution_active = False
        self._execution_check: Callable[[], None] | None = None
        self._check_failure: BaseException | None = None
        self._preparing = False
        self._work: WorkQuantum | None = None
        self._application_work: WorkReservation | None = None
        self._admission_gate = Lock()
        self._capture_owner: object | None = None

    @property
    def work(self) -> WorkQuantum | None:
        return self._work

    def account(self, amount: WorkAmount) -> None:
        """Charge actual work against this original execution/application."""
        self.require_usable()
        work = self._work
        if work is None or not any(amount.coordinates):
            return
        admitted = self._application_work
        if admitted is not None:
            try:
                admitted.consume(amount)
            except NativeWorkCapacity as error:
                raise RuntimeError(
                    "actual Native application work exceeds its original admission"
                ) from error
        elif self._phase != "quiescent" or self.db.in_transaction:
            raise RuntimeError("Native application work requires pre-writer admission")
        else:
            work.consume(amount)

    def _run_check(self, check: Callable[[], object]) -> None:
        try:
            if check() is not None:
                raise TypeError("Native execution check must return None or raise its cause")
        except BaseException as error:
            self._check_failure = error
            raise

    def check(self) -> None:
        """Run the original bounded live-memory execution check."""
        self.require_usable()
        check = self._execution_check
        if check is not None:
            self._run_check(check)

    def raised_by_check(self, error: BaseException) -> bool:
        """Distinguish this exact execution cause from a semantic rejection."""
        return self._check_failure is error

    @property
    def quiescent(self) -> bool:
        return self._phase == "quiescent" and not self.db.in_transaction

    def require_usable(self) -> None:
        if self._failure is not None:
            raise RuntimeError(
                "Native executor requires reconstruction after a failed epoch"
            ) from (self._failure)

    def require_quiescent(self) -> None:
        self.require_usable()
        if not self.quiescent:
            raise RuntimeError("Native private epoch is already active or has a foreign writer")

    def retain_capture(self, owner: object) -> None:
        """Exclude semantic execution until this exact original capture receives."""
        if owner is None:
            raise TypeError("Native capture requires its exact nonempty original owner")
        with self._admission_gate:
            self.require_quiescent()
            if self._execution_active or self._preparing or self._capture_owner is not None:
                raise BlockingIOError("original Native execution or capture has not received")
            self._capture_owner = owner

    def require_capture(self, owner: object) -> None:
        self.require_quiescent()
        if self._capture_owner is None or self._capture_owner is not owner:
            raise RuntimeError("Native capture requires its original exclusive hold")

    def receive_capture(self, owner: object) -> None:
        with self._admission_gate:
            if self._capture_owner is None or self._capture_owner is not owner:
                raise RuntimeError("Native capture requires its original exclusive hold")
            # The final storage lease may already have received SQLite. Releasing
            # this exact token must not query that closed original connection.
            if self._execution_active or self._preparing or self._phase != "quiescent":
                raise RuntimeError("original Native execution has not received")
            self._capture_owner = None

    def require_execution_available(self) -> None:
        if self._capture_owner is not None:
            raise NativeCaptureBusy("original Native capture has not received")

    def require(self, db: sqlite3.Connection) -> None:
        """Admit child mutations only within this exact original private epoch."""
        if db is not self.db or self._phase != "applying" or not db.in_transaction:
            raise RuntimeError("Native mutation requires its original active private epoch")

    @contextmanager
    def execution(
        self, check: Callable[[], None] | None, *, work: WorkQuantum | None = None
    ) -> Iterator[None]:
        """Bind one owner's bounded live-memory check for its admitted execution."""
        with self._admission_gate:
            self.require_quiescent()
            self.require_execution_available()
            if self._execution_active:
                raise RuntimeError("the original Native execution is already active")
            if check is not None and not callable(check):
                raise TypeError("Native execution requires a callable check")
            if work is not None and not isinstance(work, WorkQuantum):
                raise TypeError("Native execution requires its explicit original work quantum")
            if work is not None:
                work.bind_execution(self)
            self._execution_active, self._execution_check, self._work = True, check, work
            self._check_failure = None
        try:
            self.check()
            yield
            self.require_unit_received()
        finally:
            with self._admission_gate:
                self._execution_active, self._execution_check, self._work = False, None, None
                self._check_failure = None

    def require_unit_received(self) -> None:
        """Permit the next unit only after this original execution has received work."""
        self.require_quiescent()
        if self._work is not None:
            try:
                self._work.require_received()
            except BaseException as error:
                self._phase, self._failure = "unusable", error
                raise

    @contextmanager
    def preparing(self) -> Iterator[None]:
        """Observe detached reads; receive their registration before a writer begins."""
        check = self._execution_check
        if check is None or self._preparing:
            yield
            return
        self.require_quiescent()
        with self._admission_gate:
            self.require_execution_available()
            self._preparing = True
        try:
            with observe_sqlite_execution(self.db, self.check):
                yield
        finally:
            with self._admission_gate:
                self._preparing = False

    def require_receiving(self, db: sqlite3.Connection) -> None:
        """Receive live control only after this exact original commit returned."""
        if db is not self.db or self._phase != "receiving" or db.in_transaction:
            raise RuntimeError("Native live control requires original post-commit receiving")

    def apply[T](
        self,
        mutation: Callable[[EpochContext], T],
        *,
        receive: Callable[[T], None],
        check: Callable[[], None] | None = None,
        admission: WorkAmount | None = None,
    ) -> T:
        """Reserve before the writer, commit, then receive the original live state.

        An ordinary admission refusal opens no transaction. Actual application
        work remains charged on failure; only unused admission is returned
        after original post-commit receiving or failed-epoch cleanup.
        """
        self.require_quiescent()
        if self._preparing:
            raise RuntimeError("Native preparation must be received before private application")
        if (
            check is not None
            and self._execution_check is not None
            and check is not self._execution_check
        ):
            raise RuntimeError("Native application cannot replace its original execution check")
        selected_check = self._execution_check if check is None else check

        def check_original() -> None:
            if selected_check is not None:
                self._run_check(selected_check)

        if admission is not None and (
            not isinstance(admission, WorkAmount) or not any(admission.coordinates)
        ):
            raise ValueError("Native application requires positive explicit work admission")
        with self._admission_gate:
            self.require_quiescent()
            self.require_execution_available()
            reserved = (
                None if self._work is None or admission is None else self._work.reserve(admission)
            )
            self._application_work = reserved
            self._phase = "applying"
        try:
            observation = (
                nullcontext()
                if selected_check is None
                else observe_sqlite_execution(self.db, check_original)
            )
            with observation:
                self.db.execute("BEGIN IMMEDIATE").close()
                result = mutation(self)
                self.require(self.db)
                if selected_check is not None:
                    check_original()
                self._phase = "committing"
                self.db.commit()
            self._phase = "receiving"
            receive(result)
            if self.db.in_transaction:
                raise RuntimeError("Native live receiving left a private writer open")
            if reserved is not None:
                reserved.close()
                reserved = None
            self._phase = "quiescent"
            return result
        except BaseException as error:
            self._phase = "unusable"
            self._failure = error
            # The exact execution registration has exited before private cleanup.
            self.db.rollback()
            raise
        finally:
            self._application_work = None
            if reserved is not None:
                reserved.close()


def bind_epoch(db: sqlite3.Connection, original: EpochContext | None) -> EpochContext:
    if original is not None and original.db is not db:
        raise ValueError("Native child requires its original epoch connection")
    return EpochContext(db) if original is None else original


class _PreparationOwner(Protocol):
    epoch: EpochContext


def observe_preparation[Owner: _PreparationOwner, **P, R](
    operation: Callable[Concatenate[Owner, P], R],
) -> Callable[Concatenate[Owner, P], R]:
    @wraps(operation)
    def observed(owner: Owner, /, *args: P.args, **kwargs: P.kwargs) -> R:
        if owner.epoch._execution_check is None:
            return operation(owner, *args, **kwargs)
        with owner.epoch.preparing():
            return operation(owner, *args, **kwargs)

    return observed
