"""Private transaction and live-control receiving for one Native executor."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from contextlib import nullcontext

from sqlite_admission import observe_sqlite_execution


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
        self._phase = "quiescent"
        self._failure: BaseException | None = None

    @property
    def quiescent(self) -> bool:
        return self._phase == "quiescent" and not self.db.in_transaction

    def require_quiescent(self) -> None:
        if self._failure is not None:
            raise RuntimeError(
                "Native executor requires reconstruction after a failed epoch"
            ) from (self._failure)
        if not self.quiescent:
            raise RuntimeError("Native private epoch is already active or has a foreign writer")

    def require(self, db: sqlite3.Connection) -> None:
        """Admit child mutations only within this exact original private epoch."""
        if db is not self.db or self._phase != "applying" or not db.in_transaction:
            raise RuntimeError("Native mutation requires its original active private epoch")

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
    ) -> T:
        """Commit pending semantic state, then receive its corresponding live state."""
        self.require_quiescent()
        self._phase = "applying"
        try:
            observation = (
                nullcontext() if check is None else observe_sqlite_execution(self.db, check)
            )
            with observation:
                self.db.execute("BEGIN IMMEDIATE").close()
                result = mutation(self)
                self.require(self.db)
                if check is not None and check() is not None:
                    raise TypeError("Native execution check must return None or raise its cause")
                self._phase = "committing"
                self.db.commit()
            self._phase = "receiving"
            receive(result)
            if self.db.in_transaction:
                raise RuntimeError("Native live receiving left a private writer open")
            self._phase = "quiescent"
            return result
        except BaseException as error:
            self._phase = "unusable"
            self._failure = error
            # The exact execution registration has exited before private cleanup.
            self.db.rollback()
            raise
