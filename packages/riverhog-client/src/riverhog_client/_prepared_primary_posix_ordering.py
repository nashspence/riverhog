"""POSIX-owned disposable ordering scratch under one construction lease."""

from __future__ import annotations

import os
import sqlite3
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING

from riverhog_client._prepared_primary_ordering import OrderedRows
from riverhog_client._prepared_primary_storage import PrimaryOrdering, RetainedPrimaryOrdering
from riverhog_client._prepared_primary_types import (
    PrimaryStateBusy,
    PrimaryStateCapacity,
    PrimaryStateError,
)

if TYPE_CHECKING:
    from riverhog_client._prepared_primary_posix import PrimaryFiles


_SCRATCH_NAMES = (
    "selection.sqlite3-journal",
    "selection.sqlite3",
    "candidate-selection.sqlite3",
)


def reserved_bytes(owner: PrimaryFiles) -> int:
    """Observe only known retained scratch names; never read database contents."""
    try:
        opened = owner._owner_root.child("inventory-ordering", initialize=False)
    except FileNotFoundError:
        return 0
    total = 0
    with opened as files:
        for name in _SCRATCH_NAMES:
            try:
                descriptor = files._open(name)
            except FileNotFoundError:
                continue
            try:
                total += os.fstat(descriptor).st_size
            finally:
                os.close(descriptor)
    return total


def cleanup_step(owner: PrimaryFiles) -> bool:
    """A fixed number of unlinks removes derived state without enumerating the owner."""
    retained = owner._owner_root._retained_ordering
    if retained is not None:
        retained.invalidate()
    try:
        opened = owner._owner_root.child("inventory-ordering", initialize=False)
    except FileNotFoundError:
        owner._owner_root._retained_ordering = None
        return True
    with opened as files:
        for name in _SCRATCH_NAMES:
            files.remove(name)
    owner._owner_root._retained_ordering = None
    return True


@contextmanager
def ordered_rows(
    owner: PrimaryFiles, rows: Iterable[tuple[str, int, str]], *, byte_limit: int
) -> Iterator[PrimaryOrdering]:
    """Order opaque rows in admitted scratch; retain no membership authority."""
    if type(byte_limit) is not int or byte_limit < 0:
        raise ValueError("ordering scratch admission must be nonnegative")
    page_size = 4096
    page_limit = (byte_limit - 2 * page_size) // page_size
    if page_limit < 1:
        raise PrimaryStateCapacity("owner inventory ordering has no scratch admission")
    cleanup_step(owner)
    try:
        opened = owner._owner_root.child("inventory-ordering", initialize=True)
    except FileExistsError:
        opened = owner._owner_root.child("inventory-ordering", initialize=False)
    with opened as files:
        for name in _SCRATCH_NAMES:
            files.remove(name)
        files.put(
            "selection.sqlite3",
            (),
            byte_limit=1,
            temporary_name="candidate-selection.sqlite3",
        )
        files.check_current()
        connection = sqlite3.connect(
            files.root / "selection.sqlite3", isolation_level=None, timeout=0
        )
        try:
            with _errors():
                for setting in (
                    f"page_size = {page_size}",
                    f"max_page_count = {page_limit}",
                    "cache_size = -512",
                    "journal_mode = OFF",
                    "synchronous = OFF",
                ):
                    connection.execute("PRAGMA " + setting).close()
                connection.execute(
                    "CREATE TABLE members (artifact_id TEXT PRIMARY KEY, bytes TEXT NOT NULL, "
                    "sha256 TEXT NOT NULL) WITHOUT ROWID"
                ).close()
                count = 0
                for key, byte_count, digest in rows:
                    owner.check_current()
                    files.check_current()
                    connection.execute(
                        "INSERT INTO members VALUES (?, ?, ?)", (key, str(byte_count), digest)
                    ).close()
                    count += 1
                files.sync()

            def retain() -> RetainedPrimaryOrdering:
                owner.check_current()
                retained = owner._owner_root._retained_ordering
                if retained is None:
                    retained = _RetainedOrdering(owner, count, _file_stamp(files))
                    owner._owner_root._retained_ordering = retained
                return retained

            selected = OrderedRows(
                count, lambda after: _read(owner, files, connection, after), retain
            )
            try:
                yield selected
            finally:
                selected.close()
        finally:
            connection.close()
            if owner._owner_root._retained_ordering is None:
                files.remove("selection.sqlite3")


def _file_stamp(files: PrimaryFiles) -> tuple[int, int, int, int, int]:
    descriptor = files._open("selection.sqlite3")
    try:
        value = os.fstat(descriptor)
        return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns
    finally:
        os.close(descriptor)


@dataclass(slots=True)
class _RetainedOrdering:
    owner: PrimaryFiles
    count: int
    stamp: tuple[int, int, int, int, int]
    valid: bool = True

    def invalidate(self) -> None:
        self.valid = False

    def _require(self, files: PrimaryFiles) -> None:
        self.owner.check_current()
        if (
            not self.valid
            or self.owner._owner_root._retained_ordering is not self
            or _file_stamp(files) != self.stamp
        ):
            raise PrimaryStateError("prepared-primary retained ordering is no longer selected")

    @contextmanager
    def open(self) -> Iterator[PrimaryOrdering]:
        """Reopen only this derivation under its original live construction."""
        self.owner.check_current()
        guard = self.owner._owner_root._ordering_guard
        if not guard.acquire(blocking=False):
            raise PrimaryStateBusy("prepared-primary inventory ordering has a live reader")
        try:
            if not self.valid or self.owner._owner_root._retained_ordering is not self:
                raise PrimaryStateError("prepared-primary retained ordering is no longer selected")
            with self.owner._owner_root.child("inventory-ordering", initialize=False) as files:
                self._require(files)
                with _errors():
                    connection = sqlite3.connect(
                        (files.root / "selection.sqlite3").as_uri() + "?mode=ro",
                        uri=True,
                        isolation_level=None,
                        timeout=0,
                    )
                try:
                    with _errors():
                        connection.execute("PRAGMA cache_size=-512").close()
                        connection.execute("PRAGMA query_only=ON").close()
                    self._require(files)

                    def read(after: str | None) -> tuple[str, int, str] | None:
                        self._require(files)
                        return _read(self.owner, files, connection, after)

                    selected = OrderedRows(self.count, read, lambda: self)
                    try:
                        yield selected
                    finally:
                        selected.close()
                finally:
                    connection.close()
        finally:
            guard.release()


def _read(
    owner: PrimaryFiles,
    files: PrimaryFiles,
    connection: sqlite3.Connection,
    after: str | None,
) -> tuple[str, int, str] | None:
    owner.check_current()
    files.check_current()
    with _errors():
        cursor = connection.execute(
            "SELECT artifact_id, bytes, sha256 FROM members ORDER BY artifact_id LIMIT 1"
            if after is None
            else (
                "SELECT artifact_id, bytes, sha256 FROM members "
                "WHERE artifact_id>? ORDER BY artifact_id LIMIT 1"
            ),
            () if after is None else (after,),
        )
        try:
            row = cursor.fetchone()
        finally:
            cursor.close()
    if row is None:
        return None
    key, byte_count, digest = row
    if not isinstance(key, str) or not isinstance(byte_count, str) or not isinstance(digest, str):
        raise PrimaryStateError("prepared-primary ordering row is not its original encoding")
    return key, int(byte_count), digest


@contextmanager
def _errors() -> Iterator[None]:
    try:
        yield
    except sqlite3.IntegrityError as error:
        raise PrimaryStateError("owner inventory contains duplicate artifact IDs") from error
    except sqlite3.OperationalError as error:
        if getattr(error, "sqlite_errorcode", None) == sqlite3.SQLITE_FULL:
            raise PrimaryStateCapacity(
                "owner inventory ordering exceeds scratch admission"
            ) from error
        raise
