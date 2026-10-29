"""Preallocated SQLite control extents; public reads retain exact original bytes."""

from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

from riverhog_client._prepared_primary_types import (
    PrimaryStateBusy,
    PrimaryStateCapacity,
    PrimaryStateError,
    primary_file_name,
)

if TYPE_CHECKING:
    from riverhog_client._prepared_primary_sqlite import TransactionalPrimaryFiles

_HEADER = 9


def _slot(owner: TransactionalPrimaryFiles, name: str) -> tuple[int, int] | None:
    row = owner._one(
        "SELECT rowid,length(body),typeof(body) FROM primary_controls WHERE namespace=? AND name=?",
        (owner._namespace, name),
    )
    if row is None:
        return None
    identifier, extent, kind = row
    if type(identifier) is not int or type(extent) is not int or extent < _HEADER or kind != "blob":
        raise PrimaryStateError("prepared-primary control extent is invalid")
    return identifier, extent - _HEADER


def _length(blob: sqlite3.Blob, capacity: int) -> int | None:
    header = blob.read(_HEADER)
    if len(header) != _HEADER:
        raise PrimaryStateError("prepared-primary control header is incomplete")
    selected, size = header[0], int.from_bytes(header[1:], "big")
    if selected not in (0, 1, 2) or size > capacity or (selected != 1 and size != 0):
        raise PrimaryStateError("prepared-primary control header is invalid")
    return size if selected == 1 else None


def _body(
    owner: TransactionalPrimaryFiles, slot: tuple[int, int], *, byte_limit: int
) -> bytes | None:
    identifier, capacity = slot
    with owner._db().blobopen("primary_controls", "body", identifier, readonly=True) as blob:
        size = _length(blob, capacity)
        body = None
        if size is not None:
            if size > byte_limit:
                raise PrimaryStateCapacity("prepared-primary control exceeds explicit admission")
            body = blob.read(size)
            if len(body) != size:
                raise PrimaryStateError("prepared-primary control extent changed")
    owner._guard.check_operation()
    return body


def selected(owner: TransactionalPrimaryFiles, name: str) -> bool:
    """Read only one indexed extent header, regardless of reserved capacity."""
    with owner._mutex:
        slot = _slot(owner, name)
        if slot is None:
            return False
        with owner._db().blobopen("primary_controls", "body", slot[0], readonly=True) as blob:
            result = _length(blob, slot[1]) is not None
        owner._guard.check_operation()
        return result


def reserved(owner: TransactionalPrimaryFiles, name: str) -> bool:
    return _slot(owner, name) is not None


def prepare(owner: TransactionalPrimaryFiles, names: tuple[str, ...], *, byte_limit: int) -> None:
    """Allocate each control's immutable capacity once, before payload admission."""
    if type(byte_limit) is not int or byte_limit < 0:
        raise ValueError("prepared-primary control admission must be nonnegative")
    for name in names:
        primary_file_name(name)
    from riverhog_client._prepared_primary_sqlite_journal import (
        ensure,
        validate_control_limit,
    )
    from riverhog_client._prepared_primary_sqlite_journal import (
        prepare as prepare_journal,
    )

    with owner._mutex:
        if byte_limit > owner._db().getlimit(sqlite3.SQLITE_LIMIT_LENGTH) - _HEADER:
            raise PrimaryStateCapacity("prepared-primary control exceeds SQLite extent admission")
        for name in names:
            slot = _slot(owner, name)
            if slot is not None:
                if slot[1] != byte_limit:
                    raise PrimaryStateError("prepared-primary reserved control capacity changed")
                validate_control_limit(owner, byte_limit)
                selected(owner, name)
                with owner._db().blobopen(
                    "primary_controls", "body", slot[0], readonly=True
                ) as blob:
                    pending = blob.read(1) == b"\x02"
                if pending:
                    prepare_journal(owner, slot[0])
                else:
                    ensure(owner)
                continue
            owner._require_idle()
            validate_control_limit(owner, byte_limit)
            if owner._one(
                "SELECT 1 FROM primary_names WHERE namespace=? AND name=?",
                (owner._namespace, name),
            ):
                raise PrimaryStateError("prepared-primary control conflicts with a byte role")
            with owner._transaction() as db:
                cursor = db.execute(
                    "INSERT INTO primary_controls VALUES (?,?,zeroblob(?))",
                    (owner._namespace, name, byte_limit + _HEADER),
                )
                identifier = cursor.lastrowid
                cursor.close()
                if identifier is None:
                    raise PrimaryStateError("prepared-primary control identity is unavailable")
                with db.blobopen("primary_controls", "body", identifier) as blob:
                    blob.write(bytes((2,)))
            prepare_journal(owner, identifier)


def read(owner: TransactionalPrimaryFiles, name: str, *, byte_limit: int) -> bytes:
    primary_file_name(name)
    if type(byte_limit) is not int or byte_limit < 0:
        raise ValueError("prepared-primary control admission must be nonnegative")
    with owner._mutex:
        slot = _slot(owner, name)
        body = None if slot is None else _body(owner, slot, byte_limit=byte_limit)
        if body is None:
            raise FileNotFoundError(name)
        return body


def put(
    owner: TransactionalPrimaryFiles,
    name: str,
    body: bytes,
    *,
    byte_limit: int,
    expected: bytes | None,
) -> None:
    primary_file_name(name)
    if type(byte_limit) is not int or byte_limit < 0:
        raise ValueError("prepared-primary control admission must be nonnegative")
    if not isinstance(body, bytes) or (expected is not None and not isinstance(expected, bytes)):
        raise TypeError("prepared-primary controls must be original bytes")
    if len(body) > byte_limit:
        raise PrimaryStateCapacity("prepared-primary control exceeds explicit admission")
    with owner._mutex:
        owner._require_control_idle(name)
        if owner._one(
            "SELECT 1 FROM primary_names WHERE namespace=? AND name=?", (owner._namespace, name)
        ):
            raise PrimaryStateError("prepared-primary control conflicts with a byte role")
        slot = _slot(owner, name)
        if slot is None:
            raise PrimaryStateError("prepared-primary control capacity was not prepared")
        with owner._db().blobopen("primary_controls", "body", slot[0], readonly=True) as blob:
            if blob.read(1) == bytes((2,)):
                raise PrimaryStateBusy("prepared-primary control preparation is incomplete")
        previous = _body(owner, slot, byte_limit=slot[1])
        if expected is None and previous is not None:
            raise FileExistsError(name)
        if expected is not None and previous != expected:
            raise PrimaryStateError("prepared-primary control comparison changed")
        if len(body) > slot[1]:
            raise PrimaryStateCapacity("prepared-primary control exceeds prepared capacity")
        owner.require_journal_ready()
        with owner._transaction() as db:
            if _body(owner, slot, byte_limit=slot[1]) != previous:
                raise PrimaryStateError("prepared-primary control comparison changed")
            with db.blobopen("primary_controls", "body", slot[0]) as blob:
                blob.seek(_HEADER)
                blob.write(body)
                blob.seek(0)
                blob.write(b"\x01" + len(body).to_bytes(_HEADER - 1, "big"))


def remove(owner: TransactionalPrimaryFiles, name: str, *, expected: bytes) -> None:
    """Unselect the exact original encoding, retaining capacity for later progress."""
    primary_file_name(name)
    if not isinstance(expected, bytes):
        raise TypeError("prepared-primary controls must be original bytes")
    with owner._mutex:
        owner._require_control_idle(name)
        slot = _slot(owner, name)
        previous = None if slot is None else _body(owner, slot, byte_limit=slot[1])
        if previous is None:
            return
        if previous != expected:
            raise PrimaryStateError("prepared-primary control comparison changed")
        assert slot is not None
        owner.require_journal_ready()
        with owner._transaction() as db:
            if _body(owner, slot, byte_limit=slot[1]) != expected:
                raise PrimaryStateError("prepared-primary control comparison changed")
            with db.blobopen("primary_controls", "body", slot[0]) as blob:
                blob.write(bytes(_HEADER))
