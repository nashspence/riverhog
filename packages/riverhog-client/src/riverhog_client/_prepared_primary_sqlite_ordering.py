"""Indexed derived ordering in the primary container, separate from member authority."""

from __future__ import annotations

import uuid
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING

from riverhog_client._prepared_primary_ordering import OrderedRows
from riverhog_client._prepared_primary_sqlite import _BLOCK
from riverhog_client._prepared_primary_storage import PrimaryOrdering, RetainedPrimaryOrdering
from riverhog_client._prepared_primary_types import (
    PrimaryStateBusy,
    PrimaryStateCapacity,
    PrimaryStateError,
)

if TYPE_CHECKING:
    from riverhog_client._prepared_primary_sqlite import TransactionalPrimaryFiles


def _primary_idle(owner: TransactionalPrimaryFiles) -> None:
    if owner._one("SELECT generation FROM primary_operation WHERE id=1") is not None:
        raise PrimaryStateBusy("prepared-primary byte mutation requires bounded cleanup")


def _live_pages(owner: TransactionalPrimaryFiles) -> int:
    pages = owner._one("PRAGMA page_count")
    free = owner._one("PRAGMA freelist_count")
    if pages is None or free is None:
        raise PrimaryStateError("prepared-primary page accounting is unavailable")
    return int(pages[0]) - int(free[0])


def cleanup_step(owner: TransactionalPrimaryFiles) -> bool:
    """Delete at most one exact scratch row; never cascade or touch primary bytes."""
    with owner._mutex:
        _primary_idle(owner)
        state = owner._one("SELECT generation,phase,cursor FROM primary_ordering WHERE id=1")
        if state is None:
            return True
        generation, phase, after = state
        if phase != "cleanup":
            with owner._transaction() as db:
                db.execute(
                    "UPDATE primary_ordering SET phase='cleanup',cursor=NULL "
                    "WHERE id=1 AND generation=?",
                    (generation,),
                ).close()
            return False
        row = owner._one(
            "SELECT artifact_id FROM primary_order_rows WHERE generation=? "
            "ORDER BY artifact_id LIMIT 1"
            if after is None
            else (
                "SELECT artifact_id FROM primary_order_rows WHERE generation=? "
                "AND artifact_id>? ORDER BY artifact_id LIMIT 1"
            ),
            (generation,) if after is None else (generation, after),
        )
        with owner._transaction() as db:
            if row is None:
                db.execute(
                    "DELETE FROM primary_ordering WHERE id=1 AND generation=?", (generation,)
                ).close()
            else:
                db.execute(
                    "DELETE FROM primary_order_rows WHERE generation=? AND artifact_id=?",
                    (generation, row[0]),
                ).close()
                db.execute(
                    "UPDATE primary_ordering SET cursor=? WHERE id=1 AND generation=?",
                    (row[0], generation),
                ).close()
        return row is None


@contextmanager
def ordered_rows(
    owner: TransactionalPrimaryFiles,
    rows: Iterable[tuple[str, int, str]],
    *,
    byte_limit: int,
) -> Iterator[PrimaryOrdering]:
    """Build and dispose only in execution work; yields hold no SQL cursor or transaction.

    Charge live page growth and the scratch B-tree roots. The database ceiling,
    journal peak and progress reservations remain distinct resource accounting.
    A ready generation remains admitted until explicit bounded cleanup; closing a
    reader never launches collection-sized cleanup.
    """
    if type(byte_limit) is not int or byte_limit < 0:
        raise ValueError("ordering scratch admission must be nonnegative")
    with owner._mutex:
        _primary_idle(owner)
        roots = owner._one(
            "SELECT count(*) FROM sqlite_schema WHERE rootpage>0 "
            "AND tbl_name IN ('primary_ordering','primary_order_rows')"
        )
        if roots is None:
            raise PrimaryStateError("prepared-primary scratch geometry is unavailable")
        root_pages = int(roots[0])
        if byte_limit < root_pages * _BLOCK:
            raise PrimaryStateCapacity("owner inventory ordering has no scratch admission")
    while not cleanup_step(owner):
        pass
    generation = uuid.uuid4().hex
    with owner._mutex:
        _primary_idle(owner)
        base_pages = _live_pages(owner)
        with owner._transaction(payload=True) as db:
            db.execute(
                "INSERT INTO primary_ordering VALUES (1,?,?, 'building',NULL,?,?)",
                (generation, owner._namespace, base_pages, str(byte_limit)),
            ).close()
    count = 0
    for key, byte_count, digest in rows:
        with owner._mutex:
            if owner._one(
                "SELECT 1 FROM primary_order_rows WHERE generation=? AND artifact_id=?",
                (generation, key),
            ):
                raise PrimaryStateError("owner inventory contains duplicate artifact IDs")
            with owner._transaction(payload=True) as db:
                db.execute(
                    "INSERT INTO primary_order_rows VALUES (?,?,?,?)",
                    (generation, key, str(byte_count), digest),
                ).close()
                charged_pages = max(root_pages, root_pages + _live_pages(owner) - base_pages)
                if charged_pages * _BLOCK > byte_limit:
                    raise PrimaryStateCapacity("owner inventory ordering exceeds scratch admission")
        count += 1
    with owner._transaction(payload=True) as db:
        db.execute(
            "UPDATE primary_ordering SET phase='ready' WHERE id=1 AND generation=?",
            (generation,),
        ).close()

    selected = _selection(owner, generation, count)
    try:
        yield selected
    finally:
        selected.close()


@dataclass(frozen=True, slots=True)
class _RetainedOrdering:
    owner: TransactionalPrimaryFiles
    generation: str
    count: int

    @contextmanager
    def open(self) -> Iterator[PrimaryOrdering]:
        """Select the same ready generation, without mutation or input replay."""
        self.owner.check_current()
        guard = self.owner._root._ordering_guard
        if not guard.acquire(blocking=False):
            raise PrimaryStateBusy("prepared-primary inventory ordering has a live reader")
        try:
            with self.owner._mutex:
                state = self.owner._one(
                    "SELECT generation,namespace,phase FROM primary_ordering WHERE id=1"
                )
                if state != (self.generation, self.owner._namespace, "ready"):
                    raise PrimaryStateError(
                        "prepared-primary retained ordering is no longer selected"
                    )
            selected = _selection(self.owner, self.generation, self.count)
            try:
                yield selected
            finally:
                selected.close()
        finally:
            guard.release()


def _selection(owner: TransactionalPrimaryFiles, generation: str, count: int) -> OrderedRows:
    def read(after: str | None) -> tuple[str, int, str] | None:
        row = owner._one(
            "SELECT artifact_id,bytes,sha256 FROM primary_order_rows "
            "WHERE generation=? ORDER BY artifact_id LIMIT 1"
            if after is None
            else (
                "SELECT artifact_id,bytes,sha256 FROM primary_order_rows "
                "WHERE generation=? AND artifact_id>? ORDER BY artifact_id LIMIT 1"
            ),
            (generation,) if after is None else (generation, after),
        )
        if row is None:
            return None
        key, byte_count, digest = row
        if (
            not isinstance(key, str)
            or not isinstance(byte_count, str)
            or not isinstance(digest, str)
        ):
            raise PrimaryStateError("prepared-primary ordering row is not its original encoding")
        return key, int(byte_count), digest

    def retain() -> RetainedPrimaryOrdering:
        owner.check_current()
        return _RetainedOrdering(owner, generation, count)

    return OrderedRows(count, read, retain)
