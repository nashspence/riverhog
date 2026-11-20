"""Retain bounded exact SQLite controls used by fixed selector preparations."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Any

from riverhog_core.canonical_discovery_selector_work import _encoded


@dataclass(frozen=True, slots=True)
class SelectorSourceReceipt:
    sql: str
    parameters: tuple[object, ...]
    exact_json: str


def _scalar(value: object) -> list[Any]:
    if value is None:
        return ["null"]
    if type(value) is str:
        return ["text", value]
    if type(value) is bytes:
        return ["blob", value.hex()]
    if type(value) is int:
        return ["integer", str(value)]
    if type(value) is float:
        return ["real", value.hex()]
    if type(value) is bool:
        return ["boolean", value]
    raise TypeError("selector source controls require exact SQLite scalar values")


def source_control_json(
    sql: str, parameters: tuple[object, ...], row: Any, octet_budget: int
) -> str:
    return _encoded(
        {
            "sql": sql,
            "parameters": [_scalar(value) for value in parameters],
            "row": None if row is None else [_scalar(value) for value in row],
        },
        octet_budget,
    )


class _CaptureCursor:
    def __init__(
        self,
        actual: sqlite3.Cursor,
        owner: SelectorSourceReads,
        sql: str,
        parameters: tuple[object, ...],
    ) -> None:
        self.actual, self.owner, self.sql, self.parameters = actual, owner, sql, parameters
        self._read = False

    def fetchone(self) -> Any:
        if self._read:
            raise RuntimeError("selector source reads must detach at most one control row")
        self._read = True
        row = self.actual.fetchone()
        self.owner.receipts.append(
            SelectorSourceReceipt(
                self.sql,
                self.parameters,
                source_control_json(self.sql, self.parameters, row, self.owner.octet_budget),
            )
        )
        return row

    def close(self) -> None:
        self.actual.close()


class SelectorSourceReads:
    """Capture maintained fixed one-row SELECTs; callers own atomic apply.

    A receipt preserves scalar types and complete BLOB bytes. The selector owner
    bounds each row and the read count, replays every receipt in its apply transaction
    and retains its existing Native leases, metadata custody and worker fences.
    """

    def __init__(self, db: sqlite3.Connection, *, octet_budget: int) -> None:
        if type(octet_budget) is not int or octet_budget < 1:
            raise ValueError("selector source control capacity must be positive")
        self.db, self.octet_budget = db, octet_budget
        self.receipts: list[SelectorSourceReceipt] = []

    @property
    def in_transaction(self) -> bool:
        return self.db.in_transaction

    def execute(self, sql: str, parameters: tuple[object, ...] = ()) -> _CaptureCursor:
        if not sql.lstrip().startswith("SELECT "):
            raise RuntimeError("selector source preparation must only read immutable controls")
        return _CaptureCursor(self.db.execute(sql, parameters), self, sql, parameters)
