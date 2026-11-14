"""Bounded SQL row mechanics; the caller retains scope and disposal authority."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, cast

from riverhog_canonical_json import canonical_json_bytes
from sqlalchemy import Table, bindparam, delete, select
from sqlalchemy.engine import CursorResult
from sqlalchemy.orm import Session
from sqlalchemy.sql.elements import ColumnElement

from riverhog_core.incremental_emission import VolumeEmissionCapacity


def metadata_row_bytes(row: Mapping[str, Any]) -> int:
    payload: dict[str, Any] = {}
    binary_bytes = 0
    for name, value in row.items():
        if type(value) is int:
            payload[name] = str(value)
        elif isinstance(value, bytes):
            binary_bytes += len(value)
            payload[name] = {"octets": str(len(value))}
        else:
            payload[name] = value
    return len(canonical_json_bytes(payload)) + binary_bytes


@dataclass(frozen=True, slots=True)
class CatalogRetirementRow:
    values: Mapping[str, Any]
    related: Mapping[str, Any] | None


def read_catalog_window(
    session: Session,
    table: Table,
    scope: Sequence[ColumnElement[bool]],
    *,
    count: int,
    metadata_bytes: int,
    related: Callable[[Mapping[str, Any]], Mapping[str, Any] | None] | None = None,
) -> tuple[list[CatalogRetirementRow], int]:
    """Read a first-live window, charging complete row and related fence bytes."""
    if type(count) is not int or count < 1 or type(metadata_bytes) is not int or metadata_bytes < 1:
        raise ValueError("catalog retirement requires positive row and metadata grants")
    statement = (
        select(table)
        .where(*scope)
        .order_by(*table.primary_key.columns)
        .limit(min(count, max(1, metadata_bytes // 8)))
        .execution_options(stream_results=True, yield_per=1)
    )
    selected: list[CatalogRetirementRow] = []
    used = 0
    with session.execute(statement).mappings() as rows:
        for current in rows:
            values = dict(current)
            extra = related(values) if related is not None else None
            charge = metadata_row_bytes(values)
            if extra is not None:
                charge += metadata_row_bytes(extra)
            if charge > metadata_bytes and not selected:
                raise VolumeEmissionCapacity(
                    dimension="metadata_bytes", required=charge, admitted=metadata_bytes
                )
            if used + charge > metadata_bytes:
                break
            selected.append(CatalogRetirementRow(values, extra))
            used += charge
    return selected, used


def delete_catalog_window(
    session: Session, table: Table, selected: Sequence[CatalogRetirementRow]
) -> None:
    """Use exact complete keys and a constant bind count after caller admission."""
    if not selected:
        raise ValueError("catalog retirement requires selected rows")
    keys = tuple(table.primary_key.columns)
    statement = delete(table).where(
        *(column == bindparam("target_" + column.name) for column in keys)
    )
    result = session.execute(
        statement,
        [{"target_" + column.name: row.values[column.name] for column in keys} for row in selected],
    )
    if cast(CursorResult[Any], result).rowcount != len(selected):
        raise RuntimeError("catalog retirement selection changed")
