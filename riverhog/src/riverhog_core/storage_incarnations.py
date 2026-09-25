"""Catalog ownership for configured storage bindings.

The catalog retains a name reservation after configuration removal. A fresh
adapter observation may restore that binding only for the same incarnation.
Reachability is an observation, not a durable administrative state.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Literal, cast

from riverhog_protocol.errors import ServiceUnavailable
from riverhog_storage_adapter_protocol import validate_storage_incarnation_id
from sqlalchemy import select
from sqlalchemy.orm import Session
from time_formats import format_utc_timestamp, utc_now

from riverhog_core.catalog_db import SessionFactory, session_scope
from riverhog_core.catalog_models import StorageIncarnationRecord

StorageKind = Literal["archive", "cache"]
StorageName = tuple[StorageKind, str]


class StorageBindingConflict(ValueError):
    """A historical name was offered with a different storage owner."""


def reconcile_storage_incarnations(
    session_factory: SessionFactory,
    observations: Mapping[StorageName, str | None],
    *,
    read_modes: Mapping[StorageName, str] | None = None,
) -> dict[StorageName, str]:
    """Persist verified bindings and disable names absent from configuration.

    An unavailable configured adapter has no new authority. Its historical
    record remains bound, with reachability reported separately by the caller.
    """

    now = format_utc_timestamp(utc_now())
    read_modes = read_modes or {}
    with session_scope(session_factory) as session:
        records = {
            (cast(StorageKind, record.kind), record.name): record
            for record in session.scalars(select(StorageIncarnationRecord).with_for_update())
        }
        for key, record in records.items():
            if key not in observations and record.state == "bound":
                record.state = "disabled"
                record.binding_generation += 1
        for key, observed in observations.items():
            kind, name = key
            if kind not in {"archive", "cache"} or not name or name != name.casefold():
                raise ValueError("storage registration has an invalid kind or name")
            if observed is None:
                continue
            incarnation_id = validate_storage_incarnation_id(observed)
            read_mode = read_modes.get(key)
            if read_mode is not None and read_mode not in {"immediate", "restore_required"}:
                raise ValueError("storage read mode is invalid")
            previous = records.get(key)
            if previous is None:
                previous = StorageIncarnationRecord(
                    id=incarnation_id,
                    kind=kind,
                    name=name,
                    state="bound",
                    binding_generation=1,
                    created_at=now,
                    last_bound_at=now,
                    last_read_mode=read_mode,
                )
                session.add(previous)
                records[key] = previous
            elif previous.id != incarnation_id:
                raise StorageBindingConflict(
                    f"storage name {kind}/{name} is reserved for incarnation {previous.id}"
                )
            elif previous.state == "retired":
                raise StorageBindingConflict(
                    f"retired storage name cannot be rebound: {kind}/{name}"
                )
            elif previous.state != "bound":
                previous.state = "bound"
                previous.binding_generation += 1
                previous.last_bound_at = now
            if read_mode is not None:
                previous.last_read_mode = read_mode
        return {key: record.id for key, record in records.items()}


def require_storage_incarnation(
    session: Session,
    kind: StorageKind,
    name: str,
    *,
    bound: bool = True,
) -> str:
    record = session.scalar(
        select(StorageIncarnationRecord).where(
            StorageIncarnationRecord.kind == kind,
            StorageIncarnationRecord.name == name,
        )
    )
    if record is None or (bound and record.state != "bound"):
        raise ServiceUnavailable(f"storage binding is unavailable: {kind}/{name}")
    return record.id


__all__ = [
    "StorageBindingConflict",
    "StorageKind",
    "StorageName",
    "reconcile_storage_incarnations",
    "require_storage_incarnation",
]
