"""One selected terminal's shared cleanup resource accounting.

Callers lock their exact selected terminal and validate their own scope and
current rights. This accounting supplies no cleanup or execution authority.
Pages and independent physical attempts retain separate contributions until
their owning receiver can release them in the same transaction.
"""

from __future__ import annotations

from dataclasses import dataclass, fields

from riverhog_canonical_json import canonical_json_bytes, format_scalar, parse_scalar
from riverhog_protocol.errors import Conflict
from sqlalchemy.orm import Session

from riverhog_core.archive_cleanup_grants import ArchiveCleanupGrants
from riverhog_core.catalog_archive_write_models import TerminalCleanupCapacityRecord


@dataclass(frozen=True, slots=True)
class CleanupCharge:
    calls: int = 0
    control_bytes: int = 0
    provider_requests: int = 0
    in_flight: int = 0

    def __post_init__(self) -> None:
        if any(type(value) is not int or value < 0 for value in self.values()):
            raise ValueError("cleanup charges must be nonnegative integers")

    def values(self) -> tuple[int, ...]:
        return tuple(getattr(self, field.name) for field in fields(self))


_EMPTY_CHARGE = CleanupCharge()


def change_cleanup_capacity(
    session: Session,
    operation_id: str,
    *,
    grants: ArchiveCleanupGrants,
    added: CleanupCharge = _EMPTY_CHARGE,
    removed: CleanupCharge = _EMPTY_CHARGE,
) -> None:
    """Atomically replace only the caller's retained contribution.

    Selection creates the accounting row. Missing or damaged accounting cannot
    be reconstructed from absent pages, closed executions or provider listings.
    The parent terminal lock serializes all scope owners.
    """
    record = session.get(
        TerminalCleanupCapacityRecord,
        operation_id,
        with_for_update=True,
        populate_existing=True,
    )
    if record is None:
        raise Conflict("selected terminal cleanup accounting is unavailable")
    names = tuple(field.name for field in fields(CleanupCharge))
    try:
        original = tuple(parse_scalar("nonnegative", getattr(record, name)) for name in names)
    except (TypeError, ValueError) as exc:
        raise Conflict("selected terminal cleanup accounting is invalid") from exc
    if any(value < release for value, release in zip(original, removed.values(), strict=True)):
        raise Conflict("cleanup release exceeds its retained resource accounting")
    updated = tuple(
        value - release + reserve
        for value, release, reserve in zip(original, removed.values(), added.values(), strict=True)
    )
    encoded = dict(
        zip(names, (format_scalar("nonnegative", value) for value in updated), strict=True)
    )
    # Charge this shared row's full encoded header as well as scope-owned state.
    header_bytes = len(canonical_json_bytes({"terminal_id": operation_id, **encoded}))
    calls, controls, requests, in_flight = updated
    increasing = any(
        reserve > release for reserve, release in zip(added.values(), removed.values(), strict=True)
    )
    if increasing and (
        calls > grants.calls
        or controls + header_bytes > grants.control_bytes
        or requests > grants.provider_requests
        or in_flight > grants.in_flight
    ):
        raise Conflict("cleanup outstanding reservations exhaust the retention grant")
    for name, value in encoded.items():
        setattr(record, name, value)
    session.flush([record])
