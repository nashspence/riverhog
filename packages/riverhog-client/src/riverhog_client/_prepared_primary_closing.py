"""One frozen owner inventory and a resumable materialization barrier."""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING

from riverhog_protocol import ArtifactMemberIdentityDocument

from riverhog_client._prepared_primary_documents import (
    PrimaryClosing,
    PrimaryClosingIntent,
    PrimaryCustodyClosing,
    PrimaryCustodySettled,
    PrimaryFinishReady,
    PrimaryInventory,
    PrimaryInventoryEntry,
    PrimaryUsage,
    primary_control_bytes,
)
from riverhog_client._prepared_primary_types import PrimaryStateCapacity, PrimaryStateError

if TYPE_CHECKING:
    from riverhog_client._prepared_primary_store import PrimaryStore

_DOMAIN = b"riverhog-prepared-inventory-chain/v1\n"
_EMPTY = hashlib.sha256(_DOMAIN).hexdigest()


def _identity(document: object) -> str:
    from pydantic import BaseModel

    if not isinstance(document, BaseModel):
        raise TypeError("prepared-primary commitment requires a control")
    return hashlib.sha256(primary_control_bytes(document)).hexdigest()


def _chain(previous: str, content: bytes) -> str:
    return hashlib.sha256(
        _DOMAIN + bytes.fromhex(previous) + len(content).to_bytes(8, "big") + content
    ).hexdigest()


def recover_closing(store: PrimaryStore) -> None:
    files = store._files
    expected_reservation = (
        0
        if store._usage.inventory_selected
        else store._usage.members * (store.limits.control_bytes + 1)
    )
    if store._usage.inventory_reserved_bytes != expected_reservation:
        raise PrimaryStateError("prepared inventory reservation differs from retained membership")
    intent = store._optional(files, "closing-intent", PrimaryClosingIntent)
    closing = store._optional(files, "closing", PrimaryClosing)
    ready = store._optional(files, "finish-ready", PrimaryFinishReady)
    draining = store._optional(files, "custody-closing", PrimaryCustodyClosing)
    settled = store._optional(files, "custody-settled", PrimaryCustodySettled)
    if (draining is not None or settled is not None) and ready is None:
        raise PrimaryStateError("terminal custody has no original decision barrier")
    if closing is None:
        if store._usage.inventory_selected:
            raise PrimaryStateError("selected inventory has lost its closing control")
        if ready is not None:
            raise PrimaryStateError("finish-ready has no original frozen inventory")
        if intent is not None:
            if (
                intent.session_sha256 != _identity(store.session)
                or intent.members != store._usage.members
            ):
                raise PrimaryStateError("closing inventory differs from its allocation")
            if intent.before is not None and store._usage != intent.before:
                raise PrimaryStateError("uncommitted inventory has changed accounting")
            files.remove("inventory")
            if intent.inventory is not None:
                store._write(
                    files,
                    "closing-intent",
                    PrimaryClosingIntent.model_validate(
                        {
                            "session_sha256": intent.session_sha256,
                            "members": str(intent.members),
                        }
                    ),
                    expected=intent,
                )
        return
    if (
        closing.session_sha256 != _identity(store.session)
        or closing.members != store._usage.members
        or closing.inventory.name != "inventory"
    ):
        raise PrimaryStateError("frozen inventory differs from its accepted allocation")
    if intent is not None:
        if (
            intent.inventory != closing.inventory
            or intent.before is None
            or intent.after is None
            or intent.members != closing.members
            or intent.session_sha256 != closing.session_sha256
            or store._usage not in (intent.before, intent.after)
        ):
            raise PrimaryStateError("frozen inventory has no matching accounting transition")
        files.sync()
        store._write(files, "usage", intent.after, expected=store._usage)
        store._usage = intent.after
        files.remove_control("closing-intent", expected=primary_control_bytes(intent))
    if (
        not store._usage.inventory_selected
        or store._usage.inventory_bytes != closing.inventory.bytes
    ):
        raise PrimaryStateError("frozen inventory differs from retained byte admission")
    with files.extent_reader(closing.inventory.name, byte_count=closing.inventory.bytes):
        pass
    if ready is not None and (
        ready.session_sha256 != closing.session_sha256
        or ready.inventory != closing.inventory
        or ready.members != closing.members
        or ready.manifest_chain_sha256 != closing.inventory.chain_sha256
        or closing.after_member != closing.members
        or closing.after_offset != closing.inventory.bytes
        or closing.manifest_chain_sha256 != ready.manifest_chain_sha256
    ):
        raise PrimaryStateError("finish-ready differs from its exact inventory closure")
    if draining is not None and (
        ready is None
        or draining.session_sha256 != ready.session_sha256
        or draining.inventory != ready.inventory
        or draining.members != ready.members
    ):
        raise PrimaryStateError("custody cursor differs from its original frozen inventory")
    if settled is not None and (
        draining is None
        or settled.session_sha256 != draining.session_sha256
        or settled.inventory != draining.inventory
        or settled.members != draining.members
        or settled.manifest_chain_sha256 != draining.inventory.chain_sha256
        or draining.after_member != draining.members
        or draining.after_offset != draining.inventory.bytes
        or draining.manifest_chain_sha256 != settled.manifest_chain_sha256
    ):
        raise PrimaryStateError("settled custody differs from its exact inventory closure")


def freeze_inventory(
    store: PrimaryStore, members: Iterable[ArtifactMemberIdentityDocument]
) -> PrimaryClosing:
    """Freeze the owner's unique, strictly ordered inventory once, with no new index."""
    with store._mutex:
        store._recover()
        closing = store._optional(store._files, "closing", PrimaryClosing)
        if closing is not None:
            return closing
        intent = store._optional(store._files, "closing-intent", PrimaryClosingIntent)
        if intent is None:
            intent = PrimaryClosingIntent.model_validate(
                {
                    "session_sha256": _identity(store.session),
                    "members": str(store._usage.members),
                }
            )
            store._write(store._files, "closing-intent", intent)
        grant = (
            store.limits.total_bytes
            - store.limits.root_reservation
            - store._usage.control_bytes
            - store._usage.pending_bytes
        )
        if grant < 0:
            raise PrimaryStateCapacity("frozen inventory cannot enter allocation admission")
        count = 0
        previous_member: str | None = None
        chain = _EMPTY

        def chunks() -> Iterable[bytes]:
            nonlocal count, previous_member, chain
            for member in members:
                key = str(member.artifact_id)
                if previous_member is not None and key <= previous_member:
                    raise PrimaryStateError("owner inventory must have unique ordered artifact IDs")
                previous_member = key
                try:
                    with store._member(key, create=False) as files:
                        record = store._record(files, member)
                except FileNotFoundError as exc:
                    raise PrimaryStateError(
                        "allocated closing member has no original preparation"
                    ) from exc
                if record is None:
                    raise PrimaryStateError("allocated closing member has no original preparation")
                row = PrimaryInventoryEntry(
                    member=member, prepared_sha256=_identity(record.prepared)
                )
                raw = primary_control_bytes(row)
                if len(raw) > store.limits.control_bytes:
                    raise PrimaryStateCapacity("owner inventory row exceeds control admission")
                count += 1
                if count > intent.members:
                    raise PrimaryStateError("owner inventory exceeds the prepared allocation")
                chain = _chain(chain, raw)
                yield raw + b"\n"
            if count != intent.members:
                raise PrimaryStateError("owner inventory omits prepared allocation members")

        store._authoring = True
        try:
            retained = store._files.put(
                "inventory", chunks(), byte_limit=grant, temporary_name="candidate-inventory"
            )
        finally:
            store._authoring = False
        inventory = PrimaryInventory.model_validate(
            {
                "name": retained.name,
                "bytes": str(retained.bytes),
                "chain_sha256": chain,
            }
        )
        after = PrimaryUsage.model_validate(
            {
                **store._usage.model_dump(mode="json"),
                "sequence": str(store._usage.sequence + 1),
                "inventory_bytes": str(retained.bytes),
                "inventory_reserved_bytes": "0",
                "inventory_selected": True,
            }
        )
        completed_intent = PrimaryClosingIntent.model_validate(
            {
                "session_sha256": intent.session_sha256,
                "members": str(intent.members),
                "inventory": inventory,
                "before": store._usage,
                "after": after,
            }
        )
        store._write(store._files, "closing-intent", completed_intent, expected=intent)
        closing = PrimaryClosing.model_validate(
            {
                "session_sha256": intent.session_sha256,
                "inventory": inventory,
                "members": str(count),
                "after_member": "0",
                "after_offset": "0",
                "last_member": None,
                "manifest_chain_sha256": _EMPTY,
            }
        )
        store._install(store._files, "closing", closing)
        store._write(store._files, "usage", after, expected=store._usage)
        store._usage = after
        store._files.remove_control(
            "closing-intent", expected=primary_control_bytes(completed_intent)
        )
        return closing


def advance_closing(
    store: PrimaryStore,
    *,
    limit: int,
    settle: Callable[[ArtifactMemberIdentityDocument], None],
) -> bool:
    """Advance a bounded cursor; only the final complete chain permits finish-ready."""
    if type(limit) is not int or limit <= 0:
        raise ValueError("closing batch admission must be a positive integer")
    with store._mutex:
        store._recover()
        closing = store._read(store._files, "closing", PrimaryClosing)
        ready = store._optional(store._files, "finish-ready", PrimaryFinishReady)
        if ready is not None:
            store._files.sync()
            return True
        if store._closing_advancing:
            raise PrimaryStateError("closing callback cannot advance its own cursor")
        count = closing.after_member
        offset = closing.after_offset
        chain = closing.manifest_chain_sha256
        previous_member: str | None = closing.last_member
        store._closing_advancing = True
        try:
            with store._files.extent_reader(
                closing.inventory.name, byte_count=closing.inventory.bytes
            ) as stream:
                stream.seek(offset)
                for _ in range(limit):
                    raw = stream.readline(store.limits.control_bytes + 2)
                    if not raw:
                        break
                    if len(raw) > store.limits.control_bytes + 1 or not raw.endswith(b"\n"):
                        raise PrimaryStateError("frozen inventory has an invalid framed row")
                    entry = PrimaryInventoryEntry.model_validate_json(raw[:-1])
                    if primary_control_bytes(entry) + b"\n" != raw:
                        raise PrimaryStateError("frozen inventory row is not its exact encoding")
                    key = str(entry.member.artifact_id)
                    if previous_member is not None and key <= previous_member:
                        raise PrimaryStateError("frozen inventory member order changed")
                    if count >= closing.members:
                        raise PrimaryStateError("frozen inventory exceeds its exact member count")
                    record = store.load(entry.member)
                    if record is None or _identity(record.prepared) != entry.prepared_sha256:
                        raise PrimaryStateError(
                            "frozen member differs from its original preparation"
                        )
                    if record.decision_accepted is None:
                        settle(entry.member)
                        record = store.load(entry.member)
                        if (
                            record is None
                            or record.decision_accepted is None
                            or _identity(record.prepared) != entry.prepared_sha256
                        ):
                            raise PrimaryStateError("closing member has no durable exact decision")
                    chain = _chain(chain, raw[:-1])
                    offset += len(raw)
                    count += 1
                    previous_member = key
                complete = count == closing.members
                if complete and (
                    offset != closing.inventory.bytes
                    or stream.read(1)
                    or chain != closing.inventory.chain_sha256
                ):
                    raise PrimaryStateError(
                        "closing inventory does not match its frozen commitment"
                    )
                if not complete and offset >= closing.inventory.bytes:
                    raise PrimaryStateError("closing inventory ended before its declared members")
        finally:
            store._closing_advancing = False
        next_control = PrimaryClosing.model_validate(
            {
                **closing.model_dump(mode="json"),
                "after_member": str(count),
                "after_offset": str(offset),
                "last_member": previous_member,
                "manifest_chain_sha256": chain,
            }
        )
        store._write(store._files, "closing", next_control, expected=closing)
        if complete:
            store._install(
                store._files,
                "finish-ready",
                PrimaryFinishReady.model_validate(
                    {
                        "session_sha256": closing.session_sha256,
                        "inventory": closing.inventory,
                        "members": str(count),
                        "manifest_chain_sha256": chain,
                    }
                ),
            )
        return complete


def advance_custody(
    store: PrimaryStore,
    *,
    limit: int,
    settle: Callable[[ArtifactMemberIdentityDocument], None],
) -> bool:
    """Retain receipts and reclaim preimages through a bounded, resumable cursor.

    A not-yet-available receipt leaves its member pending. An earlier lost cursor
    never repeats a receipt call whose exact result is already retained locally.
    This barrier settles artifact custody; collection finalization keeps its owner.
    """
    if type(limit) is not int or limit <= 0:
        raise ValueError("custody batch admission must be a positive integer")
    with store._mutex:
        store._recover()
        ready = store._read(store._files, "finish-ready", PrimaryFinishReady)
        cursor = store._optional(store._files, "custody-closing", PrimaryCustodyClosing)
        if cursor is None:
            cursor = PrimaryCustodyClosing.model_validate(
                {
                    "session_sha256": ready.session_sha256,
                    "inventory": ready.inventory,
                    "members": str(ready.members),
                    "after_member": "0",
                    "after_offset": "0",
                    "last_member": None,
                    "manifest_chain_sha256": _EMPTY,
                }
            )
            store._install(store._files, "custody-closing", cursor)
        if store._optional(store._files, "custody-settled", PrimaryCustodySettled) is not None:
            store._files.sync()
            return True
        if store._closing_advancing:
            raise PrimaryStateError("custody callback cannot advance its own cursor")
        count = cursor.after_member
        offset = cursor.after_offset
        chain = cursor.manifest_chain_sha256
        previous_member = cursor.last_member
        store._closing_advancing = True
        try:
            with store._files.extent_reader(
                cursor.inventory.name, byte_count=cursor.inventory.bytes
            ) as stream:
                stream.seek(offset)
                for _ in range(limit):
                    raw = stream.readline(store.limits.control_bytes + 2)
                    if not raw:
                        break
                    if len(raw) > store.limits.control_bytes + 1 or not raw.endswith(b"\n"):
                        raise PrimaryStateError("custody inventory has an invalid framed row")
                    entry = PrimaryInventoryEntry.model_validate_json(raw[:-1])
                    if primary_control_bytes(entry) + b"\n" != raw:
                        raise PrimaryStateError("custody inventory row is not its exact encoding")
                    key = str(entry.member.artifact_id)
                    if previous_member is not None and key <= previous_member:
                        raise PrimaryStateError("custody inventory member order changed")
                    if count >= cursor.members:
                        raise PrimaryStateError("custody inventory exceeds its exact member count")
                    record = store.load_controls(entry.member)
                    if record is None or _identity(record.prepared) != entry.prepared_sha256:
                        raise PrimaryStateError("custody member lost its original preparation")
                    if record.decision_accepted is None:
                        raise PrimaryStateError(
                            "terminal custody lost its exact decision acknowledgement"
                        )
                    if record.custody is None:
                        settle(entry.member)
                        record = store.load_controls(entry.member)
                        if (
                            record is None
                            or record.decision_accepted is None
                            or _identity(record.prepared) != entry.prepared_sha256
                        ):
                            raise PrimaryStateError(
                                "custody callback changed its original preparation"
                            )
                        if record.custody is None:
                            break
                    if not store.reclaim(entry.member):
                        raise PrimaryStateError(
                            "custody member cannot retain compact release authority"
                        )
                    chain = _chain(chain, raw[:-1])
                    count += 1
                    offset += len(raw)
                    previous_member = entry.member.artifact_id
                complete = count == cursor.members
                if complete and (
                    offset != cursor.inventory.bytes
                    or stream.read(1)
                    or chain != cursor.inventory.chain_sha256
                ):
                    raise PrimaryStateError("custody inventory differs from its frozen commitment")
                if not complete and offset >= cursor.inventory.bytes:
                    raise PrimaryStateError("custody inventory ended before its declared members")
        finally:
            store._closing_advancing = False
        next_control = PrimaryCustodyClosing.model_validate(
            {
                **cursor.model_dump(mode="json"),
                "after_member": str(count),
                "after_offset": str(offset),
                "last_member": previous_member,
                "manifest_chain_sha256": chain,
            }
        )
        store._write(store._files, "custody-closing", next_control, expected=cursor)
        if complete:
            store._install(
                store._files,
                "custody-settled",
                PrimaryCustodySettled.model_validate(
                    {
                        "session_sha256": ready.session_sha256,
                        "inventory": ready.inventory,
                        "members": str(count),
                        "manifest_chain_sha256": chain,
                    }
                ),
            )
        return complete
