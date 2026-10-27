"""One frozen owner inventory and a resumable materialization barrier."""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING

from riverhog_protocol import ArtifactMemberIdentityDocument

from riverhog_client._prepared_primary_documents import (
    PrimaryClosing,
    PrimaryClosingIntent,
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
    intent = store._optional(files, "closing-intent", PrimaryClosingIntent)
    closing = store._optional(files, "closing", PrimaryClosing)
    ready = store._optional(files, "finish-ready", PrimaryFinishReady)
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
                    replace=True,
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
        store._write(files, "usage", intent.after, replace=True)
        store._usage = intent.after
        files.remove("closing-intent")
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
                "inventory_selected": True,
            }
        )
        store._write(
            store._files,
            "closing-intent",
            PrimaryClosingIntent.model_validate(
                {
                    "session_sha256": intent.session_sha256,
                    "members": str(intent.members),
                    "inventory": inventory,
                    "before": store._usage,
                    "after": after,
                }
            ),
            replace=True,
        )
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
        store._write(store._files, "usage", after, replace=True)
        store._usage = after
        store._files.remove("closing-intent")
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
        store._write(store._files, "closing", next_control, replace=True)
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
