"""Exact point-control transitions for a producer's pending source queue."""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from functools import cached_property
from typing import TYPE_CHECKING, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator
from riverhog_protocol import ArtifactId, ArtifactMemberIdentityDocument
from riverhog_protocol.collection_upload_transport import CollectionUploadRawPartsIn
from riverhog_protocol.exact_scalar import NonnegativeDecimal

from riverhog_client._prepared_primary_documents import (
    PrimaryControl,
    Sha256,
    primary_control_bytes,
)
from riverhog_client._prepared_primary_types import (
    PrimaryStateBusy,
    PrimaryStateCapacity,
    PrimaryStateError,
)

if TYPE_CHECKING:
    from riverhog_client._prepared_primary_store import PrimaryStore


class PendingSourceMetadata(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    member: ArtifactMemberIdentityDocument
    registration_identity_sha256: Sha256
    raw_parts: CollectionUploadRawPartsIn | None = None


class PendingSourceNode(PrimaryControl):
    kind: Literal["pending-source"] = "pending-source"
    source: PendingSourceMetadata
    previous: ArtifactId | None = None
    next: ArtifactId | None = None
    active: bool = True
    payload_sealed: bool = False

    @model_validator(mode="after")
    def exact_links(self) -> Self:
        if self.source.member.artifact_id in (self.previous, self.next):
            raise ValueError("pending source cannot link to itself")
        if not self.active and (self.previous is not None or self.next is not None):
            raise ValueError("retired pending source cannot retain queue links")
        return self


class PendingSourceQueue(PrimaryControl):
    kind: Literal["pending-sources"] = "pending-sources"
    sequence: NonnegativeDecimal = 0
    members: NonnegativeDecimal = 0
    first: ArtifactId | None = None
    last: ArtifactId | None = None

    @model_validator(mode="after")
    def exact_ends(self) -> Self:
        if self.members == 0:
            if self.first is not None or self.last is not None:
                raise ValueError("empty pending queue cannot retain endpoints")
        elif self.first is None or self.last is None:
            raise ValueError("nonempty pending queue requires both endpoints")
        elif (self.first == self.last) != (self.members == 1):
            raise ValueError("pending queue endpoints differ from its member count")
        return self


class PendingSourceLinks(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    previous: ArtifactId | None = None
    next: ArtifactId | None = None
    active: bool = True
    payload_sealed: bool = False


class PendingSourceWrite(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    artifact_id: ArtifactId
    source: PendingSourceMetadata
    before: PendingSourceLinks | None
    after: PendingSourceLinks

    @model_validator(mode="after")
    def exact_member(self) -> Self:
        if self.source.member.artifact_id != self.artifact_id:
            raise ValueError("pending write changed its member")
        _ = self.after_node
        if self.before is not None:
            _ = self.before_node
        return self

    @cached_property
    def before_node(self) -> PendingSourceNode | None:
        if self.before is None:
            return None
        return PendingSourceNode(source=self.source, **self.before.model_dump())

    @cached_property
    def after_node(self) -> PendingSourceNode:
        return PendingSourceNode(source=self.source, **self.after.model_dump())

    @classmethod
    def from_nodes(
        cls,
        artifact_id: ArtifactId,
        before: PendingSourceNode | None,
        after: PendingSourceNode,
    ) -> Self:
        if before is not None and before.source != after.source:
            raise ValueError("pending write changed original source metadata")

        def links(node: PendingSourceNode) -> PendingSourceLinks:
            return PendingSourceLinks.model_validate(
                node.model_dump(exclude={"format", "kind", "source"})
            )

        return cls(
            artifact_id=artifact_id,
            source=after.source,
            before=None if before is None else links(before),
            after=links(after),
        )


Operation = Literal["insert", "seal", "rotate", "retire"]


def _queue(queue: PendingSourceQueue, **changes: object) -> PendingSourceQueue:
    return PendingSourceQueue.model_validate(
        {
            **queue.model_dump(mode="json"),
            "sequence": str(queue.sequence + 1),
            **changes,
        }
    )


def _node(node: PendingSourceNode, **changes: object) -> PendingSourceNode:
    return PendingSourceNode.model_validate({**node.model_dump(mode="json"), **changes})


def pending_source_change(
    operation: Operation,
    queue: PendingSourceQueue,
    target: ArtifactId,
    before: dict[ArtifactId, PendingSourceNode | None],
    *,
    source: PendingSourceMetadata | None = None,
    payload_sealed: bool = False,
) -> tuple[PendingSourceQueue, dict[ArtifactId, PendingSourceNode]]:
    """Derive only the nodes touched by one explicit queue operation."""
    changed: dict[ArtifactId, PendingSourceNode] = {}

    def read(key: ArtifactId | None) -> PendingSourceNode:
        if key is None or key not in before or before[key] is None:
            raise ValueError("pending transition omits an original queue node")
        node = before[key]
        assert node is not None
        if not node.active or node.source.member.artifact_id != key:
            raise ValueError("pending transition has an invalid original queue node")
        return node

    def patch(key: ArtifactId | None, **changes: object) -> None:
        if key is None:
            return
        node = changed.get(key) or read(key)
        changed[key] = _node(node, **changes)

    if operation == "insert":
        if source is None or source.member.artifact_id != target or before.get(target) is not None:
            raise ValueError("pending insertion requires a new exact source")
        if queue.last is not None:
            if read(queue.last).next is not None:
                raise ValueError("pending queue tail has an unexpected successor")
            patch(queue.last, next=target)
        changed[target] = PendingSourceNode(
            source=source, previous=queue.last, payload_sealed=payload_sealed
        )
        after = _queue(
            queue, members=str(queue.members + 1), first=queue.first or target, last=target
        )
    else:
        node = read(target)
        if operation == "seal":
            if node.payload_sealed:
                raise ValueError("pending source is already sealed")
            patch(target, payload_sealed=True)
            after = _queue(queue)
        elif operation == "rotate":
            if queue.first != target or queue.members <= 1 or node.previous is not None:
                raise ValueError("pending rotation must advance the current queue head")
            if node.next is None or queue.last is None:
                raise ValueError("pending rotation has lost a queue endpoint")
            if read(node.next).previous != target or read(queue.last).next is not None:
                raise ValueError("pending rotation links differ from its frontier")
            patch(node.next, previous=None)
            patch(queue.last, next=target)
            patch(target, previous=queue.last, next=None)
            after = _queue(queue, first=node.next, last=target)
        elif operation == "retire":
            if node.previous is None:
                if queue.first != target:
                    raise ValueError("pending retirement has lost its first member")
            else:
                if read(node.previous).next != target:
                    raise ValueError("pending retirement has lost its predecessor")
                patch(node.previous, next=node.next)
            if node.next is None:
                if queue.last != target:
                    raise ValueError("pending retirement has lost its last member")
            else:
                if read(node.next).previous != target:
                    raise ValueError("pending retirement has lost its successor")
                patch(node.next, previous=node.previous)
            patch(target, previous=None, next=None, active=False)
            after = _queue(
                queue,
                members=str(queue.members - 1),
                first=node.next if queue.first == target else queue.first,
                last=node.previous if queue.last == target else queue.last,
            )
        else:
            raise ValueError("unknown pending source operation")
    if not 1 <= len(changed) <= 3:
        raise ValueError("pending transition exceeds its point-node shape")
    return after, changed


class PendingSourceTransition(PrimaryControl):
    kind: Literal["pending-source-transition"] = "pending-source-transition"
    operation: Operation
    target: ArtifactId
    before: PendingSourceQueue
    after: PendingSourceQueue
    writes: tuple[PendingSourceWrite, ...] = Field(min_length=1, max_length=3)

    @model_validator(mode="after")
    def exact_change(self) -> Self:
        originals = {write.artifact_id: write.before_node for write in self.writes}
        actual = {write.artifact_id: write.after_node for write in self.writes}
        if len(originals) != len(self.writes) or self.target not in actual:
            raise ValueError("pending transition repeats or omits a member")
        source = actual[self.target].source if self.operation == "insert" else None
        after, changed = pending_source_change(
            self.operation,
            self.before,
            self.target,
            originals,
            source=source,
            payload_sealed=actual[self.target].payload_sealed,
        )
        if after != self.after or changed != actual:
            raise ValueError("pending transition differs from its exact queue operation")
        return self


class PendingSourceSelection(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    artifact_id: ArtifactId
    write_sha256: Sha256


class PendingSourceIntent(PrimaryControl):
    kind: Literal["pending-source-transition"] = "pending-source-transition"
    operation: Operation
    target: ArtifactId
    before: PendingSourceQueue
    after: PendingSourceQueue
    writes: tuple[PendingSourceSelection, ...] = Field(min_length=1, max_length=3)

    @model_validator(mode="after")
    def exact_selection(self) -> Self:
        keys = [write.artifact_id for write in self.writes]
        if len(keys) != len(set(keys)) or self.target not in keys:
            raise ValueError("pending selection repeats or omits a member")
        if self.after.sequence != self.before.sequence + 1:
            raise ValueError("pending selection must advance once")
        return self


def _read_pending_node(store: PrimaryStore, artifact_id: ArtifactId) -> PendingSourceNode | None:
    try:
        with store._member(str(artifact_id), create=False) as files:
            if not files.discard_candidates(("pending-source",)):
                raise PrimaryStateBusy("pending source requires bounded candidate cleanup")
            node = store._optional(files, "pending-source", PendingSourceNode)
            if node is not None:
                if node.source.member.artifact_id != artifact_id:
                    raise PrimaryStateError("pending source changed its storage member")
                if store._control_record(files, node.source.member) is None:
                    raise PrimaryStateError("pending source has no original preparation")
            return node
    except FileNotFoundError:
        return None


def recover_pending_sources(store: PrimaryStore) -> bool:
    """Roll forward one selected intent without enumerating other members."""
    files = store._files
    try:
        queue = store._read(files, "pending-sources", PendingSourceQueue)
    except FileNotFoundError:
        raise PrimaryStateError("prepared pending source queue is missing") from None
    intent = store._optional(files, "pending-source-transition", PendingSourceIntent)
    if intent is None:
        return True
    if not files.discard_candidates(("pending-sources", "pending-source-transition")):
        return False
    if queue not in (intent.before, intent.after):
        raise PrimaryStateError("pending source queue differs from its selected transition")
    selected = []
    for selection in intent.writes:
        with store._member(str(selection.artifact_id), create=False) as member_files:
            if not member_files.discard_candidates(("pending-source-next",)):
                return False
            try:
                write = store._read(member_files, "pending-source-next", PendingSourceWrite)
            except FileNotFoundError:
                raise PrimaryStateError("selected pending source write is missing") from None
            if (
                write.artifact_id != selection.artifact_id
                or hashlib.sha256(primary_control_bytes(write)).hexdigest()
                != selection.write_sha256
            ):
                raise PrimaryStateError("selected pending source write changed")
            selected.append(write)
    transition = PendingSourceTransition(
        operation=intent.operation,
        target=intent.target,
        before=intent.before,
        after=intent.after,
        writes=tuple(selected),
    )
    for write in transition.writes:
        with store._member(str(write.artifact_id), create=False) as member_files:
            if not member_files.discard_candidates(("pending-source",)):
                return False
            current = store._optional(member_files, "pending-source", PendingSourceNode)
            if current not in (write.before_node, write.after_node):
                raise PrimaryStateError("pending source node differs from its selected transition")
            record = store._control_record(member_files, write.source.member)
            if record is None:
                raise PrimaryStateError("pending source transition has no original preparation")
            if (
                transition.operation == "retire"
                and write.artifact_id == transition.target
                and record.custody is None
            ):
                raise PrimaryStateError("pending source retirement has no original custody")
            store._write(member_files, "pending-source", write.after_node, expected=current)
    store._write(files, "pending-sources", transition.after, expected=queue)
    files.remove_control("pending-source-transition", expected=primary_control_bytes(intent))
    return True


class PendingSourceIndex:
    """A point-read view of original-owner controls, retaining no source rows."""

    def __init__(self, store: PrimaryStore) -> None:
        self._store = store

    def _start(self) -> PendingSourceQueue:
        store = self._store
        complete = store._recover(require_complete=False, verify_preimages=False)
        if store._root_candidates_pending or (not complete and not recover_pending_sources(store)):
            raise PrimaryStateBusy("pending source transition requires bounded recovery")
        return store._read(store._files, "pending-sources", PendingSourceQueue)

    def _commit(
        self,
        operation: Operation,
        queue: PendingSourceQueue,
        target: ArtifactId,
        originals: dict[ArtifactId, PendingSourceNode | None],
        *,
        source: PendingSourceMetadata | None = None,
        payload_sealed: bool = False,
    ) -> None:
        store = self._store
        after, changed = pending_source_change(
            operation, queue, target, originals, source=source, payload_sealed=payload_sealed
        )
        transition = PendingSourceTransition(
            operation=operation,
            target=target,
            before=queue,
            after=after,
            writes=tuple(
                PendingSourceWrite.from_nodes(key, originals[key], node)
                for key, node in sorted(changed.items())
            ),
        )
        intent = PendingSourceIntent(
            operation=operation,
            target=target,
            before=queue,
            after=after,
            writes=tuple(
                PendingSourceSelection(
                    artifact_id=write.artifact_id,
                    write_sha256=hashlib.sha256(primary_control_bytes(write)).hexdigest(),
                )
                for write in transition.writes
            ),
        )
        # Bound each staged point record and the selected root reference separately.
        for control in (intent, after, *transition.writes, *changed.values()):
            if len(primary_control_bytes(control)) > store.limits.control_bytes:
                raise PrimaryStateCapacity("pending source transition exceeds control admission")
        for write in transition.writes:
            with store._member(str(write.artifact_id), create=False) as files:
                if not files.discard_candidates(("pending-source",)):
                    raise PrimaryStateBusy("pending source requires bounded candidate cleanup")
                current = store._optional(files, "pending-source", PendingSourceNode)
                record = store._control_record(files, write.source.member)
                if current != write.before_node or record is None:
                    raise PrimaryStateError("pending source changed before transition selection")
                if operation == "retire" and write.artifact_id == target and record.custody is None:
                    raise PrimaryStateError("pending source retirement has no original custody")
        for write in transition.writes:
            with store._member(str(write.artifact_id), create=False) as files:
                if not files.discard_candidates(("pending-source-next",)):
                    raise PrimaryStateBusy("pending source staging requires bounded cleanup")
                previous = store._optional(files, "pending-source-next", PendingSourceWrite)
                store._write(files, "pending-source-next", write, expected=previous)
        store._write(store._files, "pending-source-transition", intent)
        if not recover_pending_sources(store):
            raise PrimaryStateBusy("pending source transition requires bounded recovery")

    @property
    def count(self) -> int:
        with self._store._mutex:
            return self._start().members

    def get(self, artifact_id: ArtifactId) -> PendingSourceNode | None:
        with self._store._mutex:
            self._start()
            node = _read_pending_node(self._store, artifact_id)
            return node if node is not None and node.active else None

    def record(self, source: PendingSourceMetadata, *, payload_sealed: bool) -> bool:
        with self._store._mutex:
            queue = self._start()
            key = source.member.artifact_id
            old = _read_pending_node(self._store, key)
            originals = {key: old}
            if old is not None:
                if old.source != source:
                    raise PrimaryStateError("pending source retry changed its original metadata")
                if not old.active:
                    with self._store._member(str(key), create=False) as files:
                        record = self._store._control_record(files, source.member)
                    if record is None or record.custody is None:
                        raise PrimaryStateError("retired pending source lost its original custody")
                    return False
                if payload_sealed and not old.payload_sealed:
                    self._commit("seal", queue, key, originals)
                return True
            if queue.last is not None:
                originals[queue.last] = _read_pending_node(self._store, queue.last)
            self._commit(
                "insert", queue, key, originals, source=source, payload_sealed=payload_sealed
            )
            return True

    def rotate_front(self, artifact_id: ArtifactId) -> None:
        with self._store._mutex:
            queue = self._start()
            if queue.last == artifact_id:
                return
            if queue.first != artifact_id:
                raise PrimaryStateError("pending source rotation changed its retry frontier")
            node = _read_pending_node(self._store, artifact_id)
            if node is None:
                raise PrimaryStateError("pending source rotation lost its current member")
            originals: dict[ArtifactId, PendingSourceNode | None] = {artifact_id: node}
            for key in (node.next, queue.last):
                if key is not None:
                    originals[key] = _read_pending_node(self._store, key)
            self._commit("rotate", queue, artifact_id, originals)

    def retire(self, artifact_id: ArtifactId) -> None:
        with self._store._mutex:
            queue = self._start()
            node = _read_pending_node(self._store, artifact_id)
            if node is None or not node.active:
                return
            originals: dict[ArtifactId, PendingSourceNode | None] = {artifact_id: node}
            for key in (node.previous, node.next):
                if key is not None:
                    originals[key] = _read_pending_node(self._store, key)
            self._commit("retire", queue, artifact_id, originals)

    def rows(self) -> Iterator[PendingSourceNode]:
        with self._store._mutex:
            queue = self._start()
        key = queue.first
        previous = None
        seen = 0
        while key is not None:
            with self._store._mutex:
                current = self._start()
                if current != queue:
                    raise PrimaryStateError("pending source traversal changed its queue")
                node = _read_pending_node(self._store, key)
                if node is None or not node.active or node.previous != previous:
                    raise PrimaryStateError("pending source traversal lost its exact links")
                seen += 1
                if seen > queue.members:
                    raise PrimaryStateError("pending source traversal exceeds its member count")
            yield node
            previous, key = key, node.next
        if seen != queue.members or previous != queue.last:
            raise PrimaryStateError("pending source traversal omitted queue members")
