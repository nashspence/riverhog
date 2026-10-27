"""Point-addressed prepared-primary controls under one live construction owner."""

from __future__ import annotations

import hashlib
import threading
from collections.abc import Callable, Iterator
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, TypeVar

from pydantic import BaseModel
from riverhog_protocol import (
    ArtifactMemberIdentityDocument,
    CollectionArtifactProvenanceBindingDocument,
    CollectionUploadArtifactCustodyReceiptDocument,
)

from riverhog_client._prepared_primary_documents import (
    ByteCommitment,
    ByteFile,
    PreparedPrimary,
    PrimaryAllocation,
    PrimaryClosing,
    PrimaryClosingIntent,
    PrimaryCreating,
    PrimaryCustody,
    PrimaryCustodySettled,
    PrimaryDecisionAccepted,
    PrimaryReclaimed,
    PrimaryScope,
    PrimarySession,
    PrimaryTransition,
    PrimaryUsage,
    primary_control_bytes,
)
from riverhog_client._prepared_primary_posix import PrimaryFiles
from riverhog_client._prepared_primary_types import (
    PrimaryStateBusy,
    PrimaryStateCapacity,
    PrimaryStateError,
)

_Control = TypeVar("_Control", bound=BaseModel)
_BYTE_NAMES = ("request", "observation", "graph", "journal", "decision")
_ROOT_NAMES = (
    "allocation",
    "usage",
    "transition",
    "create-request",
    "creating",
    "session",
    "closing-intent",
    "closing",
    "finish-ready",
    "custody-closing",
    "custody-settled",
    "inventory",
)
_MEMBER_NAMES = (*_BYTE_NAMES, "prepared", "decision-accepted", "custody", "reclaimed")


def _commit(raw: bytes) -> ByteCommitment:
    return ByteCommitment.model_validate(
        {"bytes": str(len(raw)), "sha256": hashlib.sha256(raw).hexdigest()}
    )


def _reference(name: str, raw: bytes) -> ByteFile:
    return ByteFile(name=name, **_commit(raw).model_dump(mode="json"))


def _identity(document: BaseModel) -> str:
    return hashlib.sha256(primary_control_bytes(document)).hexdigest()


@dataclass(frozen=True, slots=True)
class PrimaryStoreLimits:
    """Operator admission, including space reserved for custody during saturation."""

    pending_bytes: int
    total_bytes: int
    preparation_bytes: int
    control_bytes: int
    inventory_order_bytes: int = 0

    def __post_init__(self) -> None:
        for value in (
            self.pending_bytes,
            self.total_bytes,
            self.preparation_bytes,
            self.control_bytes,
        ):
            if type(value) is not int or value <= 0:
                raise ValueError("prepared-primary resource admission must be positive integers")
        if type(self.inventory_order_bytes) is not int or self.inventory_order_bytes < 0:
            raise ValueError("inventory ordering admission must be a nonnegative integer")
        if self.total_bytes < self.root_reservation:
            raise ValueError("prepared-primary total admission cannot reserve root controls")

    @property
    def root_reservation(self) -> int:
        return 14 * self.control_bytes + self.inventory_order_bytes

    @property
    def member_reservation(self) -> int:
        # Prepared, decision ack, custody, reclaim marker, and one publication candidate.
        return 5 * self.control_bytes


@dataclass(frozen=True, slots=True)
class PrimaryPreparation:
    """Already authored bytes; the producer owns Native validation and memory admission."""

    observation: bytes
    graph: bytes
    journal: bytes
    decision: bytes
    primary: CollectionArtifactProvenanceBindingDocument
    completion_requirement_sha256: str | None


@dataclass(frozen=True, slots=True)
class PrimaryRecord:
    prepared: PreparedPrimary
    decision_accepted: PrimaryDecisionAccepted | None
    custody: PrimaryCustody | None

    @property
    def compact(self) -> bool:
        return self.decision_accepted is not None and self.custody is not None


class PrimaryStore:
    """An explicit allocation, never an automatic replacement for missing state.

    Each transition names one member and carries both counter values. Recovery
    examines only that member, then installs the selected value. No member lookup,
    preparation or reclaim enumerates the construction's other members.
    """

    def __init__(
        self,
        root: Path,
        *,
        allocation_id: str,
        scope: PrimaryScope,
        limits: PrimaryStoreLimits,
        initialize: bool,
    ) -> None:
        self.limits = limits
        self._mutex = threading.RLock()
        self._authoring = False
        self._closing_advancing = False
        self._files = PrimaryFiles(root, initialize=initialize)
        try:
            expected = PrimaryAllocation.model_validate(
                {
                    "allocation_id": allocation_id,
                    "scope": scope,
                    "control_record_bytes": str(limits.control_bytes),
                }
            )
            if initialize:
                self._write(self._files, "allocation", expected)
                self._write(self._files, "usage", PrimaryUsage())
            self.allocation = self._read(self._files, "allocation", PrimaryAllocation)
            if self.allocation != expected:
                raise PrimaryStateError(
                    "prepared-primary allocation, scope or control admission changed"
                )
            # Only named candidates from the one interrupted root operation are removable.
            for name in _ROOT_NAMES:
                self._files.remove("candidate-" + name)
            self._usage = self._read(self._files, "usage", PrimaryUsage)
            self._files.sync()
            self._recover()
        except BaseException:
            self._files.close()
            raise

    def __enter__(self) -> PrimaryStore:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def close(self) -> None:
        """Caller must quiesce payload and heartbeat workers before closing."""
        with self._mutex:
            self._files.close()

    @property
    def publication_started(self) -> bool:
        """Distinguish pristine allocation from a possibly committed remote creation."""
        with self._mutex:
            self._recover()
            return (
                self._optional(self._files, "creating", PrimaryCreating) is not None
                or self._optional(self._files, "session", PrimarySession) is not None
                or self._files.exists("create-request")
                or self._usage.members != 0
            )

    @property
    def session_bound(self) -> bool:
        """Whether the allocation retains an accepted session, without creating one."""
        with self._mutex:
            self._recover()
            return self._optional(self._files, "session", PrimarySession) is not None

    @property
    def custody_settled(self) -> bool:
        """Observe the validated complete local receipt/reclaim barrier without a scan."""
        with self._mutex:
            self._recover()
            return self._optional(self._files, "custody-settled", PrimaryCustodySettled) is not None

    @property
    def usage(self) -> PrimaryUsage:
        with self._mutex:
            self._recover()
            return self._usage

    def _read(self, files: PrimaryFiles, name: str, model: type[_Control]) -> _Control:
        raw = files.read_control(name, byte_limit=self.limits.control_bytes)
        value = model.model_validate_json(raw)
        if primary_control_bytes(value) != raw:
            raise PrimaryStateError("prepared-primary control is not its exact current encoding")
        return value

    def _optional(self, files: PrimaryFiles, name: str, model: type[_Control]) -> _Control | None:
        try:
            return self._read(files, name, model)
        except FileNotFoundError:
            return None

    def _write(
        self, files: PrimaryFiles, name: str, document: BaseModel, *, replace: bool = False
    ) -> None:
        raw = primary_control_bytes(document)
        files.put(
            name,
            (raw,),
            byte_limit=self.limits.control_bytes,
            replace=replace,
            temporary_name="candidate-" + name,
        )

    def _install(self, files: PrimaryFiles, name: str, document: BaseModel) -> None:
        """Adopt only an identical visible winner, after a new directory barrier."""
        try:
            self._write(files, name, document)
        except FileExistsError:
            actual = files.read_control(name, byte_limit=self.limits.control_bytes)
            if actual != primary_control_bytes(document):
                raise PrimaryStateError("prepared-primary immutable control conflicts") from None
        files.sync()

    def session_for(self, create_request: bytes) -> PrimarySession | None:
        """Compare before any remote mutation; record first-create ambiguity durably."""
        with self._mutex:
            self._recover()
            session = self._optional(self._files, "session", PrimarySession)
            commitment = _commit(create_request)
            if session is not None:
                if (
                    session.allocation_sha256 != _identity(self.allocation)
                    or session.create_request != commitment
                ):
                    raise PrimaryStateError("prepared-primary creation inputs changed")
                self._files.sync()
                return session
            creating = self._optional(self._files, "creating", PrimaryCreating)
            if creating is not None:
                raise PrimaryStateError(
                    "initial session creation has an uncertain outcome; "
                    "retain state for reconciliation"
                )
            reference = _reference("create-request", create_request)
            try:
                self._files.put(
                    reference.name,
                    (create_request,),
                    byte_limit=self.limits.control_bytes,
                    temporary_name="candidate-create-request",
                )
            except FileExistsError:
                with self._files.verified(reference.byte_file()):
                    pass
            self._install(
                self._files,
                "creating",
                PrimaryCreating(allocation_sha256=_identity(self.allocation), request=reference),
            )
            return None

    def bind_session(self, session: PrimarySession) -> None:
        """Retain the accepted identities before any member publication."""
        with self._mutex:
            self._recover()
            creating = self._read(self._files, "creating", PrimaryCreating)
            if (
                creating.allocation_sha256 != _identity(self.allocation)
                or session.allocation_sha256 != creating.allocation_sha256
                or session.create_request != creating.request.commitment()
            ):
                raise PrimaryStateError("accepted session differs from its creation request")
            with self._files.verified(creating.request.byte_file()):
                pass
            self._install(self._files, "session", session)

    @property
    def session(self) -> PrimarySession:
        with self._mutex:
            session = self._read(self._files, "session", PrimarySession)
            if session.allocation_sha256 != _identity(self.allocation):
                raise PrimaryStateError("prepared-primary session differs from its allocation")
            return session

    @contextmanager
    def _member(self, artifact_id: str, *, create: bool) -> Iterator[PrimaryFiles]:
        key = hashlib.sha256(artifact_id.encode("utf-8")).hexdigest()

        def child(parent: PrimaryFiles, name: str) -> PrimaryFiles:
            if not create:
                return parent.child(name, initialize=False)
            try:
                result = parent.child(name, initialize=True)
            except FileExistsError:
                result = parent.child(name, initialize=False)
            try:
                # An orphan mkdir may be visible after an unsuccessful prior fsync.
                parent.sync()
            except BaseException:
                result.close()
                raise
            return result

        with child(self._files, "members") as members:
            with child(members, key[:2]) as shard:
                with child(shard, key) as files:
                    yield files

    def _record(
        self,
        files: PrimaryFiles,
        member: ArtifactMemberIdentityDocument,
        *,
        uncommitted: bool = False,
    ) -> PrimaryRecord | None:
        for name in ("decision-accepted", "custody", "reclaimed"):
            files.remove("candidate-" + name)
        prepared = self._optional(files, "prepared", PreparedPrimary)
        custody = self._optional(files, "custody", PrimaryCustody)
        accepted = self._optional(files, "decision-accepted", PrimaryDecisionAccepted)
        reclaimed = self._optional(files, "reclaimed", PrimaryReclaimed)
        if prepared is None:
            if custody is not None or accepted is not None or reclaimed is not None:
                raise PrimaryStateError("prepared-primary compact state has no original manifest")
            if not uncommitted and any(
                files.exists(name) or files.exists("candidate-" + name) for name in _BYTE_NAMES
            ):
                raise PrimaryStateError("prepared-primary uncommitted bytes have no transition")
            return None
        if prepared.member != member or prepared.session_sha256 != _identity(self.session):
            raise PrimaryStateError("prepared-primary member or accepted session changed")
        if tuple(file.name for file in prepared.byte_files) != _BYTE_NAMES:
            raise PrimaryStateError("prepared-primary byte inventory is not the current format")
        identity = _identity(prepared)
        if custody is not None and (
            custody.prepared != prepared
            or custody.prepared_sha256 != identity
            or custody.collection_id != self.session.collection_id
        ):
            raise PrimaryStateError("prepared-primary custody changed")
        if accepted is not None and (
            accepted.prepared_sha256 != identity
            or accepted.decision != prepared.decision.commitment()
        ):
            raise PrimaryStateError("prepared-primary decision acceptance changed")
        if reclaimed is not None and (
            reclaimed.prepared_sha256 != identity or accepted is None or custody is None
        ):
            raise PrimaryStateError("prepared-primary reclaim has no original compact authority")
        result = PrimaryRecord(prepared, accepted, custody)
        if not result.compact:
            try:
                for file in prepared.byte_files:
                    with files.verified(file.byte_file()):
                        pass
            except FileNotFoundError as exc:
                raise PrimaryStateError("prepared-primary committed bytes are missing") from exc
        files.sync()
        return result

    def load(self, member: ArtifactMemberIdentityDocument) -> PrimaryRecord | None:
        with self._mutex:
            self._recover()
            with ExitStack() as opened:
                try:
                    files = opened.enter_context(
                        self._member(str(member.artifact_id), create=False)
                    )
                except FileNotFoundError:
                    return None
                return self._record(files, member)

    def prepare_or_load(
        self,
        member: ArtifactMemberIdentityDocument,
        request: bytes,
        author: Callable[[int], PrimaryPreparation],
    ) -> PrimaryRecord:
        """Load before authoring; the author receives the available byte admission."""
        with self._mutex:
            self._recover()
            existing = self.load(member)
            if existing is not None:
                if existing.prepared.request.commitment() != _commit(request):
                    raise PrimaryStateError("prepared-primary retry inputs changed")
                return existing
            if (
                self._optional(self._files, "closing-intent", PrimaryClosingIntent) is not None
                or self._optional(self._files, "closing", PrimaryClosing) is not None
            ):
                raise PrimaryStateError("closing allocation cannot admit new preparation")
            grant = min(
                self.limits.preparation_bytes,
                self.limits.pending_bytes - self._usage.pending_bytes,
                self.limits.total_bytes
                - self.limits.root_reservation
                - self._usage.control_bytes
                - self.limits.member_reservation
                - self._usage.pending_bytes
                - self._usage.inventory_bytes
                - self._usage.inventory_reserved_bytes
                - (self.limits.control_bytes + 1),
            )
            if grant <= 0 or grant < len(request):
                raise PrimaryStateCapacity("prepared-primary allocation admission exhausted")
            after = self._next_usage(
                pending=self._usage.pending_bytes + grant,
                controls=self._usage.control_bytes + self.limits.member_reservation,
                members=self._usage.members + 1,
            )
            transition = PrimaryTransition(
                operation="prepare", member=member, before=self._usage, after=after
            )
            self._write(self._files, "transition", transition)
            self._authoring = True
            try:
                authored = author(grant - len(request))
            finally:
                self._authoring = False
            contents = (
                request,
                authored.observation,
                authored.graph,
                authored.journal,
                authored.decision,
            )
            size = sum(len(content) for content in contents)
            if size > grant:
                raise PrimaryStateCapacity("prepared-primary preparation exceeds byte admission")
            after = self._next_usage(
                pending=self._usage.pending_bytes + size,
                controls=after.control_bytes,
                members=after.members,
            )
            transition = transition.model_copy(update={"after": after})
            self._write(self._files, "transition", transition, replace=True)
            refs = tuple(
                _reference(name, raw) for name, raw in zip(_BYTE_NAMES, contents, strict=True)
            )
            prepared = PreparedPrimary.model_validate(
                {
                    "session_sha256": _identity(self.session),
                    "member": member.model_dump(mode="json"),
                    **{
                        name: ref.model_dump(mode="json")
                        for name, ref in zip(_BYTE_NAMES, refs, strict=True)
                    },
                    "primary": authored.primary,
                    "completion_requirement_sha256": authored.completion_requirement_sha256,
                }
            )
            with self._member(str(member.artifact_id), create=True) as files:
                for ref, content in zip(refs, contents, strict=True):
                    files.put(
                        ref.name,
                        (content,),
                        byte_limit=grant,
                        expected=(ref.bytes, ref.sha256),
                        temporary_name="candidate-" + ref.name,
                    )
                self._write(files, "prepared", prepared)
            self._finish_transition(after)
            return PrimaryRecord(prepared, None, None)

    def _next_usage(self, *, pending: int, controls: int, members: int) -> PrimaryUsage:
        return PrimaryUsage.model_validate(
            {
                "sequence": str(self._usage.sequence + 1),
                "pending_bytes": str(pending),
                "control_bytes": str(controls),
                "inventory_bytes": str(self._usage.inventory_bytes),
                "inventory_selected": self._usage.inventory_selected,
                "inventory_reserved_bytes": str(
                    0
                    if self._usage.inventory_selected
                    else members * (self.limits.control_bytes + 1)
                ),
                "members": str(members),
            }
        )

    def _finish_transition(self, after: PrimaryUsage) -> None:
        self._write(self._files, "usage", after, replace=True)
        self._usage = after
        self._files.remove("transition")

    def _recover(self) -> None:
        if self._authoring:
            raise PrimaryStateBusy("prepared-primary author cannot reenter its active transition")
        self._files.check_current()
        transition = self._optional(self._files, "transition", PrimaryTransition)
        if transition is None:
            from riverhog_client._prepared_primary_closing import recover_closing

            recover_closing(self)
            return
        if self._usage not in (transition.before, transition.after):
            raise PrimaryStateError(
                "prepared-primary transition counter differs from retained state"
            )
        with self._member(str(transition.member.artifact_id), create=True) as files:
            # These are the only possible unlinked publication candidates.
            for name in _MEMBER_NAMES:
                files.remove("candidate-" + name)
            record = self._record(
                files, transition.member, uncommitted=transition.operation == "prepare"
            )
            if transition.operation == "prepare":
                if record is None:
                    if self._usage != transition.before:
                        raise PrimaryStateError("prepared-primary committed manifest disappeared")
                    for name in _BYTE_NAMES:
                        files.remove(name)
                    selected = transition.before
                else:
                    expected = self._usage_from_preparation(transition.before, record.prepared)
                    if expected != transition.after:
                        raise PrimaryStateError(
                            "prepared-primary transition does not cover its manifest"
                        )
                    selected = transition.after
            else:
                if record is None or not record.compact:
                    raise PrimaryStateError(
                        "prepared-primary reclaim has no exact custody and decision"
                    )
                expected = self._usage_after_reclaim(transition.before, record.prepared)
                if transition.after != expected:
                    raise PrimaryStateError("prepared-primary reclaim accounting changed")
                for name in _BYTE_NAMES:
                    files.remove(name)
                self._install(
                    files, "reclaimed", PrimaryReclaimed(prepared_sha256=_identity(record.prepared))
                )
                selected = transition.after
        self._finish_transition(selected)
        from riverhog_client._prepared_primary_closing import recover_closing

        recover_closing(self)

    def _usage_from_preparation(
        self, before: PrimaryUsage, prepared: PreparedPrimary
    ) -> PrimaryUsage:
        return PrimaryUsage.model_validate(
            {
                "sequence": str(before.sequence + 1),
                "pending_bytes": str(
                    before.pending_bytes + sum(file.bytes for file in prepared.byte_files)
                ),
                "control_bytes": str(before.control_bytes + self.limits.member_reservation),
                "inventory_bytes": str(before.inventory_bytes),
                "inventory_selected": before.inventory_selected,
                "inventory_reserved_bytes": str(
                    before.inventory_reserved_bytes + self.limits.control_bytes + 1
                ),
                "members": str(before.members + 1),
            }
        )

    def _usage_after_reclaim(self, before: PrimaryUsage, prepared: PreparedPrimary) -> PrimaryUsage:
        pending = before.pending_bytes - sum(file.bytes for file in prepared.byte_files)
        if pending < 0:
            raise PrimaryStateError("prepared-primary reclaim exceeds retained pending bytes")
        return PrimaryUsage.model_validate(
            {
                "sequence": str(before.sequence + 1),
                "pending_bytes": str(pending),
                "control_bytes": str(before.control_bytes),
                "inventory_bytes": str(before.inventory_bytes),
                "inventory_selected": before.inventory_selected,
                "inventory_reserved_bytes": str(before.inventory_reserved_bytes),
                "members": str(before.members),
            }
        )

    @contextmanager
    def byte_reader(self, record: PrimaryRecord, name: str) -> Iterator[BinaryIO]:
        with self._mutex:
            self._recover()
            ref = next((file for file in record.prepared.byte_files if file.name == name), None)
            if ref is None:
                raise ValueError("unknown prepared-primary byte role")
            with self._member(str(record.prepared.member.artifact_id), create=False) as files:
                current = self._record(files, record.prepared.member)
                if current is None or current.prepared != record.prepared:
                    raise PrimaryStateError("prepared-primary replay handle changed")
                with files.verified(ref.byte_file()) as stream:
                    yield stream

    def accept_decision(self, record: PrimaryRecord, response: bytes) -> PrimaryRecord:
        with self._mutex:
            self._recover()
            prepared = record.prepared
            ack = PrimaryDecisionAccepted(
                prepared_sha256=_identity(prepared),
                decision=prepared.decision.commitment(),
                response=_commit(response),
            )
            with self._member(str(prepared.member.artifact_id), create=False) as files:
                current = self._record(files, prepared.member)
                if current is None or current.prepared != prepared:
                    raise PrimaryStateError("prepared-primary decision has no matching manifest")
                self._install(files, "decision-accepted", ack)
            result = self.load(prepared.member)
            assert result is not None
            return result

    def accept_custody(
        self, record: PrimaryRecord, receipt: CollectionUploadArtifactCustodyReceiptDocument
    ) -> PrimaryRecord:
        with self._mutex:
            self._recover()
            custody = PrimaryCustody.model_validate(
                {
                    "collection_id": str(self.session.collection_id),
                    "prepared_sha256": _identity(record.prepared),
                    "prepared": record.prepared,
                    "receipt": receipt,
                }
            )
            with self._member(str(record.prepared.member.artifact_id), create=False) as files:
                current = self._record(files, record.prepared.member)
                if current is None or current.prepared != record.prepared:
                    raise PrimaryStateError("prepared-primary custody has no matching manifest")
                self._install(files, "custody", custody)
            result = self.load(record.prepared.member)
            assert result is not None
            return result

    def reclaim(self, member: ArtifactMemberIdentityDocument) -> bool:
        with self._mutex:
            self._recover()
            record = self.load(member)
            if record is None or not record.compact:
                return False
            # The control is retained after deletion to make subtraction idempotent.
            with self._member(str(member.artifact_id), create=False) as files:
                reclaimed = self._optional(files, "reclaimed", PrimaryReclaimed)
                identity = _identity(record.prepared)
                if reclaimed is not None:
                    if reclaimed.prepared_sha256 != identity:
                        raise PrimaryStateError("prepared-primary reclaim commitment changed")
                    return True
                after = self._usage_after_reclaim(self._usage, record.prepared)
                transition = PrimaryTransition(
                    operation="reclaim", member=member, before=self._usage, after=after
                )
                self._write(self._files, "transition", transition)
                for name in _BYTE_NAMES:
                    files.remove(name)
                self._install(files, "reclaimed", PrimaryReclaimed(prepared_sha256=identity))
                self._finish_transition(after)
            return True
