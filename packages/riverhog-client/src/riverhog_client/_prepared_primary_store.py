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
from riverhog_client._prepared_primary_factory import posix_primary_files
from riverhog_client._prepared_primary_storage import PrimaryStorage, PrimaryStorageFactory
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
        storage_factory: PrimaryStorageFactory = posix_primary_files,
    ) -> None:
        if type(initialize) is not bool:
            raise ValueError("prepared-primary initialization requires explicit boolean authority")
        expected = PrimaryAllocation.model_validate(
            {
                "allocation_id": allocation_id,
                "scope": scope,
                "control_record_bytes": str(limits.control_bytes),
            }
        )
        descriptor = primary_control_bytes(expected)
        if len(descriptor) > limits.control_bytes:
            raise PrimaryStateCapacity("prepared-primary allocation exceeds control admission")
        self.limits = limits
        self._mutex = threading.RLock()
        self._authoring = False
        self._closing_advancing = False
        self._root_candidates_pending = True
        self._files: PrimaryStorage = storage_factory(
            root, allocation_descriptor=descriptor, initialize=initialize
        )
        try:
            if initialize:
                self._write(self._files, "allocation", expected)
                self._write(self._files, "usage", PrimaryUsage())
            self.allocation = self._read(self._files, "allocation", PrimaryAllocation)
            if self.allocation != expected:
                raise PrimaryStateError(
                    "prepared-primary allocation, scope or control admission changed"
                )
            self._usage = self._read(self._files, "usage", PrimaryUsage)
            self._files.sync()
            self._recover(require_complete=False)
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
            self._recover(require_complete=False)
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
            self._recover(require_complete=False)
            return self._optional(self._files, "session", PrimarySession) is not None

    @property
    def custody_settled(self) -> bool:
        """Observe the validated complete local receipt/reclaim barrier without a scan."""
        with self._mutex:
            self._recover(require_complete=False)
            return self._optional(self._files, "custody-settled", PrimaryCustodySettled) is not None

    @property
    def usage(self) -> PrimaryUsage:
        with self._mutex:
            self._recover(require_complete=False)
            return self._usage

    @property
    def _root_reservation(self) -> int:
        return 14 * self.limits.control_bytes + max(
            self.limits.inventory_order_bytes, self._files.ordering_reserved_bytes()
        )

    def _admit_ordering(self) -> None:
        """A changed scratch grant cannot borrow existing member or inventory admission."""
        retained = (
            self._usage.control_bytes
            + self._usage.pending_bytes
            + self._usage.inventory_bytes
            + self._usage.inventory_reserved_bytes
        )
        if self.limits.root_reservation + retained > self.limits.total_bytes:
            raise PrimaryStateCapacity("owner inventory ordering exceeds allocation admission")

    def cleanup_ordering_step(self) -> bool:
        """Explicit execution cleanup; status and close never drain derived scratch."""
        with self._mutex:
            return self._files.ordering_cleanup_step()

    def _read(self, files: PrimaryStorage, name: str, model: type[_Control]) -> _Control:
        raw = files.read_control(name, byte_limit=self.limits.control_bytes)
        value = model.model_validate_json(raw)
        if primary_control_bytes(value) != raw:
            raise PrimaryStateError("prepared-primary control is not its exact current encoding")
        return value

    def _optional(self, files: PrimaryStorage, name: str, model: type[_Control]) -> _Control | None:
        try:
            return self._read(files, name, model)
        except FileNotFoundError:
            return None

    def _write(
        self,
        files: PrimaryStorage,
        name: str,
        document: BaseModel,
        *,
        expected: BaseModel | None = None,
    ) -> None:
        raw = primary_control_bytes(document)
        if expected is not None:
            # An uncertain prior response may have left this exact selected winner.
            # Reestablish its barrier before adopting it; never overwrite another body.
            try:
                previous = files.read_control(name, byte_limit=self.limits.control_bytes)
            except FileNotFoundError:
                previous = None
            if previous == raw:
                files.sync()
                return
        files.put_control(
            name,
            raw,
            byte_limit=self.limits.control_bytes,
            expected=None if expected is None else primary_control_bytes(expected),
        )

    def _install(self, files: PrimaryStorage, name: str, document: BaseModel) -> None:
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
                self._files.put_bytes(
                    reference.name,
                    (create_request,),
                    byte_limit=self.limits.control_bytes,
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
    def _member(self, artifact_id: str, *, create: bool) -> Iterator[PrimaryStorage]:
        key = hashlib.sha256(artifact_id.encode("utf-8")).hexdigest()

        def child(parent: PrimaryStorage, name: str) -> PrimaryStorage:
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

    def _control_record(
        self,
        files: PrimaryStorage,
        member: ArtifactMemberIdentityDocument,
        *,
        uncommitted: bool = False,
    ) -> PrimaryRecord | None:
        if not files.discard_candidates(("decision-accepted", "custody", "reclaimed")):
            raise PrimaryStateBusy("prepared-primary controls require bounded candidate cleanup")
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
        session = self.session
        if (
            prepared.member != member
            or prepared.session_sha256 != _identity(session)
            or prepared.completion_requirement_sha256 != session.completion_requirement_sha256
        ):
            raise PrimaryStateError("prepared-primary member or accepted session changed")
        if tuple(file.name for file in prepared.byte_files) != _BYTE_NAMES:
            raise PrimaryStateError("prepared-primary byte inventory is not the current format")
        identity = _identity(prepared)
        if custody is not None and (
            custody.prepared_sha256 != identity
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
        files.sync()
        return PrimaryRecord(prepared, accepted, custody)

    def _record(
        self,
        files: PrimaryStorage,
        member: ArtifactMemberIdentityDocument,
        *,
        uncommitted: bool = False,
    ) -> PrimaryRecord | None:
        result = self._control_record(files, member, uncommitted=uncommitted)
        if result is not None and not result.compact:
            try:
                for file in result.prepared.byte_files:
                    with files.verified(file.byte_file()):
                        pass
            except FileNotFoundError as exc:
                raise PrimaryStateError("prepared-primary committed bytes are missing") from exc
        return result

    def _load(
        self, member: ArtifactMemberIdentityDocument, *, verify_preimages: bool
    ) -> PrimaryRecord | None:
        with self._mutex:
            self._recover(require_complete=verify_preimages)
            with ExitStack() as opened:
                try:
                    files = opened.enter_context(
                        self._member(str(member.artifact_id), create=False)
                    )
                except FileNotFoundError:
                    return None
                if verify_preimages:
                    return self._record(files, member)
                return self._control_record(files, member)

    def load(self, member: ArtifactMemberIdentityDocument) -> PrimaryRecord | None:
        """Validate original pending bytes before preparation or byte-dependent replay."""
        return self._load(member, verify_preimages=True)

    def load_controls(self, member: ArtifactMemberIdentityDocument) -> PrimaryRecord | None:
        """Receipt/reclaim authority only; byte-dependent replay must use strict load."""
        return self._load(member, verify_preimages=False)

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
                - self._root_reservation
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
            selected_transition = transition.model_copy(update={"after": after})
            self._write(self._files, "transition", selected_transition, expected=transition)
            transition = selected_transition
            refs = tuple(
                _reference(name, raw) for name, raw in zip(_BYTE_NAMES, contents, strict=True)
            )
            prepared = PreparedPrimary.seal(
                **{
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
            if prepared.completion_requirement_sha256 != self.session.completion_requirement_sha256:
                raise PrimaryStateError("prepared-primary completion differs from accepted session")
            with self._member(str(member.artifact_id), create=True) as files:
                for ref, content in zip(refs, contents, strict=True):
                    files.put_bytes(
                        ref.name,
                        (content,),
                        byte_limit=grant,
                        expected=(ref.bytes, ref.sha256),
                    )
                self._write(files, "prepared", prepared)
            self._finish_transition(after, transition)
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

    def _finish_transition(self, after: PrimaryUsage, transition: PrimaryTransition) -> None:
        self._write(self._files, "usage", after, expected=self._usage)
        self._usage = after
        self._files.remove_control("transition", expected=primary_control_bytes(transition))

    def _remove_roles(self, files: PrimaryStorage) -> bool:
        for name in _BYTE_NAMES:
            if not files.remove_step(name):
                return False
        return True

    @staticmethod
    def _cleanup_pending(*, require_complete: bool) -> bool:
        if require_complete:
            raise PrimaryStateBusy("prepared-primary transition requires bounded cleanup")
        return False

    def _recover(self, *, require_complete: bool = True) -> bool:
        """Reconcile named controls and bounded selected-generation cleanup."""
        if self._authoring:
            raise PrimaryStateBusy("prepared-primary author cannot reenter its active transition")
        self._files.check_current()
        if self._root_candidates_pending:
            if not self._files.discard_candidates(_ROOT_NAMES):
                return self._cleanup_pending(require_complete=require_complete)
            self._root_candidates_pending = False
        transition = self._optional(self._files, "transition", PrimaryTransition)
        if transition is None:
            from riverhog_client._prepared_primary_closing import recover_closing

            if not recover_closing(self):
                return self._cleanup_pending(require_complete=require_complete)
            return True
        if self._usage not in (transition.before, transition.after):
            raise PrimaryStateError(
                "prepared-primary transition counter differs from retained state"
            )
        with self._member(str(transition.member.artifact_id), create=True) as files:
            if not files.discard_candidates(_MEMBER_NAMES):
                return self._cleanup_pending(require_complete=require_complete)
            record = self._record(
                files, transition.member, uncommitted=transition.operation == "prepare"
            )
            if transition.operation == "prepare":
                if record is None:
                    if self._usage != transition.before:
                        raise PrimaryStateError("prepared-primary committed manifest disappeared")
                    if not self._remove_roles(files):
                        return self._cleanup_pending(require_complete=require_complete)
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
                if not self._remove_roles(files):
                    return self._cleanup_pending(require_complete=require_complete)
                self._install(
                    files, "reclaimed", PrimaryReclaimed(prepared_sha256=_identity(record.prepared))
                )
                selected = transition.after
        self._finish_transition(selected, transition)
        from riverhog_client._prepared_primary_closing import recover_closing

        if not recover_closing(self):
            return self._cleanup_pending(require_complete=require_complete)
        return True

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
            custody = PrimaryCustody.from_receipt(
                self.session.collection_id, record.prepared, receipt
            )
            with self._member(str(record.prepared.member.artifact_id), create=False) as files:
                current = self._control_record(files, record.prepared.member)
                if current is None or current.prepared != record.prepared:
                    raise PrimaryStateError("prepared-primary custody has no matching manifest")
                self._install(files, "custody", custody)
            result = self.load_controls(record.prepared.member)
            assert result is not None
            return result

    def reclaim(self, member: ArtifactMemberIdentityDocument) -> bool:
        with self._mutex:
            if not self._recover(require_complete=False):
                return False
            record = self.load_controls(member)
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
                if not self._remove_roles(files):
                    return False
                self._install(files, "reclaimed", PrimaryReclaimed(prepared_sha256=identity))
                self._finish_transition(after, transition)
            return True
