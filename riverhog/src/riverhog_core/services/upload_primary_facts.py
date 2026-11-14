"""Admit immutable primary facts across independent original catalog turns."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from contextlib import closing
from dataclasses import asdict, dataclass
from pathlib import Path
from threading import Lock
from typing import Any
from uuid import uuid4
from weakref import ReferenceType, ref

from riverhog_archive_contracts import HistoryJournalAnchor
from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)
from riverhog_protocol.artifact_identity import ArtifactMemberIdentityDocument
from riverhog_protocol.collection_completion import CollectionCompletionRequirementDocument
from riverhog_protocol.collection_production_provenance import (
    COLLECTION_MEMBER_HISTORY_ROLE,
    COLLECTION_PRODUCTION_CONTRACT_ID,
    COMPLETION_REQUIREMENT_SCHEMA_ID,
    EXECUTION_OUTPUT_SCHEMA_ID,
    collection_production_contract,
)
from riverhog_protocol.errors import Conflict
from riverhog_protocol.provenance_transport import CollectionArtifactProvenanceBindingDocument
from riverhog_provenance import CanonicalCorpusValidator, JournalValidationCapacity
from riverhog_provenance.constants import PROVENANCE_JOURNAL_ENTRY_BYTES_MAX
from riverhog_provenance.corpus_selection import (
    HistoricalSelection,
    enable_selection,
    selection_revision,
    source_extent,
)
from riverhog_provenance_contracts import ContractCatalog
from sqlalchemy.orm import Session, SessionTransaction
from sqlite_admission import SqliteCapacity, SqliteCleanupPending

from riverhog_core.catalog_models import CollectionUploadRecord
from riverhog_core.primary_facts_workspace import (
    PrimaryFactsWorkspace,
    capture_root_identity,
)
from riverhog_core.provenance_binding import (
    VerifiedMemberBinding,
    verify_selected_member_binding,
)
from riverhog_core.services import primary_facts_jobs as jobs
from riverhog_core.services.collection_upload_authority import require_open_upload_writes
from riverhog_core.services.journal_storage_resources import JournalStorageResources
from riverhog_core.services.primary_facts_jobs import PrimaryFactsJob
from riverhog_core.services.primary_production import PrimaryProductionWork, install
from riverhog_core.services.upload_journal_result import (
    SealedJournalResultAdmission,
    open_upload_journal_result,
    prepare_upload_journal_result,
)
from riverhog_core.services.upload_journal_result_receiving import UploadJournalResultResources

_PROPERTIES = tuple(
    COLLECTION_PRODUCTION_CONTRACT_ID + "/" + suffix
    for suffix in ("producer", "record-manifest", "record-fragment")
)
_REQUIREMENT = COLLECTION_PRODUCTION_CONTRACT_ID + "/required-completion"
_OUTPUT = COLLECTION_PRODUCTION_CONTRACT_ID + "/execution-output"


def _turn_declaration() -> Declaration:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    return Declaration(
        id="riverhog.primary-member-turn.growth/v1",
        authority="riverhog",
        subjects=("riverhog_core.services.upload_primary_facts.UploadPrimaryFactsOwner.verify",),
        scope="one primary preparation, received capacity yield or member-check turn",
        starts_at="receive the original catalog turn's current construction and source",
        ends_at="commit one preparation atom, yield received capacity or return one member's facts",
        dimensions=(
            Dimension("source_frames", COUNT, "Accepted frames in this distinct complete source."),
            Dimension("source_octets", OCTETS, "Wire extent of this distinct complete source."),
        ),
        bounds=(
            Bound("source_frame_reads", COUNT, "upper", count(1), "One accepted Native frame."),
            Bound(
                "source_frame_octets",
                OCTETS,
                "upper",
                Expression("constant", value=PROVENANCE_JOURNAL_ENTRY_BYTES_MAX + 2, unit=OCTETS),
                "One existing Native entry limit plus its RS and LF framing bytes.",
            ),
            Bound("producer_steps", COUNT, "upper", count(1), "One producer work atom."),
            Bound("member_checks", COUNT, "upper", count(1), "One exact selected member."),
            Bound("native_openings", COUNT, "upper", count(1), "One fresh retained result."),
            Bound("source_admissions", COUNT, "upper", count(1), "One detached source admission."),
            Bound(
                "native_validation_advances",
                COUNT,
                "equal",
                count(0),
                "Preparation consumes accepted Native facts without semantic replay.",
            ),
            Bound(
                "source_payload_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "No catalog original/captured journal payload accessor is used.",
            ),
        ),
        conditions=(
            "The caller owns its current finalizer claim, cursor and catalog write transaction.",
            "Preparation and member checks require the exact live source/job/workspace/catalog "
            "admission and construction epoch; a capacity yield grants none.",
            "Native frame, selector and producer atoms retain their own byte admission.",
        ),
        invariants=(
            "A prepared source advances monotonically once per distinct source in this owner.",
            "Distinct source and exact selected-context preparation never repeats per member.",
            "Every actual result opening receives through the original catalog turn.",
            "A completed preparation is immutable input, never a previous permission.",
            "No member cursor advances on incomplete preparation or a failed member check.",
        ),
        limitations=(
            "This profile counts caller atoms, not complete transitive SQL or CPU costs. "
            "Native source indexing, keyed selectors, sealed admission, producer validation "
            "and original receiving have separately observed source-owned profiles.",
            "Bounded source frames retain Native's existing eight-MiB entry admission; "
            "no aggregate Python heap or process RSS ceiling is claimed.",
            "A real owner restart rebuilds used source facts; failed atoms and retries are "
            "additional observed work, not completion or a durable checkpoint promise.",
            "Member history traversal and the complete synchronous current raw terminal "
            "barrier remain separate operations.",
        ),
    )


def _one(db: sqlite3.Connection, query: str, values: tuple[Any, ...] = ()) -> Any:
    with closing(db.execute(query, values)) as rows:
        return rows.fetchone()


def _write(db: sqlite3.Connection, query: str, values: tuple[Any, ...] = ()) -> None:
    with closing(db.execute(query, values)):
        pass


def _first(rows: Iterator[dict[str, Any]]) -> dict[str, Any] | None:
    try:
        return next(rows, None)
    finally:
        finish = getattr(rows, "close", None)
        if finish is not None:
            finish()


def _pair(rows: Iterator[dict[str, Any]]) -> list[dict[str, Any]]:
    """Two matching atoms suffice to reject a non-unique declaration."""
    try:
        first = next(rows, None)
        if first is None:
            return []
        second = next(rows, None)
        return [first] if second is None else [first, second]
    finally:
        finish = getattr(rows, "close", None)
        if finish is not None:
            finish()


@dataclass(frozen=True, slots=True)
class VerifiedPrimaryFacts:
    verified: VerifiedMemberBinding
    completion_output_id: str | None


class _PrimaryIndex:
    """Two original storage scopes beneath one registered, exclusively leased namespace."""

    def __init__(
        self,
        storage: JournalStorageResources,
        job: PrimaryFactsJob,
        workspace: PrimaryFactsWorkspace,
    ) -> None:
        self.job, self.workspace = job, workspace
        self.construction, self.epoch = job.construction, job.epoch
        self.corpus_scope = storage.corpus()
        self.memo_scope = storage.journal()
        self.corpus: CanonicalCorpusValidator | None = None
        self.corpus_connection: sqlite3.Connection | None = None
        self.memo: sqlite3.Connection | None = None
        self.corpus_directory = self.memo_directory = workspace.path
        self.started = self.ready = self.closing = False
        self.source_admission_started = False
        self.namespace_started = False
        self._pending_transaction: ReferenceType[SessionTransaction] | None = None

    def await_commit(self, session: Session) -> None:
        transaction = session.get_transaction()
        if transaction is None:
            raise RuntimeError("primary allocation requires its original catalog transaction")
        # A weak identity excludes repeated calls in the same transaction while
        # retaining neither its Session nor its authority across catalog turns.
        self._pending_transaction = ref(transaction)

    def awaiting_commit(self, session: Session) -> bool:
        previous = None if self._pending_transaction is None else self._pending_transaction()
        return previous is not None and previous is session.get_transaction()

    def require_original_job(self, job: PrimaryFactsJob) -> None:
        if any(
            getattr(self.job, field) != getattr(job, field)
            for field in (
                "workspace_id",
                "collection_id",
                "construction",
                "epoch",
                "owner_id",
                "root_identity",
            )
        ):
            raise Conflict("primary preparation changed its registered original identity")
        if self.started and job.namespace_identity != self.job.namespace_identity:
            raise Conflict("primary preparation changed its committed namespace pin")
        if (
            job.namespace_identity is not None
            and self.workspace.custody is not None
            and job.namespace_identity != self.workspace.namespace_identity
        ):
            raise Conflict("primary preparation namespace pin names another original")

    def open(self) -> None:
        self.workspace.require_current(self.job)
        if self.started:
            if not self.ready or self.closing:
                raise JournalValidationCapacity("original primary preparation needs receiving")
            return
        self.started = True
        # Retain both passive scopes before their first reservation or file effect.
        self.corpus_scope.scope.reserve()
        self.memo_scope.scope.reserve()

        def connect(database: Path) -> sqlite3.Connection:
            if database != self.workspace.path / "corpus.sqlite3":
                raise Conflict("primary corpus selected another registered database path")
            self.corpus_scope.acquire_guard(database, create=True)
            connection = self.corpus_scope.scope.connect(
                database.as_uri() + "?mode=rw", uri=True, check_same_thread=False
            )
            self.corpus_connection = connection
            self._storage_profile(connection)
            return connection

        def retain(corpus: CanonicalCorpusValidator) -> None:
            self.corpus = corpus

        self.corpus_scope.begin()
        try:
            self.corpus = CanonicalCorpusValidator(
                directory=self.workspace.path,
                connection_factory=connect,
                on_storage_open=retain,
            )
            enable_selection(self.corpus)
            assert self.corpus_connection is not None
            self.corpus_connection.commit()
            self.corpus_scope.sync_directory(self.workspace.path)
            self.corpus_scope.ready()
        finally:
            self.corpus_scope.end()
        self.memo_scope.begin()
        try:
            database = self.workspace.path / "checks.sqlite3"
            self.memo_scope.create_exclusive(database)
            self.memo = self.memo_scope.scope.connect(
                database.as_uri() + "?mode=rw", uri=True, check_same_thread=False
            )
            self._storage_profile(self.memo)
            for statement in (
                "CREATE TABLE sources(journal TEXT PRIMARY KEY, admission BLOB NOT NULL, "
                "frames INTEGER NOT NULL, prefix_bytes TEXT NOT NULL, complete INTEGER NOT NULL)",
                "CREATE TABLE checks(selection BLOB PRIMARY KEY, cursor TEXT, context BLOB)",
                "CREATE TABLE outputs(selection BLOB, state TEXT, revision BLOB, output TEXT, "
                "PRIMARY KEY(selection,state,revision)) WITHOUT ROWID",
            ):
                _write(self.memo, statement)
            install(self.memo)
            self.memo.commit()
            self.memo_scope.sync_directory(self.workspace.path)
            self.memo_scope.ready()
        finally:
            self.memo_scope.end()
        self.ready = True

    @staticmethod
    def _storage_profile(connection: sqlite3.Connection) -> None:
        if _one(connection, "PRAGMA journal_mode=DELETE") != ("delete",):
            raise Conflict("primary preparation SQLite journal profile changed")
        _write(connection, "PRAGMA synchronous=FULL")
        _write(connection, "PRAGMA fullfsync=ON")

    def close(self) -> None:
        """Do not release a slot until both original connection receipts exist."""
        self.closing = True
        if self.corpus is not None:
            self.corpus.close()
        if not self.corpus_scope.close_step():
            raise BlockingIOError("original primary source index has not received")
        if self.memo is not None:
            self.memo.close()
        if not self.memo_scope.close_step():
            raise BlockingIOError("original primary memo index has not received")
        # Namespace receiving is strictly after both actual database/file
        # constructions. Registered bytes belong to separate bounded retirement.
        self.workspace.close()
        if not self.workspace.received:
            raise BlockingIOError("original primary namespace lease has not received")


class UploadPrimaryFactsOwner:
    """Keep preparation facts, never a previous member turn's permission.

    An owner is scoped to the original construction and epoch, not its changing
    workflow claims. Each turn opens the exact currently sealed Native result,
    then closes that original through the catalog receiver. Only detached facts
    and app-owned disk checkpoints cross turns. No terminal corpus is borrowed.

    Open collection owners are explicitly limited. Their source/member extent
    is not held in Python containers. Process restart reconstructs each used
    source once in bounded atoms; this disposable preparation is not a durable
    checkpoint or a whole-finalizer restart guarantee.
    """

    def __init__(self, *, storage: JournalStorageResources, root: Path, max_owners: int) -> None:
        if type(max_owners) is not int or max_owners < 1:
            raise ValueError("primary preparation requires positive owner admission")
        self._storage, self._root, self._max_owners = storage, Path(root), max_owners
        self._owner_id = uuid4().hex
        self._owners: dict[int, _PrimaryIndex] = {}
        self._lock = Lock()
        self._closing = False
        self.metrics = dict.fromkeys(
            (
                "turns",
                "source_admissions",
                "native_openings",
                "source_frame_reads",
                "source_frame_octets",
                "producer_rows",
                "producer_steps",
                "completion_rows",
                "member_checks",
                "capacity_deferrals",
            ),
            0,
        )

    @property
    def pending(self) -> bool:
        return bool(self._owners)

    @property
    def owner_id(self) -> str:
        return self._owner_id

    def _registered_index(
        self, session: Session, upload: CollectionUploadRecord
    ) -> _PrimaryIndex | None:
        """Read committed allocation and pin stages before either database effect."""
        control = require_open_upload_writes(session, upload)
        collection_id = upload.collection_id
        index = self._owners.get(collection_id)
        job = jobs.find_pending(session, collection_id)
        if job is None:
            if index is not None:
                if index.namespace_started:
                    raise Conflict("primary preparation lost its registered namespace")
                # An unacknowledged registration can roll back. No filesystem
                # work was permitted in that original metadata-only turn.
                index.close()
                del self._owners[collection_id]
            if len(self._owners) >= self._max_owners:
                self.metrics["capacity_deferrals"] += 1
                return None
            job = jobs.register(
                session,
                collection_id=collection_id,
                construction=control.construction_json,
                epoch=control.epoch,
                owner_id=self._owner_id,
                root_identity=capture_root_identity(self._root),
            )
            index = _PrimaryIndex(self._storage, job, PrimaryFactsWorkspace(self._root, job))
            index.await_commit(session)
            self._owners[collection_id] = index
            return None
        if index is not None:
            if index.job.workspace_id == job.workspace_id:
                index.require_original_job(job)
            elif not index.namespace_started:
                # A foreign owner may finish a passive allocation before this
                # process next acquires it. No namespace original remains here.
                index.close()
                del self._owners[collection_id]
                index = None
            else:
                raise Conflict("primary preparation registration changed its original namespace")
        if job.owner_id != self._owner_id:
            # Restart retirement must prove exclusive quiescence; a foreign
            # live allocation is never adopted as a fresh preparation cache.
            return None
        if job.state not in jobs.LIVE_STATES:
            if index is not None:
                index.close()
                del self._owners[collection_id]
            return None
        if (job.construction, job.epoch) != (control.construction_json, control.epoch):
            if index is not None:
                index.close()
                del self._owners[collection_id]
            self._retire_job(session, job)
            return None
        if index is None:
            if job.state == "pinned":
                # This process removes an original only after positive receipt.
                # A lost catalog retirement acknowledgement leaves the pinned
                # row, not permission to reconstruct its disposed handles.
                self._retire_job(session, job)
                return None
            if len(self._owners) >= self._max_owners:
                self.metrics["capacity_deferrals"] += 1
                return None
            index = _PrimaryIndex(self._storage, job, PrimaryFactsWorkspace(self._root, job))
            index.await_commit(session)
            self._owners[collection_id] = index
            return None
        if index.closing:
            index.close()
            self._retire_job(session, job)
            del self._owners[collection_id]
            return None
        if index.awaiting_commit(session):
            return None
        if job.state == "allocated":
            if index.started:
                raise Conflict("primary preparation lost its committed namespace pin")
            index.namespace_started = True
            try:
                opened = index.workspace.open(lambda: jobs.current(session, job))
            except BaseException:
                # An uncertain namespace constructor is a receiving-only
                # original. A retry cannot execute its acquisition again.
                index.closing = True
                raise
            if not opened:
                if not index.workspace.received:
                    raise BlockingIOError("deferred primary namespace has not received")
                index.namespace_started = False
                index.workspace = PrimaryFactsWorkspace(self._root, job)
                self.metrics["capacity_deferrals"] += 1
                return None
            successor = jobs.pin(session, job, index.workspace.namespace_identity)
            if successor is None:
                raise Conflict("primary preparation namespace pin lost its original allocation")
            index.job = successor
            index.await_commit(session)
            return None
        # This is a fresh read after the pin turn. Neither the flushed successor
        # nor the retained local identity is treated as catalog commit evidence.
        index.workspace.require_current(job)
        index.job = job
        return index

    @staticmethod
    def _retire_job(session: Session, job: PrimaryFactsJob) -> None:
        if jobs.retire(session, job) is None:
            raise Conflict("primary preparation retirement lost its original catalog state")

    @declare_growth(_turn_declaration())
    def verify(
        self,
        session: Session,
        upload: CollectionUploadRecord,
        *,
        member: ArtifactMemberIdentityDocument,
        binding: CollectionArtifactProvenanceBindingDocument,
        result_resources: UploadJournalResultResources,
        catalog: ContractCatalog,
    ) -> VerifiedPrimaryFacts | None:
        """Prepare, yield admission, or verify one currently admitted member.

        ``None`` finishes this catalog scheduling turn without advancing its
        member cursor. A received capacity refusal can make no preparation
        progress: retaining that turn's construction reservation would prevent
        another prepared collection from finishing and releasing its slot.
        Actual unreceived originals and failures after source admission still
        raise, retaining their exact catalog receiver for resumption.
        """
        if not self._lock.acquire(blocking=False):
            return None
        try:
            if self._closing:
                raise JournalValidationCapacity("primary preparation owner is closing")
            index = self._registered_index(session, upload)
            if index is None:
                return None
            opening = not index.started
            try:
                index.open()
            except BaseException as error:
                # No preparation facts exist until opening is ready. A refused
                # reservation can retry only after these exact originals receive.
                index.close()
                self._retire_job(session, index.job)
                del self._owners[upload.collection_id]
                if (
                    opening
                    and isinstance(error, (SqliteCapacity, JournalValidationCapacity))
                    and not result_resources.pending
                ):
                    self.metrics["capacity_deferrals"] += 1
                    return None
                raise
            self.metrics["turns"] += 1
            try:
                with index.corpus_scope.execution():
                    return self._verify(
                        index,
                        session,
                        upload,
                        member=member,
                        binding=binding,
                        result_resources=result_resources,
                        catalog=catalog,
                    )
            except BaseException as error:
                # A refused first Native opening must not let two empty index
                # connections consume the headroom needed by prepared owners.
                # Facts are never evicted, and uncertain closes keep this slot.
                # This conservative original-local marker needs no additional
                # cursor or operation grant after a capacity refusal. Once a
                # source pin starts, even its uncertain outcome stays retained.
                if not index.source_admission_started:
                    index.close()
                    self._retire_job(session, index.job)
                    del self._owners[upload.collection_id]
                    if (
                        isinstance(error, (SqliteCapacity, JournalValidationCapacity))
                        and not result_resources.pending
                    ):
                        self.metrics["capacity_deferrals"] += 1
                        return None
                raise
        finally:
            self._lock.release()

    def _verify(
        self,
        index: _PrimaryIndex,
        session: Session,
        upload: CollectionUploadRecord,
        *,
        member: ArtifactMemberIdentityDocument,
        binding: CollectionArtifactProvenanceBindingDocument,
        result_resources: UploadJournalResultResources,
        catalog: ContractCatalog,
    ) -> VerifiedPrimaryFacts | None:
        assert index.memo is not None and index.corpus is not None
        assert index.corpus_connection is not None
        db, corpus = index.memo, index.corpus
        admission = prepare_upload_journal_result(
            session, upload.collection_id, binding.journal.journal_id, catalog=catalog
        )
        self.metrics["source_admissions"] += 1
        identity = canonical_json_bytes(json.loads(json.dumps(asdict(admission))))
        source = _one(
            db,
            "SELECT admission,frames,prefix_bytes,complete FROM sources WHERE journal=?",
            (binding.journal.journal_id,),
        )
        if source is not None and source[0] != identity:
            raise Conflict("prepared primary source or validation job changed")
        original = result_resources.retain()
        try:
            summary = open_upload_journal_result(
                session,
                self._root,
                upload.collection_id,
                binding.journal.journal_id,
                catalog=catalog,
                receiver=original,
            )
            self.metrics["native_openings"] += 1
            anchor = HistoryJournalAnchor.from_mapping(binding.journal.model_dump(mode="json"))
            try:
                selected_anchor = summary.anchor_at(anchor.through_sequence)
            except IndexError as error:
                raise Conflict("primary member prefix has no accepted Native frame") from error
            if selected_anchor != anchor.to_mapping():
                raise Conflict("primary member prefix differs from its accepted Native source")
            if source is None:
                # The independent Native index can commit before this app's
                # progress reply. Pin the exact admission first so such facts
                # can never be relabeled to a replacement source or job.
                index.source_admission_started = True
                _write(
                    db,
                    "INSERT INTO sources VALUES(?,?,0,'0',0)",
                    (binding.journal.journal_id, identity),
                )
                self._current(admission, session, index)
                db.commit()
                source = (identity, 0, "0", 0)
            if not source[3]:
                before = source_extent(corpus, anchor.journal_id)
                before_frames, before_octets = (0, 0) if before is None else before
                if before_frames < source[1] or before_octets < int(source[2]):
                    raise Conflict("primary preparation lost its accepted source checkpoint")
                progress = corpus.advance_source(summary, frame_budget=1)
                through = summary.anchor_at(progress.accepted_frames - 1)
                frames, octets = progress.accepted_frames, int(through["prefix_bytes"])
                self.metrics["source_frame_reads"] += frames - before_frames
                self.metrics["source_frame_octets"] += octets - before_octets
                _write(
                    db,
                    "INSERT INTO sources VALUES(?,?,?,?,?) ON CONFLICT(journal) DO UPDATE SET "
                    "frames=excluded.frames,prefix_bytes=excluded.prefix_bytes,complete=excluded.complete",
                    (
                        binding.journal.journal_id,
                        identity,
                        frames,
                        str(octets),
                        int(progress.complete),
                    ),
                )
                self._current(admission, session, index)
                index.corpus_connection.commit()
                db.commit()
                # Preparation progress is separate from a member acceptance,
                # even when this happened to be the source's final frame.
                return None
            corpus.require_source_endpoint(HistoryJournalAnchor.from_mapping(admission.anchor))
            selected = HistoricalSelection(
                corpus, anchor, byte_budget=PROVENANCE_JOURNAL_ENTRY_BYTES_MAX
            )
            key = canonical_json_bytes(
                {
                    "journal": anchor.journal_id,
                    "context": upload.delivery_context_id,
                    "requirement": upload.completion_requirement_json,
                    "revisions": list(
                        selection_revision(
                            corpus,
                            anchor,
                            kind="extension",
                            subject=self._reference(upload.delivery_context_id, "context"),
                            predicates=(*_PROPERTIES, _REQUIREMENT),
                        )
                    ),
                }
            )
            check = _one(db, "SELECT cursor,context FROM checks WHERE selection=?", (key,))
            if check is None:
                _write(db, "INSERT INTO checks VALUES(?,NULL,NULL)", (key,))
                check = (None, None)
            production = PrimaryProductionWork(db, key.decode("utf-8"))
            if not production.complete:
                row = (
                    _first(
                        selected.extensions(
                            subject=self._reference(upload.delivery_context_id, "context"),
                            properties=_PROPERTIES,
                            after=check[0],
                        )
                    )
                    if production.accepting
                    else None
                )
                production.advance(row)
                self.metrics["producer_steps"] += 1
                if row is not None:
                    self.metrics["producer_rows"] += 1
                    _write(
                        db,
                        "UPDATE checks SET cursor=? WHERE selection=?",
                        (row["assertion_id"], key),
                    )
                self._current(admission, session, index)
                db.commit()
                return None
            if check[1] is None:
                context = self._completion_context(selected, upload)
                encoded_context = json.dumps(context)
                _write(db, "UPDATE checks SET context=? WHERE selection=?", (encoded_context, key))
            else:
                context = json.loads(check[1])

            def member_history(artifact_id: str) -> bool:
                return (
                    _first(
                        selected.journal_subjects(
                            artifact=self._reference(artifact_id, "artifact"),
                            role=COLLECTION_MEMBER_HISTORY_ROLE,
                        )
                    )
                    is not None
                )

            verified = verify_selected_member_binding(
                member=member,
                binding=binding,
                journal_id=anchor.journal_id,
                anchor=anchor.to_mapping(),
                objects=selected.objects,
                has_member_history=member_history,
                delivery_context_id=upload.delivery_context_id,
            )
            association = selected.objects[binding.delivery_association_id]
            state_id = association["state"]["object_id"]
            output = self._completion_output(db, key, selected, state_id, context)
            self._current(admission, session, index)
            db.commit()
            self.metrics["member_checks"] += 1
            return VerifiedPrimaryFacts(verified, output)
        except BaseException:
            db.rollback()
            index.corpus_connection.rollback()
            raise
        finally:
            result_resources.receive(original)

    @staticmethod
    def _current(
        admission: SealedJournalResultAdmission, session: Session, index: _PrimaryIndex
    ) -> None:
        index.workspace.require_current(index.job)
        if not jobs.current(session, index.job) or not admission.verify_current(session):
            raise Conflict("primary member source or validation job changed")

    @staticmethod
    def _reference(identity: str, kind: str) -> dict[str, str]:
        return {"scope": "local", "object_id": identity, "object_type": kind}

    def _completion_context(
        self, selection: HistoricalSelection, upload: CollectionUploadRecord
    ) -> str | None:
        rows = _pair(
            selection.extensions(
                subject=self._reference(upload.delivery_context_id, "context"),
                properties=(_REQUIREMENT,),
            )
        )
        self.metrics["completion_rows"] += len(rows)
        requirement = (
            None
            if upload.completion_requirement_json is None
            else CollectionCompletionRequirementDocument.model_validate_json(
                upload.completion_requirement_json
            )
        )
        if requirement is None:
            if rows:
                raise ValueError("primary declares an unaccepted completion obligation")
            return None
        if len(rows) != 1:
            raise ValueError("primary omits or substitutes the accepted completion requirement")
        value = rows[0]["value"]
        contract = collection_production_contract()
        if value["type"] != "json":
            raise ValueError("execution declaration is not a profile value")
        profile = value["value"]
        if profile["profile"] != {
            "contract_id": contract.contract_id,
            "contract_sha256": contract.contract_sha256,
            "schema_id": COMPLETION_REQUIREMENT_SCHEMA_ID,
        }:
            raise ValueError("execution declaration profile differs")
        if profile["data"] != requirement.model_dump(mode="json"):
            raise ValueError("primary omits or substitutes the accepted completion requirement")
        # The key pins the exact accepted requirement and selected source. Keep
        # only its execution identity after this complete context check; large
        # selected context data must not be decoded again for another member.
        return requirement.execution_id

    def _completion_output(
        self,
        db: sqlite3.Connection,
        key: bytes,
        selection: HistoricalSelection,
        state_id: str,
        context: str | None,
    ) -> str | None:
        revision = canonical_json_bytes(
            list(
                selection_revision(
                    selection.corpus,
                    selection.anchor,
                    kind="extension",
                    subject=self._reference(state_id, "state"),
                    predicates=(_OUTPUT,),
                )
            )
        )
        existing = _one(
            db,
            "SELECT output FROM outputs WHERE selection=? AND state=? AND revision=?",
            (key, state_id, revision),
        )
        if existing is not None:
            return existing[0]  # type: ignore[no-any-return]
        outputs = _pair(
            selection.extensions(subject=self._reference(state_id, "state"), properties=(_OUTPUT,))
        )
        self.metrics["completion_rows"] += len(outputs)
        if context is None:
            if outputs:
                raise ValueError("primary declares an unaccepted completion obligation")
            result = None
        else:
            if len(outputs) != 1:
                raise ValueError("primary lacks its exact accepted execution output key")
            value = outputs[0]["value"]
            if value["type"] != "json":
                raise ValueError("execution declaration is not a profile value")
            profile = value["value"]
            contract = collection_production_contract()
            if profile["profile"] != {
                "contract_id": contract.contract_id,
                "contract_sha256": contract.contract_sha256,
                "schema_id": EXECUTION_OUTPUT_SCHEMA_ID,
            }:
                raise ValueError("execution declaration profile differs")
            if profile["data"]["execution_id"] != context:
                raise ValueError("primary lacks its exact accepted execution output key")
            result = str(profile["data"]["output_id"])
        _write(db, "INSERT INTO outputs VALUES(?,?,?,?)", (key, state_id, revision, result))
        return result

    def retire(self, session: Session, collection_id: int) -> bool:
        """Receive local originals, then stage irreversible bounded namespace retirement."""
        if not self._lock.acquire(blocking=False):
            return False
        try:
            original = self._owners.get(collection_id)
            job = jobs.find_pending(session, collection_id)
            if original is not None:
                if job is None or job.workspace_id != original.job.workspace_id:
                    raise Conflict("primary retirement lost its registered original namespace")
                original.require_original_job(job)
                try:
                    original.close()
                except (OSError, SqliteCleanupPending, JournalValidationCapacity):
                    return False
                del self._owners[collection_id]
            if job is None or job.state in jobs.RETIRED_STATES:
                return True
            if job.owner_id != self._owner_id:
                return False
            self._retire_job(session, job)
            return True
        finally:
            self._lock.release()

    def close(self) -> None:
        if not self._lock.acquire(blocking=False):
            raise JournalValidationCapacity("primary preparation is still executing")
        try:
            self._closing = True
            while self._owners:
                key = next(iter(self._owners))
                self._owners[key].close()
                del self._owners[key]
        finally:
            self._lock.release()
