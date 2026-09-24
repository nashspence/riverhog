"""NON-AUTHORITATIVE #869 server-side transactional handoff prototype.

All durable writes use the caller's Riverhog catalog Session. There is no
second database, automatic schema upgrade, network delivery, or copy executor.
See README.md before adapting this reference into the application.
"""
from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol

from sqlalchemy import (
    BigInteger, Boolean, CheckConstraint, Column, ForeignKey, Index, Integer,
    MetaData, String, Table, Text, insert, select, update,
)
from sqlalchemy.orm import Session, sessionmaker

metadata = MetaData()
operations = Table(
    "upload_copy_operations", metadata,
    Column("collection_id", BigInteger, primary_key=True, autoincrement=False),
    Column("initiated_by_app", Text, nullable=False),
    Column("initiated_by_key_id", Text, nullable=False),
    Column("base_identity", String(64), nullable=False),
    Column("creation_identity", String(64), nullable=False),
    Column("choices_json", Text, nullable=False),
    Column("tags_json", Text, nullable=False),
    Column("event_context_json", Text, nullable=False),
    Column("state", String(16), nullable=False),
    Column("accepted_at", Text, nullable=False),
    Column("published_at", Text),
    Column("canceled_at", Text),
    CheckConstraint("state IN ('accepted','published','canceled')", name="ck_upload_copy_operation_state"),
    CheckConstraint(
        "(state = 'accepted' AND published_at IS NULL AND canceled_at IS NULL) OR "
        "(state = 'published' AND published_at IS NOT NULL AND canceled_at IS NULL) OR "
        "(state = 'canceled' AND published_at IS NULL AND canceled_at IS NOT NULL)",
        name="ck_upload_copy_operation_times",
    ),
)
intents = Table(
    "upload_copy_intents", metadata,
    Column("collection_id", BigInteger, ForeignKey("upload_copy_operations.collection_id", ondelete="RESTRICT"), primary_key=True),
    Column("destination_store", Text, primary_key=True),
    Column("state", String(16), nullable=False),
    Column("attempts", Integer, nullable=False, default=0),
    Column("next_attempt_at", Text),
    Column("failure_code", Text),
    Column("finished_at", Text),
    Column("job_receipt_json", Text),
    CheckConstraint("state IN ('accepted','pending','handed_off','failed','canceled')", name="ck_upload_copy_intent_state"),
    CheckConstraint("attempts >= 0", name="ck_upload_copy_intent_attempts"),
    CheckConstraint(
        "(state = 'handed_off' AND job_receipt_json IS NOT NULL) OR "
        "(state <> 'handed_off' AND job_receipt_json IS NULL)",
        name="ck_upload_copy_intent_receipt",
    ),
    CheckConstraint(
        "(state IN ('handed_off','failed','canceled') AND finished_at IS NOT NULL AND next_attempt_at IS NULL) OR "
        "(state = 'accepted' AND finished_at IS NULL AND next_attempt_at IS NULL) OR "
        "(state = 'pending' AND finished_at IS NULL AND next_attempt_at IS NOT NULL)",
        name="ck_upload_copy_intent_times",
    ),
    CheckConstraint("state <> 'failed' OR failure_code IS NOT NULL", name="ck_upload_copy_intent_failure"),
)
Index("ix_upload_copy_intents_due", intents.c.state, intents.c.next_attempt_at, intents.c.collection_id)


class InvalidRequest(ValueError):
    pass


class IdentityConflict(ValueError):
    pass


class AccessDenied(PermissionError):
    pass


class TerminalHandoffError(Exception):
    """A safe, bounded classification; never persist arbitrary exception text."""
    CODES = frozenset({
        "authorization_denied", "configuration_changed", "publication_missing",
        "destination_already_present", "source_unavailable",
    })

    def __init__(self, code: str) -> None:
        if code not in self.CODES:
            raise ValueError("unrecognized terminal handoff code")
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class Actor:
    app: str
    key_id: str

    def validate(self) -> None:
        # No anonymous/system-principal escape hatch in the prototype.
        if not isinstance(self.app, str) or not self.app or not isinstance(self.key_id, str) or not self.key_id:
            raise AccessDenied("an attributable application key is required")


@dataclass(frozen=True)
class Binding:
    name: str
    identity: str


@dataclass(frozen=True)
class Choices:
    archive: Binding
    use_cache: bool
    copy_to: tuple[Binding, ...]

    def document(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def parse(cls, text: str) -> Choices:
        data = json.loads(text)
        return cls(Binding(**data["archive"]), data["use_cache"], tuple(Binding(**v) for v in data["copy_to"]))


@dataclass(frozen=True)
class JobReceipt:
    collection_id: int
    destination_store: str
    # False means an ordinary job already existed: do not steal attribution.
    created: bool


class Stores(Protocol):
    def default_archive(self) -> str: ...
    def default_use_cache(self, archive_store: str) -> bool: ...
    def binding(self, name: str) -> Binding: ...


class Authority(Protocol):
    def require_accept(self, session: Session, actor: Actor, tags: tuple[str, ...], *, copies: bool) -> None:
        """Load current key/grants, check create, plus archive-manage for copies."""
        ...

    def require_handoff(self, session: Session, actor: Actor, collection_id: int) -> None:
        """Load current original key/grants and check ordinary archive authority."""
        ...


class Catalog(Protocol):
    def require_publication(self, session: Session, collection_id: int, actor: Actor, archive_store: str) -> None:
        """Require this upload's published collection AND its complete archive copy."""
        ...


class Jobs(Protocol):
    def ensure(self, session: Session, collection_id: int, destination_store: str, actor: Actor,
               event_context: dict[str, Any], now: str) -> JobReceipt:
        """Insert ordinary job + lifecycle event in THIS Session, or observe it.

        Must not commit, open another transaction/Session, perform payload I/O,
        or restart an existing terminal job. Unknown exceptions roll back.
        """
        ...

    def observe(self, session: Session, receipt: JobReceipt) -> dict[str, Any] | None: ...


def canonical(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    except (ValueError, TypeError) as exc:
        raise InvalidRequest("operation metadata must be JSON") from exc


def utc_now() -> str:
    seconds, nanos = divmod(time.time_ns(), 1_000_000_000)
    return datetime.fromtimestamp(seconds, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S") + f".{nanos:09d}Z"


def after_seconds(now: str, seconds: int) -> str:
    base = datetime.strptime(now[:19], "%Y-%m-%dT%H:%M:%S") + timedelta(seconds=seconds)
    return base.strftime("%Y-%m-%dT%H:%M:%S") + now[19:]


def names(values: Sequence[str]) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)) or not isinstance(values, Sequence) or len(values) > 64:
        raise InvalidRequest("copy_to must contain at most 64 store names")
    for value in values:
        if not isinstance(value, str) or not value or value != value.strip() or len(value) > 255:
            raise InvalidRequest("invalid archive-store name")
    return tuple(sorted(set(values)))


class Handoff:
    def __init__(self, *, stores: Stores, authority: Authority, catalog: Catalog, jobs: Jobs,
                 clock: Callable[[], str] = utc_now) -> None:
        self.stores, self.authority, self.catalog, self.jobs = stores, authority, catalog, jobs
        self.clock = clock

    @staticmethod
    def _operation(session: Session, collection_id: int) -> Mapping[str, Any]:
        row = session.execute(select(operations).where(operations.c.collection_id == collection_id).with_for_update()).mappings().first()
        if row is None:
            raise InvalidRequest("copy operation not found")
        return row

    def accept(self, session: Session, *, collection_id: int, actor: Actor, base_identity: str,
               archive_store: str | None = None, use_cache: bool | None = None,
               copy_to: Sequence[str] | None = None, tags: Sequence[str] = (),
               event_context: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """Call in upload-creation transaction BEFORE accepting payload.

        base_identity is the identity of non-placement upload semantics. Omitted
        choices on a retry retain their accepted values, not today's defaults.
        The upload's creation-identity calculation must include the returned
        creation_identity; see the integration notes (not wired by this branch).
        """
        actor.validate()
        if type(collection_id) is not int or not 0 < collection_id < 2**63:
            raise InvalidRequest("invalid collection id")
        if not isinstance(base_identity, str) or len(base_identity) != 64 or any(c not in "0123456789abcdef" for c in base_identity):
            raise InvalidRequest("base_identity must be a SHA-256 hex digest")
        if use_cache is not None and type(use_cache) is not bool:
            raise InvalidRequest("use_cache must be a boolean")
        if isinstance(tags, (str, bytes)) or any(not isinstance(t, str) or not t for t in tags):
            raise InvalidRequest("invalid tags")
        normalized_tags = tuple(sorted(set(tags)))
        if event_context is not None and not isinstance(event_context, Mapping):
            raise InvalidRequest("event_context must be a mapping")
        context_json = canonical(dict(event_context or {}))
        if len(context_json.encode("utf-8")) > 8192:
            raise InvalidRequest("event_context exceeds prototype bound")
        old = session.execute(select(operations).where(operations.c.collection_id == collection_id).with_for_update()).mappings().first()
        prior = Choices.parse(old["choices_json"]) if old is not None else None
        destinations = names(copy_to) if copy_to is not None else tuple(b.name for b in prior.copy_to) if prior else ()
        self.authority.require_accept(session, actor, normalized_tags, copies=bool(destinations))
        source = archive_store if archive_store is not None else prior.archive.name if prior else self.stores.default_archive()
        names((source,))
        cache = use_cache if use_cache is not None else prior.use_cache if prior else self.stores.default_use_cache(source)
        if type(cache) is not bool:
            raise InvalidRequest("cache policy did not resolve to a boolean")
        if source in destinations:
            raise InvalidRequest("archive source and copy destination must differ")
        if prior is not None:
            # Do not recompute a durable binding from changed server config.
            if (source, cache, destinations) != (prior.archive.name, prior.use_cache, tuple(b.name for b in prior.copy_to)):
                raise IdentityConflict("accepted placement choices changed")
            choices = prior
        else:
            choices = Choices(self.stores.binding(source), cache, tuple(self.stores.binding(n) for n in destinations))
            for requested, binding in zip((source, *destinations), (choices.archive, *choices.copy_to), strict=True):
                if binding.name != requested or not isinstance(binding.identity, str) or not binding.identity:
                    raise InvalidRequest("store binding must retain name and a nonempty identity")
        identity = hashlib.sha256(canonical({
            "format": "riverhog-upload-copy-reference-v1", "collection_id": collection_id,
            "actor": asdict(actor), "base_identity": base_identity, "choices": choices.document(),
            "tags": normalized_tags, "event_context": json.loads(context_json),
        }).encode()).hexdigest()
        if old is not None:
            if old["creation_identity"] != identity:
                raise IdentityConflict("accepted operation identity changed")
            if old["state"] == "canceled":
                raise IdentityConflict("canceled upload cannot be resumed")
            if old["state"] == "published" and choices.copy_to:
                self.authority.require_handoff(session, actor, collection_id)
            return self._snapshot(session, old)
        now = self.clock()
        session.execute(insert(operations).values(
            collection_id=collection_id, initiated_by_app=actor.app, initiated_by_key_id=actor.key_id,
            base_identity=base_identity, creation_identity=identity, choices_json=canonical(choices.document()),
            tags_json=canonical(normalized_tags), event_context_json=context_json, state="accepted", accepted_at=now,
        ))
        if destinations:
            session.execute(insert(intents), [{"collection_id": collection_id, "destination_store": d, "state": "accepted", "attempts": 0} for d in destinations])
        return self._snapshot(session, self._operation(session, collection_id))

    def publish(self, session: Session, collection_id: int) -> None:
        """Call in the SAME transaction that makes CollectionRecord published.

        Upload deletion and publication may commit only together with this
        transition. The startup/periodic sweeper then owns the continuation.
        """
        op = self._operation(session, collection_id)
        if op["state"] == "canceled":
            raise IdentityConflict("canceled upload cannot be published")
        choices = Choices.parse(op["choices_json"])
        actor = Actor(op["initiated_by_app"], op["initiated_by_key_id"])
        self.catalog.require_publication(session, collection_id, actor, choices.archive.name)
        if op["state"] == "published":
            return
        now = self.clock()
        session.execute(update(operations).where(operations.c.collection_id == collection_id).values(state="published", published_at=now))
        session.execute(update(intents).where(intents.c.collection_id == collection_id, intents.c.state == "accepted").values(state="pending", next_attempt_at=now))

    def cancel(self, session: Session, collection_id: int, actor: Actor) -> None:
        op = self._operation(session, collection_id)
        self._require_owner(op, actor)
        self.authority.require_accept(session, actor, tuple(json.loads(op["tags_json"])), copies=False)
        if op["state"] == "published":
            raise IdentityConflict("published copies use ordinary copy-job cancellation")
        if op["state"] == "canceled":
            return
        now = self.clock()
        session.execute(update(operations).where(operations.c.collection_id == collection_id).values(state="canceled", canceled_at=now))
        session.execute(update(intents).where(intents.c.collection_id == collection_id, intents.c.state == "accepted").values(state="canceled", finished_at=now))

    def handoff_one(self, session: Session, collection_id: int, destination: str) -> bool:
        """One short SQL transaction: authority + ordinary job/event + receipt.

        Lock order is operation -> intent -> authority/catalog/job via adapters.
        Do not call the existing independently committing create_or_resume API.
        """
        op = self._operation(session, collection_id)
        intent = session.execute(select(intents).where(intents.c.collection_id == collection_id, intents.c.destination_store == destination).with_for_update()).mappings().first()
        now = self.clock()
        if intent is None or op["state"] != "published" or intent["state"] != "pending" or intent["next_attempt_at"] > now:
            return False
        actor = Actor(op["initiated_by_app"], op["initiated_by_key_id"])
        choices = Choices.parse(op["choices_json"])
        expected = next(b for b in choices.copy_to if b.name == destination)
        receipt = None
        code = None
        try:
            # A terminal error after an adapter's SQL write must not leave a
            # partially inserted job/event in the outer transaction.
            with session.begin_nested():
                try:
                    self.authority.require_handoff(session, actor, collection_id)
                except AccessDenied as exc:
                    raise TerminalHandoffError("authorization_denied") from exc
                for binding in (choices.archive, expected):
                    try:
                        current = self.stores.binding(binding.name)
                    except InvalidRequest as exc:
                        raise TerminalHandoffError("configuration_changed") from exc
                    if current != binding:
                        raise TerminalHandoffError("configuration_changed")
                self.catalog.require_publication(session, collection_id, actor, choices.archive.name)
                receipt = self.jobs.ensure(session, collection_id, destination, actor, json.loads(op["event_context_json"]), now)
                if receipt.collection_id != collection_id or receipt.destination_store != destination or type(receipt.created) is not bool:
                    raise RuntimeError("job gateway returned an unrelated receipt")
        except TerminalHandoffError as exc:
            code = exc.code
        values: dict[str, Any] = {
            "state": "failed" if code else "handed_off", "failure_code": code,
            "finished_at": now, "next_attempt_at": None, "attempts": intent["attempts"] + 1,
        }
        if receipt is not None and code is None:
            values["job_receipt_json"] = canonical(asdict(receipt))
        session.execute(update(intents).where(intents.c.collection_id == collection_id, intents.c.destination_store == destination).values(**values))
        return True

    def process_due(self, sessions: sessionmaker[Session], *, limit: int = 100) -> dict[str, int]:
        """Call on startup AND periodically. No in-memory ownership or leases.

        Retry serialization errors/unknown failures in a NEW transaction with
        bounded backoff. A failing destination does not starve the batch.
        """
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise InvalidRequest("limit must be between 1 and 1000")
        with sessions.begin() as session:
            due = list(session.execute(select(intents.c.collection_id, intents.c.destination_store).join(operations).where(
                operations.c.state == "published", intents.c.state == "pending", intents.c.next_attempt_at <= self.clock(),
            ).order_by(intents.c.next_attempt_at, intents.c.collection_id, intents.c.destination_store).limit(limit)))
        result = {"progressed": 0, "retry_scheduled": 0}
        for collection_id, destination in due:
            try:
                with sessions.begin() as session:
                    changed = self.handoff_one(session, collection_id, destination)
                result["progressed"] += int(changed)
            except Exception:
                # Deliberately no exception string in durable/user-visible state.
                # A commit/connection uncertainty is also safe: first reload.
                with sessions.begin() as session:
                    self._operation(session, collection_id)
                    row = session.execute(select(intents).where(intents.c.collection_id == collection_id, intents.c.destination_store == destination).with_for_update()).mappings().one()
                    if row["state"] == "pending":
                        attempts = row["attempts"] + 1
                        delay = min(300, 2 ** min(attempts, 8))
                        session.execute(update(intents).where(intents.c.collection_id == collection_id, intents.c.destination_store == destination).values(
                            attempts=attempts, failure_code="retryable_handoff_error", next_attempt_at=after_seconds(self.clock(), delay),
                        ))
                        result["retry_scheduled"] += 1
        return result

    @staticmethod
    def _require_owner(op: Mapping[str, Any], actor: Actor) -> None:
        actor.validate()
        if (actor.app, actor.key_id) != (op["initiated_by_app"], op["initiated_by_key_id"]):
            raise AccessDenied("copy operation not available")

    def get(self, session: Session, collection_id: int, actor: Actor) -> dict[str, Any]:
        op = self._operation(session, collection_id)
        self._require_owner(op, actor)
        if op["state"] == "published" and Choices.parse(op["choices_json"]).copy_to:
            self.authority.require_handoff(session, actor, collection_id)
        else:
            self.authority.require_accept(session, actor, tuple(json.loads(op["tags_json"])), copies=False)
        return self._snapshot(session, op)

    def inspect_published(self, session: Session, collection_id: int, actor: Actor) -> dict[str, Any]:
        """Operator observation survives revocation of the original uploader.

        The inspecting actor must CURRENTLY hold ordinary archive-management
        scope. This never transfers the authority/attribution of pending work.
        """
        actor.validate()
        op = self._operation(session, collection_id)
        if op["state"] != "published":
            raise AccessDenied("unpublished copy operation not available")
        self.authority.require_handoff(session, actor, collection_id)
        return self._snapshot(session, op)

    def _snapshot(self, session: Session, op: Mapping[str, Any]) -> dict[str, Any]:
        choices = Choices.parse(op["choices_json"])
        return {
            "collection_id": op["collection_id"], "state": op["state"],
            "creation_identity": op["creation_identity"], "archive_store": choices.archive.name,
            "use_cache": choices.use_cache, "copy_to": [b.name for b in choices.copy_to],
            "initiated_by_app": op["initiated_by_app"], "initiated_by_key_id": op["initiated_by_key_id"],
            "copies": [self._intent_snapshot(session, row) for row in session.execute(select(intents).where(
                intents.c.collection_id == op["collection_id"],
            ).order_by(intents.c.destination_store)).mappings()],
        }

    def _intent_snapshot(self, session: Session, row: Mapping[str, Any]) -> dict[str, Any]:
        receipt = JobReceipt(**json.loads(row["job_receipt_json"])) if row["job_receipt_json"] else None
        return {
            "destination_store": row["destination_store"], "state": row["state"],
            "attempts": row["attempts"], "failure_code": row["failure_code"],
            "next_attempt_at": row["next_attempt_at"], "finished_at": row["finished_at"],
            "job": asdict(receipt) if receipt else None,
            "job_observation": self.jobs.observe(session, receipt) if receipt else None,
        }
