"""Executable design reference, NOT a Riverhog implementation or state format.

Production must use CatalogFollower, time-formats, canonical JSON, qualified
filesystem ports and application-owned state-schema. This dependency-free model
isolates placement and transaction invariants so the handoff can be exercised
without installing the repository, contacting Riverhog or registering OS jobs.
"""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

TIMESTAMP = re.compile(
    r"([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):"
    r"([0-9]{2})\.([0-9]{9})Z\Z"
)
HEX = re.compile(r"[0-9a-f]{64}\Z")
DECIMAL = re.compile(r"[1-9][0-9]*\Z")
MAX_ID = (1 << 63) - 1
MAX_REVISION = 8_999_999_999_999_999_999
RESET_REASONS = {
    "catalog_sync_cursor_expired", "catalog_sync_history_expired",
    "catalog_sync_source_changed", "catalog_sync_view_changed",
    "unauthorized", "forbidden", "precondition_failed",
}


class AuthorityError(ValueError):
    pass


class StaleProposal(RuntimeError):
    pass


def canonical(value: Any) -> str:
    """Only the simple string/integer/boolean reference documents use this helper."""
    return json.dumps(value, ensure_ascii=False, allow_nan=False, sort_keys=True,
                      separators=(",", ":"))


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def positive_decimal(value: str, maximum: int) -> int:
    if not isinstance(value, str) or not DECIMAL.fullmatch(value):
        raise ValueError("expected a positive canonical decimal string")
    if len(value) > 19 or int(value) > maximum:
        raise ValueError("decimal is outside the reference contract bound")
    return int(value)


def timestamp_parts(value: str) -> tuple[str, ...]:
    match = TIMESTAMP.fullmatch(value) if isinstance(value, str) else None
    if match is None:
        raise ValueError("expected canonical UTC with nine fractional digits")
    parts = match.groups()
    # Calendar validation only: never round-trip the fractional tail via datetime.
    datetime(*(int(v) for v in parts[:6]))
    return parts


def placement(value: str, partition: str = "%Y/%m") -> str:
    """Date partition plus a fixed full-precision UTC collection basename."""
    year, month, day, hour, minute, second, ns = timestamp_parts(value)
    if not isinstance(partition, str) or len(partition.encode("utf-8")) > 256:
        raise ValueError("partition pattern exceeds its syntax bound")
    fields = {"Y": year, "m": month, "d": day, "H": hour, "M": minute, "S": second}
    parents: list[str] = []
    if partition:
        pieces = partition.split("/")
        if len(pieces) > 8 or any(not piece for piece in pieces):
            raise ValueError("partition requires one to eight nonempty components")
        for piece in pieces:
            if re.fullmatch(r"(?:%[YmdHMS]|[0-9_-])+", piece) is None:
                raise ValueError("unsupported partition token or unsafe literal")
            rendered = re.sub(r"%([YmdHMS])", lambda m: fields[m.group(1)], piece)
            parents.append(rendered)
    basename = f"{year}{month}{day}T{hour}{minute}{second}.{ns}Z"
    return "/".join((*parents, basename))


def membership(receipt: dict[str, Any], descriptor: dict[str, Any], tag: str) -> bool:
    """Reference for exact response checking; production owns full wire validation."""
    if not isinstance(receipt, dict):
        raise AuthorityError("tag membership must be an object")
    expected = {
        "collection_id": descriptor["collection_id"],
        "revision": descriptor["tag_revision"],
        "tag_set_identity": descriptor["tag_set_identity"], "tag": tag,
    }
    if any(receipt.get(k) != v for k, v in expected.items()):
        raise AuthorityError("tag membership changed its requested authority")
    if type(receipt.get("revision")) is not int or type(receipt.get("present")) is not bool:
        raise AuthorityError("tag membership requires exact scalar types")
    return receipt["present"]


@dataclass(frozen=True)
class Observation:
    document: dict[str, Any]
    matched: bool

    def __post_init__(self) -> None:
        doc = json.loads(canonical(self.document))
        object.__setattr__(self, "document", doc)
        positive_decimal(doc["collection_id"], MAX_ID)
        positive_decimal(doc["revision"], MAX_REVISION)
        if type(self.matched) is not bool:
            raise ValueError("match truth must be explicit, not unknown")
        if doc.get("operation") == "upsert":
            if not HEX.fullmatch(doc["archive_root_sha256"]):
                raise ValueError("invalid archive root")
            timestamp_parts(doc["finalized_at"])
        elif doc.get("operation") == "departure":
            if doc.get("cause") not in {"collection_deleted", "visibility_lost"}:
                raise ValueError("invalid departure cause")
            if self.matched:
                raise ValueError("a departure cannot match")
        else:
            raise ValueError("invalid operation")

    @property
    def collection_id(self) -> str:
        return self.document["collection_id"]

    @property
    def revision(self) -> int:
        return int(self.document["revision"])

    @property
    def authority(self) -> str:
        return digest(self.document)


@dataclass(frozen=True)
class Position:
    generation: int
    serial: int
    phase: str
    cursor: str | None
    through: int
    source: str
    view: str


class ModelStore:
    """Small transaction model. It is deliberately not production DDL or an ORM."""

    @classmethod
    def initialize(cls, path: Path, *, source: str, view: str, tag: str,
                   backfill: bool = False, partition: str = "%Y/%m") -> ModelStore:
        placement("2025-12-24T08:00:12.123456789Z", partition)
        if not HEX.fullmatch(source) or not HEX.fullmatch(view):
            raise ValueError("source and authorization view require exact identities")
        if type(backfill) is not bool or not isinstance(tag, str) or not tag:
            raise ValueError("invalid root configuration")
        with path.open("xb"):
            pass
        db = sqlite3.connect(path, isolation_level=None)
        try:
            db.executescript("""
                CREATE TABLE progress (
                    singleton INTEGER PRIMARY KEY CHECK (singleton=1),
                    generation INTEGER NOT NULL, serial INTEGER NOT NULL,
                    phase TEXT NOT NULL, cursor TEXT, through_revision INTEGER NOT NULL,
                    source TEXT NOT NULL, view TEXT NOT NULL, tag TEXT NOT NULL,
                    backfill INTEGER NOT NULL, partition_pattern TEXT NOT NULL,
                    reset_reason TEXT
                );
                CREATE TABLE observed (
                    generation INTEGER NOT NULL, collection_id TEXT NOT NULL,
                    revision INTEGER NOT NULL, authority TEXT NOT NULL, matched INTEGER NOT NULL,
                    PRIMARY KEY (generation, collection_id)
                );
                CREATE TABLE identity (
                    collection_id TEXT PRIMARY KEY, archive_root TEXT NOT NULL,
                    finalized_at TEXT NOT NULL
                );
                CREATE TABLE intents (
                    collection_id TEXT PRIMARY KEY, identity_sha256 TEXT NOT NULL,
                    relative_path TEXT NOT NULL UNIQUE, state TEXT NOT NULL,
                    suppressed INTEGER NOT NULL DEFAULT 0
                );
            """)
            db.execute("INSERT INTO progress VALUES (1,1,0,'catalog','catalog:0',0,?,?,?,?,?,NULL)",
                       (source, view, tag, int(backfill), partition))
        finally:
            db.close()
        return cls(path)

    def __init__(self, path: Path) -> None:
        self.db = sqlite3.connect(path.as_uri() + "?mode=rw", uri=True, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA synchronous=FULL")
        # Short single-writer transactions; no WAL or network-filesystem claim.
        if self.db.execute("PRAGMA journal_mode").fetchone()[0] != "delete":
            raise ValueError("reference state must use rollback journaling")
        self.position()

    def close(self) -> None:
        self.db.close()

    def position(self) -> Position:
        row = self.db.execute("SELECT * FROM progress").fetchone()
        if row is None:
            raise ValueError("missing reference progress")
        return Position(row["generation"], row["serial"], row["phase"], row["cursor"],
                        row["through_revision"], row["source"], row["view"])

    def intents(self) -> list[dict[str, Any]]:
        return [dict(row) for row in self.db.execute("SELECT * FROM intents ORDER BY collection_id")]

    def _fence(self, expected: Position) -> None:
        if self.position() != expected:
            raise StaleProposal("generation, serial, phase, cursor or authority changed")

    def accept(self, expected: Position, observations: Iterable[Observation], *,
               source: str, view: str, next_cursor: str, next_phase: str,
               through: int = 0, fail_before_commit: bool = False) -> Position:
        """Accept a model page, its decisions and progress atomically.

        The production equivalent receives a validated CatalogFollowBatch. This
        model checks only the invariants directly under test, not the full wire.
        """
        items = tuple(observations)
        if len(items) > 100:
            raise ValueError("reference page exceeds the catalog page bound")
        if not isinstance(next_cursor, str) or not next_cursor:
            raise ValueError("next cursor must remain opaque and nonempty")
        if type(through) is not int or not 0 <= through <= MAX_REVISION:
            raise ValueError("invalid through revision")
        self.db.execute("BEGIN IMMEDIATE")
        try:
            self._fence(expected)
            if expected.phase == "reset_required":
                raise AuthorityError("explicit rebaseline required")
            if (source, view) != (expected.source, expected.view):
                raise AuthorityError("page changed source or authorization view")
            baseline = expected.phase == "catalog"
            allowed = {"catalog", "catchup"} if baseline else {expected.phase, "following"}
            if next_phase not in allowed:
                raise ValueError("invalid following transition")
            if baseline:
                if through != 0 or any(o.document["operation"] != "upsert" for o in items):
                    raise ValueError("bootstrap descriptors are not feed watermarks")
            else:
                previous = expected.through
                for item in items:
                    if item.revision <= previous:
                        raise ValueError("feed revisions must strictly advance")
                    previous = item.revision
                if through < previous:
                    raise ValueError("through revision precedes feed observations")
            config = self.db.execute("SELECT * FROM progress").fetchone()
            for item in items:
                self._apply(expected, item, baseline=baseline, backfill=bool(config["backfill"]),
                            partition=config["partition_pattern"])
            self.db.execute(
                "UPDATE progress SET serial=serial+1,phase=?,cursor=?,through_revision=?",
                (next_phase, next_cursor, through),
            )
            if fail_before_commit:
                raise RuntimeError("injected crash window before commit")
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        return self.position()

    def _apply(self, expected: Position, item: Observation, *, baseline: bool,
               backfill: bool, partition: str) -> None:
        previous = self.db.execute(
            "SELECT * FROM observed WHERE generation=? AND collection_id=?",
            (expected.generation, item.collection_id),
        ).fetchone()
        if previous is not None:
            if item.revision < previous["revision"]:
                return
            if item.revision == previous["revision"]:
                if item.authority != previous["authority"] or item.matched != bool(previous["matched"]):
                    raise AuthorityError("equal revisions have different exact authority")
                return
        doc = item.document
        if doc["operation"] == "upsert":
            old = self.db.execute("SELECT * FROM identity WHERE collection_id=?",
                                  (item.collection_id,)).fetchone()
            if old is not None and (old["archive_root"], old["finalized_at"]) != (
                doc["archive_root_sha256"], doc["finalized_at"]
            ):
                raise AuthorityError("immutable collection metadata changed")
            self.db.execute("INSERT OR IGNORE INTO identity VALUES (?,?,?)",
                            (item.collection_id, doc["archive_root_sha256"], doc["finalized_at"]))
        prior_matched = previous is not None and bool(previous["matched"])
        admit = item.matched and (backfill if baseline else not prior_matched)
        self.db.execute("INSERT OR REPLACE INTO observed VALUES (?,?,?,?,?)",
                        (expected.generation, item.collection_id, item.revision,
                         item.authority, int(item.matched)))
        if admit:
            self._reserve(expected.source, doc, partition)

    def _reserve(self, source: str, doc: dict[str, Any], partition: str) -> None:
        existing = self.db.execute("SELECT * FROM intents WHERE collection_id=?",
                                   (doc["collection_id"],)).fetchone()
        if existing is not None:
            # Explicit eviction is durable; a later tag edge does not undo it.
            return
        identity = digest({"source_identity": source, "collection_id": doc["collection_id"],
                           "archive_root_sha256": doc["archive_root_sha256"]})
        path = placement(doc["finalized_at"], partition)
        if self.db.execute("SELECT 1 FROM intents WHERE relative_path=?", (path,)).fetchone():
            path += "--" + identity
        if self.db.execute("SELECT 1 FROM intents WHERE relative_path=?", (path,)).fetchone():
            raise AuthorityError("placement collision even after full identity disambiguation")
        self.db.execute("INSERT INTO intents VALUES (?,?,?,'pending',0)",
                        (doc["collection_id"], identity, path))

    def reset(self, expected: Position, reason: str) -> Position:
        if reason not in RESET_REASONS:
            raise ValueError("unknown reset reason")
        self.db.execute("BEGIN IMMEDIATE")
        try:
            self._fence(expected)
            self.db.execute("UPDATE progress SET phase='reset_required',cursor=NULL,"
                            "serial=serial+1,reset_reason=?", (reason,))
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        return self.position()

    def rebaseline(self, expected: Position, *, source: str, view: str,
                   cursor: str, backfill: bool = False) -> Position:
        if source != expected.source:
            raise AuthorityError("another source requires another destination")
        if not HEX.fullmatch(view) or not cursor or type(backfill) is not bool:
            raise ValueError("invalid explicit checkpoint")
        self.db.execute("BEGIN IMMEDIATE")
        try:
            self._fence(expected)
            self.db.execute("UPDATE progress SET generation=generation+1,serial=serial+1,"
                            "phase='catalog',cursor=?,through_revision=0,view=?,backfill=?,"
                            "reset_reason=NULL", (cursor, view, int(backfill)))
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        return self.position()

    def suppress(self, collection_id: str) -> None:
        positive_decimal(collection_id, MAX_ID)
        self.db.execute("UPDATE intents SET suppressed=1,state='evicted' WHERE collection_id=?",
                        (collection_id,))


def schedule_health(*, installed: bool, enabled: bool, active: bool,
                    registration_valid: bool, overdue: bool = False) -> str:
    if not installed:
        return "absent"
    if not registration_valid:
        return "failed"
    if not enabled:
        return "disabled"
    if active:
        return "running"
    return "overdue" if overdue else "idle"


def catalog_run_boundary(kind: str, caught_up: bool | None) -> bool:
    """The proposed public batch signal; phase alone is insufficient after bootstrap."""
    if kind not in {"checkpoint", "catalog", "changes", "reset"}:
        raise ValueError("unknown batch kind")
    if kind == "changes":
        if type(caught_up) is not bool:
            raise ValueError("a change batch requires explicit caught_up truth")
        return caught_up
    if caught_up is not None:
        raise ValueError("only a change batch carries caught_up")
    return False
