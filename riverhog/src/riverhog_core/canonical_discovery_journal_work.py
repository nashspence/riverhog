"""Durable direct source positions and atomic assertion-child obligations.

The build owner supplies an archive admission verified for this exact Native
prefix. This private workspace records documentary birth/retirement intervals;
it neither grants current access nor publishes a discovery generation.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, replace
from typing import Any, cast

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
from riverhog_provenance import JournalSummary, assertion_reference
from riverhog_provenance_contracts import require_canonical_uuid_urn

from riverhog_core.canonical_discovery_fact_work import (
    FactTaskKey,
    StaleFactTask,
    begin_assertion,
    fact_assertion_binding,
    fact_task_progress,
    initialize_fact_work,
)


class StaleJournalTask(RuntimeError):
    """An exact source, worker, cursor or prepared-page binding changed."""


def _digest(value: str) -> None:
    if (
        type(value) is not str
        or len(value) != 64
        or any(c not in "0123456789abcdef" for c in value)
    ):
        raise ValueError("source identities must be lowercase SHA-256")


def _identity(value: str) -> None:
    if type(value) is not str or not value or len(value) > 128 or not value.isascii():
        raise ValueError("work identity must be a bounded nonempty ASCII identifier")


def journal_source_binding(admission_sha256: str, anchor: Mapping[str, Any]) -> str:
    """Encode exact source coordinates without establishing admission authority."""
    _digest(admission_sha256)
    return canonical_json_bytes(
        {
            "admission_sha256": admission_sha256,
            "anchor": anchor,
            "domain": "riverhog-discovery-journal-source/v1",
        }
    ).decode()


def _one(db: sqlite3.Connection, sql: str, parameters: tuple[object, ...]) -> Any:
    cursor = db.execute(sql, parameters)
    try:
        return cursor.fetchone()
    finally:
        cursor.close()


@dataclass(frozen=True, slots=True)
class JournalTaskKey:
    build_id: str
    source_key: str
    worker_id: str


@dataclass(frozen=True, slots=True)
class JournalCursor:
    sequence: int = 0
    phase: str = "header"
    group: int = 0
    item: int = 0


@dataclass(frozen=True, slots=True)
class JournalProgress:
    version: int
    cursor: JournalCursor
    assertions: int
    retractions: int
    pending: int
    complete: bool


@dataclass(frozen=True, slots=True)
class AssertionObligation:
    assertion_id: str
    referent_id: str
    kind: str
    origin_entry_json: str
    canonical_stream_ref: str
    group: str
    item: int
    task_id: str


@dataclass(frozen=True, slots=True)
class RetirementObligation:
    assertion_id: str
    origin_entry_json: str
    origin_sequence: int


@dataclass(frozen=True, slots=True)
class JournalAction:
    cursor: JournalCursor
    next_cursor: JournalCursor
    assertion: AssertionObligation | None = None
    retirement: RetirementObligation | None = None


@dataclass(frozen=True, slots=True)
class PreparedJournalPage:
    key: JournalTaskKey
    expected: JournalProgress
    actions: tuple[JournalAction, ...]


@dataclass(slots=True)
class _Entry:
    sequence: int
    reference_json: str
    previous_json: str | None
    groups: tuple[str, ...]
    arrays: dict[str, list[dict[str, Any]]]
    prefix: tuple[int, ...]
    retracts: list[Any]

    @property
    def assertion_count(self) -> int:
        return self.prefix[-1]

    def position(self, cursor: JournalCursor) -> tuple[int, int]:
        if cursor.sequence != self.sequence:
            raise StaleJournalTask("cursor names another source entry")
        if cursor.phase == "header" and cursor.group == cursor.item == 0:
            return 0, 0
        if (
            cursor.phase == "retracts"
            and cursor.group == 0
            and 0 <= cursor.item < len(self.retracts)
        ):
            return 0, cursor.item
        if cursor.phase == "assertions" and 0 <= cursor.group < len(self.groups):
            if 0 <= cursor.item < len(self.arrays[self.groups[cursor.group]]):
                return self.prefix[cursor.group] + cursor.item, len(self.retracts)
        if cursor.phase == "end" and cursor.group == cursor.item == 0:
            return self.assertion_count, len(self.retracts)
        raise StaleJournalTask("cursor is outside the independently admitted entry directory")

    def assertions_cursor(self, group: int, item: int) -> JournalCursor:
        while group < len(self.groups):
            if item < len(self.arrays[self.groups[group]]):
                return JournalCursor(self.sequence, "assertions", group, item)
            group, item = group + 1, 0
        return JournalCursor(self.sequence, "end")


class QualifiedJournalSource:
    """Own an independent Native lease and one decoded entry at a time.

    Admission is supplied by the archive owner, not established by this digest.
    This binding does not establish shared-prefix or corpus reconciliation.
    The selected source union must establish those independent terminals before
    merging sources or publication. Source identity includes the selected exact
    anchor; journal UUID or equal payload bytes alone never deduplicate sources.
    """

    def __init__(
        self, summary: JournalSummary, admission_sha256: str, *, through: int | None = None
    ) -> None:
        _digest(admission_sha256)
        self._summary = summary.borrow()
        self._closed = False
        self._entry: _Entry | None = None
        self._prepared: PreparedJournalPage | None = None
        try:
            sequence = len(self._summary.frames) - 1 if through is None else through
            if type(sequence) is not int or sequence < 0:
                raise ValueError("a source prefix must contain its initial entry")
            self._anchor = self._summary.anchor_at(sequence)
            self.entry_count = sequence + 1
            self.binding_json = journal_source_binding(admission_sha256, self._anchor)
            self.source_key = hashlib.sha256(self.binding_json.encode()).hexdigest()
            self.terminal_json = canonical_json_bytes(self._anchor["through"]).decode()
        except BaseException:
            self.close()
            raise

    def close(self) -> None:
        if not self._closed:
            self._summary.close()
            self._entry = self._prepared = None
            self._closed = True

    def __enter__(self) -> QualifiedJournalSource:
        self.check_open()
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def check_open(self) -> None:
        if self._closed or self._summary.anchor_at(self.entry_count - 1) != self._anchor:
            raise StaleJournalTask("source lease is closed or its exact prefix changed")

    def anchor_at(self, sequence: int) -> dict[str, Any]:
        """Detach an exact admitted Native prefix without loading an entry body."""
        self.check_open()
        if type(sequence) is not int or not 0 <= sequence < self.entry_count:
            raise StaleJournalTask("snapshot exceeds the admitted exact source prefix")
        return self._summary.anchor_at(sequence)

    def fork_anchor(self) -> dict[str, Any] | None:
        """Detach the original Native fork, without interpreting its metadata in Core."""
        self.check_open()
        return self._summary.fork_anchor()

    def reference_anchor(self, reference: Mapping[str, Any]) -> dict[str, Any]:
        """Resolve a documentary dependency inside this exact admitted source."""
        self.check_open()
        anchor = self._summary.reference_anchor(reference)
        if int(anchor["through"]["sequence"]) >= self.entry_count:
            raise StaleJournalTask("dependency exceeds the admitted exact source prefix")
        return anchor

    def reference_for_assertion(
        self, assertion_id: str, *, object_id: str, object_type: str
    ) -> dict[str, Any]:
        """Detach an original Native address within this exact admitted source.

        Native verifies the full documentary reference, including retired
        assertions. An original address does not grant effective attribution.
        """
        self.check_open()
        require_canonical_uuid_urn(assertion_id)
        require_canonical_uuid_urn(object_id)
        selected = {
            "scope": "external",
            "journal_id": self._anchor["journal_id"],
            **assertion_reference(self._summary, assertion_id),
            "object_id": object_id,
            "object_type": object_type,
        }
        self.reference_anchor(selected)
        return selected

    def entry(self, sequence: int) -> _Entry:
        self.check_open()
        if type(sequence) is not int or not 0 <= sequence < self.entry_count:
            raise StaleJournalTask("entry coordinate is outside the selected source prefix")
        if self._entry is None or self._entry.sequence != sequence:
            document = self._summary.frames[sequence].document
            reference = self._summary.anchor_at(sequence)["through"]
            if (
                document["id"] != reference["entry_id"]
                or document["sequence"] != str(sequence)
                or document["journal_id"] != self._anchor["journal_id"]
            ):
                raise StaleJournalTask("source frame differs from its authenticated header")
            body = document["body"]
            arrays = body.get("assertions", {})
            groups = tuple(sorted(arrays))
            prefix = [0]
            for group in groups:
                prefix.append(prefix[-1] + len(arrays[group]))
            self._entry = _Entry(
                sequence,
                canonical_json_bytes(reference).decode(),
                None
                if sequence == 0
                else canonical_json_bytes(document["previous_entry"]).decode(),
                groups,
                arrays,
                tuple(prefix),
                body.get("retracts", []),
            )
        return self._entry

    def assertion(self, sequence: int, group: str, item: int) -> dict[str, Any]:
        """Reload exactly one child by its retained direct source coordinates."""
        entry = self.entry(sequence)
        if (
            group not in entry.arrays
            or type(item) is not int
            or not 0 <= item < len(entry.arrays[group])
        ):
            raise StaleJournalTask("assertion coordinate is outside its admitted source array")
        return entry.arrays[group][item]


def initialize_journal_work(db: sqlite3.Connection) -> None:
    initialize_fact_work(db)
    db.executescript("""
        CREATE TABLE IF NOT EXISTS discovery_journal_tasks (
            build_id TEXT NOT NULL, source_key TEXT NOT NULL, worker_id TEXT NOT NULL,
            binding_json TEXT NOT NULL, terminal_json TEXT NOT NULL, entry_count TEXT NOT NULL,
            version TEXT NOT NULL, sequence TEXT NOT NULL, phase TEXT NOT NULL,
            group_position INTEGER NOT NULL, item_position INTEGER NOT NULL,
            entry_assertions TEXT NOT NULL, entry_retractions TEXT NOT NULL,
            assertions TEXT NOT NULL, retractions TEXT NOT NULL, pending TEXT NOT NULL,
            last_entry_json TEXT, complete INTEGER NOT NULL CHECK(complete IN (0,1)),
            terminal_version TEXT,
            PRIMARY KEY(build_id,source_key)
        ) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS discovery_journal_assertions (
            build_id TEXT NOT NULL, source_key TEXT NOT NULL, assertion_id TEXT NOT NULL,
            referent_id TEXT NOT NULL, kind TEXT NOT NULL, origin_entry_json TEXT NOT NULL,
            born_sequence TEXT NOT NULL, retired_sequence TEXT,
            source_group TEXT NOT NULL, source_item INTEGER NOT NULL,
            task_id TEXT NOT NULL, canonical_stream_ref TEXT NOT NULL,
            acknowledged INTEGER NOT NULL CHECK(acknowledged IN (0,1)),
            PRIMARY KEY(build_id,source_key,assertion_id),
            UNIQUE(build_id,task_id),
            FOREIGN KEY(build_id,source_key) REFERENCES discovery_journal_tasks(build_id,source_key)
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS discovery_journal_historical_objects
            ON discovery_journal_assertions (
                build_id,source_key,referent_id,length(born_sequence),born_sequence,assertion_id
            );
        CREATE INDEX IF NOT EXISTS discovery_journal_pending_assertions
            ON discovery_journal_assertions (
                build_id,source_key,length(born_sequence),born_sequence,source_group,source_item
            ) WHERE acknowledged=0;
    """)


@contextmanager
def _atomic(db: sqlite3.Connection) -> Iterator[None]:
    if not db.in_transaction:
        raise ValueError("journal work requires an owner-managed transaction")
    db.execute("SAVEPOINT discovery_journal_step")
    try:
        yield
    except BaseException:
        db.execute("ROLLBACK TO discovery_journal_step")
        db.execute("RELEASE discovery_journal_step")
        raise
    else:
        db.execute("RELEASE discovery_journal_step")


def _task(db: sqlite3.Connection, key: JournalTaskKey) -> tuple[Any, ...]:
    _identity(key.build_id)
    _identity(key.worker_id)
    _digest(key.source_key)
    row = _one(
        db,
        "SELECT worker_id,binding_json,terminal_json,entry_count,version,sequence,phase,"
        "group_position,item_position,entry_assertions,entry_retractions,assertions,retractions,"
        "pending,last_entry_json,complete,terminal_version FROM discovery_journal_tasks "
        "WHERE build_id=? AND source_key=?",
        (key.build_id, key.source_key),
    )
    if row is None or row[0] != key.worker_id:
        raise StaleJournalTask("journal has no matching build/source/worker binding")
    return cast(tuple[Any, ...], row)


def _progress(row: tuple[Any, ...]) -> JournalProgress:
    return JournalProgress(
        int(row[4]),
        JournalCursor(int(row[5]), row[6], int(row[7]), int(row[8])),
        int(row[11]),
        int(row[12]),
        int(row[13]),
        bool(row[15]),
    )


def journal_progress(db: sqlite3.Connection, key: JournalTaskKey) -> JournalProgress:
    return _progress(_task(db, key))


def begin_journal(
    db: sqlite3.Connection, key: JournalTaskKey, source: QualifiedJournalSource
) -> JournalProgress:
    source.check_open()
    if key.source_key != source.source_key:
        raise StaleJournalTask("journal source binding changed")
    _identity(key.build_id)
    _identity(key.worker_id)
    with _atomic(db):
        old = _one(
            db,
            "SELECT worker_id,binding_json,terminal_json,entry_count "
            "FROM discovery_journal_tasks WHERE build_id=? AND source_key=?",
            (key.build_id, key.source_key),
        )
        expected = key.worker_id, source.binding_json, source.terminal_json, str(source.entry_count)
        if old is None:
            db.execute(
                "INSERT INTO discovery_journal_tasks VALUES "
                "(?,?,?,?,?,?,'0','0','header',0,0,'0','0','0','0','0',NULL,0,NULL)",
                (key.build_id, key.source_key, *expected),
            )
        elif old != expected:
            raise StaleJournalTask("journal registration differs from its exact source")
        return journal_progress(db, key)


def _action(
    key: JournalTaskKey, source: QualifiedJournalSource, entry: _Entry, cursor: JournalCursor
) -> JournalAction:
    entry.position(cursor)
    if cursor.phase == "header":
        next_cursor = (
            JournalCursor(cursor.sequence, "retracts")
            if entry.retracts
            else entry.assertions_cursor(0, 0)
        )
        return JournalAction(cursor, next_cursor)
    if cursor.phase == "retracts":
        next_cursor = (
            replace(cursor, item=cursor.item + 1)
            if cursor.item + 1 < len(entry.retracts)
            else entry.assertions_cursor(0, 0)
        )
        target = entry.retracts[cursor.item]
        return JournalAction(
            cursor,
            next_cursor,
            retirement=RetirementObligation(
                target["assertion_id"],
                canonical_json_bytes(target["entry"]).decode(),
                int(target["entry"]["sequence"]),
            ),
        )
    if cursor.phase == "assertions":
        group = entry.groups[cursor.group]
        row = entry.arrays[group][cursor.item]
        task_id = hashlib.sha256(
            canonical_json_bytes(
                ["riverhog-discovery-assertion-task/v1", key.source_key, row["assertion_id"]]
            )
        ).hexdigest()
        obligation = AssertionObligation(
            row["assertion_id"],
            row["id"],
            row["type"],
            entry.reference_json,
            hashlib.sha256(canonical_json_bytes(row)).hexdigest(),
            group,
            cursor.item,
            task_id,
        )
        return JournalAction(
            cursor, entry.assertions_cursor(cursor.group, cursor.item + 1), obligation
        )
    next_cursor = JournalCursor(
        cursor.sequence + 1, "done" if cursor.sequence + 1 == source.entry_count else "header"
    )
    return JournalAction(cursor, next_cursor)


@declare_growth(
    Declaration(
        id="riverhog-server.discovery-journal-page-preparation.growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.canonical_discovery_journal_work.prepare_journal_page",),
        scope="one detached source-registration page within one Native entry",
        starts_at="receive a qualified frozen Native prefix, retained entry cache and work key",
        ends_at="return bounded child/control actions and the exact expected parent checkpoint",
        dimensions=(Dimension("action_budget", COUNT, "Requested positive action count."),),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                Expression("constant", value=1, unit=COUNT),
                "One exact parent header read from the Core work database.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                Expression("constant", value=1, unit=COUNT),
                "The parent primary key returns at most its one exact header.",
            ),
            Bound(
                "prepared_actions",
                COUNT,
                "upper",
                Expression("dimension", name="action_budget"),
                "Headers, children, retirements and entry ends each consume one action.",
            ),
        ),
        conditions=(
            "Native has authenticated the finite immutable prefix and owns its lease.",
            "The archive owner has verified admission for this exact selected prefix.",
            "Retain one decoded entry across its pages; restart loads only the current entry.",
            "Decoded JSON is immutable, and preparation has no owner write transaction.",
        ),
        invariants=(
            "Direct array/item positions never regenerate prior assertions to skip them.",
            "Each page stops at an entry boundary, including checkpoint-only entries.",
            "Canonicalization and hashing precede child registration and parent advancement.",
        ),
        limitations=(
            "These counters measure Core work SQL and action output, not composite bounds.",
            "Native anchor reads and one permitted entry decode remain separately owned costs.",
            "Hashing, encoding, CPU, backend I/O and memory are not measured by this declaration.",
            "Apply, source-union completeness, admission and publication have separate owners.",
        ),
    )
)
def prepare_journal_page(
    db: sqlite3.Connection,
    key: JournalTaskKey,
    source: QualifiedJournalSource,
    *,
    action_budget: int,
) -> PreparedJournalPage:
    """Prepare at most one entry's bounded actions with no write transaction."""
    if db.in_transaction:
        raise ValueError("source decoding and hashing require no active write transaction")
    if type(action_budget) is not int or action_budget < 1:
        raise ValueError("journal action budget must be positive")
    source.check_open()
    row = _task(db, key)
    progress = _progress(row)
    if (
        key.source_key != source.source_key
        or row[1] != source.binding_json
        or row[2] != source.terminal_json
        or int(row[3]) != source.entry_count
        or progress.complete
        or progress.cursor.phase == "done"
    ):
        raise StaleJournalTask("journal source or input terminal changed")
    entry = source.entry(progress.cursor.sequence)
    if entry.position(progress.cursor) != (int(row[9]), int(row[10])):
        raise StaleJournalTask("journal cursor skipped admitted source items")
    actions = []
    cursor = progress.cursor
    for _ in range(action_budget):
        action = _action(key, source, entry, cursor)
        actions.append(action)
        cursor = action.next_cursor
        if cursor.sequence != entry.sequence:
            break
    page = PreparedJournalPage(key, progress, tuple(actions))
    source._prepared = page
    return page


@declare_growth(
    Declaration(
        id="riverhog-server.discovery-journal-page-apply.growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.canonical_discovery_journal_work.apply_journal_page",),
        scope="one prepared journal page committed or rejected within an owner transaction",
        starts_at="receive a current detached page and an exact owner-leased work key",
        ends_at="return parent progress or roll back every child and cursor write in the page",
        dimensions=(Dimension("page_actions", COUNT, "Actual prepared actions in this page."),),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        Expression("constant", value=6, unit=COUNT),
                        Expression(
                            "product",
                            terms=(
                                Expression("constant", value=8),
                                Expression("dimension", name="page_actions"),
                            ),
                        ),
                    ),
                ),
                "Fixed parent/savepoint work plus at most eight statements per child.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        Expression("constant", value=2, unit=COUNT),
                        Expression(
                            "product",
                            terms=(
                                Expression("constant", value=3),
                                Expression("dimension", name="page_actions"),
                            ),
                        ),
                    ),
                ),
                "Two parent reads and at most three child binding/cursor reads per action.",
            ),
            Bound(
                "hashed_octets",
                OCTETS,
                "upper",
                Expression("constant", value=0, unit=OCTETS),
                "This transition uses detached exact commitments without source hashing.",
            ),
        ),
        conditions=(
            "The caller owns a bounded write transaction and the exact workspace lease.",
            "The current private schema has its exact keys and no added triggers.",
            "The page is the current preparation of the still-open immutable source handle.",
        ),
        invariants=(
            "Child creation, raw intervals and the parent cursor share one savepoint.",
            "A stale version, worker or source cannot acknowledge a new source position.",
            "No earlier entry, assertion, completed fact stream or segment is enumerated.",
        ),
        limitations=(
            "SQL counts exclude storage-engine work, transaction commit and lease acquisition.",
            "Preparation and Native validation have separately declared owners and costs.",
            "Storage completion is not source-manifest completeness or publication authority.",
        ),
    )
)
def apply_journal_page(
    db: sqlite3.Connection,
    key: JournalTaskKey,
    source: QualifiedJournalSource,
    page: PreparedJournalPage,
) -> JournalProgress:
    """Commit children, raw intervals and the source checkpoint as one step."""
    if source._closed or source._prepared is not page or page.key != key:
        raise StaleJournalTask("page is not the current detached preparation of this source")
    entry = source._entry
    if entry is None:
        raise StaleJournalTask("prepared source entry was released")
    with _atomic(db):
        row = _task(db, key)
        if (
            _progress(row) != page.expected
            or row[1] != source.binding_json
            or row[2] != source.terminal_json
            or int(row[3]) != source.entry_count
        ):
            raise StaleJournalTask("journal page version or exact source changed")
        version, assertions, retractions, pending = (
            page.expected.version,
            page.expected.assertions,
            page.expected.retractions,
            page.expected.pending,
        )
        cursor = page.expected.cursor
        last = row[14]
        counts = int(row[9]), int(row[10])
        for action in page.actions:
            if action.cursor != cursor or entry.position(cursor) != counts:
                raise StaleJournalTask("prepared page omitted a source position")
            if cursor.phase == "header":
                if last != entry.previous_json:
                    raise StaleJournalTask(
                        "source predecessor differs from the registered exact chain"
                    )
            elif cursor.phase == "retracts":
                target = action.retirement
                assert target is not None
                found = _one(
                    db,
                    "SELECT origin_entry_json,retired_sequence "
                    "FROM discovery_journal_assertions WHERE build_id=? AND source_key=? "
                    "AND assertion_id=?",
                    (key.build_id, key.source_key, target.assertion_id),
                )
                if (
                    found != (target.origin_entry_json, None)
                    or target.origin_sequence >= cursor.sequence
                ):
                    raise StaleJournalTask(
                        "retirement does not name an exact earlier active assertion"
                    )
                db.execute(
                    "UPDATE discovery_journal_assertions SET retired_sequence=? "
                    "WHERE build_id=? AND source_key=? AND assertion_id=?",
                    (str(cursor.sequence), key.build_id, key.source_key, target.assertion_id),
                )
                retractions += 1
            elif cursor.phase == "assertions":
                child = action.assertion
                assert child is not None
                child_key = FactTaskKey(key.build_id, child.task_id, key.source_key, key.worker_id)
                begin_assertion(
                    db,
                    child_key,
                    assertion_id=child.assertion_id,
                    origin_sha256=json.loads(child.origin_entry_json)["json_sha256"],
                    canonical_stream_ref=child.canonical_stream_ref,
                )
                db.execute(
                    "INSERT INTO discovery_journal_assertions VALUES "
                    "(?,?,?,?,?,?,?,NULL,?,?,?,?,0)",
                    (
                        key.build_id,
                        key.source_key,
                        child.assertion_id,
                        child.referent_id,
                        child.kind,
                        child.origin_entry_json,
                        str(cursor.sequence),
                        child.group,
                        child.item,
                        child.task_id,
                        child.canonical_stream_ref,
                    ),
                )
                assertions += 1
                pending += 1
            elif cursor.phase == "end":
                if counts != (entry.assertion_count, len(entry.retracts)):
                    raise StaleJournalTask(
                        "entry coverage differs from its Native source directory"
                    )
                last = entry.reference_json
                if action.next_cursor.phase == "done" and last != source.terminal_json:
                    raise StaleJournalTask(
                        "source input terminal differs from the selected exact anchor"
                    )
            cursor = action.next_cursor
            counts = entry.position(cursor) if cursor.sequence == entry.sequence else (0, 0)
            version += 1
        db.execute(
            "UPDATE discovery_journal_tasks SET version=?,sequence=?,phase=?,"
            "group_position=?,item_position=?,entry_assertions=?,entry_retractions=?,"
            "assertions=?,retractions=?,pending=?,last_entry_json=? "
            "WHERE build_id=? AND source_key=?",
            (
                str(version),
                str(cursor.sequence),
                cursor.phase,
                cursor.group,
                cursor.item,
                str(counts[0]),
                str(counts[1]),
                str(assertions),
                str(retractions),
                str(pending),
                last,
                key.build_id,
                key.source_key,
            ),
        )
        return journal_progress(db, key)


def acknowledge_assertion(
    db: sqlite3.Connection, key: JournalTaskKey, assertion_id: str
) -> JournalProgress:
    """Acknowledge only an exactly bound, completed child; exact retries do not recount."""
    with _atomic(db):
        parent = _task(db, key)
        child = _one(
            db,
            "SELECT task_id,origin_entry_json,canonical_stream_ref,acknowledged "
            "FROM discovery_journal_assertions WHERE build_id=? AND source_key=? "
            "AND assertion_id=?",
            (key.build_id, key.source_key, assertion_id),
        )
        if child is None:
            raise StaleJournalTask("assertion is not a registered child of this source")
        task_key = FactTaskKey(key.build_id, child[0], key.source_key, key.worker_id)
        binding = fact_assertion_binding(db, task_key)
        if (
            binding.assertion_id != assertion_id
            or binding.origin_sha256 != json.loads(child[1])["json_sha256"]
            or binding.canonical_stream_ref != child[2]
            or not fact_task_progress(db, task_key).complete
        ):
            raise StaleFactTask("assertion child has no exactly bound completion")
        if not child[3]:
            if int(parent[13]) < 1 or parent[15]:
                raise StaleJournalTask("parent child accounting is inconsistent")
            db.execute(
                "UPDATE discovery_journal_assertions SET acknowledged=1 "
                "WHERE build_id=? AND source_key=? AND assertion_id=?",
                (key.build_id, key.source_key, assertion_id),
            )
            db.execute(
                "UPDATE discovery_journal_tasks SET pending=?,version=? "
                "WHERE build_id=? AND source_key=?",
                (str(int(parent[13]) - 1), str(int(parent[4]) + 1), key.build_id, key.source_key),
            )
        return journal_progress(db, key)


def finish_journal(
    db: sqlite3.Connection, key: JournalTaskKey, *, expected_version: int
) -> JournalProgress:
    """Record storage completion only after independent input and child terminals."""
    with _atomic(db):
        row = _task(db, key)
        progress = _progress(row)
        if type(expected_version) is not int or expected_version < 0:
            raise ValueError("expected journal version must be nonnegative")
        if progress.complete:
            if row[16] != str(expected_version):
                raise StaleJournalTask("journal terminal retry version changed")
            return progress
        if (
            progress.version != expected_version
            or progress.cursor.phase != "done"
            or progress.cursor.sequence != int(row[3])
            or progress.pending
            or row[14] != row[2]
        ):
            raise StaleJournalTask("journal input, child or version terminal is incomplete")
        if not progress.complete:
            db.execute(
                "UPDATE discovery_journal_tasks SET complete=1,version=?,terminal_version=? "
                "WHERE build_id=? AND source_key=?",
                (str(progress.version + 1), str(expected_version), key.build_id, key.source_key),
            )
        return journal_progress(db, key)


@dataclass(frozen=True, slots=True)
class JournalAssertion:
    key: FactTaskKey
    assertion_id: str
    sequence: int
    group: str
    item: int
    origin_entry_json: str
    canonical_stream_ref: str


@declare_growth(
    Declaration(
        id="riverhog-server.discovery-journal-pending-child.growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.canonical_discovery_journal_work.pending_journal_assertion",),
        scope="one source-qualified pending-child selection",
        starts_at="receive the current owner-leased source key",
        ends_at="close both point/range cursors and return at most one detached child",
        dimensions=(Dimension("registered_children", COUNT, "All retained source child rows."),),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "equal",
                Expression("constant", value=2, unit=COUNT),
                "One fenced parent header and one indexed pending-child selection.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                Expression("constant", value=2, unit=COUNT),
                "Each lookup detaches at most one complete metadata row.",
            ),
            Bound(
                "hashed_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "Selection does not interpret or hash any source or fact payload.",
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        Expression("constant", value=512, unit=COUNT),
                        Expression(
                            "product",
                            terms=(
                                Expression("constant", value=128, unit=COUNT),
                                Expression(
                                    "log2_ceil",
                                    terms=(
                                        Expression(
                                            "sum",
                                            terms=(
                                                Expression("dimension", name="registered_children"),
                                                Expression("constant", value=2, unit=COUNT),
                                            ),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                "Indexed selection excludes completed rows; VM constants depend on SQLite.",
            ),
        ),
        conditions=(
            "The owner maintains the current partial pending index and exact task keys.",
            "Sequence text is canonical nonnegative decimal in the existing Native domain.",
        ),
        invariants=(
            "Completed children are excluded by the partial index.",
            "Length then decimal text preserves arbitrary-precision Native sequence order.",
            "Children within one source entry retain direct grammar/item order.",
            "Selection grants no source authority or child completion.",
        ),
        limitations=(
            "SQL and returned rows exclude storage-engine/index comparisons and metadata bytes.",
            "Child extraction, source validation and scheduling have separate costs.",
        ),
    )
)
def pending_journal_assertion(
    db: sqlite3.Connection,
    key: JournalTaskKey,
) -> JournalAssertion | None:
    _task(db, key)
    row = _one(
        db,
        "SELECT task_id,assertion_id,born_sequence,source_group,source_item,"
        "origin_entry_json,canonical_stream_ref FROM discovery_journal_assertions "
        "WHERE build_id=? AND source_key=? AND acknowledged=0 "
        "ORDER BY length(born_sequence),born_sequence,source_group,source_item LIMIT 1",
        (key.build_id, key.source_key),
    )
    if row is None:
        return None
    return JournalAssertion(
        FactTaskKey(key.build_id, row[0], key.source_key, key.worker_id),
        row[1],
        int(row[2]),
        row[3],
        int(row[4]),
        row[5],
        row[6],
    )
