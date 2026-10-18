"""Private, resumable level/fuel cycle checking over an owned SQLite workspace."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Sequence
from contextlib import closing
from dataclasses import asdict, dataclass, field, replace
from typing import Any
from uuid import uuid4

from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    OCTETS_PER_COUNT,
    SCALAR,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)
from riverhog_provenance_contracts import require_canonical_uuid_urn

from .errors import ProvenanceValidationError

ALGORITHM = "riverhog-journal-online-level-fuel/v1"
_KINDS = {"derivation", "specialization"}
_PHASES = {
    "idle",
    "initialize",
    "back-pop",
    "back-edges",
    "clear-back",
    "promote",
    "clear-incoming",
    "forward-pop",
    "forward-edges",
    "add",
    "undo",
    "clean-undo",
    "clean-seen",
    "clean-queue",
    "complete",
}
_OUTCOMES = {None, "accepted", "cycle", "self"}
_RETIRE = (
    ("dag_levels", ("vertex",)),
    ("dag_edges", ("source", "target")),
    ("dag_incoming", ("target", "source")),
    ("dag_seen", ("operation_id", "vertex")),
    ("dag_queue", ("operation_id", "direction", "vertex")),
    ("dag_undo", ("operation_id", "relation", "first_key", "second_key")),
)


def _unit_declaration(*, identifier: str, method: str, retirement: bool) -> Declaration:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    budget = Expression("dimension", name="row_budget")

    def envelope(fixed: int, multiplier: int) -> Expression:
        return Expression(
            "sum",
            terms=(
                count(fixed),
                Expression(
                    "product",
                    terms=(Expression("constant", value=multiplier, unit=SCALAR), budget),
                ),
            ),
        )

    returned_rows = envelope(8, 2)
    return Declaration(
        id=identifier,
        authority="riverhog-provenance",
        subjects=(f"riverhog_provenance._online_dag.OnlineDag.{method}",),
        scope="native-sqlite-retired-online-graph-page"
        if retirement
        else "native-sqlite-online-level-fuel-transition",
        starts_at="load one exact owned run and admitted indexed metadata page",
        ends_at="close unit cursors and commit bounded mutations with the next fenced checkpoint",
        dimensions=(
            Dimension("retained_rows", COUNT, "Rows in the owned private graph workspace."),
            Dimension("row_budget", COUNT, "One to 64 admitted adjacency or cleanup rows."),
            Dimension(
                "metadata_row_octets",
                OCTETS_PER_COUNT,
                "Input-derived encoded-row envelope including owning identity/scalar widths "
                "and the next fixed-shape checkpoint.",
            ),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                envelope(24, 4),
                "Fixed phase control plus bounded point reads, writes and transaction statements.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                returned_rows,
                "One admitted page, bounded point metadata and fixed checkpoint reads.",
            ),
            Bound(
                "changed_rows",
                COUNT,
                "upper",
                envelope(12, 3),
                "The fenced checkpoint, bounded graph/undo/frontier writes and phase metadata.",
            ),
            Bound(
                "adjacency_rows",
                COUNT,
                "upper",
                budget,
                "At most one admitted backward or forward adjacency window.",
            ),
            Bound(
                "returned_metadata_octets",
                OCTETS,
                "upper",
                Expression(
                    "product",
                    terms=(
                        returned_rows,
                        Expression("dimension", name="metadata_row_octets"),
                    ),
                ),
                "Actual encoded result columns; large owning scalars keep their measured width.",
            ),
            Bound(
                "source_payload_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "The graph transition reads no source, assertion or credential payload.",
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        count(2048),
                        Expression(
                            "product",
                            terms=(Expression("constant", value=4096, unit=SCALAR), budget),
                        ),
                        Expression(
                            "product",
                            terms=(
                                count(256),
                                Expression(
                                    "log2_ceil",
                                    terms=(
                                        Expression(
                                            "sum",
                                            terms=(
                                                count(2),
                                                Expression("dimension", name="retained_rows"),
                                            ),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                "Maintained native SQLite instruction envelope for the indexed unit.",
            ),
        ),
        conditions=(
            "The owned workspace uses this algorithm's declared primary keys and fixed schema.",
            "The caller supplies the exact current run; metadata is owner-created execution state.",
            "The metadata-row envelope includes the current and next canonical checkpoint "
            "and actual owning identity/scalar widths.",
            "Caller source transactions are closed before the unit; the private write transaction "
            "begins only after detached planning.",
        ),
        invariants=(
            "Graph kinds and runs remain separate; stale work cannot advance "
            "or reclaim a replacement.",
            "An insertion remains quarantined through repair and complete rejection undo.",
            "The private graph outcome grants no archive, journal-prefix "
            "or source-release authority.",
            "No graph-sized clone, recursive reachability query "
            "or terminal COUNT is used by a unit.",
        ),
        limitations=(
            "Initialization, driver repetition, source custody, native semantics and other phases "
            "are separately charged.",
            "This unit profile does not certify a whole online stream's abstract amortized bound; "
            "actual graph visits, clearing, undo, indexed work and repeated rejection costs "
            "remain separately accounted.",
            "Encoded columns exclude Python/SQLite allocation overhead; instruction counts exclude "
            "internal B-tree comparisons, page I/O and lock waits.",
            "Durable restart requires an owner-retained crash-safe workspace "
            "and exact input binding; "
            "a disposable native projection is not durable source custody.",
        ),
    )


class StaleDagWork(RuntimeError):
    """The retained workspace no longer belongs to this exact run."""


@dataclass(slots=True)
class _Work:
    phase: str = "idle"
    operation_id: str | None = None
    source: str | None = None
    target: str | None = None
    a: str = "0"
    b: str = "0"
    fuel: str = "0"
    level: str = "0"
    active: str | None = None
    after: str | None = None
    promoting: str | None = None
    parent: str | None = None
    outcome: str | None = None

    def encoded(self) -> str:
        return canonical_json_bytes(asdict(self)).decode()

    @classmethod
    def decode(cls, raw: str) -> _Work:
        try:
            value = cls(**json.loads(raw))
            if value.encoded() != raw or value.phase not in _PHASES:
                raise ValueError
            if value.outcome not in _OUTCOMES:
                raise ValueError
            for name in ("a", "b", "fuel", "level"):
                number = getattr(value, name)
                if not isinstance(number, str) or int(number) < 0 or str(int(number)) != number:
                    raise ValueError
            for name in (
                "operation_id",
                "source",
                "target",
                "active",
                "after",
                "promoting",
                "parent",
            ):
                item = getattr(value, name)
                if item is not None and (not isinstance(item, str) or not item):
                    raise ValueError
            if value.phase != "idle" and not all((value.operation_id, value.source, value.target)):
                raise ValueError
            return value
        except (ValueError, TypeError, KeyError, AttributeError) as exc:
            raise RuntimeError("invalid retained online graph checkpoint") from exc


@dataclass(frozen=True, slots=True)
class Progress:
    operation_id: str | None
    version: int
    phase: str
    outcome: str | None
    adjacency_visits: int = 0
    stale: bool = False

    @property
    def complete(self) -> bool:
        return self.phase == "complete"


@dataclass(frozen=True, slots=True)
class _Snapshot:
    version: int
    work: _Work


@dataclass(slots=True)
class _Plan:
    before: _Snapshot
    work: _Work
    writes: list[tuple[str, tuple[Any, ...]]] = field(default_factory=list)
    adjacency_visits: int = 0

    def write(self, sql: str, *parameters: Any) -> None:
        self.writes.append((sql, parameters))


def install(db: sqlite3.Connection) -> None:
    """Explicitly initialize this private algorithm's state in a new owned workspace."""
    if db.in_transaction:
        raise RuntimeError("online graph initialization requires a closed transaction")
    statements = (
        "CREATE TABLE dag_algorithm(singleton INTEGER PRIMARY KEY CHECK(singleton = 1), "
        "algorithm TEXT NOT NULL)",
        "CREATE TABLE dag_runs(kind TEXT PRIMARY KEY, run_id TEXT NOT NULL, "
        "version TEXT NOT NULL, work TEXT NOT NULL)",
        "CREATE TABLE dag_levels(kind TEXT, run_id TEXT, vertex TEXT, level TEXT NOT NULL, "
        "PRIMARY KEY(kind, run_id, vertex))",
        "CREATE TABLE dag_edges(kind TEXT, run_id TEXT, source TEXT, target TEXT, "
        "PRIMARY KEY(kind, run_id, source, target))",
        "CREATE TABLE dag_incoming(kind TEXT, run_id TEXT, target TEXT, source TEXT, "
        "PRIMARY KEY(kind, run_id, target, source))",
        "CREATE TABLE dag_seen(kind TEXT, run_id TEXT, operation_id TEXT, vertex TEXT, "
        "PRIMARY KEY(kind, run_id, operation_id, vertex))",
        "CREATE TABLE dag_queue(kind TEXT, run_id TEXT, operation_id TEXT, direction TEXT, "
        "vertex TEXT, PRIMARY KEY(kind, run_id, operation_id, direction, vertex))",
        "CREATE TABLE dag_undo(kind TEXT, run_id TEXT, operation_id TEXT, relation TEXT, "
        "first_key TEXT, second_key TEXT, old_value TEXT, "
        "PRIMARY KEY(kind, run_id, operation_id, relation, first_key, second_key))",
        "CREATE TABLE dag_retired(kind TEXT, run_id TEXT, phase INTEGER NOT NULL, "
        "PRIMARY KEY(kind, run_id))",
    )
    db.execute("BEGIN IMMEDIATE").close()
    try:
        for statement in statements:
            db.execute(statement).close()
        db.execute("INSERT INTO dag_algorithm VALUES (1, ?)", (ALGORITHM,)).close()
        db.commit()
    except BaseException:
        db.rollback()
        raise


class OnlineDag:
    """Bounded private transitions; the owner supplies custody, durability and run IDs.

    A run is one insertion-only graph of one kind. In-flight changes and undo are
    quarantined until completion. Source replacement and real corrections use a
    distinct owned run; ordinary process replacement reopens the same run.
    """

    def __init__(self, db: sqlite3.Connection, kind: str, run_id: str) -> None:
        if kind not in _KINDS or not isinstance(run_id, str) or not run_id:
            raise ValueError("online graph requires an owned graph kind and run identity")
        self.db, self.kind, self.run_id = db, kind, run_id
        rows = self._rows("SELECT algorithm FROM dag_algorithm WHERE singleton = 1")
        if rows != [(ALGORITHM,)]:
            raise RuntimeError("online graph algorithm binding differs")

    def _rows(self, sql: str, parameters: Sequence[Any] = ()) -> list[tuple[Any, ...]]:
        with closing(self.db.execute(sql, parameters)) as cursor:
            return cursor.fetchall()

    @staticmethod
    def _budget(value: int) -> int:
        if type(value) is not int or not 1 <= value <= 64:
            raise ValueError("online graph step budget must be between one and 64")
        return value

    @staticmethod
    def _version(value: Any) -> int:
        try:
            if not isinstance(value, str) or int(value) < 0 or str(int(value)) != value:
                raise ValueError
            return int(value)
        except (ValueError, TypeError) as exc:
            raise RuntimeError("invalid online graph checkpoint version") from exc

    def _require_unused_run(self, run_id: str) -> None:
        for table, _keys in (*_RETIRE, ("dag_retired", ())):
            if self._rows(
                f"SELECT 1 FROM {table} WHERE kind = ? AND run_id = ? LIMIT 1",
                (self.kind, run_id),
            ):
                raise RuntimeError("replacement online graph identity is already retained")

    def bind(self) -> None:
        """Bind a new run, or verify a retained identical run without resetting it."""
        if self.db.in_transaction:
            raise RuntimeError("online graph binding requires a closed transaction")
        self.db.execute("BEGIN IMMEDIATE").close()
        try:
            rows = self._rows("SELECT run_id FROM dag_runs WHERE kind = ?", (self.kind,))
            if rows and rows[0][0] != self.run_id:
                raise StaleDagWork("online graph run changed")
            if not rows:
                self._require_unused_run(self.run_id)
                self.db.execute(
                    "INSERT INTO dag_runs VALUES (?, ?, '0', ?)",
                    (self.kind, self.run_id, _Work().encoded()),
                ).close()
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

    def _load(self) -> _Snapshot:
        if self.db.in_transaction:
            raise RuntimeError("online graph units require closed source transactions")
        rows = self._rows("SELECT run_id, version, work FROM dag_runs WHERE kind = ?", (self.kind,))
        if not rows or rows[0][0] != self.run_id:
            raise StaleDagWork("online graph run changed")
        return _Snapshot(self._version(rows[0][1]), _Work.decode(rows[0][2]))

    @staticmethod
    def _progress(snapshot: _Snapshot, *, visits: int = 0, stale: bool = False) -> Progress:
        work = snapshot.work
        return Progress(
            work.operation_id, snapshot.version, work.phase, work.outcome, visits, stale
        )

    def progress(self) -> Progress:
        return self._progress(self._load())

    def _apply(self, plan: _Plan) -> bool:
        """Apply one prepared unit inside its owner's checkpoint transaction."""
        if not self.db.in_transaction:
            raise RuntimeError("online graph application requires an owner transaction")
        with closing(
            self.db.execute(
                "UPDATE dag_runs SET version = ?, work = ? "
                "WHERE kind = ? AND run_id = ? AND version = ?",
                (
                    str(plan.before.version + 1),
                    plan.work.encoded(),
                    self.kind,
                    self.run_id,
                    str(plan.before.version),
                ),
            )
        ) as cursor:
            changed = cursor.rowcount
        if changed:
            for statement, parameters in plan.writes:
                self.db.execute(statement, parameters).close()
        return bool(changed)

    def _commit(self, plan: _Plan) -> Progress:
        if self.db.in_transaction:
            raise RuntimeError("online graph commit requires a closed source transaction")
        self.db.execute("BEGIN IMMEDIATE").close()
        try:
            if not self._apply(plan):
                self.db.rollback()
                return self._progress(self._load(), stale=True)
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        return self._progress(
            _Snapshot(plan.before.version + 1, plan.work), visits=plan.adjacency_visits
        )

    def _prepare_start(self, operation_id: str, source: str, target: str) -> _Plan:
        if not isinstance(operation_id, str) or not operation_id:
            raise ValueError("online graph operation requires an exact identity")
        require_canonical_uuid_urn(source)
        require_canonical_uuid_urn(target)
        before = self._load()
        work = before.work
        if work.operation_id == operation_id:
            if (work.source, work.target) != (source, target):
                raise RuntimeError("online graph operation inputs changed")
            return _Plan(before, replace(work))
        if work.phase not in ("idle", "complete"):
            raise RuntimeError("online graph operation is still quarantined")
        next_work = _Work(
            phase="initialize", operation_id=operation_id, source=source, target=target
        )
        if source == target:
            next_work.phase, next_work.outcome = "complete", "self"
        elif self._rows(
            "SELECT 1 FROM dag_edges WHERE kind = ? AND run_id = ? AND source = ? AND target = ?",
            (self.kind, self.run_id, source, target),
        ):
            next_work.phase, next_work.outcome = "complete", "accepted"
        return _Plan(before, next_work)

    def start(self, operation_id: str, source: str, target: str) -> Progress:
        """Admit exactly one native edge operation; repetition retains its continuation."""
        plan = self._prepare_start(operation_id, source, target)
        if plan.work == plan.before.work:
            return self._progress(plan.before)
        return self._commit(plan)

    def _undo(self, plan: _Plan, relation: str, first: str, second: str, old: str | None) -> None:
        plan.write(
            "INSERT OR IGNORE INTO dag_undo VALUES (?, ?, ?, ?, ?, ?, ?)",
            self.kind,
            self.run_id,
            plan.work.operation_id,
            relation,
            first,
            second,
            old,
        )

    def _enqueue(self, plan: _Plan, direction: str, vertex: str) -> None:
        plan.write(
            "INSERT OR IGNORE INTO dag_queue VALUES (?, ?, ?, ?, ?)",
            self.kind,
            self.run_id,
            plan.work.operation_id,
            direction,
            vertex,
        )

    def _incoming(self, plan: _Plan, target: str, source: str) -> None:
        rows = self._rows(
            "SELECT 1 FROM dag_incoming WHERE kind = ? AND run_id = ? "
            "AND target = ? AND source = ?",
            (self.kind, self.run_id, target, source),
        )
        if not rows:
            self._undo(plan, "incoming", target, source, None)
            plan.write(
                "INSERT INTO dag_incoming VALUES (?, ?, ?, ?)",
                self.kind,
                self.run_id,
                target,
                source,
            )

    def _level(self, vertex: str) -> str | None:
        rows = self._rows(
            "SELECT level FROM dag_levels WHERE kind = ? AND run_id = ? AND vertex = ?",
            (self.kind, self.run_id, vertex),
        )
        if not rows:
            return None
        value = rows[0][0]
        if not isinstance(value, str) or int(value) < 1 or str(int(value)) != value:
            raise RuntimeError("invalid online graph vertex level")
        return value

    def _pop(self, plan: _Plan, direction: str) -> str | None:
        rows = self._rows(
            "SELECT vertex FROM dag_queue WHERE kind = ? AND run_id = ? "
            "AND operation_id = ? AND direction = ? ORDER BY vertex LIMIT 1",
            (self.kind, self.run_id, plan.work.operation_id, direction),
        )
        if not rows:
            return None
        vertex = rows[0][0]
        if not isinstance(vertex, str):
            raise RuntimeError("invalid online graph queued vertex")
        try:
            require_canonical_uuid_urn(vertex)
        except ValueError as exc:
            raise RuntimeError("invalid online graph queued vertex") from exc
        plan.write(
            "DELETE FROM dag_queue WHERE kind = ? AND run_id = ? "
            "AND operation_id = ? AND direction = ? AND vertex = ?",
            self.kind,
            self.run_id,
            plan.work.operation_id,
            direction,
            vertex,
        )
        return vertex

    def _cleanup(self, plan: _Plan, table: str, keys: tuple[str, ...], budget: int) -> bool:
        columns = ", ".join(keys)
        rows = self._rows(
            f"SELECT {columns} FROM {table} WHERE kind = ? AND run_id = ? "
            f"AND operation_id = ? ORDER BY {columns} LIMIT ?",
            (self.kind, self.run_id, plan.work.operation_id, budget),
        )
        predicates = " AND ".join(key + " = ?" for key in keys)
        for row in rows:
            plan.write(
                f"DELETE FROM {table} WHERE kind = ? AND run_id = ? "
                f"AND operation_id = ? AND {predicates}",
                self.kind,
                self.run_id,
                plan.work.operation_id,
                *row,
            )
        return len(rows) < budget

    def _prepare(self, budget: int) -> _Plan:
        budget = self._budget(budget)
        before = self._load()
        work = replace(before.work)
        plan = _Plan(before, work)
        phase = work.phase
        scope = (self.kind, self.run_id)
        operation = (*scope, work.operation_id)

        if phase in ("idle", "complete"):
            return plan
        assert work.source is not None and work.target is not None
        if phase == "initialize":
            levels = {}
            for vertex in (work.source, work.target):
                old = self._level(vertex)
                levels[vertex] = old or "1"
                if old is None:
                    self._undo(plan, "level", vertex, "", None)
                    plan.write("INSERT INTO dag_levels VALUES (?, ?, ?, '1')", *scope, vertex)
            work.a, work.b = levels[work.source], levels[work.target]
            work.fuel = work.a
            if int(work.a) < int(work.b):
                work.phase = "add"
            else:
                plan.write("INSERT INTO dag_seen VALUES (?, ?, ?, ?)", *operation, work.source)
                self._enqueue(plan, "back", work.source)
                work.phase = "back-pop"
        elif phase == "back-pop":
            work.active = self._pop(plan, "back")
            work.after = None
            if work.active is not None:
                work.phase = "back-edges"
            elif work.a == work.b:
                work.phase = "add"
            else:
                work.level, work.promoting, work.parent = work.a, work.target, None
                work.phase = "promote"
        elif phase == "back-edges":
            assert work.active is not None
            after = "" if work.after is None else work.after
            rows = self._rows(
                "SELECT incoming.source, seen.vertex FROM dag_incoming AS incoming "
                "LEFT JOIN dag_seen AS seen ON seen.kind = incoming.kind "
                "AND seen.run_id = incoming.run_id AND seen.operation_id = ? "
                "AND seen.vertex = incoming.source WHERE incoming.kind = ? "
                "AND incoming.run_id = ? AND incoming.target = ? "
                "AND incoming.source > ? ORDER BY incoming.source LIMIT ?",
                (work.operation_id, *scope, work.active, after, budget),
            )
            consumed = 0
            for predecessor, seen in rows:
                if int(work.fuel) == 0:
                    work.level = str(int(work.a) + 1)
                    work.phase = "clear-back"
                    break
                plan.adjacency_visits += 1
                consumed += 1
                work.after = predecessor
                if predecessor == work.target:
                    work.phase, work.outcome = "undo", "cycle"
                    break
                work.fuel = str(int(work.fuel) - 1)
                if seen is None:
                    plan.write("INSERT INTO dag_seen VALUES (?, ?, ?, ?)", *operation, predecessor)
                    self._enqueue(plan, "back", predecessor)
            if consumed == len(rows) and len(rows) < budget and work.phase == "back-edges":
                work.active, work.after, work.phase = None, None, "back-pop"
        elif phase == "clear-back":
            rows = self._rows(
                "SELECT vertex FROM dag_queue WHERE kind = ? AND run_id = ? "
                "AND operation_id = ? AND direction = 'back' ORDER BY vertex LIMIT ?",
                (*operation, budget),
            )
            for (vertex,) in rows:
                plan.write(
                    "DELETE FROM dag_queue WHERE kind = ? AND run_id = ? "
                    "AND operation_id = ? AND direction = 'back' AND vertex = ?",
                    *operation,
                    vertex,
                )
            if len(rows) < budget:
                work.active, work.after = None, None
                work.promoting, work.parent, work.phase = work.target, None, "promote"
        elif phase == "promote":
            assert work.promoting is not None
            old = self._level(work.promoting)
            if old is None or int(old) >= int(work.level):
                raise RuntimeError("online graph promotion lost its lower-level vertex")
            self._undo(plan, "level", work.promoting, "", old)
            plan.write(
                "UPDATE dag_levels SET level = ? WHERE kind = ? AND run_id = ? AND vertex = ?",
                work.level,
                *scope,
                work.promoting,
            )
            work.phase = "clear-incoming"
        elif phase == "clear-incoming":
            assert work.promoting is not None
            rows = self._rows(
                "SELECT source FROM dag_incoming WHERE kind = ? AND run_id = ? "
                "AND target = ? ORDER BY source LIMIT ?",
                (*scope, work.promoting, budget),
            )
            for (predecessor,) in rows:
                self._undo(plan, "incoming", work.promoting, predecessor, "1")
                plan.write(
                    "DELETE FROM dag_incoming WHERE kind = ? AND run_id = ? "
                    "AND target = ? AND source = ?",
                    *scope,
                    work.promoting,
                    predecessor,
                )
            if len(rows) < budget:
                if work.parent is not None:
                    # Its old horizontal entry may be among this same page's deletes.
                    self._undo(plan, "incoming", work.promoting, work.parent, None)
                    plan.write(
                        "INSERT OR IGNORE INTO dag_incoming VALUES (?, ?, ?, ?)",
                        *scope,
                        work.promoting,
                        work.parent,
                    )
                self._enqueue(plan, "forward", work.promoting)
                work.promoting, work.parent = None, None
                work.phase = "forward-pop" if work.active is None else "forward-edges"
        elif phase == "forward-pop":
            work.active = self._pop(plan, "forward")
            work.after = None
            work.phase = "add" if work.active is None else "forward-edges"
        elif phase == "forward-edges":
            assert work.active is not None
            rows = self._rows(
                "SELECT edges.target, levels.level, seen.vertex FROM dag_edges AS edges "
                "LEFT JOIN dag_levels AS levels ON levels.kind = edges.kind "
                "AND levels.run_id = edges.run_id AND levels.vertex = edges.target "
                "LEFT JOIN dag_seen AS seen ON seen.kind = edges.kind "
                "AND seen.run_id = edges.run_id AND seen.operation_id = ? "
                "AND seen.vertex = edges.target WHERE edges.kind = ? AND edges.run_id = ? "
                "AND edges.source = ? AND edges.target > ? ORDER BY edges.target LIMIT ?",
                (work.operation_id, *scope, work.active, work.after or "", budget),
            )
            for successor, level, seen in rows:
                plan.adjacency_visits += 1
                work.after = successor
                if seen is not None:
                    work.phase, work.outcome = "undo", "cycle"
                    break
                if level is None:
                    raise RuntimeError("online graph edge lost its endpoint level")
                if int(level) < int(work.level):
                    work.promoting, work.parent, work.phase = successor, work.active, "promote"
                    break
                if int(level) == int(work.level):
                    self._incoming(plan, successor, work.active)
            if len(rows) < budget and work.phase == "forward-edges":
                work.active, work.after, work.phase = None, None, "forward-pop"
        elif phase == "add":
            first, second = self._level(work.source), self._level(work.target)
            if first is None or second is None or int(first) > int(second):
                raise RuntimeError("online graph insertion has no repaired level order")
            self._undo(plan, "edge", work.source, work.target, None)
            plan.write(
                "INSERT INTO dag_edges VALUES (?, ?, ?, ?)", *scope, work.source, work.target
            )
            if first == second:
                self._incoming(plan, work.target, work.source)
            work.outcome, work.phase = "accepted", "clean-undo"
        elif phase == "undo":
            rows = self._rows(
                "SELECT relation, first_key, second_key, old_value FROM dag_undo "
                "WHERE kind = ? AND run_id = ? AND operation_id = ? "
                "ORDER BY relation, first_key, second_key LIMIT ?",
                (*operation, budget),
            )
            for relation, first, second, old in rows:
                if relation == "level":
                    if old is None:
                        plan.write(
                            "DELETE FROM dag_levels WHERE kind = ? AND run_id = ? AND vertex = ?",
                            *scope,
                            first,
                        )
                    else:
                        plan.write(
                            "INSERT OR REPLACE INTO dag_levels VALUES (?, ?, ?, ?)",
                            *scope,
                            first,
                            old,
                        )
                elif relation in ("edge", "incoming"):
                    table = "dag_edges" if relation == "edge" else "dag_incoming"
                    keys = ("source", "target") if relation == "edge" else ("target", "source")
                    if old is None:
                        plan.write(
                            f"DELETE FROM {table} WHERE kind = ? AND run_id = ? "
                            f"AND {keys[0]} = ? AND {keys[1]} = ?",
                            *scope,
                            first,
                            second,
                        )
                    elif old == "1":
                        plan.write(
                            f"INSERT OR IGNORE INTO {table} VALUES (?, ?, ?, ?)",
                            *scope,
                            first,
                            second,
                        )
                    else:
                        raise RuntimeError("invalid online graph edge before-image")
                else:
                    raise RuntimeError("invalid online graph before-image relation")
                plan.write(
                    "DELETE FROM dag_undo WHERE kind = ? AND run_id = ? AND operation_id = ? "
                    "AND relation = ? AND first_key = ? AND second_key = ?",
                    *operation,
                    relation,
                    first,
                    second,
                )
            if len(rows) < budget:
                work.phase = "clean-seen"
        elif phase == "clean-undo":
            if self._cleanup(plan, "dag_undo", ("relation", "first_key", "second_key"), budget):
                work.phase = "clean-seen"
        elif phase == "clean-seen":
            if self._cleanup(plan, "dag_seen", ("vertex",), budget):
                work.phase = "clean-queue"
        elif phase == "clean-queue":
            if self._cleanup(plan, "dag_queue", ("direction", "vertex"), budget):
                work.active, work.after, work.promoting, work.parent = None, None, None, None
                work.phase = "complete"
        else:
            raise RuntimeError("invalid retained online graph phase")
        return plan

    @declare_growth(
        _unit_declaration(
            identifier="riverhog-provenance.sqlite-online-graph-transition.growth/v1",
            method="advance",
            retirement=False,
        )
    )
    def advance(self, budget: int = 64) -> Progress:
        plan = self._prepare(budget)
        if plan.work.phase in ("idle", "complete") and plan.work == plan.before.work:
            return self._progress(plan.before)
        return self._commit(plan)

    def result(self, operation_id: str) -> str:
        state = self._load()
        if state.work.operation_id != operation_id:
            raise StaleDagWork("online graph operation changed")
        if state.work.phase != "complete" or state.work.outcome is None:
            raise RuntimeError("online graph operation remains quarantined")
        return state.work.outcome

    def add(self, source: str, target: str) -> None:
        """Drive the same private transitions to the native accepted/rejected boundary."""
        operation = uuid4().hex
        progress = self.start(operation, source, target)
        while not progress.complete:
            progress = self.advance()
        outcome = self.result(operation)
        if outcome == "self":
            raise ProvenanceValidationError(f"{self.kind} cannot relate an entity to itself")
        if outcome == "cycle":
            raise ProvenanceValidationError(f"{self.kind} graph contains a cycle")

    def replace_run(self, run_id: str) -> OnlineDag:
        """Fence a replaced owned graph; retire its private rows separately."""
        if not isinstance(run_id, str) or not run_id or run_id == self.run_id:
            raise ValueError("replacement graph requires a distinct owned run identity")
        before = self._load()
        self._require_unused_run(run_id)
        self.db.execute("BEGIN IMMEDIATE").close()
        try:
            with closing(
                self.db.execute(
                    "UPDATE dag_runs SET run_id = ?, version = ?, work = ? "
                    "WHERE kind = ? AND run_id = ? AND version = ?",
                    (
                        run_id,
                        str(before.version + 1),
                        _Work().encoded(),
                        self.kind,
                        self.run_id,
                        str(before.version),
                    ),
                )
            ) as cursor:
                if not cursor.rowcount:
                    self.db.rollback()
                    raise StaleDagWork("online graph replacement lost its current fence")
            self.db.execute(
                "INSERT INTO dag_retired VALUES (?, ?, 0)", (self.kind, self.run_id)
            ).close()
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        return OnlineDag(self.db, self.kind, run_id)

    @declare_growth(
        _unit_declaration(
            identifier="riverhog-provenance.sqlite-online-graph-retirement.growth/v1",
            method="retire",
            retirement=True,
        )
    )
    def retire(self, budget: int = 64) -> bool:
        """Remove one bounded page from an older run; never touch the active graph."""
        budget = self._budget(budget)
        before = self._load()
        rows = self._rows(
            "SELECT run_id, phase FROM dag_retired WHERE kind = ? ORDER BY run_id LIMIT 1",
            (self.kind,),
        )
        if not rows:
            return False
        retired, phase = rows[0]
        if retired == self.run_id or type(phase) is not int or not 0 <= phase < len(_RETIRE):
            raise RuntimeError("invalid retired online graph binding")
        table, keys = _RETIRE[phase]
        columns = ", ".join(keys)
        page = self._rows(
            f"SELECT {columns} FROM {table} WHERE kind = ? AND run_id = ? "
            f"ORDER BY {columns} LIMIT ?",
            (self.kind, retired, budget),
        )
        plan = _Plan(before, replace(before.work))
        predicates = " AND ".join(key + " = ?" for key in keys)
        for row in page:
            plan.write(
                f"DELETE FROM {table} WHERE kind = ? AND run_id = ? AND {predicates}",
                self.kind,
                retired,
                *row,
            )
        if len(page) < budget:
            if phase + 1 == len(_RETIRE):
                plan.write(
                    "DELETE FROM dag_retired WHERE kind = ? AND run_id = ? AND phase = ?",
                    self.kind,
                    retired,
                    phase,
                )
            else:
                plan.write(
                    "UPDATE dag_retired SET phase = ? WHERE kind = ? AND run_id = ? AND phase = ?",
                    phase + 1,
                    self.kind,
                    retired,
                    phase,
                )
        self._commit(plan)
        return True
