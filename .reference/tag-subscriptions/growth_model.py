"""Reference-only primitive work examples; NOT production state or a reconciler.

Exercises real SQLite statements and the current source-owned growth declaration
library. Catalog semantics remain in model.py; no competing follower is added.
Dimensions are measured by tests outside the operation, never inferred from a
budget chosen by the implementation. Production owners must qualify their own
statements, filesystem operations, transport and full composition.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Callable, Iterable

from riverhog_growth_contracts import (
    COUNT, Bound, Declaration, Dimension, Expression, declare_growth,
)


def _constant(value: int) -> Expression:
    return Expression("constant", value=value, unit=COUNT)


def _dimension(name: str) -> Expression:
    return Expression("dimension", name=name)


def _scale(factor: int, expression: Expression) -> Expression:
    return Expression("product", terms=(Expression("constant", value=factor), expression))


def _sum(*terms: Expression) -> Expression:
    return Expression("sum", terms=terms)


def _lookup_work() -> Expression:
    return Expression("product", terms=(
        _constant(128),
        Expression("log2_ceil", terms=(_sum(_dimension("rows"), _constant(2)),)),
    ))


def _query_contract(name: str, meaning: str) -> Declaration:
    return Declaration(
        id=f"reference.tag-subscriptions.{name}.growth/v1",
        authority="external-reference-only",
        subjects=(f"growth_model.OperationalModel.{name}",),
        scope="successful-native-sqlite-statement",
        starts_at="invoke the bounded query after fixture construction",
        ends_at="consume its bounded result and release its cursor",
        dimensions=(Dimension("rows", COUNT, "Actual relation cardinality before execution."),
                    Dimension("limit", COUNT, "Requested positive result carrier bound.")),
        bounds=(
            Bound("statements", COUNT, "equal", _constant(1), "One actual SQL statement."),
            Bound("returned_rows", COUNT, "upper", _dimension("limit"), "Rows actually decoded."),
            Bound("mutated_rows", COUNT, "equal", _constant(0), "Read-only query."),
            Bound("vm_instructions", COUNT, "upper",
                  _sum(_constant(256), _scale(64, _dimension("limit")), _lookup_work()),
                  "Conservative native instruction envelope for the declared indexed query."),
        ),
        conditions=("SQLite 3.46.1 observed here; requalify the actual supported SQLite builds.",
                    "Required indexes exist; no user triggers/extensions; scalar rows are bounded.",
                    "Fixture construction and external I/O are excluded; preparation callbacks, if emitted, are included."),
        invariants=(meaning, "No OFFSET, whole-resource count, payload column or filesystem read."),
        limitations=("This is not a production-operation declaration or an elapsed-time promise.",
                     "SQLite VM instructions do not measure B-tree page I/O, RAM or filesystem cost."),
    )


@dataclass
class SqlObservation:
    statements: int = 0
    returned_rows: int = 0
    mutated_rows: int = 0
    vm_instructions: int = 0

    def as_dict(self) -> dict[str, int]:
        return dict(vars(self))


class Measure:
    """Observe actual executed statements, decoded rows and SQLite instructions."""
    def __init__(self, db: sqlite3.Connection):
        self.db, self.observation = db, SqlObservation()

    def __enter__(self) -> SqlObservation:
        self.before = self.db.total_changes
        self.factory = self.db.row_factory
        def row_factory(cursor, row):
            self.observation.returned_rows += 1
            return sqlite3.Row(cursor, row)
        def trace(_statement):
            self.observation.statements += 1
        def step():
            self.observation.vm_instructions += 1
            return 0
        self.db.row_factory = row_factory
        self.db.set_trace_callback(trace)
        self.db.set_progress_handler(step, 1)
        return self.observation

    def __exit__(self, *args):
        self.observation.mutated_rows = self.db.total_changes - self.before
        self.db.set_trace_callback(None)
        self.db.set_progress_handler(None, 0)
        self.db.row_factory = self.factory


class CapacityPressure(RuntimeError):
    pass


class OperationalModel:
    """Minimal queue/history/cleanup experiment; no native scheduling or files."""
    def __init__(self, db: sqlite3.Connection):
        self.db = db
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        db.executescript("""
          CREATE TABLE work (
            id INTEGER PRIMARY KEY, state TEXT NOT NULL, due INTEGER NOT NULL,
            turn INTEGER NOT NULL, payload TEXT NOT NULL DEFAULT 'inert'
          );
          CREATE INDEX work_due ON work(state, due, turn, id);
          CREATE TABLE bookkeeping (
            id INTEGER PRIMARY KEY CHECK(id=1), serial INTEGER NOT NULL,
            pending INTEGER NOT NULL, turn INTEGER NOT NULL,
            history_count INTEGER NOT NULL, scratch_octets INTEGER NOT NULL
          );
          INSERT INTO bookkeeping VALUES(1,0,0,0,0,0);
          CREATE TABLE owners(id INTEGER PRIMARY KEY);
          CREATE TABLE garbage(
            owner INTEGER NOT NULL REFERENCES owners(id), ordinal INTEGER NOT NULL,
            PRIMARY KEY(owner, ordinal)
          );
          CREATE TABLE runs(id INTEGER PRIMARY KEY, finished INTEGER NOT NULL, summary BLOB NOT NULL);
          CREATE INDEX runs_time ON runs(finished,id);
          CREATE TABLE scratch(id INTEGER PRIMARY KEY, octets INTEGER NOT NULL);
        """)
        db.commit()

    @staticmethod
    def _limit(limit: int) -> None:
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("reference page carrier is 1..100")

    @declare_growth(
        _query_contract("select_due", "Return the earliest due work only.")
    )
    def select_due(self, now: int, *, limit: int = 100) -> list[sqlite3.Row]:
        self._limit(limit)
        return self.db.execute(
            "SELECT id,due,turn FROM work WHERE state='ready' AND due<=? "
            "ORDER BY due,turn,id LIMIT ?", (now, limit),
        ).fetchall()

    @declare_growth(_query_contract("select_page", "A keyset page never revisits its prior prefix."))
    def select_page(self, after: int, upper: int, *, limit: int = 100) -> list[sqlite3.Row]:
        self._limit(limit)
        return self.db.execute(
            "SELECT id,state,due FROM work WHERE id>? AND id<=? ORDER BY id LIMIT ?",
            (after, upper, limit),
        ).fetchall()

    def status(self) -> dict[str, int]:
        row = self.db.execute(
            "SELECT serial,pending,history_count,scratch_octets FROM bookkeeping WHERE id=1"
        ).fetchone()
        return dict(row)

    def admit(self, expected_serial: int, ids: Iterable[int], *, capacity: int) -> None:
        """Minimal cursor/obligation capacity transaction, with no tag interpretation."""
        from itertools import islice
        items = tuple(islice(iter(ids), 101))
        if len(items) > 100 or len(set(items)) != len(items):
            raise ValueError("invalid bounded admission batch")
        if type(capacity) is not int or capacity < 1:
            raise ValueError("invalid capacity")
        self.db.execute("BEGIN IMMEDIATE")
        try:
            row = self.db.execute("SELECT * FROM bookkeeping WHERE id=1").fetchone()
            if row["serial"] != expected_serial:
                raise RuntimeError("stale proposal")
            new = [i for i in items if self.db.execute("SELECT 1 FROM work WHERE id=?", (i,)).fetchone() is None]
            if row["pending"] + len(new) > capacity:
                raise CapacityPressure("cursor and obligations left unchanged")
            for index, item in enumerate(new, start=1):
                self.db.execute("INSERT INTO work(id,state,due,turn) VALUES(?,'ready',0,?)",
                                (item, row["turn"] + index))
            self.db.execute("UPDATE bookkeeping SET serial=serial+1,pending=pending+?,turn=turn+? WHERE id=1",
                            (len(new), len(new)))
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

    def finish_work(self, identity: int) -> None:
        with self.db:
            changed = self.db.execute("UPDATE work SET state='complete' WHERE id=? AND state='ready'",
                                      (identity,)).rowcount
            self.db.execute("UPDATE bookkeeping SET pending=pending-? WHERE id=1", (changed,))

    def defer(self, identity: int, *, now: int, delay: int) -> None:
        if type(delay) is not int or delay <= 0:
            raise ValueError("retry requires a positive delay")
        with self.db:
            self.db.execute("UPDATE bookkeeping SET turn=turn+1 WHERE id=1")
            self.db.execute("UPDATE work SET due=?,turn=(SELECT turn FROM bookkeeping WHERE id=1) "
                            "WHERE id=? AND state='ready'", (now + delay, identity))

    @declare_growth(Declaration(
        id="reference.tag-subscriptions.prune_children.growth/v1",
        authority="external-reference-only",
        subjects=("growth_model.OperationalModel.prune_children",),
        scope="successful-native-sqlite-cleanup-turn",
        starts_at="validate the subordinate deletion budget",
        ends_at="commit bounded child/parent removals",
        dimensions=(Dimension("rows", COUNT, "Actual child relation cardinality before deletion."),
                    Dimension("limit", COUNT, "Maximum subordinate plus parent removals.")),
        bounds=(
            Bound("statements", COUNT, "upper", _sum(_dimension("limit"), _constant(5)),
                  "Explicit statements including transaction control, not cascade parents."),
            Bound("returned_rows", COUNT, "upper", _dimension("limit"), "Decoded child rows."),
            Bound("mutated_rows", COUNT, "upper", _dimension("limit"), "Every actual row removal."),
            Bound("vm_instructions", COUNT, "upper",
                  _sum(_constant(512),_scale(256,_dimension("limit")),
                       Expression("product",terms=(
                           _sum(_dimension("limit"),_constant(1)),
                           Expression("log2_ceil",terms=(
                               _sum(_dimension("rows"),_constant(2)),)),
                           Expression("constant",value=128),))),
                  "Indexed selection/deletion and foreign-key checks in the native model."),
        ),
        conditions=("SQLite 3.46.1 qualified here, foreign keys enabled, no triggers/cascades.",
                    "Maintained (owner, ordinal) index and fixed-width child rows."),
        invariants=("The parent also consumes one deletion unit.",
                    "No live work or unowned file is a cleanup target in this model."),
        limitations=("No actual filesystem deletion, SQL page-I/O or native platform proof.",),
    ))
    def prune_children(self, owner: int, *, limit: int) -> int:
        """Count every subordinate deletion; the foreign key deliberately restricts."""
        self._limit(limit)
        with self.db:
            rows = self.db.execute("SELECT ordinal FROM garbage WHERE owner=? ORDER BY ordinal LIMIT ?",
                                   (owner, limit)).fetchall()
            self.db.executemany("DELETE FROM garbage WHERE owner=? AND ordinal=?",
                                ((owner, row["ordinal"]) for row in rows))
            if len(rows) < limit:
                # Parent deletion also consumes a unit from the same budget.
                changed = self.db.execute("DELETE FROM owners WHERE id=? AND NOT EXISTS "
                                          "(SELECT 1 FROM garbage WHERE owner=?)", (owner, owner)).rowcount
            else:
                changed = 0
        return len(rows) + changed

    def retain_run(self, identity: int, finished: int, summary: bytes, *, keep: int = 1000) -> None:
        if type(summary) is not bytes or len(summary) > 1024:
            raise ValueError("reference summary carrier is at most 1024 octets")
        if type(keep) is not int or keep < 1:
            raise ValueError("invalid retention count")
        with self.db:
            count = self.db.execute("SELECT history_count FROM bookkeeping WHERE id=1").fetchone()[0]
            if count > keep:
                raise CapacityPressure("lowered retention needs bounded maintenance")
            if count == keep:
                old = self.db.execute("SELECT id FROM runs ORDER BY finished,id LIMIT 1").fetchone()[0]
                self.db.execute("DELETE FROM runs WHERE id=?", (old,))
            else:
                self.db.execute("UPDATE bookkeeping SET history_count=history_count+1 WHERE id=1")
            self.db.execute("INSERT INTO runs VALUES(?,?,?)", (identity, finished, summary))

    def prune_runs(self, cutoff: int, *, limit: int) -> int:
        self._limit(limit)
        with self.db:
            rows = self.db.execute("SELECT id FROM runs WHERE finished<? ORDER BY finished,id LIMIT ?",
                                   (cutoff, limit)).fetchall()
            self.db.executemany("DELETE FROM runs WHERE id=?", ((r["id"],) for r in rows))
            self.db.execute("UPDATE bookkeeping SET history_count=history_count-? WHERE id=1", (len(rows),))
        return len(rows)

    def reserve_scratch(self, identity: int, octets: int, *, capacity: int) -> None:
        if any(type(x) is not int or x < 0 for x in (octets, capacity)):
            raise ValueError("capacity uses nonnegative exact octets")
        with self.db:
            existing = self.db.execute("SELECT octets FROM scratch WHERE id=?", (identity,)).fetchone()
            if existing is not None:
                if existing[0] != octets:
                    raise ValueError("a retained attempt cannot silently change reservation")
                return
            current = self.db.execute("SELECT scratch_octets FROM bookkeeping WHERE id=1").fetchone()[0]
            if current + octets > capacity:
                raise CapacityPressure("scratch admission deferred, not truncated")
            self.db.execute("INSERT INTO scratch VALUES(?,?)", (identity, octets))
            self.db.execute("UPDATE bookkeeping SET scratch_octets=scratch_octets+? WHERE id=1", (octets,))


def bounded_bytes(read: Callable[[int], bytes], consume: Callable[[bytes], None], *,
                  budget: int, chunk: int) -> int:
    """One exact byte-bounded turn; hash/custody checkpointing belongs to its owner."""
    if any(type(x) is not int or x <= 0 for x in (budget, chunk)):
        raise ValueError("byte budgets are positive integers")
    consumed = 0
    while consumed < budget:
        requested = min(chunk, budget - consumed)
        raw = read(requested)
        if type(raw) is not bytes or len(raw) > requested:
            raise ValueError("source exceeded its requested extent")
        if not raw:
            break
        consume(raw)
        consumed += len(raw)
    return consumed
