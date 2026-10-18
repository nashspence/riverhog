"""Checkpointed retirement and reseeding of a private correction projection.

The owner has already checked the correction's targets and staged its exact
effective assertions. These phases own aggregate reclamation and the changed-row
directory. Native semantic replay and source custody retain their own lifetimes.
"""

from __future__ import annotations

import json
import re
import sqlite3
from contextlib import closing
from dataclasses import dataclass

from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    OCTETS_PER_COUNT,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)
from riverhog_provenance_contracts import require_canonical_uuid_urn

ALGORITHM = "riverhog-correction-projection/v1"
MAX_ROWS = 64
# Phase, UUID cursor and decimal work version have fixed shape. The version
# counts work over a SHA-256-message-sized journal's private projection; this
# envelope exceeds the resulting scalar representation without limiting journals.
_HEADER_OCTETS = 2048
_TABLES = ("maps", "findings", "fixities", "lifecycle_extrema")
_ROW_OCTETS = {
    "maps": "length(CAST(name AS BLOB)) + length(key) + length(value)",
    "findings": "length(CAST(key AS BLOB)) + length(CAST(value AS BLOB))",
    "fixities": "length(CAST(state AS BLOB)) + length(CAST(size AS BLOB)) "
    "+ length(CAST(sha256 AS BLOB))",
    "lifecycle_extrema": "length(CAST(state_id AS BLOB)) + length(CAST(earliest AS BLOB)) "
    "+ coalesce(length(CAST(latest_usage AS BLOB)), 0)",
}


def _declaration(operation: str) -> Declaration:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def dimension(name: str) -> Expression:
        return Expression("dimension", name=name)

    budget = dimension("row_budget")
    statement_budget = Expression(
        "sum",
        terms=(count(13), Expression("product", terms=(Expression("constant", value=2), budget))),
    )
    return Declaration(
        id=f"riverhog-provenance.sqlite-correction-{operation}.growth/v1",
        authority="riverhog-provenance",
        subjects=(f"riverhog_provenance._correction_projection.CorrectionWork.{operation}",),
        scope=f"native-private-sqlite-correction-{operation}-checkpoint",
        starts_at="load one owned phase header and admit a bounded row/byte batch",
        ends_at="commit exactly that batch and its next fenced phase checkpoint",
        dimensions=(
            Dimension("retained_rows", COUNT, "Rows resident in the relevant indexed relations."),
            Dimension("row_budget", COUNT, "Admitted row quantum, one to 64."),
            Dimension("byte_budget", OCTETS, "Admitted logical row bytes for this quantum."),
            Dimension("max_atom_octets", OCTETS, "Largest native row atom inspected here."),
        ),
        bounds=(
            Bound("sql_statements", COUNT, "upper", statement_budget, "Indexed row work and CAS."),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                Expression("sum", terms=(count(3), budget)),
                "Header/cursor links and at most the row quantum of key/length records.",
            ),
            Bound(
                "changed_rows",
                COUNT,
                "upper",
                Expression("sum", terms=(count(1), budget)),
                "At most one phase checkpoint and one change per admitted key.",
            ),
            Bound(
                "metadata_octets",
                OCTETS,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        Expression("constant", value=2 * _HEADER_OCTETS + 128, unit=OCTETS),
                        Expression(
                            "product",
                            terms=(
                                budget,
                                Expression("constant", value=128, unit=OCTETS_PER_COUNT),
                            ),
                        ),
                    ),
                ),
                "Fixed header/fence reads and bounded rowid/length or canonical UUID metadata.",
            ),
            Bound(
                "admitted_octets",
                OCTETS,
                "upper",
                dimension("byte_budget"),
                "Logical row bytes retired, or changed-directory keys offered for insertion.",
            ),
            Bound(
                "inspected_octets",
                OCTETS,
                "upper",
                Expression("sum", terms=(dimension("byte_budget"), dimension("max_atom_octets"))),
                "Admitted row lengths and at most one next atom that does not fit the quantum.",
            ),
            Bound(
                "payload_fetched_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "No assertion or aggregate payload is returned to Python.",
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
                            terms=(
                                Expression("constant", value=512),
                                Expression("sum", terms=(budget, count(1))),
                                Expression(
                                    "log2_ceil",
                                    terms=(
                                        Expression(
                                            "sum", terms=(dimension("retained_rows"), count(2))
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                "Point/range seeks, bounded index maintenance and fixed checkpoint control.",
            ),
        ),
        conditions=(
            "The trusted owner retains the exact source, catalog/modes and private projection.",
            "Native assertion identities are canonical UUID URNs; source row atoms keep their "
            "existing entry-derived bounds and are never treated as a total archive limit.",
            "No other owner mutates these quarantined relations outside the same work fence.",
            "Logical row lengths include index keys and stored values; native SQLite physical "
            "rowids address existing rows and are not journal sequence or cardinality counters.",
        ),
        invariants=(
            "Preparation closes every cursor before computation or the write transaction.",
            "A stale proposal cannot change rows or advance a replacement attempt.",
            "The accepted journal prefix is unchanged by these private projection phases.",
            "Missing/corrupt progress is an explicit failure; ordinary recovery uses the "
            "same exact owned checkpoint without rescanning completed rows.",
        ),
        limitations=(
            "SQLite length inspection and deletion/index work charge the actual row/key/value "
            "bytes; VM counts do not establish physical I/O, CPU, fsync or elapsed-time bounds.",
            "With adequate atom admission each retired row is deleted once, with at most one "
            "extra length inspection at a byte boundary. Each real correction still rebuilds "
            "its active view; correction count and view size are independent dimensions.",
            "Target checks, assertion staging, graph retirement, native semantic replay and "
            "terminal acceptance are separate phases. This is not a whole-validator bound "
            "or a durable source/semantic-session custody claim.",
            "Capacity failures, rejected/corrupt state, lock waits and retries are separately "
            "observable work, not semantic invalidity or successful completion.",
        ),
    )


class StaleCorrectionWork(RuntimeError):
    """The supplied operation or proposal no longer owns its private checkpoint."""


class CorrectionCapacity(RuntimeError):
    """The next exact row atom needs a larger work-byte admission."""


@dataclass(frozen=True)
class CorrectionState:
    phase: str
    after: str | None = None
    version: int = 0

    @property
    def done(self) -> bool:
        return self.phase == "done"

    def to_json(self) -> str:
        return canonical_json_bytes(
            {
                "phase": self.phase,
                "after": self.after,
                "version": str(self.version),
            }
        ).decode()

    @classmethod
    def from_json(cls, raw: str, operation: str) -> CorrectionState:
        try:
            if not isinstance(raw, str) or len(raw) > _HEADER_OCTETS:
                raise ValueError("checkpoint envelope")
            value = json.loads(raw)
            if not isinstance(value, dict) or set(value) != {"phase", "after", "version"}:
                raise ValueError("checkpoint shape")
            phases = (*_TABLES, "done") if operation == "retire" else ("seed", "done")
            if value["phase"] not in phases:
                raise ValueError("checkpoint phase")
            if (
                not isinstance(value["version"], str)
                or re.fullmatch(r"0|[1-9][0-9]*", value["version"]) is None
            ):
                raise ValueError("checkpoint version")
            if value["after"] is not None:
                if operation != "seed":
                    raise ValueError("checkpoint cursor")
                require_canonical_uuid_urn(value["after"])
            state = cls(value["phase"], value["after"], int(value["version"]))
            if state.to_json() != raw:
                raise ValueError("noncanonical checkpoint")
            return state
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
            raise RuntimeError("corrupt correction projection checkpoint") from exc


@dataclass(frozen=True)
class CorrectionBatch:
    operation: str
    attempt_id: str
    source_binding: str
    before_json: str
    after: CorrectionState
    keys: tuple[int | str, ...]


def install(db: sqlite3.Connection) -> None:
    db.executescript(
        "CREATE TABLE journal_correction_work(operation TEXT PRIMARY KEY, "
        "algorithm TEXT NOT NULL, attempt_id TEXT NOT NULL, binding TEXT NOT NULL, "
        "checkpoint TEXT NOT NULL);"
        "CREATE INDEX active_changed_seed ON assertions(assertion_id) WHERE active = 1;"
    )


class CorrectionWork:
    def __init__(
        self, db: sqlite3.Connection, *, operation: str, attempt_id: str, source_binding: str
    ) -> None:
        if operation not in {"retire", "seed"}:
            raise ValueError("unknown correction projection operation")
        if not isinstance(attempt_id, str) or not attempt_id:
            raise ValueError("an explicit correction attempt is required")
        if not isinstance(source_binding, str) or not source_binding:
            raise ValueError("an exact owned correction source binding is required")
        self.db = db
        self.operation = operation
        self.attempt_id = attempt_id
        self.source_binding = source_binding

    def _require_detached(self) -> None:
        if self.db.in_transaction:
            raise RuntimeError("correction work requires a closed owner transaction")

    def initialize(self) -> CorrectionState:
        """Explicitly start a new operation after any preceding one completed."""
        self._require_detached()
        try:
            self.db.execute("BEGIN IMMEDIATE").close()
            with closing(
                self.db.execute(
                    "SELECT algorithm, attempt_id, substr(checkpoint, 1, ?) "
                    "FROM journal_correction_work WHERE operation = ?",
                    (_HEADER_OCTETS + 1, self.operation),
                )
            ) as cursor:
                old = cursor.fetchone()
            version = 0
            if old is not None:
                if old[0] != ALGORITHM:
                    raise RuntimeError("unknown correction projection algorithm")
                prior = CorrectionState.from_json(old[2], self.operation)
                if old[1] == self.attempt_id or not prior.done:
                    raise StaleCorrectionWork("correction operation is already bound")
                version = prior.version + 1
            state = CorrectionState(
                "maps" if self.operation == "retire" else "seed", version=version
            )
            self.db.execute(
                "INSERT INTO journal_correction_work VALUES (?, ?, ?, ?, ?) "
                "ON CONFLICT(operation) DO UPDATE SET algorithm=excluded.algorithm, "
                "attempt_id=excluded.attempt_id, binding=excluded.binding, "
                "checkpoint=excluded.checkpoint",
                (self.operation, ALGORITHM, self.attempt_id, self.source_binding, state.to_json()),
            ).close()
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        return state

    def _read_header(self) -> CorrectionState:
        self._require_detached()
        with closing(
            self.db.execute(
                "SELECT algorithm, substr(checkpoint, 1, ?) FROM journal_correction_work "
                "WHERE operation = ? AND attempt_id = ? AND binding = ?",
                (_HEADER_OCTETS + 1, self.operation, self.attempt_id, self.source_binding),
            )
        ) as cursor:
            row = cursor.fetchone()
        if row is None:
            raise StaleCorrectionWork("correction checkpoint differs from its owned inputs")
        if row[0] != ALGORITHM:
            raise RuntimeError("unknown correction projection algorithm")
        return CorrectionState.from_json(row[1], self.operation)

    def load(self) -> CorrectionState:
        state = self._read_header()
        if self.operation == "retire":
            completed = len(_TABLES) if state.done else _TABLES.index(state.phase)
            for table in _TABLES[:completed]:
                with closing(
                    self.db.execute(
                        f"SELECT rowid FROM {table} WHERE rowid >= ? ORDER BY rowid LIMIT 1",
                        (-(2**63),),
                    )
                ) as cursor:
                    if cursor.fetchone() is not None:
                        raise RuntimeError("correction checkpoint has an unretired prior relation")
        else:
            if state.after is not None:
                for sql in (
                    "SELECT assertion_id FROM changes WHERE assertion_id = ?",
                    "SELECT assertion_id FROM assertions INDEXED BY active_changed_seed "
                    "WHERE active = 1 AND assertion_id = ?",
                ):
                    with closing(self.db.execute(sql, (state.after,))) as cursor:
                        if cursor.fetchone() != (state.after,):
                            raise RuntimeError(
                                "correction seed has a missing or changed cursor link"
                            )
            if state.done:
                with closing(
                    self.db.execute(
                        "SELECT assertion_id FROM assertions INDEXED BY active_changed_seed "
                        "WHERE active = 1 AND assertion_id > ? ORDER BY assertion_id LIMIT 1",
                        (state.after or "",),
                    )
                ) as cursor:
                    if cursor.fetchone() is not None:
                        raise RuntimeError("correction seed completed before its active tail")
        return state

    def prepare(self, *, row_budget: int = MAX_ROWS, byte_budget: int = 65536) -> CorrectionBatch:
        if type(row_budget) is not int or not 1 <= row_budget <= MAX_ROWS:
            raise ValueError("correction row budget must be between one and 64")
        if type(byte_budget) is not int or byte_budget < 1:
            raise ValueError("correction byte budget must be positive")
        state = self.load()
        keys: list[int | str] = []
        admitted = 0
        after, phase = state.after, state.phase
        if not state.done and self.operation == "retire":
            # rowid is the existing SQLite physical address, never a source ordinal.
            last = -(2**63)
            for _ in range(row_budget):
                comparison = ">=" if not keys else ">"
                with closing(
                    self.db.execute(
                        f"SELECT rowid, {_ROW_OCTETS[phase]} FROM {phase} "
                        f"WHERE rowid {comparison} ? ORDER BY rowid LIMIT 1",
                        (last,),
                    )
                ) as cursor:
                    row = cursor.fetchone()
                if row is None:
                    if not keys:
                        ordinal = _TABLES.index(phase) + 1
                        phase = _TABLES[ordinal] if ordinal < len(_TABLES) else "done"
                    break
                key, octets = row
                if type(key) is not int or type(octets) is not int or octets < 0:
                    raise RuntimeError("corrupt native aggregate row metadata")
                if admitted + octets > byte_budget:
                    if not keys:
                        raise CorrectionCapacity("next correction row exceeds work-byte admission")
                    break
                keys.append(key)
                admitted += octets
                last = key
        elif not state.done:
            # A native canonical UUID URN occupies exactly 45 UTF-8 octets.
            limit = min(row_budget, max(1, byte_budget // 45))
            with closing(
                self.db.execute(
                    "SELECT assertion_id FROM assertions INDEXED BY active_changed_seed "
                    "WHERE active = 1 AND assertion_id > ? ORDER BY assertion_id LIMIT ?",
                    (after or "", limit),
                )
            ) as cursor:
                rows = cursor.fetchall()
            for (identity,) in rows:
                require_canonical_uuid_urn(identity)
                octets = len(identity.encode())
                if admitted + octets > byte_budget:
                    if not keys:
                        raise CorrectionCapacity(
                            "next changed identity exceeds work-byte admission"
                        )
                    break
                keys.append(identity)
                admitted += octets
            if keys:
                after = str(keys[-1])
            else:
                phase = "done"
        next_state = state if state.done else CorrectionState(phase, after, state.version + 1)
        return CorrectionBatch(
            self.operation,
            self.attempt_id,
            self.source_binding,
            state.to_json(),
            next_state,
            tuple(keys),
        )

    def commit(self, batch: CorrectionBatch) -> CorrectionState:
        self._require_detached()
        if (batch.operation, batch.attempt_id, batch.source_binding) != (
            self.operation,
            self.attempt_id,
            self.source_binding,
        ):
            raise StaleCorrectionWork("correction proposal belongs to another operation")
        before = CorrectionState.from_json(batch.before_json, self.operation)
        if before.done:
            # A finished worker still verifies its fence before reporting completion.
            current = self._read_header()
            if current != before:
                raise StaleCorrectionWork("finished correction proposal was superseded")
            return current
        try:
            self.db.execute("BEGIN IMMEDIATE").close()
            with closing(
                self.db.execute(
                    "UPDATE journal_correction_work SET checkpoint = ? WHERE operation = ? "
                    "AND algorithm = ? AND attempt_id = ? AND binding = ? AND checkpoint = ?",
                    (
                        batch.after.to_json(),
                        self.operation,
                        ALGORITHM,
                        self.attempt_id,
                        self.source_binding,
                        batch.before_json,
                    ),
                )
            ) as cursor:
                if cursor.rowcount != 1:
                    raise StaleCorrectionWork("correction proposal was superseded")
            for key in batch.keys:
                if self.operation == "retire":
                    with closing(
                        self.db.execute(
                            f"DELETE FROM {before.phase} WHERE rowid = ?",
                            (key,),
                        )
                    ) as cursor:
                        if cursor.rowcount != 1:
                            raise RuntimeError(
                                "correction aggregate changed outside its work fence"
                            )
                else:
                    self.db.execute("INSERT OR IGNORE INTO changes VALUES (?)", (key,)).close()
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        return batch.after

    @declare_growth(_declaration("retire"))
    def retire(self, *, row_budget: int = MAX_ROWS, byte_budget: int = 65536) -> CorrectionState:
        if self.operation != "retire":
            raise ValueError("retirement requires its owning correction operation")
        return self.commit(self.prepare(row_budget=row_budget, byte_budget=byte_budget))

    @declare_growth(_declaration("seed"))
    def seed(self, *, row_budget: int = MAX_ROWS, byte_budget: int = 65536) -> CorrectionState:
        if self.operation != "seed":
            raise ValueError("seeding requires its owning correction operation")
        return self.commit(self.prepare(row_budget=row_budget, byte_budget=byte_budget))
