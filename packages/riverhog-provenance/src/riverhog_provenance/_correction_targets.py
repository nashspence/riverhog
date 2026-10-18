"""Bounded exact-target verification and retirement in a private correction.

The caller has admitted the original entry and completed its native schema and
prefix checks. Construction decodes that one entry atom. Work units use only its
targets and retained frame-reference metadata; they never reload prior payloads.
This does not supply source custody or a whole resumable semantic validator.
"""

from __future__ import annotations

import json
import re
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from typing import TYPE_CHECKING

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

from ._journal_prefix import PrefixState, _reference
from .constants import PROVENANCE_JOURNAL_ENTRY_BYTES_MAX
from .errors import ProvenanceValidationError

if TYPE_CHECKING:
    from .journal import JournalFrame

ALGORITHM = "riverhog-correction-targets/v1"
MAX_ROWS = 64
# The owning prefix codec already admits a 2048-character header containing its
# exact predecessor. These private headers contain only phases and decimal work
# cursors/versions. Entry-local target ordinals are bounded by admitted entry bytes,
# not by a journal or archive cardinality ceiling.
_HEADER_OCTETS = 2048
_ASSERTION_OCTETS = (
    "length(CAST(a.assertion_id AS BLOB)) + length(CAST(a.object_id AS BLOB)) + "
    "length(CAST(a.category AS BLOB)) + length(CAST(a.type AS BLOB)) + "
    "coalesce(length(CAST(a.state_id AS BLOB)), 0) + "
    "coalesce(length(CAST(a.observation_id AS BLOB)), 0) + "
    "length(CAST(a.sequence AS BLOB)) + length(a.value) + 1"
)


def _declaration() -> Declaration:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def dimension(name: str) -> Expression:
        return Expression("dimension", name=name)

    def multiple(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=value), term))

    budget = dimension("row_budget")
    return Declaration(
        id="riverhog-provenance.sqlite-correction-targets.growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance._correction_targets.CorrectionTargets.advance",),
        scope="native-private-sqlite-correction-target-work-unit",
        starts_at="load one owned target phase and admit a row/byte-bounded unit",
        ends_at="commit its target directory or retirement writes with its exact checkpoint",
        dimensions=(
            Dimension("retained_rows", COUNT, "Resident assertion, frame and target index rows."),
            Dimension("row_budget", COUNT, "Admitted row quantum, one to 64."),
            Dimension("byte_budget", OCTETS, "Logical source/directory/assertion byte admission."),
            Dimension("max_atom_octets", OCTETS, "Largest logical work atom inspected."),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                Expression("sum", terms=(count(32), multiple(5, budget))),
                "Indexed metadata reads, row writes and fenced checkpoint control.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                Expression("sum", terms=(count(12), multiple(3, budget))),
                "Fixed header/boundary links and indexed per-target metadata.",
            ),
            Bound(
                "changed_rows",
                COUNT,
                "upper",
                Expression("sum", terms=(count(1), budget)),
                "One checkpoint and at most one mutation for each admitted target.",
            ),
            Bound(
                "metadata_octets",
                OCTETS,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        Expression("constant", value=8 * _HEADER_OCTETS, unit=OCTETS),
                        Expression(
                            "product",
                            terms=(
                                budget,
                                Expression(
                                    "constant", value=_HEADER_OCTETS + 256, unit=OCTETS_PER_COUNT
                                ),
                            ),
                        ),
                    ),
                ),
                "Fixed headers, retained reference footers and bounded native scalar/key metadata.",
            ),
            Bound(
                "admitted_octets",
                OCTETS,
                "upper",
                dimension("byte_budget"),
                "Verification charges exact target octets; retirement charges the "
                "actual assertion row and directory; cleanup charges directory rows.",
            ),
            Bound(
                "inspected_octets",
                OCTETS,
                "upper",
                Expression("sum", terms=(dimension("byte_budget"), dimension("max_atom_octets"))),
                "Admitted logical atoms and at most one next atom at a byte boundary.",
            ),
            Bound(
                "payload_fetched_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "No prior frame or assertion payload is returned to Python.",
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        count(8192),
                        multiple(
                            1024,
                            Expression(
                                "product",
                                terms=(
                                    Expression("sum", terms=(budget, count(8))),
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
                ),
                "Indexed point/range work, bounded index maintenance and checkpoint control.",
            ),
        ),
        conditions=(
            "A trusted owner retains the exact admitted entry and authenticated private workspace.",
            "The native prefix/schema checks precede this phase; frame reference "
            "footers were published with their original accepted bytes.",
            "No owner changes quarantined semantic rows outside this work fence; "
            "the accepted prefix must remain exact throughout the operation.",
            "The existing entry envelope bounds construction and target "
            "ordinals; it is not a total journal limit.",
        ),
        invariants=(
            "Every exact earlier reference, liveness check and duplicate check "
            "succeeds before any target is deactivated.",
            "Native target order selects the first semantic error, including "
            "duplicates across work units.",
            "Readers close before publication; rows and the next exact "
            "source/attempt/prefix checkpoint commit atomically.",
            "The accepted journal prefix is unchanged; a stale worker cannot "
            "mutate a replacement operation.",
        ),
        limitations=(
            "Construction decodes and hashes the one current admitted entry "
            "atom, outside this primitive; it never replays prior source frames.",
            "SQLite assertion retirement charges actual logical row bytes even "
            "though payload values are not fetched; VM counts are not physical "
            "CPU, I/O or fsync guarantees.",
            "With adequate admission each target is verified, retired and "
            "reclaimed once, plus bounded unit and manifest overhead; real "
            "correction aggregate rebuilds retain their separate cost.",
            "Assertion staging, semantic replay, source custody, failed/corrupt "
            "attempts, lock waits and whole-session recovery retain separate "
            "ownership and evidence.",
        ),
    )


class StaleCorrectionTargets(RuntimeError):
    """This operation no longer owns the exact correction and accepted prefix."""


class CorrectionTargetCapacity(RuntimeError):
    """The next logical atom requires a larger byte admission."""


@dataclass(frozen=True)
class TargetState:
    phase: str = "verify"
    position: int = 0
    version: int = 0

    @property
    def done(self) -> bool:
        return self.phase == "done"

    def to_json(self) -> str:
        return canonical_json_bytes(
            {"phase": self.phase, "position": str(self.position), "version": str(self.version)}
        ).decode()

    @classmethod
    def from_json(cls, raw: str, count: int) -> TargetState:
        try:
            if type(raw) is not str or len(raw) > _HEADER_OCTETS:
                raise ValueError("checkpoint envelope")
            value = json.loads(raw)
            if type(value) is not dict or set(value) != {"phase", "position", "version"}:
                raise ValueError("checkpoint shape")
            if value["phase"] not in {"verify", "apply", "clean", "done"}:
                raise ValueError("checkpoint phase")
            for field in ("position", "version"):
                if (
                    type(value[field]) is not str
                    or re.fullmatch(r"0|[1-9][0-9]*", value[field]) is None
                ):
                    raise ValueError("checkpoint scalar")
            state = cls(value["phase"], int(value["position"]), int(value["version"]))
            if (
                state.position > count
                or state.done
                and state.position != count
                or state.to_json() != raw
            ):
                raise ValueError("checkpoint cursor")
            return state
        except (ValueError, TypeError, KeyError, OverflowError) as exc:
            raise RuntimeError("corrupt correction target checkpoint") from exc


@dataclass(frozen=True)
class TargetBatch:
    attempt_id: str
    source_binding: str
    prefix_json: str
    before_json: str
    after: TargetState
    rows: tuple[tuple[int, str, str], ...]


def install(db: sqlite3.Connection) -> None:
    db.executescript(
        "CREATE TABLE journal_correction_targets(singleton INTEGER PRIMARY "
        "KEY CHECK(singleton = 1), "
        "algorithm TEXT NOT NULL, attempt_id TEXT NOT NULL, binding TEXT NOT NULL, "
        "target_count INTEGER NOT NULL, prefix TEXT NOT NULL, checkpoint TEXT NOT NULL);"
        "CREATE TABLE correction_targets(position INTEGER PRIMARY KEY, "
        "assertion_id TEXT NOT NULL UNIQUE, sequence TEXT NOT NULL);"
    ).close()


class CorrectionTargets:
    def __init__(self, db: sqlite3.Connection, *, attempt_id: str, frame: JournalFrame) -> None:
        if type(attempt_id) is not str or not attempt_id:
            raise ValueError("an explicit correction attempt is required")
        if len(frame.json_bytes) > PROVENANCE_JOURNAL_ENTRY_BYTES_MAX:
            raise ValueError("correction source exceeds its owning entry envelope")
        document = frame.document
        if document["entry_kind"] != "correction":
            raise ValueError("target work requires an admitted correction entry")
        self.db, self.attempt_id = db, attempt_id
        self.source_binding = frame.sha256
        self.targets = document["body"]["retracts"]
        self.sequence, self.journal_id = document["sequence"], document["journal_id"]
        self.count = len(self.targets)

    def _detached(self) -> None:
        if self.db.in_transaction:
            raise RuntimeError("correction targets require a closed owner transaction")

    def _prefix(self) -> str:
        with closing(
            self.db.execute(
                "SELECT substr(checkpoint, 1, ?) FROM journal_prefix WHERE singleton = 1",
                (_HEADER_OCTETS + 1,),
            )
        ) as cursor:
            row = cursor.fetchone()
        if row is None:
            raise RuntimeError("missing accepted prefix for correction targets")
        checkpoint = row[0]
        if not isinstance(checkpoint, str):
            raise RuntimeError("invalid accepted prefix for correction targets")
        state = PrefixState.from_json(checkpoint)
        if str(state.frame_count) != self.sequence or state.journal_id != self.journal_id:
            raise StaleCorrectionTargets("correction differs from the current accepted prefix")
        return checkpoint

    def initialize(self) -> TargetState:
        """Explicitly admit a fresh operation; restoring uses load(), never reset."""
        self._detached()
        try:
            self.db.execute("BEGIN IMMEDIATE").close()
            prefix = self._prefix()
            with closing(
                self.db.execute(
                    "SELECT algorithm, attempt_id, target_count, substr(checkpoint, 1, "
                    "?) FROM journal_correction_targets WHERE singleton = 1",
                    (_HEADER_OCTETS + 1,),
                )
            ) as cursor:
                old = cursor.fetchone()
            version = 0
            if old is not None:
                if old[0] != ALGORITHM:
                    raise RuntimeError("unknown correction target algorithm")
                prior = TargetState.from_json(old[3], old[2])
                if old[1] == self.attempt_id or not prior.done:
                    raise StaleCorrectionTargets("correction target operation is already bound")
                version = prior.version + 1
            with closing(
                self.db.execute(
                    "SELECT position FROM correction_targets WHERE position >= 0 ORDER "
                    "BY position LIMIT 1"
                )
            ) as cursor:
                if cursor.fetchone() is not None:
                    raise RuntimeError("unreclaimed correction target directory")
            state = TargetState(version=version)
            self.db.execute(
                "INSERT INTO journal_correction_targets VALUES (1, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(singleton) DO UPDATE SET algorithm=excluded.algorithm, "
                "attempt_id=excluded.attempt_id, binding=excluded.binding, "
                "target_count=excluded.target_count, prefix=excluded.prefix, "
                "checkpoint=excluded.checkpoint",
                (
                    ALGORITHM,
                    self.attempt_id,
                    self.source_binding,
                    self.count,
                    prefix,
                    state.to_json(),
                ),
            ).close()
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        return state

    def _header(self) -> tuple[TargetState, str]:
        self._detached()
        with closing(
            self.db.execute(
                "SELECT algorithm, target_count, substr(prefix, 1, ?), "
                "substr(checkpoint, 1, ?) FROM journal_correction_targets WHERE "
                "singleton = 1 AND attempt_id = ? AND binding = ?",
                (_HEADER_OCTETS + 1, _HEADER_OCTETS + 1, self.attempt_id, self.source_binding),
            )
        ) as cursor:
            row = cursor.fetchone()
        if row is None:
            raise StaleCorrectionTargets("correction target inputs were superseded")
        if row[0] != ALGORITHM:
            raise RuntimeError("unknown correction target algorithm")
        if type(row[1]) is not int or row[1] != self.count:
            raise RuntimeError("correction target count differs from its source")
        if row[2] != self._prefix():
            raise StaleCorrectionTargets("accepted prefix changed during correction work")
        return TargetState.from_json(row[3], self.count), row[2]

    def _source_row(self, row: tuple[int, str, str]) -> None:
        position, identity, sequence = row
        if type(position) is not int or not 0 <= position < self.count:
            raise RuntimeError("correction target directory ordinal differs from source")
        target = self.targets[position]
        if (identity, sequence) != (target["assertion_id"], target["entry"]["sequence"]):
            raise RuntimeError("correction target directory differs from its exact source")

    def _load(self) -> tuple[TargetState, str]:
        state, prefix = self._header()
        edges = []
        for order in ("ASC", "DESC"):
            with closing(
                self.db.execute(
                    f"SELECT position, assertion_id, sequence FROM correction_targets "
                    f"WHERE position >= 0 ORDER BY position {order} LIMIT 1"
                )
            ) as cursor:
                edges.append(cursor.fetchone())
        first, last = edges
        end = state.position if state.phase == "verify" else self.count
        start = state.position if state.phase in {"clean", "done"} else 0
        if start == end:
            if first is not None or last is not None:
                raise RuntimeError("correction target directory has unexpected rows")
        else:
            if first is None or last is None or first[0] != start or last[0] != end - 1:
                raise RuntimeError("correction target directory has missing boundary links")
            self._source_row(first)
            self._source_row(last)
        if state.phase == "apply" and state.position:
            with closing(
                self.db.execute(
                    "SELECT t.position, t.assertion_id, t.sequence, a.sequence, a.active "
                    "FROM correction_targets t LEFT JOIN assertions a ON a.assertion_id "
                    "= t.assertion_id WHERE t.position = ?",
                    (state.position - 1,),
                )
            ) as cursor:
                link = cursor.fetchone()
            if link is None:
                raise RuntimeError("missing retired correction cursor link")
            self._source_row(link[:3])
            if link[3:] != (link[2], 0):
                raise RuntimeError("changed retired correction cursor link")
        if state.phase in {"clean", "done"} and self.count:
            target = self.targets[max(0, state.position - 1)]
            with closing(
                self.db.execute(
                    "SELECT sequence, active FROM assertions WHERE assertion_id = ?",
                    (target["assertion_id"],),
                )
            ) as cursor:
                link = cursor.fetchone()
            if link != (target["entry"]["sequence"], 0):
                raise RuntimeError("changed correction reclamation cursor link")
        return state, prefix

    def load(self) -> TargetState:
        return self._load()[0]

    def prepare(self, *, row_budget: int = MAX_ROWS, byte_budget: int = 65536) -> TargetBatch:
        if type(row_budget) is not int or not 1 <= row_budget <= MAX_ROWS:
            raise ValueError("correction target row budget must be between one and 64")
        if type(byte_budget) is not int or byte_budget < 1:
            raise ValueError("correction target byte budget must be positive")
        state, prefix = self._load()
        rows: list[tuple[int, str, str]] = []
        admitted, position, phase = 0, state.position, state.phase
        seen: set[str] = set()
        if not state.done:
            for _ in range(row_budget):
                if position == self.count:
                    if not rows:
                        phase, position = {
                            "verify": ("apply", 0),
                            "apply": ("clean", 0),
                            "clean": ("done", self.count),
                        }[phase]
                    break
                if phase == "verify":
                    target = self.targets[position]
                    identity = target["assertion_id"]
                    candidate = (position, identity, target["entry"]["sequence"])
                    octets = len(canonical_json_bytes(target))
                else:
                    if phase == "apply":
                        sql = (
                            f"SELECT t.position, t.assertion_id, t.sequence, a.sequence, "
                            f"a.active, {_ASSERTION_OCTETS} FROM correction_targets t LEFT JOIN "
                            f"assertions a ON a.assertion_id = t.assertion_id WHERE t.position >= "
                            f"? ORDER BY t.position LIMIT 1"
                        )
                    else:
                        sql = (
                            "SELECT t.position, t.assertion_id, t.sequence, a.sequence, a.active "
                            "FROM correction_targets t LEFT JOIN assertions a "
                            "ON a.assertion_id = t.assertion_id "
                            "WHERE t.position >= ? ORDER BY t.position LIMIT 1"
                        )
                    with closing(self.db.execute(sql, (position,))) as cursor:
                        row = cursor.fetchone()
                    if row is None or row[0] != position:
                        raise RuntimeError("missing correction target work row")
                    candidate = row[:3]
                    self._source_row(candidate)
                    octets = sum(len(str(value).encode()) for value in candidate)
                    if phase == "apply":
                        if row[3:5] != (candidate[2], 1) or type(row[5]) is not int or row[5] < 0:
                            raise RuntimeError("correction target changed outside its work fence")
                        octets += row[5]
                    elif row[3:5] != (candidate[2], 0):
                        raise RuntimeError("correction target is not retired during reclamation")
                if admitted + octets > byte_budget:
                    if not rows:
                        raise CorrectionTargetCapacity(
                            "next correction target atom exceeds admission"
                        )
                    break
                if phase == "verify":
                    with closing(
                        self.db.execute(
                            "SELECT 1 FROM correction_targets WHERE assertion_id = ?", (identity,)
                        )
                    ) as cursor:
                        duplicate = cursor.fetchone() is not None
                    if identity in seen or duplicate:
                        raise ProvenanceValidationError("duplicate correction target")
                    seen.add(identity)
                    with closing(
                        self.db.execute(
                            "SELECT a.sequence, a.active, substr(f.reference, 1, ?) FROM "
                            "assertions a LEFT JOIN frames f ON f.sequence = a.sequence WHERE "
                            "a.assertion_id = ?",
                            (_HEADER_OCTETS + 1, identity),
                        )
                    ) as cursor:
                        prior = cursor.fetchone()
                    if prior is None:
                        raise ProvenanceValidationError(
                            "correction target has no exact earlier assertion"
                        )
                    try:
                        if type(prior[2]) is not bytes or len(prior[2]) > _HEADER_OCTETS:
                            raise ValueError("missing or oversized reference footer")
                        reference = _reference(json.loads(prior[2]))
                        if (
                            canonical_json_bytes(reference) != prior[2]
                            or reference["sequence"] != prior[0]
                            or int(prior[0]) >= int(self.sequence)
                            or prior[1] not in (0, 1)
                        ):
                            raise ValueError("reference differs from accepted assertion")
                    except (ValueError, TypeError, KeyError) as exc:
                        raise RuntimeError("corrupt retained correction reference") from exc
                    if reference != target["entry"]:
                        raise ProvenanceValidationError(
                            "correction target has no exact earlier assertion"
                        )
                    if not prior[1]:
                        raise ProvenanceValidationError(
                            "correction cannot reactivate or retract an already retired assertion"
                        )
                rows.append(candidate)
                admitted += octets
                position += 1
        after = state if state.done else TargetState(phase, position, state.version + 1)
        return TargetBatch(
            self.attempt_id, self.source_binding, prefix, state.to_json(), after, tuple(rows)
        )

    def commit(self, batch: TargetBatch) -> TargetState:
        self._detached()
        if (batch.attempt_id, batch.source_binding) != (self.attempt_id, self.source_binding):
            raise StaleCorrectionTargets("correction proposal belongs to another input")
        before = TargetState.from_json(batch.before_json, self.count)
        if before.done:
            current, prefix = self._header()
            if current != before or prefix != batch.prefix_json:
                raise StaleCorrectionTargets("finished correction proposal was superseded")
            return current
        try:
            self.db.execute("BEGIN IMMEDIATE").close()
            with closing(
                self.db.execute(
                    "UPDATE journal_correction_targets SET checkpoint = ? WHERE "
                    "singleton = 1 AND algorithm = ? AND attempt_id = ? AND binding = ? "
                    "AND target_count = ? AND prefix = ? AND checkpoint = ? AND "
                    "EXISTS(SELECT 1 FROM journal_prefix WHERE singleton = 1 AND "
                    "checkpoint = ?)",
                    (
                        batch.after.to_json(),
                        ALGORITHM,
                        self.attempt_id,
                        self.source_binding,
                        self.count,
                        batch.prefix_json,
                        batch.before_json,
                        batch.prefix_json,
                    ),
                )
            ) as cursor:
                if cursor.rowcount != 1:
                    raise StaleCorrectionTargets("correction target proposal was superseded")
            parameters: tuple[int | str, ...]
            for row in batch.rows:
                if before.phase == "verify":
                    sql, parameters = "INSERT INTO correction_targets VALUES (?, ?, ?)", row
                elif before.phase == "apply":
                    sql, parameters = (
                        "UPDATE assertions SET active = 0 WHERE assertion_id = ? AND "
                        "sequence = ? AND active = 1",
                        row[1:],
                    )
                else:
                    sql, parameters = (
                        "DELETE FROM correction_targets WHERE position = ? AND assertion_id "
                        "= ? AND sequence = ?",
                        row,
                    )
                with closing(self.db.execute(sql, parameters)) as cursor:
                    if cursor.rowcount != 1:
                        raise RuntimeError("correction target changed outside its work fence")
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        return batch.after

    @declare_growth(_declaration())
    def advance(self, *, row_budget: int = MAX_ROWS, byte_budget: int = 65536) -> TargetState:
        return self.commit(self.prepare(row_budget=row_budget, byte_budget=byte_budget))
