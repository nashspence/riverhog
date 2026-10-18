"""Checkpoint native post-row checks without accepting an in-flight prefix.

The owner has staged and natively checked the effective rows. This private phase
preserves their UUID order, the lifecycle index's type/physical-row order, and
the reverse-reference index's physical-row order. It does not provide source
custody, whole-session recovery, or application publication authority.
"""

from __future__ import annotations

import json
import re
import sqlite3
from contextlib import closing
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any

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

from ._journal_prefix import PrefixState
from .common import fingerprint
from .constants import PROVENANCE_JOURNAL_ENTRY_BYTES_MAX
from .errors import ProvenanceValidationError
from .graph import _Validator, time_ns

if TYPE_CHECKING:
    from .journal import JournalFrame
    from .journal_store import JournalStore

ALGORITHM = "riverhog-semantic-finish/v1"
MAX_ROWS = 64
_HEADER_OCTETS = 2048
_BINDING_OCTETS = 8192
_PHASES = {"row", "lifecycle", "invalidation", "usage", "content", "externals", "done"}


class StaleSemanticFinish(RuntimeError):
    """The exact semantic operation, source or accepted prefix was replaced."""


class SemanticFinishCapacity(RuntimeError):
    """The next existing entry-bounded atom needs more byte admission."""


@dataclass(frozen=True)
class FinishState:
    phase: str = "row"
    completed: str | None = None
    current: str | None = None
    scan: int | None = None
    generated_at: str | None = None
    invalidated_at: str | None = None
    version: int = 0

    @property
    def done(self) -> bool:
        return self.phase == "done"

    def to_json(self) -> str:
        return canonical_json_bytes(
            {
                "phase": self.phase,
                "completed": self.completed,
                "current": self.current,
                "scan": None if self.scan is None else str(self.scan),
                "generated_at": self.generated_at,
                "invalidated_at": self.invalidated_at,
                "version": str(self.version),
            }
        ).decode()

    @classmethod
    def from_json(cls, raw: str) -> FinishState:
        try:
            if type(raw) is not str or len(raw) > _HEADER_OCTETS:
                raise ValueError("checkpoint envelope")
            value = json.loads(raw)
            if (
                type(value) is not dict
                or set(value)
                != {
                    "phase",
                    "completed",
                    "current",
                    "scan",
                    "generated_at",
                    "invalidated_at",
                    "version",
                }
                or value["phase"] not in _PHASES
            ):
                raise ValueError("checkpoint shape")
            for name in ("completed", "current"):
                if value[name] is not None:
                    require_canonical_uuid_urn(value[name])
            version = value["version"]
            if type(version) is not str or re.fullmatch(r"0|[1-9][0-9]*", version) is None:
                raise ValueError("checkpoint version")
            scan = value["scan"]
            if scan is not None and (
                type(scan) is not str
                or re.fullmatch(r"0|-?[1-9][0-9]*", scan) is None
                or not -(2**63) <= int(scan) <= 2**63 - 1
            ):
                raise ValueError("physical SQLite cursor")
            state = cls(
                value["phase"],
                value["completed"],
                value["current"],
                None if scan is None else int(scan),
                value["generated_at"],
                value["invalidated_at"],
                int(version),
            )
            if (state.phase in {"row", "done"}) != (state.current is None):
                raise ValueError("current assertion cursor")
            if (
                state.current is not None
                and state.completed is not None
                and state.current <= state.completed
            ):
                raise ValueError("assertion ordering")
            if state.scan is not None and state.phase not in {"invalidation", "usage", "externals"}:
                raise ValueError("scan phase")
            for stamp in (state.generated_at, state.invalidated_at):
                if stamp is not None:
                    if type(stamp) is not str or len(stamp) > 40:
                        raise ValueError("native timestamp envelope")
                    time_ns(stamp)
                    if state.phase not in {"invalidation", "usage"}:
                        raise ValueError("diagnostic timestamp phase")
            if state.to_json() != raw:
                raise ValueError("noncanonical checkpoint")
            return state
        except (ValueError, TypeError, KeyError, OverflowError) as exc:
            raise RuntimeError("corrupt semantic finish checkpoint") from exc


@dataclass(frozen=True)
class FinishBatch:
    binding: bytes
    attempt_id: str
    prefix: tuple[str, bytes, str]
    before_json: str
    after: FinishState
    writes: tuple[tuple[str, tuple[Any, ...]], ...]


def install(db: sqlite3.Connection) -> None:
    db.execute(
        "CREATE TABLE journal_semantic_finish(singleton INTEGER PRIMARY KEY CHECK(singleton=1), "
        "algorithm TEXT NOT NULL, attempt_id TEXT NOT NULL, binding BLOB NOT NULL, "
        "prefix_attempt TEXT NOT NULL, prefix_binding BLOB NOT NULL, prefix TEXT NOT NULL, "
        "checkpoint TEXT NOT NULL)"
    ).close()
    db.execute(
        "CREATE INDEX lifecycle_bound_times ON maps(name,key,json_extract(value,'$.at')) "
        "WHERE name IN ('generation','invalidation')"
    ).close()


class SemanticFinish:
    def __init__(
        self,
        store: JournalStore,
        *,
        attempt_id: str,
        frame: JournalFrame,
        catalog_sha256: str,
        require_profiles: bool,
    ) -> None:
        if type(attempt_id) is not str or not attempt_id:
            raise ValueError("an explicit semantic finish attempt is required")
        if type(catalog_sha256) is not str or re.fullmatch(r"[0-9a-f]{64}", catalog_sha256) is None:
            raise ValueError("an exact semantic catalog identity is required")
        if type(require_profiles) is not bool:
            raise ValueError("an exact profile mode is required")
        if len(frame.json_bytes) > PROVENANCE_JOURNAL_ENTRY_BYTES_MAX:
            raise ValueError("semantic source exceeds its owning entry envelope")
        document = frame.document
        self.store, self.attempt_id = store, attempt_id
        self.sequence, self.journal_id = document["sequence"], document["journal_id"]
        self.binding = canonical_json_bytes(
            {
                "algorithm": ALGORITHM,
                "source_sha256": frame.sha256,
                "catalog_sha256": catalog_sha256,
                "require_profiles": require_profiles,
            }
        )

    @property
    def db(self) -> sqlite3.Connection:
        return self.store.db

    def _detached(self) -> None:
        if self.db.in_transaction:
            raise RuntimeError("semantic finish requires a closed owner transaction")

    def _one(self, sql: str, parameters: tuple[Any, ...] = ()) -> Any:
        with closing(self.db.execute(sql, parameters)) as rows:
            return rows.fetchone()

    def _prefix(self) -> tuple[str, bytes, str]:
        row = self._one(
            "SELECT substr(attempt_id,1,?), substr(binding,1,?), substr(checkpoint,1,?) "
            "FROM journal_prefix WHERE singleton=1",
            (_HEADER_OCTETS + 1, _BINDING_OCTETS + 1, _HEADER_OCTETS + 1),
        )
        if (
            row is None
            or type(row[0]) is not str
            or not row[0]
            or len(row[0]) > _HEADER_OCTETS
            or type(row[1]) is not bytes
            or len(row[1]) > _BINDING_OCTETS
        ):
            raise RuntimeError("missing or invalid semantic accepted-prefix manifest")
        state = PrefixState.from_json(row[2])
        if str(state.frame_count) != self.sequence or state.journal_id not in (
            None,
            self.journal_id,
        ):
            raise StaleSemanticFinish("semantic frame differs from its accepted prefix")
        return row[0], row[1], row[2]

    def initialize(self) -> FinishState:
        """Admit a new phase explicitly; restoration uses load without resetting."""
        self._detached()
        try:
            self.db.execute("BEGIN IMMEDIATE").close()
            prefix = self._prefix()
            old = self._one(
                "SELECT algorithm, attempt_id, substr(checkpoint,1,?) "
                "FROM journal_semantic_finish WHERE singleton=1",
                (_HEADER_OCTETS + 1,),
            )
            version = 0
            if old is not None:
                if old[0] != ALGORITHM:
                    raise RuntimeError("unknown semantic finish algorithm")
                prior = FinishState.from_json(old[2])
                if old[1] == self.attempt_id or not prior.done:
                    raise StaleSemanticFinish("semantic finish operation is already bound")
                version = prior.version + 1
            state = FinishState(version=version)
            self.db.execute(
                "INSERT INTO journal_semantic_finish VALUES (1,?,?,?,?,?,?,?) "
                "ON CONFLICT(singleton) DO UPDATE SET algorithm=excluded.algorithm, "
                "attempt_id=excluded.attempt_id, binding=excluded.binding, "
                "prefix_attempt=excluded.prefix_attempt, prefix_binding=excluded.prefix_binding, "
                "prefix=excluded.prefix, checkpoint=excluded.checkpoint",
                (ALGORITHM, self.attempt_id, self.binding, *prefix, state.to_json()),
            ).close()
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        return state

    def _load(self) -> tuple[FinishState, tuple[str, bytes, str], Any]:
        self._detached()
        header = self._one(
            "SELECT algorithm, substr(prefix_attempt,1,?), substr(prefix_binding,1,?), "
            "substr(prefix,1,?), substr(checkpoint,1,?) FROM journal_semantic_finish "
            "WHERE singleton=1 AND attempt_id=? AND binding=?",
            (
                _HEADER_OCTETS + 1,
                _BINDING_OCTETS + 1,
                _HEADER_OCTETS + 1,
                _HEADER_OCTETS + 1,
                self.attempt_id,
                self.binding,
            ),
        )
        if header is None:
            raise StaleSemanticFinish("semantic finish inputs were superseded")
        if header[0] != ALGORITHM:
            raise RuntimeError("unknown semantic finish algorithm")
        prefix = self._prefix()
        if header[1:4] != prefix:
            raise StaleSemanticFinish("accepted prefix changed during semantic finish")
        state = FinishState.from_json(header[4])
        current = None
        for identity in (state.completed, state.current):
            if identity is None:
                continue
            link = self._one(
                "SELECT a.object_id,a.type,a.state_id,a.active FROM changes c LEFT JOIN "
                "assertions a ON a.assertion_id=c.assertion_id WHERE c.assertion_id=?",
                (identity,),
            )
            if link is None or link[0] is None or identity == state.current and link[3] != 1:
                raise RuntimeError("missing or changed semantic assertion cursor link")
            if identity == state.current:
                current = link
        if state.scan is not None:
            if current is None:
                raise RuntimeError("missing semantic scan subject")
            if state.phase == "externals":
                link = self._one(
                    "SELECT 1 FROM maps INDEXED BY external_types WHERE name='externals' "
                    "AND json_extract(value,'$.object_id')=? AND rowid=?",
                    (current[0], state.scan),
                )
            else:
                link = self._one(
                    "SELECT 1 FROM assertions WHERE rowid=? AND active=1 AND state_id=? AND type=?",
                    (state.scan, current[2], state.phase),
                )
            if link is None:
                raise RuntimeError("missing or changed semantic scan cursor link")
        return state, prefix, current

    def load(self) -> FinishState:
        return self._load()[0]

    def prepare(self, *, row_budget: int = MAX_ROWS, byte_budget: int = 65536) -> FinishBatch:
        if type(row_budget) is not int or not 1 <= row_budget <= MAX_ROWS:
            raise ValueError("semantic finish row budget must be between one and 64")
        if type(byte_budget) is not int or byte_budget < 1:
            raise ValueError("semantic finish byte budget must be positive")
        state, prefix, current = self._load()
        after = state
        writes: list[tuple[str, tuple[Any, ...]]] = []
        admitted = 0

        def atom(sql: str, parameters: tuple[Any, ...]) -> dict[str, Any] | None:
            nonlocal admitted
            size = self._one("SELECT length(value) " + sql, parameters)
            if size is None:
                return None
            if type(size[0]) is not int or not 0 <= size[0] <= PROVENANCE_JOURNAL_ENTRY_BYTES_MAX:
                raise RuntimeError("invalid retained semantic atom envelope")
            if admitted + size[0] > byte_budget:
                raise SemanticFinishCapacity("next semantic atom exceeds byte admission")
            row = self._one("SELECT value " + sql, parameters)
            if row is None or type(row[0]) is not bytes or len(row[0]) != size[0]:
                raise RuntimeError("semantic atom changed outside its work fence")
            admitted += size[0]
            value = json.loads(row[0])
            if type(value) is not dict:
                raise RuntimeError("invalid retained semantic atom")
            return value

        if state.phase == "row":
            row = self._one(
                "SELECT c.assertion_id,a.active FROM changes c LEFT JOIN assertions a "
                "ON a.assertion_id=c.assertion_id WHERE c.assertion_id>? "
                "ORDER BY c.assertion_id LIMIT 1",
                (state.completed or "",),
            )
            if row is None:
                after = replace(state, phase="done")
            elif row[1] == 0:
                after = replace(state, completed=row[0])
            elif row[1] == 1:
                after = replace(state, phase="lifecycle", current=row[0])
            else:
                raise RuntimeError("missing changed semantic assertion")
        elif state.phase == "lifecycle":
            after = replace(state, phase="content")
            if current[1] in {"usage", "generation", "invalidation"}:
                # Map keys retain their native j-prefixed canonical JSON encoding.
                key = b"j" + canonical_json_bytes(current[2])
                stamps = []
                for kind in ("generation", "invalidation"):
                    value = self._one(
                        "SELECT json_extract(value,'$.at') FROM maps INDEXED BY "
                        "lifecycle_bound_times WHERE name IN ('generation','invalidation') "
                        "AND name=? AND key=?",
                        (kind, key),
                    )
                    stamp = value[0] if value else None
                    if stamp is not None:
                        if type(stamp) is not str or len(stamp) > 40:
                            raise RuntimeError("invalid retained semantic lifecycle timestamp")
                        time_ns(stamp)
                    stamps.append(stamp)
                generated_at, invalidated_at = stamps
                if not self.store._lifecycle_bounds_hold(
                    current[2], generated_at=generated_at, invalidated_at=invalidated_at
                ):
                    after = replace(
                        state,
                        phase="invalidation",
                        generated_at=generated_at,
                        invalidated_at=invalidated_at,
                    )
        elif state.phase in {"invalidation", "usage", "externals"}:
            scan = state.scan
            parameters: tuple[Any, ...]
            for _ in range(row_budget):
                if state.phase == "externals":
                    relation = (
                        "FROM maps WHERE name='externals' AND json_extract(value,'$.object_id')=? "
                    )
                    parameters = (current[0],)
                else:
                    relation = "FROM assertions WHERE active=1 AND state_id=? AND type=? "
                    parameters = (current[2], state.phase)
                suffix = "" if scan is None else "AND rowid>? "
                if scan is not None:
                    parameters += (scan,)
                row = self._one(
                    "SELECT rowid,length(value) " + relation + suffix + "ORDER BY rowid LIMIT 1",
                    parameters,
                )
                if row is None:
                    if state.phase == "invalidation":
                        after = replace(state, phase="usage", scan=None)
                    elif state.phase == "usage":
                        after = replace(
                            state,
                            phase="content",
                            scan=None,
                            generated_at=None,
                            invalidated_at=None,
                        )
                    else:
                        after = FinishState(completed=state.current, version=state.version)
                    break
                if type(row[1]) is not int or not 0 <= row[1] <= PROVENANCE_JOURNAL_ENTRY_BYTES_MAX:
                    raise RuntimeError("invalid retained semantic scan atom envelope")
                if admitted + row[1] > byte_budget:
                    if scan == state.scan:
                        raise SemanticFinishCapacity(
                            "next semantic scan atom exceeds byte admission"
                        )
                    break
                table = "maps" if state.phase == "externals" else "assertions"
                value = atom("FROM " + table + " WHERE rowid=?", (row[0],))
                if value is None:
                    raise RuntimeError("missing semantic scan atom")
                if state.phase == "externals":
                    _Validator._node_type({"type": current[1]}, value["object_type"])
                elif "at" in value:
                    if state.generated_at is not None and time_ns(value["at"]) < time_ns(
                        state.generated_at
                    ):
                        raise ProvenanceValidationError(
                            "state used or invalidated before known generation"
                        )
                    if (
                        state.phase == "usage"
                        and state.invalidated_at is not None
                        and time_ns(value["at"]) > time_ns(state.invalidated_at)
                    ):
                        raise ProvenanceValidationError("state used after known invalidation")
                scan = row[0]
                after = replace(state, scan=scan)
        elif state.phase == "content":
            after = replace(state, phase="externals")
            if current[1] in {"observation", "reported_description"}:
                value = atom("FROM assertions WHERE assertion_id=? AND active=1", (state.current,))
                if value is None:
                    raise RuntimeError("missing semantic content atom")
                if (
                    current[1] == "observation"
                    and value["address_status"] == "known"
                    and self._one(
                        "SELECT 1 FROM assertions WHERE active=1 AND observation_id=? "
                        "AND type='locator_binding' LIMIT 1",
                        (value["id"],),
                    )
                    is None
                ):
                    raise ProvenanceValidationError(
                        "known address requires an observed locator binding"
                    )
                if "content" in value:
                    subject = value["state"]["object_id"]
                    size, digest = fingerprint(value["content"])
                    with closing(
                        self.db.execute(
                            "SELECT 1 FROM fixities WHERE state=? LIMIT 2",
                            (subject,),
                        )
                    ) as rows:
                        existing = rows.fetchall()
                    same = self._one(
                        "SELECT 1 FROM fixities WHERE state=? AND size=? AND sha256=?",
                        (subject, str(size), digest),
                    )
                    writes.append(
                        (
                            "INSERT OR IGNORE INTO fixities VALUES (?,?,?)",
                            (subject, str(size), digest),
                        )
                    )
                    if len(existing) > 1 or existing and same is None:
                        finding = (
                            f"{subject}: conflicting primary-content descriptions; "
                            "no value selected"
                        )
                        writes.append(
                            ("INSERT OR IGNORE INTO findings VALUES (?,?)", (finding, finding))
                        )
        if not state.done:
            after = replace(after, version=state.version + 1)
        return FinishBatch(
            self.binding, self.attempt_id, prefix, state.to_json(), after, tuple(writes)
        )

    def commit(self, batch: FinishBatch) -> FinishState:
        self._detached()
        if (batch.binding, batch.attempt_id) != (self.binding, self.attempt_id):
            raise StaleSemanticFinish("semantic proposal belongs to another input")
        before = FinishState.from_json(batch.before_json)
        if before.done:
            current, prefix, _ = self._load()
            if current != before or prefix != batch.prefix:
                raise StaleSemanticFinish("finished semantic proposal was superseded")
            return current
        try:
            self.db.execute("BEGIN IMMEDIATE").close()
            with closing(
                self.db.execute(
                    "UPDATE journal_semantic_finish SET checkpoint=? WHERE singleton=1 "
                    "AND algorithm=? AND attempt_id=? AND binding=? AND prefix_attempt=? "
                    "AND prefix_binding=? AND prefix=? AND checkpoint=? AND EXISTS(SELECT 1 "
                    "FROM journal_prefix WHERE singleton=1 AND attempt_id=? "
                    "AND binding=? AND checkpoint=?)",
                    (
                        batch.after.to_json(),
                        ALGORITHM,
                        self.attempt_id,
                        self.binding,
                        *batch.prefix,
                        batch.before_json,
                        *batch.prefix,
                    ),
                )
            ) as cursor:
                if cursor.rowcount != 1:
                    raise StaleSemanticFinish("semantic proposal was superseded")
            for sql, parameters in batch.writes:
                self.db.execute(sql, parameters).close()
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        return batch.after

    @declare_growth(
        Declaration(
            id="riverhog-provenance.sqlite-semantic-finish.growth/v1",
            authority="riverhog-provenance",
            subjects=("riverhog_provenance._semantic_finish.SemanticFinish.advance",),
            scope="native-private-sqlite-post-row-check-work-unit",
            starts_at="load the exact owned post-row phase and its indexed cursors",
            ends_at="commit bounded findings/fixities and the next fenced checkpoint",
            dimensions=(
                Dimension("retained_rows", COUNT, "Resident semantic and reference index rows."),
                Dimension("row_budget", COUNT, "Scan admission between one and 64."),
                Dimension("byte_budget", OCTETS, "Actual existing payload atom bytes admitted."),
            ),
            bounds=(
                Bound(
                    "sql_statements",
                    COUNT,
                    "upper",
                    Expression(
                        "sum",
                        terms=(
                            Expression("constant", value=64, unit=COUNT),
                            Expression(
                                "product",
                                terms=(
                                    Expression("constant", value=4),
                                    Expression("dimension", name="row_budget"),
                                ),
                            ),
                        ),
                    ),
                    "Fixed manifest/link checks and at most four indexed statements per scan row.",
                ),
                Bound(
                    "returned_rows",
                    COUNT,
                    "upper",
                    Expression(
                        "sum",
                        terms=(
                            Expression("constant", value=32, unit=COUNT),
                            Expression(
                                "product",
                                terms=(
                                    Expression("constant", value=3),
                                    Expression("dimension", name="row_budget"),
                                ),
                            ),
                        ),
                    ),
                    "Fixed headers and links plus admitted scan rows and one boundary probe.",
                ),
                Bound(
                    "changed_rows",
                    COUNT,
                    "upper",
                    Expression("constant", value=3, unit=COUNT),
                    "One checkpoint, one fixity and one deduplicated finding.",
                ),
                Bound(
                    "payload_fetched_octets",
                    OCTETS,
                    "upper",
                    Expression("dimension", name="byte_budget"),
                    "Payload lengths are admitted before any corresponding value is fetched.",
                ),
                Bound(
                    "metadata_octets",
                    OCTETS,
                    "upper",
                    Expression(
                        "sum",
                        terms=(
                            Expression("constant", value=65536, unit=OCTETS),
                            Expression(
                                "product",
                                terms=(
                                    Expression("constant", value=512, unit=OCTETS_PER_COUNT),
                                    Expression("dimension", name="row_budget"),
                                ),
                            ),
                        ),
                    ),
                    "Fixed headers and bounded UUID, timestamp, rowid and atom-length metadata.",
                ),
                Bound(
                    "written_value_octets",
                    OCTETS,
                    "upper",
                    Expression(
                        "sum",
                        terms=(
                            Expression("constant", value=4096, unit=OCTETS),
                            Expression("dimension", name="byte_budget"),
                        ),
                    ),
                    "The bounded checkpoint, admitted fixity value and fixed finding metadata.",
                ),
                Bound(
                    "json_function_calls",
                    COUNT,
                    "equal",
                    Expression("constant", value=0, unit=COUNT),
                    "Covering indexes supply lifecycle timestamps and reverse-reference keys.",
                ),
                Bound(
                    "sqlite_vm_instructions",
                    COUNT,
                    "upper",
                    Expression(
                        "sum",
                        terms=(
                            Expression("constant", value=16384, unit=COUNT),
                            Expression(
                                "product",
                                terms=(
                                    Expression("constant", value=2048),
                                    Expression(
                                        "sum",
                                        terms=(
                                            Expression("dimension", name="row_budget"),
                                            Expression("constant", value=16, unit=COUNT),
                                        ),
                                    ),
                                    Expression(
                                        "log2_ceil",
                                        terms=(
                                            Expression(
                                                "sum",
                                                terms=(
                                                    Expression("dimension", name="retained_rows"),
                                                    Expression("constant", value=2, unit=COUNT),
                                                ),
                                            ),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                    "Indexed seek work and fixed checkpoint control, excluding "
                    "SQLite internal B-tree and JSON CPU.",
                ),
            ),
            conditions=(
                "A trusted owner retains the exact source, catalog, modes and "
                "private semantic workspace.",
                "Native row checks and lifecycle extrema complete before post-row "
                "work is admitted.",
                "No owner changes quarantined facts or indexes outside the bound attempt.",
                "Existing source entries retain their native byte envelope; SQL "
                "lengths precede atom admission.",
            ),
            invariants=(
                "Changed assertion UUID order and lifecycle type/physical-row "
                "diagnostic order are preserved.",
                "Address completeness precedes fixity findings, which precede "
                "reverse reference checks.",
                "All readers close before atomic fenced progress and output "
                "publication; the accepted prefix stays unchanged.",
                "High-degree scans resume from indexed physical-row cursors "
                "without replaying earlier rows.",
            ),
            limitations=(
                "Constructor source decoding and catalog identity are "
                "entry/catalog delegates outside the work unit.",
                "Logical metadata is fixed-shape headers and native "
                "UUID/time/type/rowid values; physical pages, JSON CPU, lock "
                "waits and fsync are separately observed costs.",
                "Each changed row has constant control phases; each required "
                "diagnostic/reference row is visited once, plus bounded lookahead "
                "and restart work.",
                "This phase does not checkpoint initial semantic row checks, "
                "graph insertion or recorder checks, and supplies no "
                "whole-session custody or durability claim.",
            ),
        )
    )
    def advance(self, *, row_budget: int = MAX_ROWS, byte_budget: int = 65536) -> FinishState:
        return self.commit(self.prepare(row_budget=row_budget, byte_budget=byte_budget))
