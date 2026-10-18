"""Bounded staging of one admitted entry's native assertion/identity rows.

Construction decodes the current entry atom. The owner has completed journal and
entry identity admission, changed-row retirement and any exact correction-target
work. Staged facts remain private until every later semantic check accepts the
frame. This phase does not supply source custody or a resumable whole validator.
"""

from __future__ import annotations

import json
import re
import sqlite3
from contextlib import closing
from dataclasses import dataclass
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
from riverhog_provenance_contracts import canonical_document

from ._journal_prefix import PrefixState
from .constants import PROVENANCE_JOURNAL_ENTRY_BYTES_MAX
from .errors import ProvenanceValidationError
from .graph import iter_assertions

if TYPE_CHECKING:
    from .journal import JournalFrame

ALGORITHM = "riverhog-assertion-staging/v1"
MAX_ROWS = 64
_HEADER_OCTETS = 2048
_BINDING_OCTETS = 8192


class StaleAssertionStaging(RuntimeError):
    """This work no longer owns the exact source, mode, attempt and prefix."""


class AssertionStagingCapacity(RuntimeError):
    """The next assertion and identity atom needs more byte admission."""


@dataclass(frozen=True)
class StageState:
    phase: str = "stage"
    position: int = 0
    version: int = 0

    @property
    def done(self) -> bool:
        return self.phase == "done"

    def to_json(self) -> str:
        return canonical_json_bytes(
            {
                "phase": self.phase,
                "position": str(self.position),
                "version": str(self.version),
            }
        ).decode()

    @classmethod
    def from_json(cls, raw: str, count: int) -> StageState:
        try:
            if type(raw) is not str or len(raw) > _HEADER_OCTETS:
                raise ValueError("checkpoint envelope")
            value = json.loads(raw)
            if type(value) is not dict or set(value) != {"phase", "position", "version"}:
                raise ValueError("checkpoint shape")
            if value["phase"] not in {"stage", "done"}:
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
            raise RuntimeError("corrupt assertion staging checkpoint") from exc


@dataclass(frozen=True)
class StageRow:
    assertion_id: str
    object_id: str
    category: str
    kind: str
    state_id: str | None
    observation_id: str | None
    value: bytes
    signature: bytes


@dataclass(frozen=True)
class StageBatch:
    attempt_id: str
    binding: bytes
    prefix: tuple[str, bytes, str]
    before_json: str
    after: StageState
    rows: tuple[StageRow, ...]


def install(db: sqlite3.Connection) -> None:
    db.execute(
        "CREATE TABLE journal_assertion_staging(singleton INTEGER PRIMARY KEY CHECK(singleton=1), "
        "algorithm TEXT NOT NULL, attempt_id TEXT NOT NULL, binding BLOB NOT NULL, "
        "row_count INTEGER NOT NULL, prefix_attempt TEXT NOT NULL, prefix_binding BLOB NOT NULL, "
        "prefix TEXT NOT NULL, checkpoint TEXT NOT NULL)"
    ).close()


def _declaration() -> Declaration:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def dimension(name: str) -> Expression:
        return Expression("dimension", name=name)

    def multiple(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=value), term))

    budget = dimension("row_budget")
    return Declaration(
        id="riverhog-provenance.sqlite-assertion-staging.growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance._assertion_staging.AssertionStaging.advance",),
        scope="native-private-sqlite-entry-assertion-staging-unit",
        starts_at="load the exact staging cursor and admit source/identity bytes",
        ends_at="commit bounded immutable identities, assertion/change rows and fenced progress",
        dimensions=(
            Dimension(
                "retained_rows", COUNT, "Resident assertion, immutable identity and change rows."
            ),
            Dimension("row_budget", COUNT, "One to 64 source assertions admitted for this unit."),
            Dimension(
                "byte_budget", OCTETS, "Logical source/write and existing signature admission."
            ),
            Dimension(
                "max_atom_octets", OCTETS, "Largest inspected source/assertion/identity atom."
            ),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                Expression("sum", terms=(count(48), multiple(9, budget))),
                "Fixed manifest/boundary checks and indexed per-assertion reads and writes.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                Expression("sum", terms=(count(20), multiple(5, budget))),
                "Fixed bounded manifests and identity/active-object metadata; no history scan.",
            ),
            Bound(
                "changed_rows",
                COUNT,
                "upper",
                Expression("sum", terms=(count(1), multiple(4, budget))),
                "One checkpoint and at most two identities, one assertion and one change per row.",
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
                                Expression("constant", value=1024, unit=OCTETS_PER_COUNT),
                                budget,
                            ),
                        ),
                    ),
                ),
                "Fixed headers and native domain, identity, category, type, "
                "size and flag metadata.",
            ),
            Bound(
                "admitted_octets",
                OCTETS,
                "upper",
                dimension("byte_budget"),
                "Logical proposed writes, including ignored identities, plus "
                "existing values compared.",
            ),
            Bound(
                "inspected_octets",
                OCTETS,
                "upper",
                Expression("sum", terms=(dimension("byte_budget"), dimension("max_atom_octets"))),
                "Admitted source/identity atoms and at most one next capacity-boundary atom.",
            ),
            Bound(
                "historical_payload_fetched_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "Existing identity equality is checked by indexed SQLite "
                "comparison; no payload is fetched.",
            ),
            Bound(
                "written_value_octets",
                OCTETS,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        Expression("constant", value=_HEADER_OCTETS, unit=OCTETS),
                        dimension("byte_budget"),
                    ),
                ),
                "Admitted actual logical row writes and the fixed-shape checkpoint.",
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        count(16384),
                        multiple(
                            2048,
                            Expression(
                                "product",
                                terms=(
                                    Expression("sum", terms=(budget, count(16))),
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
                "Indexed seeks and bounded index/checkpoint maintenance, "
                "excluding internal B-tree and byte comparison CPU.",
            ),
        ),
        conditions=(
            "A trusted owner retains the exact admitted entry, catalog/modes and "
            "private workspace.",
            "Journal/entry identities, old change retirement and exact correction "
            "targets precede staging.",
            "No owner changes quarantined facts or indexes outside the bound attempt and prefix.",
            "The existing entry byte envelope bounds source-local ordinals, not "
            "total journal cardinality.",
        ),
        invariants=(
            "Native category and in-array source order chooses identity and "
            "duplicate-object errors.",
            "No assertion identity can be reused; immutable referent signatures "
            "and domain separation remain exact.",
            "Readers close before atomic output/checkpoint commit; stale workers "
            "cannot stage into replacements.",
            "Staging never advances accepted source SHA, predecessor, watermark "
            "or frame publication.",
        ),
        limitations=(
            "Construction decodes one admitted source entry and retains its "
            "bounded row directory outside the unit.",
            "Source canonicalization and immutable-signature extraction are "
            "measured entry-local CPU/byte delegates.",
            "With adequate admission, each source row is staged once with indexed "
            "history lookups; unit headers, retries, schema, physical pages, lock "
            "waits and fsync remain separately charged.",
            "Boundary links verify source identities, native metadata and "
            "domain/liveness links; authenticated workspace integrity remains the "
            "owner's responsibility.",
            "Entry identity admission, old change retirement, graph/reference "
            "checks and whole-session custody/recovery have separate ownership.",
        ),
    )


class AssertionStaging:
    def __init__(
        self,
        db: sqlite3.Connection,
        *,
        attempt_id: str,
        frame: JournalFrame,
        catalog_sha256: str,
        require_profiles: bool,
    ) -> None:
        from .journal import _body_assertions

        if type(attempt_id) is not str or not attempt_id:
            raise ValueError("an explicit assertion staging attempt is required")
        if type(catalog_sha256) is not str or re.fullmatch(r"[0-9a-f]{64}", catalog_sha256) is None:
            raise ValueError("an exact staging catalog identity is required")
        if type(require_profiles) is not bool:
            raise ValueError("an exact staging profile mode is required")
        if len(frame.json_bytes) > PROVENANCE_JOURNAL_ENTRY_BYTES_MAX:
            raise ValueError("staging source exceeds its owning entry envelope")
        document = frame.document
        self.db, self.attempt_id = db, attempt_id
        self.rows = list(iter_assertions(_body_assertions(document)))
        self.count = len(self.rows)
        self.sequence, self.journal_id = document["sequence"], document["journal_id"]
        self.binding = canonical_json_bytes(
            {
                "algorithm": ALGORITHM,
                "source_sha256": frame.sha256,
                "catalog_sha256": catalog_sha256,
                "require_profiles": require_profiles,
            }
        )

    def _one(self, sql: str, parameters: tuple[Any, ...] = ()) -> Any:
        with closing(self.db.execute(sql, parameters)) as rows:
            return rows.fetchone()

    def _detached(self) -> None:
        if self.db.in_transaction:
            raise RuntimeError("assertion staging requires a closed owner transaction")

    def _prefix(self) -> tuple[str, bytes, str]:
        row = self._one(
            "SELECT substr(attempt_id,1,?),substr(binding,1,?),substr(checkpoint,1,?) "
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
            raise RuntimeError("missing or invalid staging accepted-prefix manifest")
        state = PrefixState.from_json(row[2])
        if str(state.frame_count) != self.sequence or state.journal_id not in (
            None,
            self.journal_id,
        ):
            raise StaleAssertionStaging("staging frame differs from its accepted prefix")
        return row[0], row[1], row[2]

    def initialize(self) -> StageState:
        """Admit a fresh phase explicitly; restoring uses load without resetting."""
        self._detached()
        try:
            self.db.execute("BEGIN IMMEDIATE").close()
            prefix = self._prefix()
            old = self._one(
                "SELECT algorithm,attempt_id,row_count,substr(checkpoint,1,?) "
                "FROM journal_assertion_staging WHERE singleton=1",
                (_HEADER_OCTETS + 1,),
            )
            version = 0
            if old is not None:
                if old[0] != ALGORITHM:
                    raise RuntimeError("unknown assertion staging algorithm")
                state = StageState.from_json(old[3], old[2])
                if old[1] == self.attempt_id or not state.done:
                    raise StaleAssertionStaging("assertion staging is already bound")
                version = state.version + 1
            if (
                self._one(
                    "SELECT assertion_id FROM changes WHERE assertion_id>'' "
                    "ORDER BY assertion_id LIMIT 1"
                )
                is not None
            ):
                raise RuntimeError("assertion staging requires a retired change queue")
            state = StageState(version=version)
            self.db.execute(
                "INSERT INTO journal_assertion_staging VALUES (1,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(singleton) DO UPDATE SET algorithm=excluded.algorithm, "
                "attempt_id=excluded.attempt_id,binding=excluded.binding,row_"
                "count=excluded.row_count, "
                "prefix_attempt=excluded.prefix_attempt,prefix_binding=excluded.prefix_binding, "
                "prefix=excluded.prefix,checkpoint=excluded.checkpoint",
                (ALGORITHM, self.attempt_id, self.binding, self.count, *prefix, state.to_json()),
            ).close()
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        return state

    def _load(self) -> tuple[StageState, tuple[str, bytes, str]]:
        self._detached()
        header = self._one(
            "SELECT algorithm,row_count,substr(prefix_attempt,1,?),substr(prefix_binding,1,?), "
            "substr(prefix,1,?),substr(checkpoint,1,?) FROM journal_assertion_staging "
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
            raise StaleAssertionStaging("assertion staging inputs were superseded")
        if header[0] != ALGORITHM or type(header[1]) is not int or header[1] != self.count:
            raise RuntimeError("changed assertion staging algorithm or source count")
        prefix = self._prefix()
        if header[2:5] != prefix:
            raise StaleAssertionStaging("accepted prefix changed during assertion staging")
        state = StageState.from_json(header[5], self.count)
        if state.position:
            category, source = self.rows[state.position - 1]
            link = self._one(
                "SELECT "
                "a.object_id,a.category,a.type,a.state_id,a.observation_id,a."
                "sequence,a.active, "
                "i.domain,length(i.value),r.domain FROM changes c LEFT JOIN assertions a "
                "ON a.assertion_id=c.assertion_id LEFT JOIN identities i ON i.id=a.assertion_id "
                "LEFT JOIN identities r ON r.id=a.object_id WHERE c.assertion_id=?",
                (source["assertion_id"],),
            )
            expected = (
                source["id"],
                category,
                source["type"],
                source.get("state", {}).get("object_id"),
                source.get("observation_id"),
                self.sequence,
                1,
                "assertion",
                0,
                "referent",
            )
            if link != expected:
                raise RuntimeError("missing or changed staged assertion cursor link")
        elif (
            self._one(
                "SELECT assertion_id FROM changes WHERE assertion_id>'' "
                "ORDER BY assertion_id LIMIT 1"
            )
            is not None
        ):
            raise RuntimeError("uncheckpointed assertion changes")
        return state, prefix

    def load(self) -> StageState:
        return self._load()[0]

    def prepare(self, *, row_budget: int = MAX_ROWS, byte_budget: int = 65536) -> StageBatch:
        from .journal import _identity_signature

        if type(row_budget) is not int or not 1 <= row_budget <= MAX_ROWS:
            raise ValueError("assertion staging row budget must be between one and 64")
        if type(byte_budget) is not int or byte_budget < 1:
            raise ValueError("assertion staging byte budget must be positive")
        state, prefix = self._load()
        position, admitted = state.position, 0
        rows: list[StageRow] = []
        pending: dict[str, tuple[str, bytes]] = {}
        objects: set[str] = set()
        if not state.done:
            for _ in range(row_budget):
                if position == self.count:
                    break
                category, source = self.rows[position]
                row = StageRow(
                    source["assertion_id"],
                    source["id"],
                    category,
                    source["type"],
                    source.get("state", {}).get("object_id"),
                    source.get("observation_id"),
                    canonical_document(source),
                    _identity_signature(source),
                )
                octets = (
                    len(row.assertion_id.encode()) * 3
                    + len(row.object_id.encode()) * 2
                    + len(category.encode())
                    + len(row.kind.encode())
                    + len((row.state_id or "").encode())
                    + len((row.observation_id or "").encode())
                    + len(self.sequence.encode())
                    + len(row.value)
                    + len(row.signature)
                    + len("assertion")
                    + len("referent")
                    + 1
                )
                if admitted + octets > byte_budget:
                    if not rows:
                        raise AssertionStagingCapacity(
                            "next assertion source atom exceeds admission"
                        )
                    break
                if (
                    row.assertion_id in pending
                    or self._one("SELECT 1 FROM identities WHERE id=?", (row.assertion_id,))
                    is not None
                ):
                    raise ProvenanceValidationError(
                        "assertion identities cannot be reused or overlap another domain"
                    )
                pending[row.assertion_id] = ("assertion", b"")
                prior = pending.get(row.object_id)
                retained = (
                    None
                    if prior is not None
                    else self._one(
                        "SELECT domain,length(value) FROM identities WHERE id=?", (row.object_id,)
                    )
                )
                prior_octets = (
                    len(prior[1]) if prior is not None else retained[1] if retained else 0
                )
                if (
                    type(prior_octets) is not int
                    or not 0 <= prior_octets <= PROVENANCE_JOURNAL_ENTRY_BYTES_MAX
                ):
                    raise RuntimeError("invalid retained immutable signature envelope")
                octets += prior_octets
                if admitted + octets > byte_budget:
                    if not rows:
                        raise AssertionStagingCapacity(
                            "next immutable signature atom exceeds admission"
                        )
                    break
                if prior is not None:
                    matches = prior == ("referent", row.signature)
                elif retained is not None:
                    matches = retained[0] == "referent" and self._one(
                        "SELECT value=? FROM identities WHERE id=?", (row.signature, row.object_id)
                    ) == (1,)
                else:
                    matches = True
                if not matches:
                    raise ProvenanceValidationError(
                        "an immutable referent or identity domain was redefined"
                    )
                if (
                    row.object_id in objects
                    or self._one(
                        "SELECT 1 FROM assertions WHERE active=1 AND object_id=?", (row.object_id,)
                    )
                    is not None
                ):
                    raise ProvenanceValidationError("duplicate effective entity/record identity")
                pending[row.object_id] = ("referent", row.signature)
                objects.add(row.object_id)
                rows.append(row)
                admitted += octets
                position += 1
        after = (
            state
            if state.done
            else StageState(
                "done" if position == self.count else "stage", position, state.version + 1
            )
        )
        return StageBatch(
            self.attempt_id, self.binding, prefix, state.to_json(), after, tuple(rows)
        )

    def commit(self, batch: StageBatch) -> StageState:
        self._detached()
        if (batch.attempt_id, batch.binding) != (self.attempt_id, self.binding):
            raise StaleAssertionStaging("staging proposal belongs to another input")
        before = StageState.from_json(batch.before_json, self.count)
        if before.done:
            current, prefix = self._load()
            if current != before or prefix != batch.prefix:
                raise StaleAssertionStaging("finished staging proposal was superseded")
            return current
        try:
            self.db.execute("BEGIN IMMEDIATE").close()
            with closing(
                self.db.execute(
                    "UPDATE journal_assertion_staging SET checkpoint=? WHERE singleton=1 AND "
                    "algorithm=? AND attempt_id=? AND binding=? AND "
                    "row_count=? AND prefix_attempt=? "
                    "AND prefix_binding=? AND prefix=? AND checkpoint=? AND EXISTS(SELECT 1 FROM "
                    "journal_prefix WHERE singleton=1 AND attempt_id=? AND "
                    "binding=? AND checkpoint=?)",
                    (
                        batch.after.to_json(),
                        ALGORITHM,
                        self.attempt_id,
                        self.binding,
                        self.count,
                        *batch.prefix,
                        batch.before_json,
                        *batch.prefix,
                    ),
                )
            ) as cursor:
                if cursor.rowcount != 1:
                    raise StaleAssertionStaging("staging proposal was superseded")
            for row in batch.rows:
                self.db.execute(
                    "INSERT INTO identities VALUES (?, 'assertion', ?)", (row.assertion_id, b"")
                ).close()
                self.db.execute(
                    "INSERT OR IGNORE INTO identities VALUES (?, 'referent', ?)",
                    (row.object_id, row.signature),
                ).close()
                try:
                    self.db.execute(
                        "INSERT INTO assertions VALUES (?,?,?,?,?,?,?,?,1)",
                        (
                            row.assertion_id,
                            row.object_id,
                            row.category,
                            row.kind,
                            row.state_id,
                            row.observation_id,
                            self.sequence,
                            row.value,
                        ),
                    ).close()
                except sqlite3.IntegrityError as exc:
                    raise ProvenanceValidationError(
                        "duplicate effective entity/record identity"
                    ) from exc
                self.db.execute("INSERT INTO changes VALUES (?)", (row.assertion_id,)).close()
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        return batch.after

    @declare_growth(_declaration())
    def advance(self, *, row_budget: int = MAX_ROWS, byte_budget: int = 65536) -> StageState:
        return self.commit(self.prepare(row_budget=row_budget, byte_budget=byte_budget))
