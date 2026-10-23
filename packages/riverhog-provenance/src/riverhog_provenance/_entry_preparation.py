"""Private checkpointed journal/entry identity admission and change retirement.

The owner supplies one schema-admitted entry and an authenticated workspace.
Every unit keeps native error order and the exact accepted prefix. Completion
admits later semantic work; it supplies no semantic acceptance or source custody.
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
    SCALAR,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)
from riverhog_provenance_contracts import require_canonical_uuid_urn

from ._admission_growth import admission_declaration
from ._checkpoint_manifest import PREFIX_BINDING_OCTETS_MAX
from ._journal_prefix import PrefixState
from ._semantic_source import SemanticSource
from .constants import PROVENANCE_JOURNAL_ENTRY_BYTES_MAX
from .errors import JournalValidationCapacity, ProvenanceValidationError

if TYPE_CHECKING:
    from .journal import JournalFrame
    from .journal_store import JournalStore

ALGORITHM = "riverhog-entry-preparation/v1"
MAX_ROWS = 256
_HEADER_OCTETS = 2048
_DEFAULT_BYTES = 4 * PROVENANCE_JOURNAL_ENTRY_BYTES_MAX + 8192


class StaleEntryPreparation(RuntimeError):
    """The source, catalog/mode, attempt, prefix or prepared state was replaced."""


class EntryPreparationCapacity(JournalValidationCapacity):
    """The next existing entry atom requires more byte admission."""


@dataclass(frozen=True)
class PreparationState:
    phase: str = "journal"
    retired: int = 0
    version: int = 0

    @property
    def done(self) -> bool:
        return self.phase == "done"

    def to_json(self) -> str:
        return canonical_json_bytes(
            {"phase": self.phase, "retired": str(self.retired), "version": str(self.version)}
        ).decode()

    @classmethod
    def from_json(cls, raw: str) -> PreparationState:
        try:
            if type(raw) is not str or len(raw) > _HEADER_OCTETS:
                raise ValueError("checkpoint envelope")
            value = json.loads(raw)
            if type(value) is not dict or set(value) != {"phase", "retired", "version"}:
                raise ValueError("checkpoint shape")
            if value["phase"] not in {"journal", "entry", "changes", "done"}:
                raise ValueError("checkpoint phase")
            for field in ("retired", "version"):
                if (
                    type(value[field]) is not str
                    or re.fullmatch(r"0|[1-9][0-9]*", value[field]) is None
                ):
                    raise ValueError("checkpoint scalar")
            state = cls(value["phase"], int(value["retired"]), int(value["version"]))
            if state.phase in {"journal", "entry"} and state.retired or state.to_json() != raw:
                raise ValueError("checkpoint cursor")
            return state
        except (ValueError, TypeError, KeyError, OverflowError) as exc:
            raise RuntimeError("corrupt entry preparation checkpoint") from exc


@dataclass(frozen=True)
class PreparationAdmission:
    binding: bytes
    attempt_id: str
    prefix: tuple[str, bytes, str]
    prior: tuple[object, ...] | None
    after: PreparationState


@dataclass(frozen=True)
class PreparationBatch:
    attempt_id: str
    binding: bytes
    prefix: tuple[str, bytes, str]
    before_json: str
    identity_before: tuple[Any, ...] | None
    row_budget: int


def install(db: sqlite3.Connection) -> None:
    db.execute(
        "CREATE TABLE journal_entry_preparation(singleton INTEGER PRIMARY KEY CHECK(singleton=1), "
        "algorithm TEXT NOT NULL,attempt_id TEXT NOT NULL,binding BLOB NOT NULL, "
        "prefix_attempt TEXT NOT NULL,prefix_binding BLOB NOT NULL,prefix TEXT NOT NULL, "
        "checkpoint TEXT NOT NULL)"
    ).close()


def _declaration() -> Declaration:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def dimension(name: str) -> Expression:
        return Expression("dimension", name=name)

    def multiple(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=value, unit=SCALAR), term))

    rows, control, octets = (
        dimension(name) for name in ("row_budget", "control_octets", "byte_budget")
    )
    return Declaration(
        id="riverhog-provenance.sqlite-entry-preparation.growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance._entry_preparation.EntryPreparation.advance",),
        scope="private-sqlite-entry-identity-and-change-retirement-unit",
        starts_at="load one retained source/prefix-bound preparation checkpoint",
        ends_at="commit bounded native identities or changed-ID retirement with exact progress",
        dimensions=(
            Dimension(
                "retained_rows", COUNT, "Resident immutable identity, change and control rows."
            ),
            Dimension("row_budget", COUNT, "One to 256 changed identities admitted to this unit."),
            Dimension(
                "byte_budget", OCTETS, "Logical control, entry-write and retired-ID admission."
            ),
            Dimension(
                "control_octets",
                OCTETS,
                "Actual attempt/binding/prefix/checkpoint metadata widths.",
            ),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                count(48),
                "Fixed indexed checks, one native retirement statement and checkpoint commit.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                Expression("sum", terms=(count(32), rows)),
                "Fixed headers/identity metadata plus at most the admitted retired identities.",
            ),
            Bound(
                "changed_rows",
                COUNT,
                "upper",
                Expression("sum", terms=(count(3), rows)),
                "At most one native identity or admitted queue rows, and the exact checkpoint.",
            ),
            Bound(
                "returned_metadata_octets",
                OCTETS,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        multiple(32, control),
                        Expression(
                            "product",
                            terms=(Expression("constant", value=45, unit=OCTETS_PER_COUNT), rows),
                        ),
                    ),
                ),
                "Bounded control metadata and canonical UUID identity results.",
            ),
            Bound(
                "bound_parameter_octets",
                OCTETS,
                "upper",
                Expression("sum", terms=(multiple(2, octets), multiple(32, control))),
                "Actual entry bytes are admitted once; all remaining "
                "bindings are bounded control values.",
            ),
            Bound(
                "historical_payload_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "Identity checks return domain/type/length metadata; "
                "historical entry/assertion payloads are not materialized.",
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
                            512,
                            Expression(
                                "product",
                                terms=(
                                    Expression("sum", terms=(rows, count(16))),
                                    Expression(
                                        "log2_ceil",
                                        terms=(
                                            Expression(
                                                "sum", terms=(dimension("retained_rows"), count(4))
                                            ),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                "Indexed identity/header seeks and bounded native "
                "retirement; no unbounded queue or history scan.",
            ),
        ),
        conditions=(
            "The trusted owner retains the exact schema-admitted entry, "
            "catalog/mode pin and workspace.",
            "Native immutable identities and the changed queue keep their identity primary keys.",
            "Changed keys are canonical UUID URNs, and row_budget admits one to 256 keys.",
            "Owner transactions are closed before detached planning; this phase owns its commit.",
        ),
        invariants=(
            "Journal-domain errors precede reused entry identities, "
            "which precede queue retirement.",
            "Identity writes, retirement and progress share the exact attempt/source/prefix fence.",
            "Load resumes retained work and refuses missing, malformed or replaced state.",
            "Completion admits later semantic work and advances no "
            "accepted source or archive authority.",
        ),
        limitations=(
            "Source decoding, schema checks and exact frame hashing are "
            "separately admitted entry atoms.",
            "Metadata boundary links require owner-authenticated "
            "workspace integrity, not client-supplied state.",
            "SQLite native B-tree/byte work, page I/O, lock waits, "
            "allocation and fsync are separate costs.",
            "Later semantic failure remains quarantined; whole-session "
            "recovery and reclamation belong to its owner.",
        ),
    )


def _phase_declaration(operation: str) -> Declaration:
    planning = operation == "prepare"
    base = _declaration()
    return replace(
        base,
        id="riverhog-provenance.sqlite-entry-preparation-"
        + ("plan" if planning else "application")
        + ".growth/v1",
        subjects=("riverhog_provenance._entry_preparation.EntryPreparation." + operation,),
        scope="detached-entry-preparation-plan"
        if planning
        else "owner-transaction-entry-preparation-application",
        starts_at=(
            "load one source/prefix-bound checkpoint outside a transaction"
            if planning
            else "accept one exact admitted proposal inside the owner's open transaction"
        ),
        ends_at=(
            "return a bounded detached proposal without changing native state"
            if planning
            else "return native changes and progress while leaving the owner transaction open"
        ),
        bounds=tuple(
            replace(bound, relation="equal", expression=Expression("constant", value=0, unit=COUNT))
            if planning and bound.metric == "changed_rows"
            else bound
            for bound in base.bounds
        ),
        conditions=(
            *base.conditions[:-1],
            "Planning runs with the owner transaction closed; application runs with it open.",
            "Application receives the exact proposal admitted under the recorded row/byte budget; "
            "the owner composes transaction control and parent publication.",
        ),
        invariants=(
            *base.invariants,
            "Planning never writes; application never commits or rolls back its owner's "
            "transaction.",
        ),
        limitations=(
            *base.limitations,
            "This phase profile does not claim that EntryPreparation.advance executed; admission, "
            "parent publication and transaction control require actual caller composition.",
        ),
    )


def _result_declaration() -> Declaration:
    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def metadata() -> Expression:
        return Expression(
            "product",
            terms=(
                Expression("constant", value=8, unit=SCALAR),
                Expression("dimension", name="control_octets"),
            ),
        )

    zero_octets = Expression("constant", value=0, unit=OCTETS)
    return Declaration(
        id="riverhog-provenance.sqlite-entry-preparation-result.growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance._entry_preparation.EntryPreparation.result",),
        scope="native-sqlite-fenced-entry-preparation-terminal-result",
        starts_at="load exact source/prefix/preparation controls and terminal boundaries",
        ends_at="return completion without changing native or accepted state",
        dimensions=(
            Dimension("retained_rows", COUNT, "Actual owned identity, queue and control rows."),
            Dimension(
                "control_octets",
                OCTETS,
                "Actual input attempt/binding/prefix/checkpoint control envelope.",
            ),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                count(8),
                "Fixed header/prefix/identity and empty-queue index probes.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                count(8),
                "Only fixed controls and identity domain/type/length metadata.",
            ),
            Bound(
                "changed_rows",
                COUNT,
                "equal",
                count(0),
                "Terminal inspection does not mutate native state.",
            ),
            Bound(
                "returned_metadata_octets",
                OCTETS,
                "upper",
                metadata(),
                "Returned controls retain their actual scalar byte widths.",
            ),
            Bound(
                "bound_parameter_octets",
                OCTETS,
                "upper",
                metadata(),
                "Only fixed identity and exact owner/source/prefix keys are bound.",
            ),
            Bound(
                "historical_payload_octets",
                OCTETS,
                "equal",
                zero_octets,
                "Identity payloads are measured by type and length, never returned.",
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        count(8192),
                        Expression(
                            "product",
                            terms=(
                                count(512),
                                Expression(
                                    "log2_ceil",
                                    terms=(
                                        Expression(
                                            "sum",
                                            terms=(
                                                count(4),
                                                Expression("dimension", name="retained_rows"),
                                            ),
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                "Fixed control and indexed identity/queue seeks.",
            ),
        ),
        conditions=(
            "The exact source atom, catalog/modes and private workspace remain owned.",
            "The owner closes its transaction before terminal inspection.",
            "The checkpoint and its identity/queue boundaries agree and are complete.",
            "control_octets retains actual source/prefix/attempt/checkpoint widths.",
        ),
        invariants=(
            "The accepted prefix, immutable history and queue remain unchanged.",
            "Incomplete, malformed or replaced preparation state cannot return success.",
            "Preparation completion grants no semantic or archive acceptance.",
        ),
        limitations=(
            "Constructor source decoding/hash and catalog admission are separate work.",
            "Later semantics, parent publication and source custody remain owner work.",
            "B-tree byte comparisons, physical I/O and waits remain separate costs.",
        ),
    )


class EntryPreparation:
    def __init__(
        self,
        store: JournalStore,
        *,
        attempt_id: str,
        frame: JournalFrame,
        catalog_sha256: str,
        require_profiles: bool,
    ) -> None:
        if (
            type(attempt_id) is not str
            or not attempt_id
            or len(attempt_id.encode()) > _HEADER_OCTETS
        ):
            raise ValueError("an explicit bounded preparation attempt identity is required")
        if len(frame.json_bytes) > PROVENANCE_JOURNAL_ENTRY_BYTES_MAX:
            raise ValueError("preparation source exceeds its owning entry envelope")
        document = frame.document
        self.source = SemanticSource(
            frame.sha256,
            document["sequence"],
            document["journal_id"],
            catalog_sha256,
            require_profiles,
        )
        self.store, self.db, self.attempt_id = store, store.db, attempt_id
        self.body = frame.json_bytes
        self.entry_id = document["id"]
        require_canonical_uuid_urn(self.entry_id)
        self.binding = canonical_json_bytes(
            {
                "algorithm": ALGORITHM,
                "source_sha256": self.source.sha256,
                "catalog_sha256": catalog_sha256,
                "require_profiles": require_profiles,
                "entry_id": self.entry_id,
            }
        )

    def _one(self, sql: str, parameters: tuple[Any, ...] = ()) -> Any:
        with closing(self.db.execute(sql, parameters)) as rows:
            return rows.fetchone()

    def _detached(self) -> None:
        if self.db.in_transaction:
            raise RuntimeError("entry preparation requires a closed owner transaction")

    def _prefix(self) -> tuple[str, bytes, str]:
        row = self._one(
            "SELECT substr(attempt_id,1,?),substr(binding,1,?),substr(checkpoint,1,?) "
            "FROM journal_prefix WHERE singleton=1",
            (_HEADER_OCTETS + 1, PREFIX_BINDING_OCTETS_MAX + 1, _HEADER_OCTETS + 1),
        )
        if (
            row is None
            or type(row[0]) is not str
            or not row[0]
            or len(row[0]) > _HEADER_OCTETS
            or type(row[1]) is not bytes
            or len(row[1]) > PREFIX_BINDING_OCTETS_MAX
        ):
            raise RuntimeError("missing or invalid preparation accepted-prefix manifest")
        state = PrefixState.from_json(row[2])
        if str(state.frame_count) != self.source.sequence or state.journal_id not in (
            None,
            self.source.journal_id,
        ):
            raise StaleEntryPreparation("preparation frame differs from its accepted prefix")
        return row[0], row[1], row[2]

    def _identity(self, identity: str) -> tuple[Any, ...] | None:
        row = self._one(
            "SELECT domain,typeof(value),length(value) FROM identities WHERE id=?", (identity,)
        )
        return None if row is None else (row[0], row[1], row[2])

    def _boundaries(self, state: PreparationState) -> None:
        if state.phase != "journal" and self._identity(self.source.journal_id) != (
            "journal",
            "blob",
            0,
        ):
            raise RuntimeError("missing or changed admitted journal identity")
        if state.phase in {"changes", "done"} and self._identity(self.entry_id) != (
            "entry",
            "blob",
            len(self.body),
        ):
            raise RuntimeError("missing or changed admitted entry identity")
        if (
            state.done
            and self._one(
                "SELECT 1 FROM changes WHERE assertion_id>'' ORDER BY assertion_id LIMIT 1"
            )
            is not None
        ):
            raise RuntimeError("entry preparation completion retains changed identities")

    def _admission(self) -> PreparationAdmission:
        prefix = self._prefix()
        old = self._one(
            "SELECT algorithm,attempt_id,substr(checkpoint,1,?) FROM "
            "journal_entry_preparation WHERE singleton=1",
            (_HEADER_OCTETS + 1,),
        )
        version = 0
        if old is not None:
            if old[0] != ALGORITHM:
                raise RuntimeError("unknown entry preparation algorithm")
            before = PreparationState.from_json(old[2])
            if old[1] == self.attempt_id or not before.done:
                raise StaleEntryPreparation("entry preparation is already bound")
            version = before.version + 1
        state = PreparationState(version=version)
        return PreparationAdmission(
            self.binding, self.attempt_id, prefix, None if old is None else tuple(old), state
        )

    @declare_growth(
        admission_declaration(
            identifier="entry-preparation",
            subject="riverhog_provenance._entry_preparation.EntryPreparation.prepare_admission",
            planning=True,
        )
    )
    def prepare_admission(self) -> PreparationAdmission:
        self._detached()
        return self._admission()

    @declare_growth(
        admission_declaration(
            identifier="entry-preparation",
            subject="riverhog_provenance._entry_preparation.EntryPreparation._admit",
            planning=False,
        )
    )
    def _admit(self, batch: PreparationAdmission) -> PreparationState:
        if not self.db.in_transaction:
            raise RuntimeError("child admission requires an owner transaction")
        if self._admission() != batch:
            raise StaleEntryPreparation("child admission was superseded")
        self.db.execute(
            "INSERT INTO journal_entry_preparation VALUES (1,?,?,?,?,?,?,?) "
            "ON CONFLICT(singleton) DO UPDATE SET "
            "algorithm=excluded.algorithm,attempt_id=excluded.attempt_id,"
            " "
            "binding=excluded.binding,prefix_attempt=excluded.prefix_atte"
            "mpt,prefix_binding=excluded.prefix_binding, "
            "prefix=excluded.prefix,checkpoint=excluded.checkpoint",
            (ALGORITHM, self.attempt_id, self.binding, *batch.prefix, batch.after.to_json()),
        ).close()
        return batch.after

    def initialize(self) -> PreparationState:
        batch = self.prepare_admission()
        self.db.execute("BEGIN IMMEDIATE").close()
        try:
            state = self._admit(batch)
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        return state

    def _load(self) -> tuple[PreparationState, tuple[str, bytes, str]]:
        self._detached()
        row = self._one(
            "SELECT algorithm,substr(prefix_attempt,1,?),substr(prefix_binding,1,?), "
            "substr(prefix,1,?),substr(checkpoint,1,?) FROM journal_entry_preparation "
            "WHERE singleton=1 AND attempt_id=? AND binding=?",
            (
                _HEADER_OCTETS + 1,
                PREFIX_BINDING_OCTETS_MAX + 1,
                _HEADER_OCTETS + 1,
                _HEADER_OCTETS + 1,
                self.attempt_id,
                self.binding,
            ),
        )
        if row is None:
            raise StaleEntryPreparation("preparation inputs were superseded")
        if row[0] != ALGORITHM:
            raise RuntimeError("unknown entry preparation algorithm")
        prefix = self._prefix()
        if row[1:4] != prefix:
            raise StaleEntryPreparation("accepted prefix changed during entry preparation")
        state = PreparationState.from_json(row[4])
        self._boundaries(state)
        return state, prefix

    def load(self) -> PreparationState:
        return self._load()[0]

    @declare_growth(_phase_declaration("prepare"))
    def prepare(
        self, *, row_budget: int = MAX_ROWS, byte_budget: int = _DEFAULT_BYTES
    ) -> PreparationBatch:
        if type(row_budget) is not int or not 1 <= row_budget <= MAX_ROWS:
            raise ValueError("preparation row budget must be one to 256")
        if type(byte_budget) is not int or byte_budget < 16384:
            raise ValueError("preparation byte budget must admit fixed control metadata")
        state, prefix = self._load()
        control = 8192 + 4 * (
            len(self.attempt_id.encode())
            + len(self.binding)
            + len(state.to_json().encode())
            + sum(len(v.encode()) if isinstance(v, str) else len(v) for v in prefix)
        )
        if control > byte_budget:
            raise EntryPreparationCapacity("byte budget does not admit preparation control")
        identity_before = None
        if state.phase == "journal":
            identity_before = self._identity(self.source.journal_id)
            if identity_before not in (None, ("journal", "blob", 0)):
                raise ProvenanceValidationError(
                    "an immutable referent or identity domain was redefined"
                )
        elif state.phase == "entry":
            identity_before = self._identity(self.entry_id)
            if identity_before is not None:
                raise ProvenanceValidationError("entry identity reused or identity domains overlap")
            if control + len(self.body) + len(self.entry_id) + 16 > byte_budget:
                raise EntryPreparationCapacity("byte budget does not admit the source entry atom")
        elif state.phase == "changes":
            row_budget = min(row_budget, (byte_budget - control) // 90)
            if row_budget < 1:
                raise EntryPreparationCapacity("byte budget does not admit a changed identity")
        return PreparationBatch(
            self.attempt_id, self.binding, prefix, state.to_json(), identity_before, row_budget
        )

    @declare_growth(_phase_declaration("_apply"))
    def _apply(self, batch: PreparationBatch) -> PreparationState:
        if not self.db.in_transaction:
            raise RuntimeError("child application requires an owner transaction")
        if (batch.attempt_id, batch.binding) != (self.attempt_id, self.binding):
            raise StaleEntryPreparation("preparation proposal belongs to another input")
        state = PreparationState.from_json(batch.before_json)
        if self._prefix() != batch.prefix:
            raise StaleEntryPreparation("accepted prefix changed before preparation commit")
        self._boundaries(state)
        row = self._one(
            "SELECT 1 FROM journal_entry_preparation WHERE singleton=1 AND algorithm=? "
            "AND attempt_id=? AND binding=? AND prefix_attempt=? AND "
            "prefix_binding=? AND prefix=? AND checkpoint=?",
            (ALGORITHM, self.attempt_id, self.binding, *batch.prefix, batch.before_json),
        )
        if row is None:
            raise StaleEntryPreparation("preparation checkpoint was replaced")
        after = state
        if state.phase == "journal":
            if self._identity(self.source.journal_id) != batch.identity_before:
                raise StaleEntryPreparation("journal identity changed before admission")
            if batch.identity_before is None:
                self.db.execute(
                    "INSERT INTO identities VALUES (?,?,?)",
                    (self.source.journal_id, "journal", b""),
                ).close()
            after = replace(state, phase="entry", version=state.version + 1)
        elif state.phase == "entry":
            if self._identity(self.entry_id) is not None:
                raise StaleEntryPreparation("entry identity changed before admission")
            self.db.execute(
                "INSERT INTO identities VALUES (?,?,?)", (self.entry_id, "entry", self.body)
            ).close()
            after = replace(state, phase="changes", version=state.version + 1)
        elif state.phase == "changes":
            retired = self.store._reset_changed_assertions(row_budget=batch.row_budget)
            after = replace(
                state,
                phase="done" if retired == 0 else "changes",
                retired=state.retired + retired,
                version=state.version + 1,
            )
        if after != state:
            with closing(
                self.db.execute(
                    "UPDATE journal_entry_preparation SET checkpoint=? WHERE singleton=1 "
                    "AND algorithm=? AND attempt_id=? AND binding=? AND "
                    "prefix_attempt=? AND prefix_binding=? AND prefix=? AND "
                    "checkpoint=?",
                    (
                        after.to_json(),
                        ALGORITHM,
                        self.attempt_id,
                        self.binding,
                        *batch.prefix,
                        batch.before_json,
                    ),
                )
            ) as cursor:
                if cursor.rowcount != 1:
                    raise StaleEntryPreparation("preparation progress changed")
        return after

    def commit(self, batch: PreparationBatch) -> PreparationState:
        self._detached()
        if (batch.attempt_id, batch.binding) != (self.attempt_id, self.binding):
            raise StaleEntryPreparation("preparation proposal belongs to another input")
        state = PreparationState.from_json(batch.before_json)
        self.db.execute("BEGIN IMMEDIATE").close()
        try:
            state = self._apply(batch)
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        return state

    @declare_growth(_declaration())
    def advance(
        self, *, row_budget: int = MAX_ROWS, byte_budget: int = _DEFAULT_BYTES
    ) -> PreparationState:
        return self.commit(self.prepare(row_budget=row_budget, byte_budget=byte_budget))

    @declare_growth(_result_declaration())
    def result(self) -> None:
        if not self.load().done:
            raise RuntimeError("entry preparation remains quarantined")
