"""Fence one private graph insertion to its exact native semantic source.

The owner supplies an already checked assertion and retains source custody. Graph
repair, rejection undo and cleanup share each bounded graph transaction with this
phase's source/prefix fence and checkpoint. No accepted journal marker advances.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from contextlib import closing
from dataclasses import dataclass, replace
from typing import Any

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

from ._journal_prefix import PrefixState
from ._online_dag import OnlineDag, _Plan, _Snapshot, _Work
from ._semantic_source import SemanticSource
from .errors import ProvenanceValidationError

ALGORITHM = "riverhog-semantic-edge/v1"
_HEADER_OCTETS = 2048
_BINDING_OCTETS = 8192


class StaleSemanticEdge(RuntimeError):
    """The exact semantic input, prefix, graph run or operation was replaced."""


@dataclass(frozen=True)
class EdgeState:
    phase: str = "start"
    version: int = 0

    @property
    def done(self) -> bool:
        return self.phase == "done"

    def to_json(self) -> str:
        return canonical_json_bytes({"phase": self.phase, "version": str(self.version)}).decode()

    @classmethod
    def from_json(cls, raw: str) -> EdgeState:
        try:
            if type(raw) is not str or len(raw) > _HEADER_OCTETS:
                raise ValueError("checkpoint envelope")
            value = json.loads(raw)
            if (
                type(value) is not dict
                or set(value) != {"phase", "version"}
                or value["phase"] not in {"start", "graph", "done"}
                or type(value["version"]) is not str
                or re.fullmatch(r"0|[1-9][0-9]*", value["version"]) is None
            ):
                raise ValueError("checkpoint shape")
            result = cls(value["phase"], int(value["version"]))
            if result.to_json() != raw:
                raise ValueError("noncanonical checkpoint")
            return result
        except (ValueError, TypeError, KeyError) as exc:
            raise RuntimeError("corrupt semantic graph checkpoint") from exc


@dataclass(frozen=True)
class EdgeBatch:
    binding: bytes
    attempt_id: str
    prefix: tuple[str, bytes, str]
    before_json: str
    after: EdgeState
    graph_before: _Snapshot
    graph_plan: _Plan | None


def install(db: sqlite3.Connection) -> None:
    db.execute(
        "CREATE TABLE journal_semantic_edges(kind TEXT PRIMARY KEY, algorithm TEXT NOT NULL, "
        "attempt_id TEXT NOT NULL, binding BLOB NOT NULL, prefix_attempt TEXT NOT NULL, "
        "prefix_binding BLOB NOT NULL, prefix TEXT NOT NULL, checkpoint TEXT NOT NULL)"
    ).close()


def _declaration() -> Declaration:
    budget = Expression("dimension", name="row_budget")

    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def linear(fixed: int, multiplier: int) -> Expression:
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

    returned = linear(32, 2)
    return Declaration(
        id="riverhog-provenance.sqlite-semantic-edge.growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance._semantic_edge.SemanticEdge.advance",),
        scope="native-sqlite-source-fenced-semantic-graph-unit",
        starts_at="load one exact semantic source, accepted prefix and owned graph continuation",
        ends_at="commit graph work with its fenced checkpoint, then close the unit",
        dimensions=(
            Dimension("retained_rows", COUNT, "Rows retained in the owned private workspace."),
            Dimension("row_budget", COUNT, "One to 64 admitted graph adjacency or cleanup rows."),
            Dimension(
                "metadata_row_octets",
                OCTETS_PER_COUNT,
                "Actual column envelope including fixed-shape source and graph headers.",
            ),
        ),
        bounds=(
            Bound(
                "sql_statements",
                COUNT,
                "upper",
                linear(64, 4),
                "One bounded native graph unit plus indexed semantic fence and checkpoint work.",
            ),
            Bound(
                "returned_rows",
                COUNT,
                "upper",
                returned,
                "Bounded graph metadata and fixed source/prefix/subject point reads.",
            ),
            Bound(
                "changed_rows",
                COUNT,
                "upper",
                linear(16, 3),
                "Bounded graph/frontier/undo writes and one semantic checkpoint.",
            ),
            Bound(
                "adjacency_rows",
                COUNT,
                "upper",
                budget,
                "At most one admitted native adjacency window.",
            ),
            Bound(
                "returned_metadata_octets",
                OCTETS,
                "upper",
                Expression(
                    "product", terms=(returned, Expression("dimension", name="metadata_row_octets"))
                ),
                "Actual encoded result columns retain their owning scalar widths.",
            ),
            Bound(
                "source_payload_octets",
                OCTETS,
                "equal",
                Expression("constant", value=0, unit=OCTETS),
                "No original frame or assertion payload is returned to the work unit.",
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        linear(8192, 4096),
                        Expression(
                            "product",
                            terms=(
                                count(1024),
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
                "Bounded graph instructions and indexed owner checks; physical I/O is separate.",
            ),
        ),
        conditions=(
            "The owner retains the exact immutable assertion/source/catalog/modes and graph run.",
            "The workspace has the native graph, assertion, changed-row and prefix indexes.",
            "Close source transactions before planning; the unit owns its checkpoint transaction.",
            "The metadata envelope includes actual source/checkpoint/scalar column widths.",
        ),
        invariants=(
            "Graph changes and the semantic checkpoint share the exact accepted-prefix fence.",
            "Ordinary restoration never initializes or starts an already retained operation again.",
            "Rejected graph work remains quarantined until bounded undo and cleanup complete.",
            "This private phase publishes no accepted prefix and releases no source custody.",
        ),
        limitations=(
            "Entry decoding, row/reference checks and catalog identity are separate delegates.",
            "Graph visits and repeated rejection retain the native online amortized model.",
            "Allocation, B-tree comparisons, I/O, waits and fsync are separate costs.",
            "Whole-session durability requires an owner-retained crash-safe workspace and source.",
        ),
    )


class SemanticEdge:
    def __init__(
        self,
        db: sqlite3.Connection,
        *,
        kind: str,
        run_id: str,
        attempt_id: str,
        source_binding: SemanticSource,
        assertion_id: str,
        object_id: str,
        source: str,
        target: str,
    ) -> None:
        if type(attempt_id) is not str or not attempt_id:
            raise ValueError("an explicit semantic graph attempt is required")
        if type(source_binding) is not SemanticSource:
            raise ValueError("an exact semantic source descriptor is required")
        for identity in (assertion_id, object_id, source, target):
            require_canonical_uuid_urn(identity)
        self.db, self.kind, self.run_id, self.attempt_id = db, kind, run_id, attempt_id
        self.assertion_id, self.object_id, self.source, self.target = (
            assertion_id,
            object_id,
            source,
            target,
        )
        self.sequence, self.journal_id = source_binding.sequence, source_binding.journal_id
        self.checker = OnlineDag(db, kind, run_id)
        self.binding = canonical_json_bytes(
            {
                "algorithm": ALGORITHM,
                "source_sha256": source_binding.sha256,
                "catalog_sha256": source_binding.catalog_sha256,
                "require_profiles": source_binding.require_profiles,
                "kind": kind,
                "run_id": run_id,
                "assertion_id": assertion_id,
                "object_id": object_id,
                "source": source,
                "target": target,
            }
        )

    def _one(self, sql: str, parameters: tuple[Any, ...] = ()) -> Any:
        with closing(self.db.execute(sql, parameters)) as rows:
            return rows.fetchone()

    def _detached(self) -> None:
        if self.db.in_transaction:
            raise RuntimeError("semantic graph units require a closed owner transaction")

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
            raise RuntimeError("missing or invalid semantic accepted-prefix manifest")
        prefix = PrefixState.from_json(row[2])
        if str(prefix.frame_count) != self.sequence or prefix.journal_id not in (
            None,
            self.journal_id,
        ):
            raise StaleSemanticEdge("semantic frame differs from its accepted prefix")
        return row[0], row[1], row[2]

    def _subject(self) -> None:
        row = self._one(
            "SELECT a.object_id,a.type,a.active FROM changes c LEFT JOIN assertions a "
            "INDEXED BY semantic_subject "
            "ON a.assertion_id=c.assertion_id WHERE c.assertion_id=?",
            (self.assertion_id,),
        )
        if row != (self.object_id, self.kind, 1):
            raise StaleSemanticEdge("semantic graph subject is missing or changed")

    def _dag(self) -> _Snapshot:
        row = self._one("SELECT run_id,version,work FROM dag_runs WHERE kind=?", (self.kind,))
        if row is None or row[0] != self.run_id:
            raise StaleSemanticEdge("semantic graph run changed")
        return _Snapshot(OnlineDag._version(row[1]), _Work.decode(row[2]))

    def _operation(self, prefix: tuple[str, bytes, str]) -> str:
        return hashlib.sha256(
            canonical_json_bytes(
                {
                    "attempt_id": self.attempt_id,
                    "binding": self.binding.decode(),
                    "prefix_attempt": prefix[0],
                    "prefix_binding_sha256": hashlib.sha256(prefix[1]).hexdigest(),
                    "prefix": prefix[2],
                }
            )
        ).hexdigest()

    def initialize(self) -> EdgeState:
        """Explicitly admit a new operation; restoration calls load/advance instead."""
        self._detached()
        self.db.execute("BEGIN IMMEDIATE").close()
        try:
            prefix = self._prefix()
            self._subject()
            graph = self._dag()
            if graph.work.phase not in {"idle", "complete"}:
                raise StaleSemanticEdge("a graph operation is still quarantined")
            old = self._one(
                "SELECT algorithm,attempt_id,substr(checkpoint,1,?) "
                "FROM journal_semantic_edges WHERE kind=?",
                (_HEADER_OCTETS + 1, self.kind),
            )
            version = 0
            if old is not None:
                if old[0] != ALGORITHM:
                    raise RuntimeError("unknown semantic graph algorithm")
                prior = EdgeState.from_json(old[2])
                if old[1] == self.attempt_id or not prior.done:
                    raise StaleSemanticEdge("semantic graph operation is already bound")
                version = prior.version + 1
            state = EdgeState(version=version)
            self.db.execute(
                "INSERT INTO journal_semantic_edges VALUES (?,?,?,?,?,?,?,?) "
                "ON CONFLICT(kind) DO UPDATE SET algorithm=excluded.algorithm, "
                "attempt_id=excluded.attempt_id,binding=excluded.binding, "
                "prefix_attempt=excluded.prefix_attempt,prefix_binding=excluded.prefix_binding, "
                "prefix=excluded.prefix,checkpoint=excluded.checkpoint",
                (self.kind, ALGORITHM, self.attempt_id, self.binding, *prefix, state.to_json()),
            ).close()
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        return state

    def _load(self) -> tuple[EdgeState, tuple[str, bytes, str], _Snapshot]:
        self._detached()
        row = self._one(
            "SELECT algorithm,substr(prefix_attempt,1,?),substr(prefix_binding,1,?), "
            "substr(prefix,1,?),substr(checkpoint,1,?) FROM journal_semantic_edges "
            "WHERE kind=? AND attempt_id=? AND binding=?",
            (
                _HEADER_OCTETS + 1,
                _BINDING_OCTETS + 1,
                _HEADER_OCTETS + 1,
                _HEADER_OCTETS + 1,
                self.kind,
                self.attempt_id,
                self.binding,
            ),
        )
        if row is None:
            raise StaleSemanticEdge("semantic graph inputs were superseded")
        if row[0] != ALGORITHM:
            raise RuntimeError("unknown semantic graph algorithm")
        prefix = self._prefix()
        if row[1:4] != prefix:
            raise StaleSemanticEdge("accepted prefix changed during semantic graph work")
        self._subject()
        state, graph = EdgeState.from_json(row[4]), self._dag()
        operation = self._operation(prefix)
        if state.phase == "start":
            if graph.work.phase not in {"idle", "complete"} or graph.work.operation_id == operation:
                raise RuntimeError("semantic graph start cursor differs from its graph")
        elif (
            graph.work.operation_id != operation
            or (graph.work.source, graph.work.target) != (self.source, self.target)
            or state.done
            and graph.work.phase != "complete"
        ):
            raise StaleSemanticEdge("semantic graph continuation changed")
        return state, prefix, graph

    def load(self) -> EdgeState:
        return self._load()[0]

    def prepare(self, *, row_budget: int = 64) -> EdgeBatch:
        OnlineDag._budget(row_budget)
        state, prefix, graph = self._load()
        plan = None
        after = state
        if state.phase == "start":
            plan = self.checker._prepare_start(self._operation(prefix), self.source, self.target)
            after = replace(state, phase="graph", version=state.version + 1)
        elif state.phase == "graph":
            if graph.work.phase == "complete":
                after = replace(state, phase="done", version=state.version + 1)
            else:
                plan = self.checker._prepare(row_budget)
                after = replace(state, version=state.version + 1)
        if plan is not None and plan.before != graph:
            raise StaleSemanticEdge("graph changed during detached semantic planning")
        return EdgeBatch(self.binding, self.attempt_id, prefix, state.to_json(), after, graph, plan)

    def commit(self, batch: EdgeBatch) -> EdgeState:
        self._detached()
        if (batch.binding, batch.attempt_id) != (self.binding, self.attempt_id):
            raise StaleSemanticEdge("semantic graph proposal belongs to another input")
        before = EdgeState.from_json(batch.before_json)
        self.db.execute("BEGIN IMMEDIATE").close()
        try:
            if self._prefix() != batch.prefix or self._dag() != batch.graph_before:
                raise StaleSemanticEdge("semantic graph or accepted prefix changed before commit")
            self._subject()
            if batch.after == before:
                row = self._one(
                    "SELECT 1 FROM journal_semantic_edges WHERE kind=? AND attempt_id=? "
                    "AND binding=? AND prefix_attempt=? AND prefix_binding=? AND prefix=? "
                    "AND checkpoint=? AND algorithm=?",
                    (
                        self.kind,
                        self.attempt_id,
                        self.binding,
                        *batch.prefix,
                        batch.before_json,
                        ALGORITHM,
                    ),
                )
                if row is None:
                    raise StaleSemanticEdge("finished semantic graph worker was replaced")
            else:
                with closing(
                    self.db.execute(
                        "UPDATE journal_semantic_edges SET checkpoint=? WHERE kind=? "
                        "AND attempt_id=? "
                        "AND binding=? AND prefix_attempt=? AND prefix_binding=? AND prefix=? "
                        "AND checkpoint=? AND algorithm=?",
                        (
                            batch.after.to_json(),
                            self.kind,
                            self.attempt_id,
                            self.binding,
                            *batch.prefix,
                            batch.before_json,
                            ALGORITHM,
                        ),
                    )
                ) as cursor:
                    if cursor.rowcount != 1:
                        raise StaleSemanticEdge("semantic graph checkpoint changed")
                if batch.graph_plan is not None and not self.checker._apply(batch.graph_plan):
                    raise StaleSemanticEdge("semantic graph proposal was superseded")
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        return batch.after

    @declare_growth(_declaration())
    def advance(self, *, row_budget: int = 64) -> EdgeState:
        return self.commit(self.prepare(row_budget=row_budget))

    def result(self) -> None:
        state, _prefix, graph = self._load()
        if not state.done:
            raise RuntimeError("semantic graph operation remains quarantined")
        if graph.work.outcome == "self":
            raise ProvenanceValidationError(f"{self.kind} cannot relate an entity to itself")
        if graph.work.outcome == "cycle":
            raise ProvenanceValidationError(f"{self.kind} graph contains a cycle")
        if graph.work.outcome != "accepted":
            raise RuntimeError("semantic graph has no accepted or rejected outcome")
