"""Private exact-prefix parent checkpoints for bounded journal validation.

The owning session prepares detached child work, checks this fence before any
mutation, and publishes child state plus its next parent checkpoint in one
transaction. A checkpoint is execution evidence in an owner-created workspace;
it does not authenticate caller-supplied databases or confer source custody.
"""

from __future__ import annotations

import json
import re
import sqlite3
from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass, replace
from typing import Any

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

from ._checkpoint_manifest import PREFIX_BINDING_OCTETS_MAX
from ._journal_prefix import PrefixState

ALGORITHM = "riverhog-journal-validation-checkpoint/v1"
_HEADER_CHARACTERS = 8192
_PHASES = frozenset(
    {
        "frame",
        "entry-admission",
        "entry",
        "targets-admission",
        "targets",
        "staging-admission",
        "staging",
        "retire-admission",
        "retire",
        "derivation-replace",
        "derivation-retire",
        "specialization-replace",
        "specialization-retire",
        "seed-admission",
        "seed",
        "row-admit",
        "generic-admission",
        "generic",
        "row-admission",
        "row",
        "lifecycle",
        "finish-admission",
        "finish",
        "recording",
        "publish",
        "terminal",
        "done",
        "failed",
    }
)
_CHILD_PHASES = frozenset(
    {"entry", "targets", "staging", "retire", "seed", "generic", "row", "finish"}
)
_ROW_PHASES = frozenset({"generic-admission", "generic", "row-admission", "row", "lifecycle"})


class StaleValidationCheckpoint(RuntimeError):
    """The attempt, inputs, worker, accepted prefix or proposed state changed."""


def _natural(value: object) -> int:
    if type(value) is not str or re.fullmatch(r"0|[1-9][0-9]*", value) is None:
        raise ValueError("checkpoint scalar")
    return int(value)


@dataclass(frozen=True)
class ValidationCheckpoint:
    version: int = 0
    phase: str = "frame"
    frame_ordinal: int = 0
    frame_sha256: str | None = None
    frame_octets: int = 0
    row_after: str | None = None
    row_key: str | None = None
    row_sha256: str | None = None
    child_id: str | None = None
    child_sha256: str | None = None

    def to_json(self) -> str:
        return canonical_json_bytes(
            {
                "version": str(self.version),
                "phase": self.phase,
                "frame_ordinal": str(self.frame_ordinal),
                "frame_sha256": self.frame_sha256,
                "frame_octets": str(self.frame_octets),
                "row_after": self.row_after,
                "row_key": self.row_key,
                "row_sha256": self.row_sha256,
                "child_id": self.child_id,
                "child_sha256": self.child_sha256,
            }
        ).decode()

    @classmethod
    def from_json(cls, raw: str) -> ValidationCheckpoint:
        try:
            if type(raw) is not str or len(raw) > _HEADER_CHARACTERS:
                raise ValueError("checkpoint envelope")
            value = json.loads(raw)
            if type(value) is not dict or set(value) != {
                "version",
                "phase",
                "frame_ordinal",
                "frame_sha256",
                "frame_octets",
                "row_after",
                "row_key",
                "row_sha256",
                "child_id",
                "child_sha256",
            }:
                raise ValueError("checkpoint shape")
            state = cls(
                _natural(value["version"]),
                value["phase"],
                _natural(value["frame_ordinal"]),
                value["frame_sha256"],
                _natural(value["frame_octets"]),
                value["row_after"],
                value["row_key"],
                value["row_sha256"],
                value["child_id"],
                value["child_sha256"],
            )
            if type(state.phase) is not str or state.phase not in _PHASES:
                raise ValueError("checkpoint phase")
            for digest in (state.frame_sha256, state.row_sha256, state.child_sha256):
                if digest is not None and (
                    type(digest) is not str or re.fullmatch(r"[0-9a-f]{64}", digest) is None
                ):
                    raise ValueError("checkpoint atom digest")
            for identity in (state.row_after, state.row_key):
                if identity is not None:
                    require_canonical_uuid_urn(identity)
            if state.child_id is not None and (
                type(state.child_id) is not str
                or re.fullmatch(r"[0-9a-f]{32}", state.child_id) is None
            ):
                raise ValueError("checkpoint child identity")
            if state.phase in {"frame", "terminal", "done"}:
                if (
                    state.frame_sha256 is not None
                    or state.frame_octets
                    or state.row_after is not None
                    or state.row_key is not None
                    or state.row_sha256 is not None
                    or state.child_id is not None
                    or state.child_sha256 is not None
                ):
                    raise ValueError("checkpoint inactive frame")
            elif state.phase != "failed" and (
                state.frame_sha256 is None or state.frame_octets == 0
            ):
                raise ValueError("checkpoint active frame")
            if state.phase in _CHILD_PHASES and (
                state.child_id is None or state.child_sha256 is None
            ):
                raise ValueError("checkpoint active child")
            if state.phase in _ROW_PHASES and (state.row_key is None or state.row_sha256 is None):
                raise ValueError("checkpoint active row")
            if (
                state.row_key is not None
                and state.row_after is not None
                and state.row_key <= state.row_after
            ):
                raise ValueError("checkpoint row cursor")
            if state.to_json() != raw:
                raise ValueError("noncanonical checkpoint")
            return state
        except (ValueError, TypeError, KeyError, OverflowError) as exc:
            raise RuntimeError("corrupt journal validation checkpoint") from exc


@dataclass(frozen=True)
class ValidationSnapshot:
    checkpoint: ValidationCheckpoint
    prefix: tuple[str, bytes, str]


def install(db: sqlite3.Connection) -> None:
    db.execute(
        "CREATE TABLE journal_validation(singleton INTEGER PRIMARY KEY CHECK(singleton=1), "
        "algorithm TEXT NOT NULL,attempt_id TEXT NOT NULL,binding BLOB NOT NULL, "
        "worker_epoch TEXT NOT NULL,checkpoint TEXT NOT NULL, "
        "prefix_attempt TEXT NOT NULL,prefix_binding BLOB NOT NULL,prefix TEXT NOT NULL)"
    ).close()


def _checkpoint_declaration(
    operation: str, *, delegates: tuple[tuple[str, Callable[..., Any]], ...] = ()
) -> Declaration:
    mutation = operation in {"_initialize", "initialize", "_publish", "replace_worker"}

    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    width = Expression("dimension", name="metadata_row_octets")

    def octets(rows: int) -> Expression:
        return Expression("product", terms=(count(rows), width))

    bounds = (
        Bound(
            "sql_statements",
            COUNT,
            "upper",
            count(14),
            "Fixed point probes, one permitted mutation and transaction control.",
        ),
        Bound(
            "returned_rows",
            COUNT,
            "upper",
            count(8),
            "Only fixed parent/prefix controls and exact predecessor references are returned.",
        ),
        Bound(
            "changed_rows",
            COUNT,
            "upper" if operation == "_publish" else "equal",
            count(int(mutation)),
            "Publication attempts change at most one parent row; successful publication changes "
            "exactly one, including SQL work later rolled back after interruption.",
        ),
        Bound(
            "returned_metadata_octets",
            OCTETS,
            "upper",
            octets(8),
            "Actual checkpoint, binding, epoch and predecessor metadata retain their byte widths.",
        ),
        Bound(
            "bound_parameter_octets",
            OCTETS,
            "upper",
            octets(14),
            "Exact point keys and old/new checkpoint/source/worker fences.",
        ),
        Bound(
            "written_value_octets",
            OCTETS,
            "upper" if mutation else "equal",
            octets(1) if mutation else Expression("constant", value=0, unit=OCTETS),
            "The one mutation contains fixed-shape old/new controls, never archive payload.",
        ),
        Bound(
            "adjacency_rows",
            COUNT,
            "equal",
            count(0),
            "Parent checkpoint control never walks native graph adjacency.",
        ),
        Bound(
            "source_payload_octets",
            OCTETS,
            "equal",
            Expression("constant", value=0, unit=OCTETS),
            "The exact predecessor probe returns its reference, never historical frame bytes.",
        ),
        Bound(
            "sqlite_vm_instructions",
            COUNT,
            "upper",
            Expression(
                "sum",
                terms=(
                    count(16384),
                    Expression(
                        "product",
                        terms=(
                            count(2048),
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
            "Fixed indexed work; byte comparisons and physical I/O stay separate.",
        ),
    )

    def child_total(metric: str) -> Expression:
        terms = tuple(
            Expression(
                "delegate",
                name=child_name,
                metric=metric,
                terms=(count(1),),
                arguments=(
                    ("retained_rows", Expression("dimension", name="retained_rows")),
                    ("metadata_row_octets", width),
                ),
            )
            for child_name, _child in delegates
        )
        return terms[0] if len(terms) == 1 else Expression("sum", terms=terms)

    child_bounds = (
        tuple(
            Bound(
                "checkpoint_" + bound.metric,
                bound.unit,
                "upper",
                child_total(bound.metric),
                "Actual child subtotal already included in the inclusive counters.",
            )
            for bound in bounds
        )
        if delegates
        else ()
    )
    return Declaration(
        id="riverhog-provenance.sqlite-validation-checkpoint-"
        + (
            "initialization-application"
            if operation == "_initialize"
            else operation.strip("_").replace("_", "-")
        )
        + ".growth/v1",
        authority="riverhog-provenance",
        subjects=("riverhog_provenance._session_checkpoint.ValidationLedger." + operation,),
        scope="native-sqlite-exact-source-parent-checkpoint-control",
        starts_at=("receive exact owner/prefix/worker inputs under declared transaction custody"),
        ends_at=(
            "return fixed state or publish one fenced checkpoint without history payload; "
            "load, fence and publication include typed stale-state rejection"
        ),
        dimensions=(
            Dimension(
                "retained_rows", COUNT, "Actual rows in the owned parent/prefix/frame indexes."
            ),
            Dimension(
                "metadata_row_octets",
                OCTETS_PER_COUNT,
                "Actual old/new checkpoint, prefix, binding and worker control envelope.",
            ),
        ),
        bounds=(*bounds, *child_bounds),
        conditions=(
            "The owner retains verified private state and an exact source/prefix binding.",
            "Initialization begins with an empty accepted prefix and absent parent checkpoint.",
            "load is detached; private application requires the owner transaction. "
            "initialize and worker replacement own their transactions.",
            "The metadata envelope retains actual old/new binding, epoch and state widths.",
        ),
        invariants=(
            "Only the parent checkpoint changes; frames and accepted prefix stay intact.",
            "Rejected load/fence/publication leaves state unchanged. Interrupted publication "
            "retains the caller transaction; rollback removes its single attempted mutation.",
            "Private application neither commits nor rolls back the caller transaction.",
            "Publication advances one version; replaced workers cannot change progress.",
            "Named child subtotals remain subsets of inclusive primitive observations.",
        ),
        limitations=(
            "Private checkpoints confer no source authentication or archive authority.",
            "Source/index/catalog work, semantics and prefix publication "
            "require their owning evidence.",
            "Lock waits, fsync, I/O and B-tree byte comparisons remain separate costs.",
        ),
    )


class ValidationLedger:
    """Fixed-shape worker and parent fence; the session owns every transaction."""

    def __init__(
        self, db: sqlite3.Connection, *, attempt_id: str, binding: bytes, worker_epoch: int
    ) -> None:
        if type(attempt_id) is not str or re.fullmatch(r"[0-9a-f]{32}", attempt_id) is None:
            raise ValueError("validation requires an exact owned attempt identity")
        if type(binding) is not bytes or not binding or len(binding) > PREFIX_BINDING_OCTETS_MAX:
            raise ValueError("invalid private validation binding envelope")
        if type(worker_epoch) is not int or worker_epoch < 0:
            raise ValueError("validation requires a nonnegative worker epoch")
        self.db, self.attempt_id, self.binding, self.worker_epoch = (
            db,
            attempt_id,
            binding,
            worker_epoch,
        )

    def _detached(self) -> None:
        if self.db.in_transaction:
            raise RuntimeError("validation preparation requires a closed owner transaction")

    def _prefix(self) -> tuple[str, bytes, str]:
        with closing(
            self.db.execute(
                "SELECT substr(attempt_id,1,33),substr(binding,1,?),substr(checkpoint,1,?) "
                "FROM journal_prefix WHERE singleton=1",
                (PREFIX_BINDING_OCTETS_MAX + 1, _HEADER_CHARACTERS + 1),
            )
        ) as cursor:
            row = cursor.fetchone()
        if (
            row is None
            or row[0] != self.attempt_id
            or type(row[1]) is not bytes
            or len(row[1]) > PREFIX_BINDING_OCTETS_MAX
        ):
            raise StaleValidationCheckpoint("validation prefix belongs to another attempt")
        state = PrefixState.from_json(row[2])
        if state.frame_count:
            with closing(
                self.db.execute(
                    "SELECT substr(reference,1,513) FROM frames WHERE sequence=?",
                    (str(state.frame_count - 1),),
                )
            ) as cursor:
                previous = cursor.fetchone()
            if (
                previous is None
                or state.previous_json is None
                or previous[0] != state.previous_json.encode()
            ):
                raise StaleValidationCheckpoint("validation prefix predecessor link changed")
        return row[0], row[1], row[2]

    @declare_growth(_checkpoint_declaration("_initialize"))
    def _initialize(self) -> ValidationSnapshot:
        if not self.db.in_transaction:
            raise RuntimeError("validation initialization requires an owner transaction")
        prefix = self._prefix()
        if PrefixState.from_json(prefix[2]).frame_count:
            raise RuntimeError("validation initialization requires an empty accepted prefix")
        state = ValidationCheckpoint()
        self.db.execute(
            "INSERT INTO journal_validation VALUES(1,?,?,?,?,?,?,?,?)",
            (
                ALGORITHM,
                self.attempt_id,
                self.binding,
                str(self.worker_epoch),
                state.to_json(),
                *prefix,
            ),
        ).close()
        return ValidationSnapshot(state, prefix)

    @declare_growth(
        _checkpoint_declaration("initialize", delegates=(("initialization", _initialize),)),
        delegates=(("initialization", _initialize),),
    )
    def initialize(self) -> ValidationSnapshot:
        self._detached()
        try:
            self.db.execute("BEGIN IMMEDIATE").close()
            result = self._initialize()
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        return result

    def _read(self) -> ValidationSnapshot:
        with closing(
            self.db.execute(
                "SELECT substr(checkpoint,1,?),substr(prefix_attempt,1,33),"
                "substr(prefix_binding,1,?),substr(prefix,1,?) "
                "FROM journal_validation WHERE singleton=1 AND algorithm=? AND attempt_id=? "
                "AND binding=? AND worker_epoch=?",
                (
                    _HEADER_CHARACTERS + 1,
                    PREFIX_BINDING_OCTETS_MAX + 1,
                    _HEADER_CHARACTERS + 1,
                    ALGORITHM,
                    self.attempt_id,
                    self.binding,
                    str(self.worker_epoch),
                ),
            )
        ) as cursor:
            row = cursor.fetchone()
        if row is None:
            raise StaleValidationCheckpoint("validation attempt, inputs or worker changed")
        state = ValidationCheckpoint.from_json(row[0])
        prefix = self._prefix()
        if (
            tuple(row[1:]) != prefix
            or PrefixState.from_json(prefix[2]).frame_count != state.frame_ordinal
        ):
            raise StaleValidationCheckpoint("validation accepted-prefix marker changed")
        return ValidationSnapshot(state, prefix)

    @declare_growth(_checkpoint_declaration("load"))
    def load(self) -> ValidationSnapshot:
        self._detached()
        return self._read()

    @declare_growth(_checkpoint_declaration("_fence"))
    def _fence(self, before: ValidationSnapshot) -> None:
        if not self.db.in_transaction:
            raise RuntimeError("validation fencing requires an owner transaction")
        if self._read() != before:
            raise StaleValidationCheckpoint("validation proposal was superseded")

    @declare_growth(_checkpoint_declaration("_publish"))
    def _publish(
        self,
        before: ValidationSnapshot,
        after: ValidationCheckpoint,
        *,
        prefix: tuple[str, bytes, str] | None = None,
    ) -> ValidationSnapshot:
        if not self.db.in_transaction:
            raise RuntimeError("validation publication requires an owner transaction")
        if after.version != before.checkpoint.version + 1:
            raise ValueError("validation checkpoint must advance exactly one version")
        # Validate even internally constructed state before retaining it.
        ValidationCheckpoint.from_json(after.to_json())
        selected = before.prefix if prefix is None else prefix
        if (
            selected != self._prefix()
            or PrefixState.from_json(selected[2]).frame_count != after.frame_ordinal
        ):
            raise StaleValidationCheckpoint("validation publication lost its exact prefix")
        with closing(
            self.db.execute(
                "UPDATE journal_validation SET checkpoint=?,"
                "prefix_attempt=?,prefix_binding=?,prefix=? "
                "WHERE singleton=1 AND algorithm=? AND attempt_id=? "
                "AND binding=? AND worker_epoch=? "
                "AND checkpoint=? AND prefix_attempt=? AND prefix_binding=? AND prefix=?",
                (
                    after.to_json(),
                    *selected,
                    ALGORITHM,
                    self.attempt_id,
                    self.binding,
                    str(self.worker_epoch),
                    before.checkpoint.to_json(),
                    *before.prefix,
                ),
            )
        ) as cursor:
            if cursor.rowcount != 1:
                raise StaleValidationCheckpoint("validation proposal was superseded")
        return ValidationSnapshot(after, selected)

    @declare_growth(
        _checkpoint_declaration("replace_worker", delegates=(("load", load), ("fence", _fence))),
        delegates=(("load", load), ("fence", _fence)),
    )
    def replace_worker(self, worker_epoch: int) -> ValidationLedger:
        """Fence a replaced worker without replaying or discarding completed work."""
        if type(worker_epoch) is not int or worker_epoch <= self.worker_epoch:
            raise ValueError("replacement worker requires a newer exact epoch")
        self._detached()
        before = self.load()
        after = replace(before.checkpoint, version=before.checkpoint.version + 1)
        try:
            self.db.execute("BEGIN IMMEDIATE").close()
            self._fence(before)
            with closing(
                self.db.execute(
                    "UPDATE journal_validation SET worker_epoch=?,checkpoint=? WHERE singleton=1 "
                    "AND algorithm=? AND attempt_id=? AND binding=? "
                    "AND worker_epoch=? AND checkpoint=?",
                    (
                        str(worker_epoch),
                        after.to_json(),
                        ALGORITHM,
                        self.attempt_id,
                        self.binding,
                        str(self.worker_epoch),
                        before.checkpoint.to_json(),
                    ),
                )
            ) as cursor:
                if cursor.rowcount != 1:
                    raise StaleValidationCheckpoint("validation worker replacement was superseded")
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise
        return ValidationLedger(
            self.db, attempt_id=self.attempt_id, binding=self.binding, worker_epoch=worker_epoch
        )
