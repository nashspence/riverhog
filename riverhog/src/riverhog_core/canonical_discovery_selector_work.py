"""Bounded fixed-selector deduplication, pending work and atomic incidence output."""

from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import dataclass, replace
from typing import Any, cast

from riverhog_canonical_json import (
    canonical_json_bytes,
    format_scalar,
    parse_scalar,
    require_canonical_json,
)
from riverhog_provenance_contracts import ExternalReference

from riverhog_core.canonical_discovery_archive_work import (
    ArchiveTaskKey,
    StaleArchiveTask,
    _atomic,
    _one,
    _task,
)
from riverhog_core.canonical_discovery_selector_growth import declare_growth_for_selectors
from riverhog_core.canonical_discovery_selectors import ImportedLateGuard, Selector

_COLUMNS = "worker_id,coordinates_json,version,position_json,complete,last_output_json"
_UNARY = {
    "member": {
        "state",
        "collection",
        "history-documentary",
        "late-bound",
        "continuity",
        "import-set-reachable",
    },
    "history-documentary": {"root-set-documentary", "import-set-documentary"},
    "root-set-documentary": {"snapshot-documentary"},
    "import-set-documentary": {"history-documentary"},
    "import-set-reachable": {"import-reachable"},
    "snapshot-documentary": {"prefix-dependencies"},
    "prefix-dependencies": {"prefix-dependencies", "snapshot-documentary"},
    "import-reachable": {"import-set-reachable"},
    "late-bound": {"bound-endpoint"},
    "continuity": {"state"},
    "own": {"direct"},
    "observed": {"direct"},
    "activity": {"own", "direct"},
    "state": {"own", "observed", "direct", "causal-state", "activity", "state"},
}


class SelectorCapacity(RuntimeError):
    """The current execution quantum cannot hold one complete selector control."""


@dataclass(frozen=True, slots=True)
class SelectorProgress:
    selector: Selector
    identity: str
    version: int
    position: tuple[str, ...]
    complete: bool
    last_output_json: str | None


@dataclass(frozen=True, slots=True)
class PreparedSelectorPage:
    key: ArchiveTaskKey
    expected: SelectorProgress
    result: SelectorProgress
    output: Selector | ImportedLateGuard | None


class SelectorWork:
    """Retain one bounded preparation; metadata retains custody and worker epoch."""

    def __init__(self, key: ArchiveTaskKey, *, octet_budget: int) -> None:
        if type(octet_budget) is not int or octet_budget < 1:
            raise ValueError("selector execution capacity must be positive")
        self.key, self.octet_budget = key, octet_budget
        self._prepared: PreparedSelectorPage | None = None


def initialize_selector_work(db: sqlite3.Connection) -> None:
    if db.in_transaction:
        raise ValueError("selector initialization requires no active transaction")
    db.executescript("""
        CREATE TABLE IF NOT EXISTS discovery_selectors (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL,
            selector_id TEXT NOT NULL, worker_id TEXT NOT NULL,
            coordinates_json TEXT NOT NULL, version TEXT NOT NULL,
            position_json TEXT NOT NULL, complete INTEGER NOT NULL CHECK(complete IN (0,1)),
            last_output_json TEXT,
            PRIMARY KEY(build_id,admission_sha256,selector_id),
            FOREIGN KEY(build_id,admission_sha256)
                REFERENCES discovery_archive_tasks(build_id,admission_sha256)
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS discovery_selector_pending
        ON discovery_selectors(build_id,admission_sha256,selector_id) WHERE complete=0;
        CREATE TABLE IF NOT EXISTS discovery_selector_unary (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL,
            parent_id TEXT NOT NULL, child_id TEXT NOT NULL,
            PRIMARY KEY(build_id,admission_sha256,parent_id,child_id),
            FOREIGN KEY(build_id,admission_sha256,parent_id)
                REFERENCES discovery_selectors(build_id,admission_sha256,selector_id),
            FOREIGN KEY(build_id,admission_sha256,child_id)
                REFERENCES discovery_selectors(build_id,admission_sha256,selector_id)
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS discovery_selector_parents
        ON discovery_selector_unary(build_id,admission_sha256,child_id,parent_id);
        CREATE TABLE IF NOT EXISTS discovery_selector_imported_late (
            build_id TEXT NOT NULL, admission_sha256 TEXT NOT NULL,
            import_id TEXT NOT NULL, causal_id TEXT NOT NULL, target_id TEXT NOT NULL,
            PRIMARY KEY(build_id,admission_sha256,import_id,causal_id,target_id),
            FOREIGN KEY(build_id,admission_sha256,import_id)
                REFERENCES discovery_selectors(build_id,admission_sha256,selector_id),
            FOREIGN KEY(build_id,admission_sha256,causal_id)
                REFERENCES discovery_selectors(build_id,admission_sha256,selector_id),
            FOREIGN KEY(build_id,admission_sha256,target_id)
                REFERENCES discovery_selectors(build_id,admission_sha256,selector_id)
        ) WITHOUT ROWID;
        CREATE INDEX IF NOT EXISTS discovery_selector_guard_causal
        ON discovery_selector_imported_late(
            build_id,admission_sha256,causal_id,import_id,target_id
        );
        CREATE INDEX IF NOT EXISTS discovery_selector_guard_target
        ON discovery_selector_imported_late(
            build_id,admission_sha256,target_id,import_id,causal_id
        );
    """)


def _encoded(value: object, budget: int) -> str:
    raw = canonical_json_bytes(value)
    if len(raw) > budget:
        raise SelectorCapacity("selector control exceeds the current execution capacity")
    return raw.decode()


def _identity(selector: Selector, budget: int) -> tuple[str, str]:
    raw = _encoded(selector.to_mapping(), budget)
    return hashlib.sha256(raw.encode()).hexdigest(), raw


def _equal_coordinates(left: str, right: str) -> bool:
    return left == right


def _binding(db: sqlite3.Connection, key: ArchiveTaskKey, identity: str, raw: str) -> Any:
    row = _one(
        db,
        "SELECT " + _COLUMNS + " FROM discovery_selectors "
        "WHERE build_id=? AND admission_sha256=? AND selector_id=?",
        (key.build_id, key.admission_sha256, identity),
    )
    if row is not None and (row[0] != key.worker_id or not _equal_coordinates(row[1], raw)):
        raise StaleArchiveTask("selector digest candidate differs from its original coordinates")
    return row


def _intern(db: sqlite3.Connection, key: ArchiveTaskKey, selector: Selector, budget: int) -> str:
    identity, raw = _identity(selector, budget)
    if _binding(db, key, identity, raw) is None:
        db.execute(
            "INSERT INTO discovery_selectors VALUES (?,?,?,?,?,'0','[]',0,NULL)",
            (key.build_id, key.admission_sha256, identity, key.worker_id, raw),
        ).close()
    return identity


def intern_selector(
    db: sqlite3.Connection, key: ArchiveTaskKey, selector: Selector, *, octet_budget: int
) -> str:
    """Intern exact fixed coordinates once, without a path/depth dedup key."""
    if type(octet_budget) is not int or octet_budget < 1:
        raise ValueError("selector execution capacity must be positive")
    with _atomic(db):
        _task(db, key)
        return _intern(db, key, selector, octet_budget)


def _output_mapping(output: Selector | ImportedLateGuard | None) -> Any:
    if isinstance(output, Selector):
        return {"unary": output.to_mapping()}
    if isinstance(output, ImportedLateGuard):
        return {
            "imported-late": {
                "history_sha256": output.history_sha256,
                "primary_state": output.primary_state.model_dump(mode="json"),
            }
        }
    if output is not None:
        raise TypeError("selector output must be one fixed incidence or no output")
    return None


def _output_value(raw: str) -> Selector | ImportedLateGuard | None:
    value = require_canonical_json(raw.encode())
    if value is None:
        return None
    if isinstance(value, dict) and set(value) == {"unary"}:
        return Selector.from_mapping(value["unary"])
    if isinstance(value, dict) and set(value) == {"imported-late"}:
        guard = value["imported-late"]
        if (
            isinstance(guard, dict)
            and set(guard) == {"history_sha256", "primary_state"}
            and isinstance(guard["history_sha256"], str)
        ):
            return ImportedLateGuard(
                guard["history_sha256"], ExternalReference.model_validate(guard["primary_state"])
            )
    raise StaleArchiveTask("selector has an invalid fixed output receipt")


def _guard_primary(db: sqlite3.Connection, key: ArchiveTaskKey, guard: ImportedLateGuard) -> None:
    row = _one(
        db,
        "SELECT worker_id,state_reference_json FROM discovery_history_primary_bindings "
        "WHERE build_id=? AND admission_sha256=? AND history_sha256=?",
        (key.build_id, key.admission_sha256, guard.history_sha256),
    )
    expected = canonical_json_bytes(guard.primary_state.model_dump(mode="json")).decode()
    if row is None or row[0] != key.worker_id or not _equal_coordinates(row[1], expected):
        raise StaleArchiveTask(
            "imported late guard differs from the Native-verified history primary"
        )


def _incidence(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    parent: SelectorProgress,
    output: Selector | ImportedLateGuard | None,
    budget: int,
    *,
    write: bool,
) -> None:
    if output is None:
        return
    specs: tuple[Selector, ...]
    if isinstance(output, ImportedLateGuard):
        operands = output.operands
        if parent.selector != operands[0]:
            raise StaleArchiveTask("imported late guard must belong to its exact import selector")
        _guard_primary(db, key, output)
        specs = operands
        table, columns = "discovery_selector_imported_late", "import_id,causal_id,target_id"
    else:
        if output.kind not in _UNARY.get(parent.selector.kind, set()) or (
            output.kind == "late-bound" and output.scope == "input-history"
        ):
            raise StaleArchiveTask("incidence is not a fixed unary selector rule")
        specs = (parent.selector, output)
        table, columns = "discovery_selector_unary", "parent_id,child_id"
    identities = []
    for spec in specs:
        if write:
            identity = _intern(db, key, spec, budget)
        else:
            identity, raw = _identity(spec, budget)
            if _binding(db, key, identity, raw) is None:
                raise StaleArchiveTask("selector output lost its exact operand")
        identities.append(identity)
    parameters = (key.build_id, key.admission_sha256, *identities)
    if write:
        db.execute(
            "INSERT OR IGNORE INTO " + table + "(build_id,admission_sha256," + columns + ") "
            "VALUES (" + ",".join("?" for _ in parameters) + ")",
            parameters,
        ).close()
    elif (
        _one(
            db,
            "SELECT 1 FROM "
            + table
            + " WHERE build_id=? AND admission_sha256=? AND "
            + " AND ".join(column + "=?" for column in columns.split(",")),
            parameters,
        )
        is None
    ):
        raise StaleArchiveTask("selector cursor lost its committed incidence")


def _progress(
    db: sqlite3.Connection, key: ArchiveTaskKey, identity: str, row: Any, budget: int
) -> SelectorProgress:
    if row[0] != key.worker_id:
        raise StaleArchiveTask("selector belongs to another archive attempt")
    if len(row[1].encode()) > budget or len(row[3].encode()) > budget:
        raise SelectorCapacity("retained selector control exceeds execution capacity")
    selector = Selector.from_mapping(require_canonical_json(row[1].encode()))
    encoded_identity, encoded = _identity(selector, budget)
    if encoded_identity != identity or not _equal_coordinates(encoded, row[1]):
        raise StaleArchiveTask("selector coordinates differ from their exact digest candidate")
    position = require_canonical_json(row[3].encode())
    if not isinstance(position, list) or any(type(item) is not str for item in position):
        raise StaleArchiveTask("selector cursor must contain exact string coordinates")
    result = SelectorProgress(
        selector,
        identity,
        parse_scalar("nonnegative", row[2]),
        cast(tuple[str, ...], tuple(position)),
        bool(row[4]),
        row[5],
    )
    if result.version == 0 and (result.position or result.complete or row[5] is not None):
        raise StaleArchiveTask("initial selector cursor has changed without an advancing version")
    if row[5] is not None:
        if len(row[5].encode()) > budget:
            raise SelectorCapacity("retained selector receipt exceeds execution capacity")
        _incidence(db, key, result, _output_value(row[5]), budget, write=False)
    elif result.version != 0 or result.position or result.complete:
        raise StaleArchiveTask("selector cursor is missing its committed output receipt")
    return result


def next_selector(
    db: sqlite3.Connection, key: ArchiveTaskKey, *, octet_budget: int
) -> SelectorProgress | None:
    """Seek one pending shared selector, independent of completed backlog size."""
    if db.in_transaction:
        raise ValueError("selector pending reads require no active transaction")
    if type(octet_budget) is not int or octet_budget < 1:
        raise ValueError("selector execution capacity must be positive")
    _task(db, key)
    row = _one(
        db,
        "SELECT selector_id," + _COLUMNS + " FROM discovery_selectors "
        "INDEXED BY discovery_selector_pending WHERE build_id=? AND admission_sha256=? "
        "AND complete=0 ORDER BY selector_id LIMIT 1",
        (key.build_id, key.admission_sha256),
    )
    return None if row is None else _progress(db, key, row[0], row[1:], octet_budget)


def prepare_selector_page(
    work: SelectorWork,
    expected: SelectorProgress,
    *,
    position: tuple[str, ...],
    output: Selector | ImportedLateGuard | None = None,
    complete: bool = False,
) -> PreparedSelectorPage:
    """Detach one fixed incidence and cursor; never retain a traversal path."""
    if expected.complete or type(complete) is not bool:
        raise ValueError("selector preparation requires pending work and an exact terminal flag")
    if not isinstance(position, tuple) or any(type(item) is not str for item in position):
        raise ValueError("selector position must contain exact string coordinates")
    if not complete and position == expected.position:
        raise ValueError("selector preparation must advance its bounded cursor")
    _identity(expected.selector, work.octet_budget)
    _encoded(list(position), work.octet_budget)
    receipt = _encoded(_output_mapping(output), work.octet_budget)
    result = replace(
        expected,
        version=expected.version + 1,
        position=position,
        complete=complete,
        last_output_json=receipt,
    )
    page = PreparedSelectorPage(work.key, expected, result, output)
    work._prepared = page
    return page


def apply_selector_page(
    db: sqlite3.Connection, key: ArchiveTaskKey, work: SelectorWork, page: PreparedSelectorPage
) -> SelectorProgress:
    """Commit one original incidence and its exact cursor in the same transaction."""
    if work.key != key or page.key != key or work._prepared is not page:
        raise StaleArchiveTask("selector page is not the current exact detached preparation")
    with _atomic(db):
        _task(db, key)
        identity, raw = _identity(page.expected.selector, work.octet_budget)
        if identity != page.expected.identity:
            raise StaleArchiveTask("selector preparation has another exact identity")
        row = _binding(db, key, identity, raw)
        if row is None:
            raise StaleArchiveTask("selector preparation lost its original pending record")
        current = _progress(db, key, identity, row, work.octet_budget)
        if current == page.result:
            _incidence(db, key, current, page.output, work.octet_budget, write=False)
            return current
        if current != page.expected:
            raise StaleArchiveTask("selector cursor changed after detached preparation")
        _incidence(db, key, current, page.output, work.octet_budget, write=True)
        cursor = db.execute(
            "UPDATE discovery_selectors SET version=?,position_json=?,complete=?,"
            "last_output_json=? "
            "WHERE build_id=? AND admission_sha256=? AND selector_id=? AND version=?",
            (
                format_scalar("nonnegative", page.result.version),
                _encoded(list(page.result.position), work.octet_budget),
                int(page.result.complete),
                page.result.last_output_json,
                key.build_id,
                key.admission_sha256,
                identity,
                format_scalar("nonnegative", page.expected.version),
            ),
        )
        try:
            if cursor.rowcount != 1:
                raise StaleArchiveTask("selector cursor compare-and-swap did not commit")
        finally:
            cursor.close()
        return page.result


declare_growth_for_selectors(globals())
