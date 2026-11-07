"""Shared original-source dependency continuations under the prefix registry."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass

from riverhog_archive_contracts import HistoryJournalAnchor
from riverhog_canonical_json import canonical_json_bytes, format_scalar, require_canonical_json
from riverhog_growth_contracts import (
    COUNT,
    OCTETS,
    Bound,
    Declaration,
    Dimension,
    Expression,
    declare_growth,
)

from riverhog_core.canonical_discovery_archive_work import (
    ArchiveDiscoveryAdmission,
    ArchiveTaskKey,
    StaleArchiveTask,
)
from riverhog_core.canonical_discovery_corpus_inputs import FrozenCorpusSourceInputs, _natural, _one
from riverhog_core.canonical_discovery_dependency_facts import (
    DocumentaryPosition,
    next_documentary_reference,
)
from riverhog_core.canonical_discovery_dependency_source import next_source_assertion
from riverhog_core.canonical_discovery_fact_work import FactTaskKey
from riverhog_core.canonical_discovery_journal_work import JournalAssertion, JournalTaskKey
from riverhog_core.canonical_discovery_prefix_inputs import (
    CorpusPrefixMaximum,
    CorpusPrefixWork,
    PreparedCorpusPrefix,
    _header,
    _maximum,
    apply_corpus_prefix,
    prepare_corpus_prefix,
)


@dataclass(frozen=True, slots=True)
class CorpusDependencyProgress:
    maximum: CorpusPrefixMaximum
    version: int
    fork_complete: bool
    after: JournalAssertion | None
    active: JournalAssertion | None
    after_node: int | None
    scanned_through: int | None
    receipt_json: str | None


@dataclass(frozen=True, slots=True)
class PreparedCorpusDependency:
    key: ArchiveTaskKey
    expected: CorpusDependencyProgress
    action: str
    after_json: str | None
    active_json: str | None
    after_node: str | None
    scanned_through: str | None
    receipt_json: str
    dependency_owner: CorpusPrefixWork | None
    dependency: PreparedCorpusPrefix | None


class CorpusDependencyWork:
    """Borrow one original source; resolved targets stay borrowed through apply."""

    def __init__(self, source: CorpusPrefixWork) -> None:
        self.source = source
        self._prepared: PreparedCorpusDependency | None = None


def _declaration(mode: str) -> Declaration:
    subject = {
        "next": "next_corpus_dependency",
        "progress": "corpus_dependency_progress",
        "prepare": "prepare_corpus_dependency",
        "apply": "apply_corpus_dependency",
    }[mode]

    def count(value: int) -> Expression:
        return Expression("constant", value=value, unit=COUNT)

    def dim(name: str) -> Expression:
        return Expression("dimension", name=name)

    def scale(value: int, term: Expression) -> Expression:
        return Expression("product", terms=(Expression("constant", value=value, unit=()), term))

    return Declaration(
        id="riverhog-server.discovery-corpus-dependency-" + mode + ".growth/v1",
        authority="riverhog-server",
        subjects=("riverhog_core.canonical_discovery_dependency_work." + subject,),
        scope="one shared source queue or original dependency continuation",
        starts_at="receive exact archive/build/worker and fixed original source manifest",
        ends_at="detach one state/preparation or atomically apply its exact child and cursor",
        dimensions=(
            Dimension(
                "registered_inputs",
                COUNT,
                "All retained Core source, fact, queue and prefix index rows.",
            ),
            Dimension(
                "metadata_octets", OCTETS, "Largest complete metadata input, row or receipt."
            ),
            Dimension(
                "initial_atom_octets",
                OCTETS,
                "Original initialization bytes for a fork preparation; zero for other steps.",
            ),
        ),
        bounds=(
            Bound("sql_statements", COUNT, "upper", count(64), "Fixed indexed Core operations."),
            Bound(
                "returned_rows", COUNT, "upper", count(48), "Point metadata and one source seek."
            ),
            *(
                Bound(
                    metric,
                    OCTETS,
                    "upper",
                    Expression(
                        "sum",
                        terms=(
                            scale(multiplier, dim("metadata_octets")),
                            scale(8, dim("initial_atom_octets")),
                        ),
                    ),
                    "Complete metadata and the single original initialization atom, when read.",
                )
                for metric, multiplier in (
                    ("parameter_octets", 256),
                    ("returned_octets", 64),
                    ("written_octets", 128),
                    ("encoded_octets", 128),
                    ("hashed_octets", 128),
                )
            ),
            *(
                Bound(
                    metric,
                    COUNT,
                    "upper",
                    count(int(mode == "prepare")),
                    "At most one selected source operation in this preparation.",
                )
                for metric in ("fork_calls", "assertion_calls", "reference_calls", "target_calls")
            ),
            Bound(
                "sqlite_vm_instructions",
                COUNT,
                "upper",
                Expression(
                    "sum",
                    terms=(
                        count(32768),
                        Expression(
                            "product",
                            terms=(
                                count(4096),
                                Expression(
                                    "log2_ceil",
                                    terms=(
                                        Expression(
                                            "sum", terms=(dim("registered_inputs"), count(2))
                                        ),
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
                "Indexed Core metadata work, independent of completed source/node rows.",
            ),
        ),
        conditions=(
            "Source and target leases remain current from preparation through apply.",
            "Original source extraction has its qualified terminals before assertion scanning.",
            "Resolve targets by the same independently fixed complete-source manifest.",
            "Preparation is outside writes; apply uses the current owner's transaction.",
        ),
        invariants=(
            "Fork completion and outgoing assertion/node continuations are separate.",
            "A higher maximum reactivates the existing position without replaying completed nodes.",
            "Child prefix registration commits with its parent cursor, including exact retries.",
            "Queue exhaustion agrees with the registry's independently maintained pending counter.",
        ),
        limitations=(
            "SQL and VM observations cover Core work connections, not Native's own databases.",
            "Native fork/header/reference work retains its separate source-owned growth evidence.",
            "This closure traversal does not establish Native semantic/cycle/custody terminals.",
            "Independent seed coverage and selected-manifest freeze remain separate obligations.",
        ),
    )


def _assertion_json(assertion: JournalAssertion | None) -> str | None:
    if assertion is None:
        return None
    return canonical_json_bytes(
        {
            "task_id": assertion.key.task_id,
            "assertion_id": assertion.assertion_id,
            "sequence": format_scalar("nonnegative", assertion.sequence),
            "group": assertion.group,
            "item": format_scalar("nonnegative", assertion.item),
            "origin_entry_json": assertion.origin_entry_json,
            "canonical_stream_ref": assertion.canonical_stream_ref,
        }
    ).decode()


def _text(value: object) -> str:
    if not isinstance(value, str):
        raise StaleArchiveTask("dependency cursor text has another value type")
    return value


def _assertion(key: ArchiveTaskKey, source_key: str, raw: str | None) -> JournalAssertion | None:
    if raw is None:
        return None
    value = require_canonical_json(raw.encode())
    if not isinstance(value, dict) or set(value) != {
        "task_id",
        "assertion_id",
        "sequence",
        "group",
        "item",
        "origin_entry_json",
        "canonical_stream_ref",
    }:
        raise StaleArchiveTask("dependency cursor has a different original assertion shape")
    result = JournalAssertion(
        FactTaskKey(key.build_id, _text(value["task_id"]), source_key, key.worker_id),
        _text(value["assertion_id"]),
        _natural(value["sequence"]),
        _text(value["group"]),
        _natural(value["item"]),
        _text(value["origin_entry_json"]),
        _text(value["canonical_stream_ref"]),
    )
    if _assertion_json(result) != raw:
        raise StaleArchiveTask("dependency assertion coordinates changed their encoding")
    return result


def _cursor(
    db: sqlite3.Connection, key: ArchiveTaskKey, maximum: CorpusPrefixMaximum
) -> CorpusDependencyProgress:
    row = _one(
        db,
        "SELECT version,fork_complete,after_json,active_json,after_node,scanned_through,"
        "receipt_json FROM discovery_corpus_dependency_cursors "
        "WHERE build_id=? AND admission_sha256=? AND source_key=?",
        (key.build_id, key.admission_sha256, maximum.source_key),
    )
    if row is None:
        raise StaleArchiveTask("requested source has no registered dependency cursor")
    after = _assertion(key, maximum.source_key, row[2])
    active = _assertion(key, maximum.source_key, row[3])
    node = _natural(row[4]) if row[4] is not None else None
    through = _natural(row[5]) if row[5] is not None else None
    version = _natural(row[0])
    if (
        row[1] not in (0, 1)
        or node is not None
        and active is None
        or through is not None
        and through > maximum.anchor.through_sequence
        or not row[1]
        and any(item is not None for item in (after, active, node, through))
        or not maximum.pending
        and (not row[1] or active is not None or through != maximum.anchor.through_sequence)
        or after is not None
        and active is not None
        and (after.sequence, after.group, after.item)
        >= (active.sequence, active.group, active.item)
        or any(
            item is not None and item.sequence > maximum.anchor.through_sequence
            for item in (after, active)
        )
        or (version == 0) != (row[6] is None)
    ):
        raise StaleArchiveTask("dependency cursor differs from its original source maximum")
    if (version == 0) != (row[1] == 0):
        raise StaleArchiveTask("dependency fork terminal differs from its cursor version")
    if row[6] is not None:
        receipt = require_canonical_json(row[6].encode())
        if (
            not isinstance(receipt, dict)
            or set(receipt)
            != {
                "version",
                "source_key",
                "action",
                "after_json",
                "active_json",
                "after_node",
                "scanned_through",
                "dependency_source",
                "dependency_anchor",
            }
            or receipt["version"] != format_scalar("nonnegative", version - 1)
            or receipt["source_key"] != maximum.source_key
            or receipt["action"]
            not in {"fork", "assertion", "reference", "assertion-done", "source-done"}
            or (
                receipt["after_json"],
                receipt["active_json"],
                receipt["after_node"],
                receipt["scanned_through"],
            )
            != row[2:6]
            or (receipt["dependency_source"] is None) != (receipt["dependency_anchor"] is None)
            or receipt["action"] == "reference"
            and receipt["dependency_source"] is None
        ):
            raise StaleArchiveTask("dependency receipt differs from its saved original position")
        if receipt["dependency_source"] is not None:
            anchor = HistoryJournalAnchor.from_mapping(
                require_canonical_json(_text(receipt["dependency_anchor"]).encode())
            )
            registered = _one(
                db,
                "SELECT anchor_json FROM discovery_corpus_prefix_inputs "
                "WHERE build_id=? AND admission_sha256=? AND source_key=? AND prefix_sha256=?",
                (
                    key.build_id,
                    key.admission_sha256,
                    receipt["dependency_source"],
                    anchor.prefix_sha256,
                ),
            )
            if registered != (receipt["dependency_anchor"],):
                raise StaleArchiveTask("dependency receipt lost its exact registered child prefix")
    return CorpusDependencyProgress(
        maximum, version, bool(row[1]), after, active, node, through, row[6]
    )


@declare_growth(_declaration("progress"))
def corpus_dependency_progress(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    admission: ArchiveDiscoveryAdmission,
    manifest: FrozenCorpusSourceInputs,
    source_key: str,
) -> CorpusDependencyProgress:
    _header(db, key, admission, manifest)
    maximum = _maximum(db, key, source_key)
    if maximum is None:
        raise StaleArchiveTask("dependency source has no admitted prefix request")
    return _cursor(db, key, maximum)


@declare_growth(_declaration("next"))
def next_corpus_dependency(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    admission: ArchiveDiscoveryAdmission,
    manifest: FrozenCorpusSourceInputs,
) -> CorpusDependencyProgress | None:
    header = _header(db, key, admission, manifest)
    row = _one(
        db,
        "SELECT source_key FROM discovery_corpus_prefix_maxima "
        "INDEXED BY discovery_corpus_dependency_pending "
        "WHERE build_id=? AND admission_sha256=? AND pending=1 ORDER BY source_key LIMIT 1",
        (key.build_id, key.admission_sha256),
    )
    if (row is None) != (header.pending == 0):
        raise StaleArchiveTask("dependency queue differs from registered pending sources")
    if row is None:
        return None
    maximum = _maximum(db, key, row[0])
    if maximum is None:
        raise StaleArchiveTask("pending source lost its exact original maximum")
    return _cursor(db, key, maximum)


@declare_growth(_declaration("prepare"))
def prepare_corpus_dependency(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    owner: CorpusDependencyWork,
    *,
    resolve: Callable[[str], CorpusPrefixWork],
    octet_budget: int,
) -> PreparedCorpusDependency:
    if db.in_transaction:
        raise ValueError("dependency preparation requires no active transaction")
    if type(octet_budget) is not int or octet_budget < 1:
        raise ValueError("dependency capacity must be positive octets")
    source = owner.source
    source.original.check_source()
    source.original.source.check_open()
    current = corpus_dependency_progress(
        db, key, source.original.admission, source.manifest, source.original.source_key
    )
    if not current.maximum.pending or (
        current.maximum.source_binding_json != source.original.source_binding_json
    ):
        raise StaleArchiveTask("source is not the exact pending original dependency")
    after, active, node = current.after, current.active, current.after_node
    through = current.scanned_through
    target = None
    request = None
    anchor = None
    reference = None
    if not current.fork_complete:
        action = "fork"
        fork = source.original.source.fork_anchor()
        if fork is not None:
            anchor = HistoryJournalAnchor.from_mapping(fork)
    elif active is not None:
        position = DocumentaryPosition(active, node) if node is not None else None
        selected = next_documentary_reference(db, active, octet_budget=octet_budget, after=position)
        if selected is None:
            action, after, active, node = "assertion-done", active, None, None
        else:
            action, node, reference = "reference", selected.position.node, selected.reference
    else:
        active = next_source_assertion(
            db,
            JournalTaskKey(key.build_id, source.original.source_key, key.worker_id),
            through=current.maximum.anchor.through_sequence,
            after=after,
        )
        if active is None:
            action, through = "source-done", current.maximum.anchor.through_sequence
        else:
            action = "assertion"
    if anchor is not None or reference is not None:
        if anchor is not None:
            journal_id = anchor.journal_id
        else:
            assert reference is not None
            journal_id = reference["journal_id"]
        target = resolve(journal_id)
        if (
            target.original.admission != source.original.admission
            or target.manifest != source.manifest
        ):
            raise StaleArchiveTask("dependency target belongs to another original source manifest")
        if reference is not None:
            anchor = HistoryJournalAnchor.from_mapping(
                target.original.source.reference_anchor(reference)
            )
        assert anchor is not None
        request = prepare_corpus_prefix(db, key, target, anchor)
    after_json, active_json = _assertion_json(after), _assertion_json(active)
    node_json = format_scalar("nonnegative", node) if node is not None else None
    through_json = format_scalar("nonnegative", through) if through is not None else None
    receipt = canonical_json_bytes(
        {
            "version": format_scalar("nonnegative", current.version),
            "source_key": source.original.source_key,
            "action": action,
            "after_json": after_json,
            "active_json": active_json,
            "after_node": node_json,
            "scanned_through": through_json,
            "dependency_source": request.source_key if request is not None else None,
            "dependency_anchor": request.anchor_json if request is not None else None,
        }
    ).decode()
    page = PreparedCorpusDependency(
        key,
        current,
        action,
        after_json,
        active_json,
        node_json,
        through_json,
        receipt,
        target,
        request,
    )
    owner._prepared = page
    return page


@contextmanager
def _atomic(db: sqlite3.Connection) -> Iterator[None]:
    if not db.in_transaction:
        raise ValueError("dependency application requires an owner-managed transaction")
    db.execute("SAVEPOINT discovery_corpus_dependency")
    try:
        yield
    except BaseException:
        db.execute("ROLLBACK TO discovery_corpus_dependency")
        db.execute("RELEASE discovery_corpus_dependency")
        raise
    else:
        db.execute("RELEASE discovery_corpus_dependency")


@declare_growth(_declaration("apply"))
def apply_corpus_dependency(
    db: sqlite3.Connection,
    key: ArchiveTaskKey,
    owner: CorpusDependencyWork,
    page: PreparedCorpusDependency,
) -> CorpusDependencyProgress:
    source = owner.source
    if owner._prepared is not page or page.key != key:
        raise StaleArchiveTask("dependency transition requires its exact preparation")
    source.original.check_source()
    source.original.source.check_open()
    with _atomic(db):
        current = corpus_dependency_progress(
            db, key, source.original.admission, source.manifest, source.original.source_key
        )
        retry = (
            current.version == page.expected.version + 1
            and current.receipt_json == page.receipt_json
        )
        if not retry and current != page.expected:
            raise StaleArchiveTask("dependency source position or maximum changed")
        if page.dependency is not None:
            assert page.dependency_owner is not None
            apply_corpus_prefix(db, key, page.dependency_owner, page.dependency)
        if retry:
            return current
        changed = db.execute(
            "UPDATE discovery_corpus_dependency_cursors SET version=?,fork_complete=1,"
            "after_json=?,active_json=?,after_node=?,scanned_through=?,receipt_json=? "
            "WHERE build_id=? AND admission_sha256=? AND source_key=? AND version=?",
            (
                format_scalar("nonnegative", current.version + 1),
                page.after_json,
                page.active_json,
                page.after_node,
                page.scanned_through,
                page.receipt_json,
                key.build_id,
                key.admission_sha256,
                source.original.source_key,
                format_scalar("nonnegative", current.version),
            ),
        ).rowcount
        if changed != 1:
            raise StaleArchiveTask("dependency cursor worker or version changed")
        if page.action == "source-done":
            header = _header(db, key, source.original.admission, source.manifest)
            if header.pending < 1:
                raise StaleArchiveTask("dependency terminal has no pending source")
            changed = db.execute(
                "UPDATE discovery_corpus_prefix_maxima SET pending=0 "
                "WHERE build_id=? AND admission_sha256=? AND source_key=? "
                "AND version=? AND pending=1",
                (
                    key.build_id,
                    key.admission_sha256,
                    source.original.source_key,
                    format_scalar("nonnegative", current.maximum.version),
                ),
            ).rowcount
            if changed != 1:
                raise StaleArchiveTask("dependency terminal lost its requested maximum")
            db.execute(
                "UPDATE discovery_corpus_prefix_headers SET pending_sources=? "
                "WHERE build_id=? AND admission_sha256=? AND worker_id=?",
                (
                    format_scalar("nonnegative", header.pending - 1),
                    key.build_id,
                    key.admission_sha256,
                    key.worker_id,
                ),
            )
        return corpus_dependency_progress(
            db, key, source.original.admission, source.manifest, source.original.source_key
        )
