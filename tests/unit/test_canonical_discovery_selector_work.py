from __future__ import annotations

import sqlite3
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_archive_contracts import (
    BOUND_HISTORY_EXTENT,
    RETAINED_HISTORY_EXTENT,
    HistoryJournalAnchor,
)
from riverhog_canonical_json import canonical_json_bytes, format_scalar
from riverhog_core import canonical_discovery_selector_work as owner
from riverhog_core.canonical_discovery_archive_work import StaleArchiveTask
from riverhog_core.canonical_discovery_primary_work import initialize_primary_work
from riverhog_core.canonical_discovery_selectors import ImportedLateGuard, Selector
from riverhog_provenance_contracts import ExternalReference

from tests.support.canonical_discovery_work import write
from tests.support.provenance_archive import archive_reader_fixture, discovery_archive_admission
from tests.unit.test_canonical_discovery_archive_work import open_work, root_for

INTERN, NEXT, PREPARE, APPLY = (
    owner.intern_selector,
    owner.next_selector,
    owner.prepare_selector_page,
    owner.apply_selector_page,
)
BUDGET = 16384


def uid(number):
    return f"urn:uuid:00000000-0000-4000-8000-{number:012x}"


ANCHOR = HistoryJournalAnchor(uid(1), uid(2), 7, "a" * 64, "b" * 64, 1234)
SNAPSHOT = {"source_key": "c" * 64, "anchor": ANCHOR}
HISTORY = "d" * 64
STATE = ExternalReference.model_validate(
    {
        "journal_id": uid(1),
        "entry": ANCHOR.to_mapping()["through"],
        "assertion_id": uid(3),
        "object_id": uid(4),
        "object_type": "state",
    }
)


@pytest.fixture
def case(tmp_path):
    _, objects, _, _ = archive_reader_fixture()
    admission = discovery_archive_admission(root_for(objects))
    path = tmp_path / "selectors.sqlite3"
    db, key = open_work(path, admission)
    db.execute("PRAGMA foreign_keys=ON").close()
    initialize_primary_work(db)
    owner.initialize_selector_work(db)
    selected = SimpleNamespace(db=db, key=key, path=path)
    try:
        yield selected
    finally:
        selected.db.close()


def intern(case, selector):
    with write(case.db):
        return INTERN(case.db, case.key, selector, octet_budget=BUDGET)


def progress(case, selector):
    identity, raw = owner._identity(selector, BUDGET)
    row = owner._binding(case.db, case.key, identity, raw)
    return owner._progress(case.db, case.key, identity, row, BUDGET)


def primary(case, reference=STATE, history=HISTORY):
    case.db.execute(
        "INSERT INTO discovery_history_primary_bindings VALUES (?,?,?,?,?,?,?,?,?)",
        (
            case.key.build_id,
            case.key.admission_sha256,
            history,
            case.key.worker_id,
            SNAPSHOT["source_key"],
            canonical_json_bytes(ANCHOR.to_mapping()).decode(),
            canonical_json_bytes(reference.model_dump(mode="json")).decode(),
            "{}",
            "urn:test:member",
        ),
    ).close()


def all_kinds():
    shared = dict(SNAPSHOT, object_id=uid(4), scope="member")
    return [
        Selector(
            "member",
            **SNAPSHOT,
            member_id=uid(8),
            history_sha256=HISTORY,
            association_id=uid(9),
            context_id=uid(10),
        ),
        *(Selector(kind, **shared) for kind in ("state", "own", "observed", "activity", "direct")),
        Selector("history-documentary", history_sha256=HISTORY, extent=BOUND_HISTORY_EXTENT),
        Selector("history-documentary", history_sha256=HISTORY, extent=RETAINED_HISTORY_EXTENT),
        *(Selector(kind, **SNAPSHOT) for kind in ("snapshot-documentary", "prefix-dependencies")),
        *(Selector(kind, **SNAPSHOT, object_id=uid(4)) for kind in ("collection", "continuity")),
        Selector("import-reachable", history_sha256=HISTORY),
        Selector("causal-state", reference=STATE),
        Selector("late-bound", history_sha256=HISTORY, scope="member"),
        Selector("bound-endpoint", **SNAPSHOT, reference=STATE, scope="input-history"),
    ]


def test_fixed_kinds_and_exact_coordinates_round_trip_without_prefix_or_scope_aliases(case):
    values = all_kinds()
    base = Selector("state", **SNAPSHOT, object_id=uid(4), scope="member")
    values += [
        replace(base, source_key="e" * 64),
        replace(base, object_id=uid(5)),
        replace(base, scope="input-history"),
        replace(base, scope="recorded-history"),
    ]
    for name, value in (
        ("journal_id", uid(20)),
        ("through_entry_id", uid(21)),
        ("through_sequence", 8),
        ("through_json_sha256", "e" * 64),
        ("prefix_sha256", "f" * 64),
        ("prefix_bytes", 1235),
    ):
        values.append(replace(base, anchor=replace(ANCHOR, **{name: value})))
    ref = STATE.model_dump(mode="json")
    ref["entry"]["sequence"] = "8"
    values.append(Selector("causal-state", reference=ExternalReference.model_validate(ref)))
    ids = []
    for value in values:
        assert Selector.from_mapping(value.to_mapping()) == value
        identity = intern(case, value)
        assert intern(case, value) == identity
        ids.append(identity)
    assert len(ids) == len(set(ids))
    assert case.db.execute("SELECT count(*) FROM discovery_selectors").fetchone() == (len(ids),)


@pytest.mark.parametrize(
    "value",
    [
        {"kind": "custom-rule"},
        {"kind": "state", **SNAPSHOT, "object_id": uid(4)},
        {"kind": "snapshot-documentary", **SNAPSHOT, "scope": "member"},
        {"kind": "history-documentary", "history_sha256": HISTORY, "extent": "latest"},
        {"kind": "late-bound", "history_sha256": HISTORY, "scope": "recorded-history"},
        {"kind": "state", **SNAPSHOT, "object_id": uid(4), "scope": "collection"},
    ],
)
def test_selector_shapes_are_fixed_and_reject_missing_or_extra_semantic_coordinates(value):
    with pytest.raises(ValueError):
        Selector(**value)


def test_forced_digest_collision_checks_original_preimage_and_rolls_back(case, monkeypatch):
    class Collision:
        def hexdigest(self):
            return "a" * 64

    monkeypatch.setattr(owner, "hashlib", SimpleNamespace(sha256=lambda value: Collision()))
    first, second = all_kinds()[:2]
    intern(case, first)
    with write(case.db), pytest.raises(StaleArchiveTask, match="original coordinates"):
        INTERN(case.db, case.key, second, octet_budget=BUDGET)
    assert case.db.execute("SELECT count(*) FROM discovery_selectors").fetchone() == (1,)


def test_unary_output_cursor_retry_rollback_cold_reopen_and_obsolete_page(case):
    parent = Selector("state", **SNAPSHOT, object_id=uid(4), scope="member")
    child = replace(parent, kind="own")
    intern(case, parent)
    work = owner.SelectorWork(case.key, octet_budget=BUDGET)
    before = NEXT(case.db, case.key, octet_budget=BUDGET)
    page = PREPARE(work, before, position=("own", "1"), output=child)
    case.db.execute("BEGIN IMMEDIATE").close()
    assert APPLY(case.db, case.key, work, page) == page.result
    case.db.rollback()
    assert progress(case, parent) == before
    assert case.db.execute("SELECT count(*) FROM discovery_selector_unary").fetchone() == (0,)
    with write(case.db):
        assert APPLY(case.db, case.key, work, page) == page.result
        assert APPLY(case.db, case.key, work, page) == page.result
    case.db.close()
    case.db = sqlite3.connect(case.path, isolation_level=None)
    assert progress(case, parent) == page.result
    work = owner.SelectorWork(case.key, octet_budget=BUDGET)
    terminal = PREPARE(work, page.result, position=("done",), complete=True)
    with write(case.db), pytest.raises(StaleArchiveTask, match="current exact detached"):
        APPLY(case.db, case.key, work, page)
    with write(case.db):
        APPLY(case.db, case.key, work, terminal)
    assert NEXT(case.db, case.key, octet_budget=BUDGET).selector == child


def test_failed_cursor_write_does_not_leave_partial_operand_or_incidence(case):
    parent = Selector("state", **SNAPSHOT, object_id=uid(4), scope="member")
    intern(case, parent)
    work = owner.SelectorWork(case.key, octet_budget=BUDGET)
    page = PREPARE(
        work, progress(case, parent), position=("own",), output=replace(parent, kind="own")
    )
    case.db.executescript("""
        CREATE TRIGGER fail_cursor BEFORE UPDATE ON discovery_selectors
        BEGIN SELECT RAISE(ABORT,'cursor interrupted'); END;
    """)
    with write(case.db):
        with pytest.raises(sqlite3.IntegrityError, match="cursor interrupted"):
            APPLY(case.db, case.key, work, page)
        assert case.db.execute("SELECT count(*) FROM discovery_selectors").fetchone() == (1,)
        assert case.db.execute("SELECT count(*) FROM discovery_selector_unary").fetchone() == (0,)
    case.db.execute("DROP TRIGGER fail_cursor").close()
    with write(case.db):
        APPLY(case.db, case.key, work, page)


def test_imported_late_rule_retains_both_exact_operands_without_unary_relaxation(case):
    primary(case)
    guard = ImportedLateGuard(HISTORY, STATE)
    parent, causal, target = guard.operands
    intern(case, parent)
    work = owner.SelectorWork(case.key, octet_budget=BUDGET)
    page = PREPARE(work, progress(case, parent), position=("guard",), output=guard)
    with write(case.db):
        APPLY(case.db, case.key, work, page)
        APPLY(case.db, case.key, work, page)
    assert case.db.execute("SELECT count(*) FROM discovery_selector_unary").fetchone() == (0,)
    assert case.db.execute(
        "SELECT import_id,causal_id,target_id FROM discovery_selector_imported_late"
    ).fetchall() == [tuple(owner._identity(spec, BUDGET)[0] for spec in (parent, causal, target))]
    assert progress(case, parent) == page.result
    for attempted_parent in (parent, causal):
        intern(case, attempted_parent)
        attempted = PREPARE(
            work, progress(case, attempted_parent), position=("illegal-OR",), output=target
        )
        with write(case.db), pytest.raises(StaleArchiveTask, match="fixed unary"):
            APPLY(case.db, case.key, work, attempted)


@pytest.mark.parametrize("damage", ["primary", "missing-primary", "wrong-parent", "worker"])
def test_guard_cannot_substitute_another_primary_history_or_attempt(case, damage):
    reference = STATE
    if damage == "primary":
        value = STATE.model_dump(mode="json")
        value["entry"]["sequence"] = "8"
        reference = ExternalReference.model_validate(value)
    if damage != "missing-primary":
        primary(case, reference)
    parent = ImportedLateGuard(HISTORY, STATE).operands[0]
    if damage == "wrong-parent":
        parent = replace(parent, history_sha256="e" * 64)
    intern(case, parent)
    work = owner.SelectorWork(case.key, octet_budget=BUDGET)
    page = PREPARE(
        work, progress(case, parent), position=("guard",), output=ImportedLateGuard(HISTORY, STATE)
    )
    if damage == "worker":
        case.db.execute("UPDATE discovery_archive_tasks SET worker_id='other'").close()
    with write(case.db), pytest.raises(StaleArchiveTask):
        APPLY(case.db, case.key, work, page)
    assert case.db.execute("SELECT count(*) FROM discovery_selector_imported_late").fetchone() == (
        0,
    )
    assert case.db.execute("SELECT count(*) FROM discovery_selectors").fetchone() == (1,)


@pytest.mark.parametrize("damage", ["incidence", "operand", "receipt", "position"])
def test_retained_cursor_checks_original_output_before_resume_or_retry(case, damage):
    parent = Selector("state", **SNAPSHOT, object_id=uid(4), scope="member")
    intern(case, parent)
    work = owner.SelectorWork(case.key, octet_budget=BUDGET)
    page = PREPARE(
        work, progress(case, parent), position=("own",), output=replace(parent, kind="own")
    )
    with write(case.db):
        APPLY(case.db, case.key, work, page)
    case.db.execute("PRAGMA foreign_keys=OFF").close()
    if damage == "incidence":
        case.db.execute("DELETE FROM discovery_selector_unary").close()
    elif damage == "operand":
        case.db.execute(
            "DELETE FROM discovery_selectors WHERE selector_id!=?", (page.expected.identity,)
        ).close()
    elif damage == "receipt":
        case.db.execute(
            "UPDATE discovery_selectors SET last_output_json=NULL WHERE selector_id=?",
            (page.expected.identity,),
        ).close()
    else:
        case.db.execute(
            "UPDATE discovery_selectors SET position_json='[1]' WHERE selector_id=?",
            (page.expected.identity,),
        ).close()
    with write(case.db), pytest.raises(StaleArchiveTask):
        APPLY(case.db, case.key, work, page)


def test_capacity_rejection_keeps_complete_coordinates_and_retries_with_larger_budget(case):
    selector = all_kinds()[0]
    with write(case.db), pytest.raises(owner.SelectorCapacity):
        INTERN(case.db, case.key, selector, octet_budget=16)
    assert case.db.execute("SELECT count(*) FROM discovery_selectors").fetchone() == (0,)
    intern(case, selector)
    with pytest.raises(owner.SelectorCapacity):
        NEXT(case.db, case.key, octet_budget=16)
    work = owner.SelectorWork(case.key, octet_budget=BUDGET)
    before = NEXT(case.db, case.key, octet_budget=BUDGET)
    with pytest.raises(owner.SelectorCapacity):
        PREPARE(work, before, position=("x" * BUDGET,))
    assert progress(case, selector) == before


def test_private_cursor_versions_keep_arbitrary_width_without_hidden_total_limit(case):
    selector = all_kinds()[0]
    intern(case, selector)
    wide = "1" + "0" * 5000
    case.db.execute(
        "UPDATE discovery_selectors SET version=?,last_output_json='null'", (wide,)
    ).close()
    work = owner.SelectorWork(case.key, octet_budget=BUDGET)
    page = PREPARE(
        work, NEXT(case.db, case.key, octet_budget=BUDGET), position=("done",), complete=True
    )
    with write(case.db):
        result = APPLY(case.db, case.key, work, page)
    assert format_scalar("nonnegative", result.version) == "1" + "0" * 4999 + "1"
    assert NEXT(case.db, case.key, octet_budget=BUDGET) is None


def test_encoded_coordinate_shape_cannot_drop_explicit_nulls_or_change_domain():
    value = Selector("snapshot-documentary", **SNAPSHOT).to_mapping()
    value["coordinates"]["scope"] = None
    with pytest.raises(ValueError):
        Selector.from_mapping(value)
    del value["coordinates"]["scope"]
    value["domain"] = "other-owner/v1"
    with pytest.raises(ValueError):
        Selector.from_mapping(value)
