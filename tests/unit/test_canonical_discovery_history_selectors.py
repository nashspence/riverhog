from __future__ import annotations

import sqlite3
from types import SimpleNamespace

import pytest
from riverhog_archive_contracts import (
    BOUND_HISTORY_EXTENT,
    RETAINED_HISTORY_EXTENT,
    MemberHistoryDocument,
)
from riverhog_core import canonical_discovery_history_selectors as owner
from riverhog_core import canonical_discovery_selector_work as selectors
from riverhog_core.canonical_discovery_archive_work import StaleArchiveTask
from riverhog_core.canonical_discovery_primary_work import (
    apply_history_primary,
    prepare_history_primary,
)
from riverhog_core.canonical_discovery_selectors import Selector

from tests.support.canonical_discovery_work import write
from tests.unit.test_canonical_discovery_primary_work import primary_work
from tests.unit.test_canonical_discovery_selector_work import progress

PREPARE, APPLY = owner.prepare_history_selector, owner.apply_history_selector
BUDGET = 65536
KINDS = owner._KINDS


@pytest.fixture(scope="module")
def original_histories(tmp_path_factory):
    path = tmp_path_factory.mktemp("native-history-selectors") / "source.sqlite3"
    with primary_work(path, inherited_extent=RETAINED_HISTORY_EXTENT, retained=True) as values:
        db, key, primary, descriptor, state, *_ = values
        descriptors = [
            MemberHistoryDocument.from_json_bytes(raw.encode())
            for (raw,) in db.execute(
                "SELECT descriptor_json FROM discovery_history_descriptors ORDER BY history_sha256"
            )
        ]
        for item in descriptors:
            page = prepare_history_primary(
                db, key, primary, item.identity, metadata_byte_budget=BUDGET
            )
            with write(db):
                apply_history_primary(db, key, primary, page)
        selectors.initialize_selector_work(db)
        yield SimpleNamespace(
            db=db,
            key=key,
            path=path,
            history=primary.history,
            descriptor=descriptor,
            descriptors=descriptors,
            state=state,
            native_source=primary.source,
        )


@pytest.fixture
def case(original_histories, tmp_path):
    source = original_histories
    db = sqlite3.connect(tmp_path / "work.sqlite3", isolation_level=None)
    db.execute("PRAGMA foreign_keys=ON").close()
    source.db.backup(db)
    case = SimpleNamespace(**vars(source))
    case.db, case.path = db, tmp_path / "work.sqlite3"
    try:
        yield case
    finally:
        case.db.close()


def intern(case, selector):
    with write(case.db):
        return selectors.intern_selector(case.db, case.key, selector, octet_budget=BUDGET)


def work(case, budget=BUDGET):
    return owner.HistorySelectorWork(case.key, case.history, octet_budget=budget)


def advance(case, selected, current, *, commit=True):
    page = PREPARE(case.db, case.key, selected, current)
    if commit:
        with write(case.db):
            assert APPLY(case.db, case.key, selected, page) == page.page.result
            assert APPLY(case.db, case.key, selected, page) == page.page.result
    return page


def graph(case):
    result = {}
    for identity, raw in case.db.execute(
        "SELECT selector_id,coordinates_json FROM discovery_selectors"
    ):
        from riverhog_canonical_json import require_canonical_json

        result[identity] = Selector.from_mapping(require_canonical_json(raw.encode()))
    edges = {
        (result[parent], result[child])
        for parent, child in case.db.execute(
            "SELECT parent_id,child_id FROM discovery_selector_unary"
        )
    }
    return result, edges


def drain_histories(case):
    selected = work(case)
    while True:
        pending = [
            progress(case, spec)
            for spec in graph(case)[0].values()
            if spec.kind in KINDS and not progress(case, spec).complete
        ]
        if not pending:
            break
        advance(case, selected, pending[0])


def test_real_native_histories_share_authored_sets_and_keep_documentary_extents(case, monkeypatch):
    def forbidden(*_args, **_kwargs):
        pytest.fail("history selector reconstructed a journal assertion or source frame")

    monkeypatch.setattr(case.native_source, "entry", forbidden)
    monkeypatch.setattr(case.native_source, "assertion", forbidden)
    for item in case.descriptors:
        for extent in (BOUND_HISTORY_EXTENT, RETAINED_HISTORY_EXTENT):
            intern(
                case, Selector("history-documentary", history_sha256=item.identity, extent=extent)
            )
        intern(case, Selector("import-reachable", history_sha256=item.identity))
    drain_histories(case)
    nodes, edges = graph(case)
    assert len({item.roots.records_sha256 for item in case.descriptors}) == 1
    roots = case.descriptor.roots.records_sha256
    root_selectors = [spec for spec in nodes.values() if spec.kind == "root-set-documentary"]
    assert set(root_selectors) == {
        Selector("root-set-documentary", records_sha256=roots, extent=extent)
        for extent in (BOUND_HISTORY_EXTENT, RETAINED_HISTORY_EXTENT)
    }
    for root in root_selectors:
        targets = [target for parent, target in edges if parent == root]
        assert all(target.kind == "snapshot-documentary" for target in targets)
        assert len(targets) == (1 if root.extent == BOUND_HISTORY_EXTENT else 2)
    for item in case.descriptors:
        parent = Selector(
            "history-documentary", history_sha256=item.identity, extent=RETAINED_HISTORY_EXTENT
        )
        assert {target.kind for source, target in edges if source == parent} == {
            "root-set-documentary",
            "import-set-documentary",
        }
    assert case.db.execute("SELECT count(*) FROM discovery_selector_imported_late").fetchone() == (
        2,
    )
    assert not any(
        source.kind == "import-reachable" and target.kind == "late-bound"
        for source, target in edges
    )


def test_source_receipts_survive_outer_rollback_retry_and_cold_reopen(case):
    selector = Selector(
        "history-documentary",
        history_sha256=case.descriptor.identity,
        extent=RETAINED_HISTORY_EXTENT,
    )
    intern(case, selector)
    selected = work(case)
    before = progress(case, selector)
    page = advance(case, selected, before, commit=False)
    case.db.execute("BEGIN IMMEDIATE").close()
    APPLY(case.db, case.key, selected, page)
    case.db.rollback()
    assert progress(case, selector) == before
    assert case.db.execute("SELECT count(*) FROM discovery_selector_unary").fetchone() == (0,)
    with write(case.db):
        APPLY(case.db, case.key, selected, page)
    case.db.close()
    case.db = sqlite3.connect(case.path, isolation_level=None)
    case.db.execute("PRAGMA foreign_keys=ON").close()
    selected = work(case)
    advance(case, selected, progress(case, selector))
    advance(case, selected, progress(case, selector))
    assert progress(case, selector).complete


@pytest.mark.parametrize("damage", ["descriptor", "set", "coverage", "record", "link", "primary"])
def test_changed_original_controls_cannot_commit_a_detached_incidence(case, damage):
    if damage in {"record", "link"}:
        selector = Selector(
            "root-set-documentary",
            records_sha256=case.descriptor.roots.records_sha256,
            extent=RETAINED_HISTORY_EXTENT,
        )
    elif damage == "primary":
        selector = Selector("import-reachable", history_sha256=case.descriptor.identity)
    else:
        selector = Selector(
            "history-documentary",
            history_sha256=case.descriptor.identity,
            extent=RETAINED_HISTORY_EXTENT,
        )
    intern(case, selector)
    selected = work(case)
    page = advance(case, selected, progress(case, selector), commit=False)
    with write(case.db):
        if damage == "descriptor":
            case.db.execute("UPDATE discovery_history_descriptors SET descriptor_json='{}'").close()
        elif damage == "set":
            case.db.execute("UPDATE discovery_history_sets SET complete=0").close()
        elif damage == "coverage":
            # The selected primary is a required original root record.
            case.db.execute("DELETE FROM discovery_history_set_records").close()
        elif damage == "record":
            case.db.execute(
                "UPDATE discovery_history_set_records SET value_json='{}' WHERE records_sha256=?",
                (case.descriptor.roots.records_sha256,),
            ).close()
        elif damage == "link":
            case.db.execute("DELETE FROM discovery_history_snapshot_links").close()
        else:
            case.db.execute("DELETE FROM discovery_history_primary_bindings").close()
    with write(case.db), pytest.raises(StaleArchiveTask, match="source changed"):
        APPLY(case.db, case.key, selected, page)
    assert progress(case, selector).version == 0
    assert case.db.execute("SELECT count(*) FROM discovery_selector_unary").fetchone() == (0,)


@pytest.mark.parametrize("extent", [BOUND_HISTORY_EXTENT, RETAINED_HISTORY_EXTENT])
def test_missing_or_excess_authored_record_cannot_become_a_successful_terminal(case, extent):
    selector = Selector(
        "root-set-documentary", records_sha256=case.descriptor.roots.records_sha256, extent=extent
    )
    intern(case, selector)
    selected = work(case)
    first = case.db.execute(
        "SELECT record_key FROM discovery_history_set_records WHERE records_sha256=? "
        "ORDER BY record_key LIMIT 1",
        (selector.records_sha256,),
    ).fetchone()[0]
    case.db.execute(
        "DELETE FROM discovery_history_set_records WHERE records_sha256=? AND record_key=?",
        (selector.records_sha256, first),
    ).close()
    advance(case, selected, progress(case, selector))
    with pytest.raises(StaleArchiveTask, match="omits independently counted"):
        advance(case, selected, progress(case, selector))


def test_retained_root_skip_is_explicit_and_counts_toward_original_terminal(case):
    selector = Selector(
        "root-set-documentary",
        records_sha256=case.descriptor.roots.records_sha256,
        extent=BOUND_HISTORY_EXTENT,
    )
    intern(case, selector)
    selected = work(case)
    pages = []
    while not (current := progress(case, selector)).complete:
        pages.append(advance(case, selected, current))
    assert len(pages) == case.descriptor.roots.record_count + 1
    assert sum(page.page.output is not None for page in pages) == 1
    skipped = [page for page in pages if not page.page.result.complete and page.page.output is None]
    assert len(skipped) == 1 and skipped[0].page.result.version > skipped[0].page.expected.version


def test_execution_capacity_and_wrong_transactions_preserve_pending_work(case):
    selector = Selector(
        "history-documentary",
        history_sha256=case.descriptor.identity,
        extent=RETAINED_HISTORY_EXTENT,
    )
    intern(case, selector)
    current = progress(case, selector)
    with pytest.raises(selectors.SelectorCapacity):
        PREPARE(case.db, case.key, work(case, budget=16), current)
    selected = work(case)
    with write(case.db), pytest.raises(ValueError, match="no active transaction"):
        PREPARE(case.db, case.key, selected, current)
    page = PREPARE(case.db, case.key, selected, current)
    with pytest.raises(ValueError, match="owner-managed transaction"):
        APPLY(case.db, case.key, selected, page)
    with write(case.db):
        APPLY(case.db, case.key, selected, page)


def test_real_archive_member_identity_uses_sha256_with_native_uuid_coordinates(case):
    descriptor = case.descriptor
    context = next(
        row["id"]
        for row in case.native_source.entry(0).arrays["contexts"]
        if row["kind"] == "delivery"
    )
    member = Selector(
        "member",
        member_id=descriptor.artifact_id,
        history_sha256=descriptor.identity,
        source_key=case.native_source.source_key,
        anchor=descriptor.primary.journal,
        association_id=descriptor.primary.delivery_association_id,
        context_id=context,
    )
    intern(case, member)
    assert progress(case, member).selector == member
    assert len(member.member_id) == 64 and member.association_id.startswith("urn:uuid:")


@pytest.mark.parametrize("extent", [BOUND_HISTORY_EXTENT, RETAINED_HISTORY_EXTENT])
def test_authored_import_extent_is_retained_on_documentary_child(tmp_path, extent):
    path = tmp_path / "extent.sqlite3"
    with primary_work(path, inherited_extent=extent, retained=True) as values:
        db, key, primary, descriptor, *_ = values
        for (raw,) in list(db.execute("SELECT descriptor_json FROM discovery_history_descriptors")):
            item = MemberHistoryDocument.from_json_bytes(raw.encode())
            page = prepare_history_primary(
                db, key, primary, item.identity, metadata_byte_budget=BUDGET
            )
            with write(db):
                apply_history_primary(db, key, primary, page)
        selectors.initialize_selector_work(db)
        case = SimpleNamespace(db=db, key=key, history=primary.history)
        parent = Selector(
            "import-set-documentary", records_sha256=descriptor.imports.records_sha256
        )
        intern(case, parent)
        selected = work(case)
        children = []
        while not (current := progress(case, parent)).complete:
            page = advance(case, selected, current)
            if page.page.output is not None:
                children.append(page.page.output)
        assert len(children) == 1
        source = db.execute(
            "SELECT history_sha256 FROM discovery_history_import_links WHERE import_set_sha256=?",
            (descriptor.imports.records_sha256,),
        ).fetchone()[0]
        assert children == [Selector("history-documentary", history_sha256=source, extent=extent)]


def test_excess_original_record_is_rejected_before_an_extra_incidence_can_commit(case):
    parent = Selector(
        "root-set-documentary",
        records_sha256=case.descriptor.roots.records_sha256,
        extent=RETAINED_HISTORY_EXTENT,
    )
    intern(case, parent)
    selected = work(case)
    while progress(case, parent).version < case.descriptor.roots.record_count:
        advance(case, selected, progress(case, parent))
    last = progress(case, parent)
    assert last.position[1] < "f" * 64
    case.db.execute(
        "INSERT INTO discovery_history_set_records "
        "SELECT build_id,admission_sha256,records_sha256,?,value_json,source_page,source_item,NULL "
        "FROM discovery_history_set_records WHERE records_sha256=? ORDER BY record_key LIMIT 1",
        ("f" * 64, parent.records_sha256),
    ).close()
    with pytest.raises(StaleArchiveTask, match="excess original records"):
        advance(case, selected, last)
    assert progress(case, parent) == last
