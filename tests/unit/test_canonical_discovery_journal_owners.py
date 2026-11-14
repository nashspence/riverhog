from __future__ import annotations

import copy
import sqlite3
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_archive_contracts import HistoryJournalAnchor
from riverhog_core import canonical_discovery_journal_owners as owners
from riverhog_core.canonical_discovery_fact_work import StaleFactTask
from riverhog_core.canonical_discovery_journal_extraction import JournalFactExtraction
from riverhog_core.canonical_discovery_journal_lookup import effective_journal_assertion
from riverhog_core.canonical_discovery_journal_work import (
    JournalTaskKey,
    QualifiedJournalSource,
    StaleJournalTask,
)
from riverhog_provenance import (
    JournalFrame,
    append_assertion_batches,
    append_checkpoint,
    append_correction,
    assertion,
    assertion_reference,
    new_id,
    validate_journal,
)

from tests.support.canonical_discovery import native_scalar_fixture
from tests.support.canonical_discovery_work import ADMISSION, open_journal_work, write
from tests.unit.test_canonical_discovery_journal_extraction import run

NEXT = owners.next_owner_assertion
ABSENT = "urn:uuid:ffffffff-ffff-4fff-8fff-ffffffffffff"


@pytest.fixture(scope="module")
def owner_originals(tmp_path_factory):
    fixture = native_scalar_fixture(0)
    with validate_journal(fixture.raw, catalog=fixture.catalog) as first:
        row = copy.deepcopy(first.frames[0].document["body"]["assertions"]["extensions"][0])
        who = first.frames[0].document["recorded_by_agent_id"]
        old = row["assertion_id"]
        target = assertion_reference(first, old)
    early = row["id"]
    row["assertion_id"] = new_id()
    replacement = row["assertion_id"]
    raw = append_correction(
        fixture.raw,
        [target],
        reason="replace original own assertion",
        recorded_by_agent_id=who,
        assertions={"extensions": [row]},
        catalog=fixture.catalog,
    )
    with validate_journal(raw, catalog=fixture.catalog) as second:
        target = assertion_reference(second, replacement)
    raw = append_correction(
        raw,
        [target],
        reason="leave an effective gap",
        recorded_by_agent_id=who,
        catalog=fixture.catalog,
    )
    for _ in range(3, 9):
        raw = append_checkpoint(
            raw, recorded_by_agent_id=who, purpose="urn:test:owner-prefix", catalog=fixture.catalog
        )
    late = assertion(
        "extension",
        who,
        subject=row["subject"],
        property="urn:test:owner-width",
        value={"type": "text", "value": "later"},
    )
    raw = append_assertion_batches(
        raw, ({"extensions": [late]},), recorded_by_agent_id=who, catalog=fixture.catalog
    )
    with validate_journal(raw, catalog=fixture.catalog) as ninth:
        target = assertion_reference(ninth, late["assertion_id"])
    last = copy.deepcopy(late)
    last["assertion_id"] = new_id()
    raw = append_correction(
        raw,
        [target],
        reason="cross decimal width",
        recorded_by_agent_id=who,
        assertions={"extensions": [last]},
        catalog=fixture.catalog,
    )
    with validate_journal(raw, catalog=fixture.catalog) as native:
        with QualifiedJournalSource(native, ADMISSION) as source:
            path = tmp_path_factory.mktemp("original-owner") / "source.sqlite3"
            db = open_journal_work(path)
            key = JournalTaskKey("build", source.source_key, "worker")
            try:
                run(JournalFactExtraction(db, key, source), 128)
                yield SimpleNamespace(
                    db=db,
                    path=path,
                    key=key,
                    native=native,
                    anchor=HistoryJournalAnchor.from_mapping(native.anchor),
                    early=early,
                    late=late["id"],
                    early_ids=(old, replacement),
                    late_ids=(late["assertion_id"], last["assertion_id"]),
                )
            finally:
                db.close()


@pytest.fixture
def owner_case(owner_originals, tmp_path):
    original = owner_originals
    path = tmp_path / "owner.sqlite3"
    db = sqlite3.connect(path, isolation_level=None)
    original.db.backup(db)
    with QualifiedJournalSource(original.native, ADMISSION) as source:
        case = SimpleNamespace(**vars(original), source=source)
        case.db, case.path = db, path
        try:
            yield case
        finally:
            case.db.close()


def collect(case, referent, anchor, after=None):
    rows = []
    while (row := NEXT(case.db, case.key, case.source, anchor, referent, after=after)) is not None:
        assert after is None or (row.born_sequence, row.assertion_id) > (
            after.born_sequence,
            after.assertion_id,
        )
        rows.append(row)
        after = row
    return rows


def test_documentary_owners_preserve_all_original_versions_and_withdrawals(owner_case, monkeypatch):
    case = owner_case
    anchors = {
        i: HistoryJournalAnchor.from_mapping(case.native.anchor_at(i)) for i in (0, 1, 2, 8, 9, 10)
    }

    def forbidden(*args, **kwargs):
        pytest.fail("owner lookup reconstructed an assertion or historical snapshot")

    monkeypatch.setattr(case.source, "entry", forbidden)
    monkeypatch.setattr(case.source, "assertion", forbidden)
    monkeypatch.setattr(JournalFrame, "document", property(forbidden))
    for sequence, anchor in anchors.items():
        early = collect(case, case.early, anchor)
        assert [row.assertion_id for row in early] == list(
            case.early_ids[:1] if sequence == 0 else case.early_ids
        )
        assert early[0].retired_sequence == 1
        if sequence:
            assert early[1].retired_sequence == 2
        late = collect(case, case.late, anchor)
        assert [row.assertion_id for row in late] == list(
            () if sequence < 9 else case.late_ids[:1] if sequence == 9 else case.late_ids
        )
        assert collect(case, ABSENT, anchor) == []
    assert (
        effective_journal_assertion(case.db, case.key, case.source, anchors[2], case.early) is None
    )


def test_owner_cursor_extends_to_a_later_prefix_and_crosses_decimal_width_without_replay(
    owner_case,
):
    case = owner_case
    ninth = HistoryJournalAnchor.from_mapping(case.native.anchor_at(9))
    tenth = case.anchor
    first = NEXT(case.db, case.key, case.source, ninth, case.late)
    assert first.born_sequence == 9 and first.assertion_id == case.late_ids[0]
    assert NEXT(case.db, case.key, case.source, ninth, case.late, after=first) is None
    second = NEXT(case.db, case.key, case.source, tenth, case.late, after=first)
    assert second.born_sequence == 10 and second.assertion_id == case.late_ids[1]
    assert NEXT(case.db, case.key, case.source, tenth, case.late, after=second) is None
    with pytest.raises(StaleJournalTask, match="owner cursor"):
        NEXT(case.db, case.key, case.source, ninth, case.late, after=second)


def test_cold_owner_cursor_preserves_original_addresses_and_source_binding(owner_case):
    case = owner_case
    first = NEXT(case.db, case.key, case.source, case.anchor, case.early)
    case.db.close()
    case.db = sqlite3.connect(case.path, isolation_level=None)
    next_row = NEXT(case.db, case.key, case.source, case.anchor, case.early, after=first)
    assert next_row.assertion_id == case.early_ids[1]
    assert first.reference_json != next_row.reference_json
    assert NEXT(case.db, case.key, case.source, case.anchor, case.early, after=next_row) is None


@pytest.mark.parametrize(
    "field",
    (
        "referent_id",
        "assertion_id",
        "born_sequence",
        "reference_json",
        "canonical_stream_ref",
        "retired_sequence",
    ),
)
def test_changed_original_owner_cursor_cannot_skip_source_claims(owner_case, field):
    case = owner_case
    first = NEXT(case.db, case.key, case.source, case.anchor, case.early)
    value = {
        "referent_id": ABSENT,
        "assertion_id": ABSENT,
        "born_sequence": 1,
        "reference_json": "{}",
        "canonical_stream_ref": "f" * 64,
        "retired_sequence": 2,
    }[field]
    with pytest.raises(StaleJournalTask, match="owner cursor"):
        NEXT(
            case.db,
            case.key,
            case.source,
            case.anchor,
            case.early,
            after=replace(first, **{field: value}),
        )


def test_incomplete_fact_child_and_missing_historical_index_fail_closed(owner_case):
    case = owner_case
    first = NEXT(case.db, case.key, case.source, case.anchor, case.early)
    with write(case.db):
        case.db.execute(
            "UPDATE discovery_fact_tasks SET complete=0 WHERE build_id=? AND task_id=?",
            (case.key.build_id, first.key.task_id),
        ).close()
    with pytest.raises((StaleJournalTask, StaleFactTask)):
        NEXT(case.db, case.key, case.source, case.anchor, case.early)
    with write(case.db):
        case.db.execute(
            "UPDATE discovery_fact_tasks SET complete=1 WHERE build_id=? AND task_id=?",
            (case.key.build_id, first.key.task_id),
        ).close()
    case.db.execute("DROP INDEX discovery_journal_historical_objects").close()
    with pytest.raises(sqlite3.OperationalError, match="index"):
        NEXT(case.db, case.key, case.source, case.anchor, case.early)


def test_absence_still_requires_exact_prefix_source_worker_and_read_boundary(owner_case):
    case = owner_case
    with pytest.raises(StaleJournalTask):
        NEXT(case.db, replace(case.key, worker_id="another"), case.source, case.anchor, ABSENT)
    with pytest.raises(StaleJournalTask):
        NEXT(case.db, case.key, case.source, replace(case.anchor, prefix_sha256="f" * 64), ABSENT)
    with write(case.db):
        with pytest.raises(ValueError, match="transaction"):
            NEXT(case.db, case.key, case.source, case.anchor, ABSENT)
    case.source.close()
    with pytest.raises(StaleJournalTask):
        NEXT(case.db, case.key, case.source, case.anchor, ABSENT)
