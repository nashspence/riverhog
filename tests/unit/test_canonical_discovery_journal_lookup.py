from __future__ import annotations

import json
from contextlib import contextmanager
from dataclasses import replace

import pytest
from riverhog_archive_contracts import HistoryJournalAnchor
from riverhog_canonical_json import format_scalar
from riverhog_core import canonical_discovery_journal_lookup as lookup
from riverhog_core.canonical_discovery_fact_work import StaleFactTask
from riverhog_core.canonical_discovery_journal_extraction import JournalFactExtraction
from riverhog_core.canonical_discovery_journal_work import (
    JournalTaskKey,
    QualifiedJournalSource,
    StaleJournalTask,
)
from riverhog_provenance import CanonicalCorpusValidator, JournalFrame, validate_journal

from tests.support.canonical_discovery_work import (
    ADMISSION,
    corrected_journal_fixture,
    open_journal_work,
    write,
)
from tests.unit.test_canonical_discovery_journal_extraction import run

ABSENT = "urn:uuid:ffffffff-ffff-4fff-8fff-ffffffffffff"


@contextmanager
def completed(path):
    fixture, raw, _ = corrected_journal_fixture()
    with validate_journal(raw, catalog=fixture.catalog) as native:
        with QualifiedJournalSource(native, ADMISSION) as source:
            db = open_journal_work(path)
            try:
                key = JournalTaskKey("build", source.source_key, "worker")
                run(JournalFactExtraction(db, key, source), 128)
                yield db, key, source, fixture, native
            finally:
                db.close()


def test_historical_headers_match_native_effective_and_documentary_intervals(tmp_path, monkeypatch):
    with completed(tmp_path / "work.sqlite3") as (db, key, source, fixture, native):
        with CanonicalCorpusValidator() as corpus:
            while not corpus.advance_source(native, frame_budget=64).complete:
                pass
            assertions = {
                row["assertion_id"]: row
                for frame in native.frames
                for group in frame.document["body"].get("assertions", {}).values()
                for row in group
            }
            anchors = [HistoryJournalAnchor.from_mapping(native.anchor_at(i)) for i in range(3)]

            def forbidden(*args, **kwargs):
                pytest.fail("header lookup loaded an original assertion payload")

            monkeypatch.setattr(JournalFrame, "document", property(forbidden))
            monkeypatch.setattr(source, "entry", forbidden)
            monkeypatch.setattr(source, "assertion", forbidden)
            for anchor in anchors:
                for assertion_id, row in assertions.items():
                    header = lookup.documentary_journal_assertion(
                        db, key, source, anchor, assertion_id
                    )
                    assert header is not None
                    assert header.assertion_id == assertion_id
                    assert header.key.source_key == source.source_key
                    assert (
                        header.reference_json
                        == lookup.canonical_json_bytes(
                            source.reference_for_assertion(
                                assertion_id, object_id=row["id"], object_type=row["type"]
                            )
                        ).decode()
                    )
                    expected = corpus.historical_assertion(anchor, row["id"], byte_budget=65536)
                    actual = lookup.effective_journal_assertion(db, key, source, anchor, row["id"])
                    assert (actual is None) == (expected is None)
                    if expected is not None:
                        assert actual.assertion_id == expected.value["assertion_id"]
                        assert json.loads(actual.reference_json) == expected.reference
                for operation in (
                    lookup.documentary_journal_assertion,
                    lookup.effective_journal_assertion,
                ):
                    assert operation(db, key, source, anchor, ABSENT) is None
            assert (
                lookup.effective_journal_assertion(
                    db, key, source, anchors[0], assertions[fixture.assertion_id]["id"]
                )
                is not None
            )
            assert (
                lookup.effective_journal_assertion(
                    db, key, source, anchors[1], assertions[fixture.assertion_id]["id"]
                )
                is None
            )
            assert (
                lookup.documentary_journal_assertion(
                    db, key, source, anchors[-1], fixture.assertion_id
                ).retired_sequence
                == 1
            )


@pytest.mark.parametrize(
    "damage",
    [
        "worker",
        "binding",
        "incomplete",
        "pending",
        "terminal",
        "version",
        "anchor",
        "child",
        "child_worker",
        "child_terminal",
        "unacknowledged",
        "interval",
        "origin",
        "closed",
    ],
)
def test_changed_source_or_child_never_supplies_historical_headers(tmp_path, damage):
    with completed(tmp_path / "work.sqlite3") as (db, key, source, fixture, native):
        anchor = HistoryJournalAnchor.from_mapping(native.anchor_at(0))
        if damage == "worker":
            key = replace(key, worker_id="another")
        elif damage == "binding":
            source.binding_json = "{}"
        elif damage == "anchor":
            anchor = replace(anchor, prefix_sha256="f" * 64)
        elif damage == "closed":
            source.close()
        else:
            if damage == "child":
                db.execute("PRAGMA foreign_keys=OFF").close()
            with write(db):
                if damage in {"incomplete", "pending", "terminal", "version"}:
                    field, value = {
                        "incomplete": ("complete", 0),
                        "pending": ("pending", "1"),
                        "terminal": ("terminal_json", "{}"),
                        "version": ("terminal_version", "0"),
                    }[damage]
                    db.execute(
                        "UPDATE discovery_journal_tasks SET " + field + "=?", (value,)
                    ).close()
                elif damage.startswith("child"):
                    task = db.execute(
                        "SELECT task_id FROM discovery_journal_assertions WHERE assertion_id=?",
                        (fixture.assertion_id,),
                    ).fetchone()[0]
                    if damage == "child":
                        db.execute(
                            "DELETE FROM discovery_fact_tasks WHERE task_id=?", (task,)
                        ).close()
                    else:
                        field, value = (
                            ("worker_id", "another")
                            if damage == "child_worker"
                            else ("canonical_stream_ref", "f" * 64)
                        )
                        db.execute(
                            "UPDATE discovery_fact_tasks SET " + field + "=? WHERE task_id=?",
                            (value, task),
                        ).close()
                else:
                    field, value = {
                        "unacknowledged": ("acknowledged", 0),
                        "interval": ("retired_sequence", "0"),
                        "origin": ("origin_entry_json", "{}"),
                    }[damage]
                    db.execute(
                        "UPDATE discovery_journal_assertions SET "
                        + field
                        + "=? WHERE assertion_id=?",
                        (value, fixture.assertion_id),
                    ).close()
        with pytest.raises((StaleJournalTask, StaleFactTask)):
            lookup.documentary_journal_assertion(db, key, source, anchor, fixture.assertion_id)


def test_incomplete_source_does_not_claim_absence_and_writes_are_not_borrow_boundaries(tmp_path):
    fixture, _, _ = corrected_journal_fixture()
    with validate_journal(fixture.raw, catalog=fixture.catalog) as native:
        with QualifiedJournalSource(native, ADMISSION) as source:
            db = open_journal_work(tmp_path / "work.sqlite3")
            try:
                key = JournalTaskKey("build", source.source_key, "worker")
                JournalFactExtraction(db, key, source)
                anchor = HistoryJournalAnchor.from_mapping(native.anchor)
                with pytest.raises(StaleJournalTask, match="completed input"):
                    lookup.effective_journal_assertion(db, key, source, anchor, ABSENT)
                with write(db), pytest.raises(ValueError, match="no write"):
                    lookup.effective_journal_assertion(db, key, source, anchor, ABSENT)
            finally:
                db.close()


def test_full_canonical_control_versions_do_not_inherit_python_digit_limits(tmp_path):
    with completed(tmp_path / "work.sqlite3") as (db, key, source, fixture, native):
        anchor = HistoryJournalAnchor.from_mapping(native.anchor_at(0))
        version = 10**5000
        with write(db):
            db.execute(
                "UPDATE discovery_journal_tasks SET version=?,terminal_version=?",
                (format_scalar("nonnegative", version + 1), format_scalar("nonnegative", version)),
            ).close()
        assert (
            lookup.documentary_journal_assertion(db, key, source, anchor, fixture.assertion_id)
            is not None
        )
