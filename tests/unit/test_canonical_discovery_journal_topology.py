from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core import canonical_discovery_journal_epochs as epochs
from riverhog_core import canonical_discovery_journal_registration as registration
from riverhog_core import canonical_discovery_journal_topology as owner
from riverhog_core.canonical_discovery_archive_work import StaleArchiveTask
from riverhog_core.services.discovery_validation_owner import DiscoveryValidationOwner
from riverhog_provenance import ProvenanceValidationError, validate_journal_chunks
from riverhog_provenance.topology_facts import advance_topology_frame, plan_topology_frame

from tests.unit.test_canonical_discovery_journal_epochs import begin as begin_epochs
from tests.unit.test_canonical_discovery_journal_epochs import finish as finish_epochs
from tests.unit.test_canonical_discovery_journal_source import source_case
from tests.unit.test_discovery_journal_owner import prepared as owned
from tests.unit.test_discovery_journal_owner import quantum


def encoded(count=129, *, padding=0):
    return canonical_json_bytes(
        {
            "entry_kind": "correction",
            "body": {
                "retracts": [{"assertion_id": "old"}],
                "assertions": {
                    "relations": [
                        {
                            "type": "derivation",
                            "assertion_id": "assertion:" + str(index),
                            "used_state": {"object_id": "source"},
                            "generated_state": {"object_id": "target:" + str(index)},
                            **({"padding": "x" * padding} if padding else {}),
                        }
                        for index in range(count)
                    ]
                },
            },
        }
    )


@contextmanager
def topology_case(path, raw, *, retained=1):
    with source_case(path, raw, segments=1, retained=retained) as (case, framing, source):
        original = epochs.DiscoveryJournalEpochWork(framing, source)
        with case.metadata.transaction() as (db, key):
            registration.register_discovery_journal(db, key, framing, source.binding)
        begin_epochs(case.metadata, original)
        finish_epochs(case.metadata, original)
        yield case, owner.DiscoveryJournalTopologyWork(original)


def begin(metadata, task):
    with metadata.transaction() as (db, key):
        return owner.begin_discovery_journal_topology(db, key, task)


def progress(metadata, task):
    with metadata.read() as (db, key):
        return owner.discovery_journal_topology_progress(db, key, task)


def prepare(metadata, task, **kwargs):
    with metadata.read() as (db, key):
        return owner.prepare_discovery_journal_topology(db, key, task, **kwargs)


def apply(metadata, task, page, *, wrapper=lambda db: db):
    with metadata.transaction() as (db, key):
        return owner.apply_discovery_journal_topology(wrapper(db), key, task, page)


def finish(metadata, task, **kwargs):
    while page := prepare(metadata, task, **kwargs):
        assert apply(metadata, task, page)
    return progress(metadata, task)


def test_actual_native_facts_prepare_once_outside_write_and_publish_bounded_items(
    tmp_path, monkeypatch
):
    raw = encoded()
    cursor, expected = plan_topology_frame(raw), []
    while True:
        page = advance_topology_frame(cursor, lambda start, size: raw[start : start + size])
        expected.extend(page.facts)
        cursor = page.cursor
        if page.complete:
            break
    with topology_case(tmp_path / "source", b"\x1e" + raw + b"\n") as (case, task):
        begin(case.metadata, task)
        actual, planned = owner.plan_topology_frame, []

        def plan(content):
            assert not case.metadata._db.in_transaction
            planned.append(content)
            return actual(content)

        monkeypatch.setattr(owner, "plan_topology_frame", plan)
        for count in (64, 128, 130):
            page = prepare(case.metadata, task)
            assert len(page.facts) <= 64 and prepare(case.metadata, task) is page
            assert apply(case.metadata, task, page)
            assert progress(case.metadata, task).checkpoint.facts == count
        assert prepare(case.metadata, task) is None and len(planned) == 1
        with case.metadata.read() as (db, key):
            values = [owner._fact(db, key, task, ordinal).item for ordinal in range(130)]
        assert values == expected and progress(case.metadata, task).phase == "complete"


def test_partial_cursor_resumes_without_prefix_replanning_and_honors_new_budget(
    tmp_path, monkeypatch
):
    with topology_case(tmp_path / "source", b"\x1e" + encoded() + b"\n") as (case, task):
        begin(case.metadata, task)
        first = prepare(case.metadata, task)
        smaller = prepare(case.metadata, task, item_budget=1)
        assert len(first.facts) == 64 and len(smaller.facts) == 1
        for forged in (first, replace(smaller)):
            with pytest.raises(StaleArchiveTask, match="owned prepared"):
                apply(case.metadata, task, forged)
        assert apply(case.metadata, task, smaller)
        with pytest.raises(StaleArchiveTask, match="owned prepared"):
            apply(case.metadata, task, smaller)
        assert apply(case.metadata, task, prepare(case.metadata, task, item_budget=1))
        before = progress(case.metadata, task)
        resumed = owner.DiscoveryJournalTopologyWork(task.epochs)
        assert begin(case.metadata, resumed) == before
        monkeypatch.setattr(
            owner, "plan_topology_frame", lambda *a: pytest.fail("replanned prefix")
        )
        actual = resumed.epochs.source.read_window

        def read(offset, size):
            assert offset >= 1 + before.checkpoint.active.relations.offset
            return actual(offset, size)

        monkeypatch.setattr(resumed.epochs.source, "read_window", read)
        second = prepare(case.metadata, resumed, item_budget=1)
        assert second.facts[0].array == "relations" and second.facts[0].index == 1
        assert apply(case.metadata, resumed, second)


def test_interrupted_publication_and_lost_reply_keep_atomic_cursor_and_facts(tmp_path):
    class Interrupted:
        def __init__(self, db):
            self.db = db

        def __getattr__(self, name):
            return getattr(self.db, name)

        def execute(self, sql, *args):
            if sql.startswith("UPDATE discovery_journal_topology"):
                raise InterruptedError("checkpoint interrupted")
            return self.db.execute(sql, *args)

    with topology_case(tmp_path / "source", b"\x1e" + encoded() + b"\n") as (case, task):
        before = begin(case.metadata, task)
        page = prepare(case.metadata, task, item_budget=1)
        with pytest.raises(InterruptedError):
            apply(case.metadata, task, page, wrapper=Interrupted)
        assert progress(case.metadata, task) == before
        with case.metadata.read() as (db, _):
            assert db.execute(
                "SELECT count(*) FROM discovery_journal_topology_facts"
            ).fetchone() == (0,)
        with pytest.raises(TimeoutError):
            assert apply(case.metadata, task, page)
            raise TimeoutError("reply lost")
        resumed = owner.DiscoveryJournalTopologyWork(task.epochs)
        assert begin(case.metadata, resumed).checkpoint.facts == 1
        assert prepare(case.metadata, resumed, item_budget=1).facts[0].array == "relations"


@pytest.mark.parametrize("damage", ["binding", "epoch", "cursor", "fact", "active"])
def test_changed_original_controls_fail_closed_without_reset(tmp_path, damage):
    with topology_case(tmp_path / "source", b"\x1e" + encoded() + b"\n") as (case, task):
        begin(case.metadata, task)
        assert apply(case.metadata, task, prepare(case.metadata, task, item_budget=1))
        with case.metadata.transaction() as (db, _):
            if damage == "binding":
                db.execute("UPDATE discovery_journal_children SET binding_json='{}'").close()
            elif damage == "epoch":
                db.execute("UPDATE discovery_journal_epochs SET version='999'").close()
            elif damage == "cursor":
                db.execute("UPDATE discovery_journal_topology SET cursor_json='{}'").close()
            elif damage == "fact":
                db.execute("DELETE FROM discovery_journal_topology_facts").close()
            else:
                raw = db.execute("SELECT cursor_json FROM discovery_journal_topology").fetchone()[0]
                checkpoint = owner.TopologyCaptureCheckpoint.from_json(raw)
                changed = replace(
                    checkpoint, active=replace(checkpoint.active, json_sha256="0" * 64)
                )
                db.execute(
                    "UPDATE discovery_journal_topology SET cursor_json=?", (changed.to_json(),)
                ).close()
        with pytest.raises(StaleArchiveTask):
            progress(case.metadata, task)


@pytest.mark.parametrize(
    "raw",
    [
        b"truncated",
        b'\x1e{"entry_kind":"unknown"}\n',
        b'\x1e{"body":{},"entry_kind":"assertion"}\ntruncated',
        b'\x1e{"body":{"assertions":{"relations":[0]}},"entry_kind":"assertion"}\n',
    ],
)
def test_deferred_zero_frame_or_later_failure_grants_no_native_acceptance(tmp_path, raw):
    with topology_case(tmp_path / "source", raw) as (case, task):
        begin(case.metadata, task)
        assert finish(case.metadata, task).phase == "deferred"
        with pytest.raises(ProvenanceValidationError):
            validate_journal_chunks((raw,))


def test_actual_owner_handoff_preserves_file_and_rejects_old_token(tmp_path):
    with owned(tmp_path / "owned") as case:
        with quantum(case) as (work, framing):
            original = epochs.DiscoveryJournalEpochWork(framing, work.journal_source(framing))
            begin_epochs(work.metadata, original)
            finish_epochs(work.metadata, original)
            task = owner.DiscoveryJournalTopologyWork(original)
            begin(work.metadata, task)
            assert apply(work.metadata, task, prepare(work.metadata, task))
            before = progress(work.metadata, task)
            old_task, old_page = task, prepare(work.metadata, task)
            path = work.metadata.custody.directory / "discovery.sqlite3"
            inode, binding = path.stat().st_ino, original.source.binding
        case.pool.close()
        case.pool = DiscoveryValidationOwner(case.factory, case.root)
        with quantum(case) as (work, framing):
            original = epochs.DiscoveryJournalEpochWork(framing, work.journal_source(framing))
            task = owner.DiscoveryJournalTopologyWork(original)
            assert original.source.binding == binding
            assert begin(work.metadata, task) == before and path.stat().st_ino == inode
            with pytest.raises(StaleArchiveTask):
                with work.metadata.transaction() as (db, key):
                    owner.apply_discovery_journal_topology(db, key, old_task, old_page)
            assert finish(work.metadata, task).phase == "complete"


@pytest.mark.parametrize("error_type", [StaleArchiveTask, OSError, ValueError])
def test_source_callback_failure_propagates_without_speculative_deferral(
    tmp_path, monkeypatch, error_type
):
    with topology_case(tmp_path / "source", b"\x1e" + encoded(1) + b"\n") as (case, task):
        before = begin(case.metadata, task)

        def unavailable(*args):
            raise error_type("original source unavailable")

        monkeypatch.setattr(task.epochs.source, "read_window", unavailable)
        with pytest.raises(error_type, match="original source unavailable"):
            prepare(case.metadata, task)
        assert task._prepared is None and progress(case.metadata, task) == before


def test_one_existing_large_item_can_cross_octet_budget_without_a_new_cap(tmp_path, monkeypatch):
    raw = encoded(1, padding=65537)
    with topology_case(tmp_path / "source", b"\x1e" + raw + b"\n") as (case, task):
        begin(case.metadata, task)
        assert apply(case.metadata, task, prepare(case.metadata, task, item_budget=1))
        actual, calls = task.epochs.source.read_window, []

        def read(offset, size):
            assert size <= 65536
            calls.append((offset, size))
            return actual(offset, size)

        monkeypatch.setattr(task.epochs.source, "read_window", read)
        page = prepare(case.metadata, task, item_budget=1, octet_budget=1)
        assert len(page.facts) == 1 and page.facts[0].json_bytes > 65536
        assert len(calls) >= 2 and apply(case.metadata, task, page)
        assert progress(case.metadata, task).phase == "complete"
