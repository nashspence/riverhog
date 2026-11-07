from __future__ import annotations

import hashlib
import subprocess
import sys
from dataclasses import replace
from pathlib import Path

import pytest
from riverhog_provenance import IncompleteSourceError, iter_journal_frames, validate_journal
from riverhog_provenance._session_checkpoint import StaleValidationCheckpoint
from riverhog_provenance.journal_session import JournalValidationBudget, JournalValidationSession
from riverhog_provenance.replay_binding import JournalReplayBinding

ATTEMPT = "c" * 32


class OwnedSource:
    def __init__(self, raw, catalog):
        self.raw = raw
        frames = tuple(iter_journal_frames((raw,)))
        self.binding = JournalReplayBinding(
            attempt_id="capture-attempt-separate-from-neutral-attempt",
            directory_id="exact-owned-source-directory",
            journal_id=frames[0].document["journal_id"],
            source_bytes=str(len(raw)),
            source_sha256=hashlib.sha256(raw).hexdigest(),
            source_version="7",
            frame_count=str(len(frames)),
            catalog_sha256=catalog.checkpoint_sha256(),
            require_profiles=True,
        )
        self.current = True
        self.calls = []

    def read_window(self, offset, maximum_octets):
        return self.read_exact(offset, maximum_octets)

    def read_exact(self, offset, octets):
        assert 0 <= offset <= offset + octets <= len(self.raw)
        self.calls.append((offset, octets))
        return self.raw[offset : offset + octets]

    def verify_current(self):
        return self.current


def open_session(path, source, catalog, *, epoch=0, create=False):
    return JournalValidationSession.open_retained(
        path, source, attempt_id=ATTEMPT, worker_epoch=epoch, create=create, catalog=catalog
    )


def finish(session, budget=None):
    if budget is None:
        budget = JournalValidationBudget(rows=1)
    for _ in range(100000):
        progress = session.advance(budget)
        if progress.complete:
            return progress
    raise AssertionError("the small native fixture did not finish")


def directory(tmp_path):
    path = tmp_path / "owned-session"
    path.mkdir(mode=0o700)
    return path


@pytest.mark.parametrize("phase", ["source", "entry", "staging", "generic", "lifecycle", "publish"])
def test_retained_session_resumes_its_exact_bounded_unit(tmp_path, journal, catalog, phase):
    path = directory(tmp_path)
    source = OwnedSource(journal, catalog)
    first = open_session(path, source, catalog, create=True)
    try:
        if phase == "source":
            first.advance(JournalValidationBudget(source_octets=7))
            assert first._input.cursor.byte_offset == 7
        else:
            for _ in range(100000):
                progress = first.advance(JournalValidationBudget(rows=1))
                if progress.phase == phase:
                    break
            else:
                raise AssertionError("the fixture did not reach its native phase")
        before = first._ledger.load()
        source.calls.clear()
        with open_session(path, source, catalog, epoch=1) as resumed:
            assert resumed._ledger.load().checkpoint.version == before.checkpoint.version + 1
            with pytest.raises(StaleValidationCheckpoint):
                first.advance()
            finish(resumed)
            result = resumed.result()
        with result, validate_journal(journal, catalog=catalog) as expected:
            assert result.materialize() == expected.materialize()
            assert result.journal_sha256 == hashlib.sha256(journal).hexdigest()
        if phase == "source":
            assert source.calls[0][0] == 7
        else:
            # The admitted exact current frame is already in the workspace.
            assert not source.calls
    finally:
        first.close()
    assert (path / "snapshot.sqlite3").is_file()


def test_completed_retained_result_reopens_without_reading_source_or_history(
    tmp_path, journal, catalog
):
    path = directory(tmp_path)
    source = OwnedSource(journal, catalog)
    with open_session(path, source, catalog, create=True) as first:
        finish(first)
    source.calls.clear()
    with open_session(path, source, catalog, epoch=1) as resumed:
        assert resumed.advance().complete
        result = resumed.result()
    with result:
        assert result.journal_bytes == len(journal)
        assert result.frames[0].json_bytes
    assert not source.calls
    assert (path / "snapshot.sqlite3").is_file()


@pytest.mark.parametrize("fault", ["attempt", "directory", "mode", "digest"])
def test_foreign_source_binding_cannot_adopt_retained_progress(tmp_path, journal, catalog, fault):
    path = directory(tmp_path)
    source = OwnedSource(journal, catalog)
    with open_session(path, source, catalog, create=True) as first:
        first.advance(JournalValidationBudget(source_octets=7))
    before = (path / "snapshot.sqlite3").read_bytes()
    changes = {
        "attempt": {"attempt_id": "another-capture"},
        "directory": {"directory_id": "another-owned-directory"},
        "mode": {"require_profiles": False},
        "digest": {"source_sha256": "0" * 64},
    }
    source.binding = replace(source.binding, **changes[fault])
    with pytest.raises(StaleValidationCheckpoint):
        open_session(path, source, catalog, epoch=1)
    assert (path / "snapshot.sqlite3").read_bytes() == before


def test_terminal_provider_fence_is_required_before_result(tmp_path, journal, catalog):
    path = directory(tmp_path)
    source = OwnedSource(journal, catalog)
    with open_session(path, source, catalog, create=True) as first:
        for _ in range(100000):
            progress = first.advance()
            if progress.phase == "terminal":
                break
        else:
            raise AssertionError("the fixture did not reach terminal")
        source.current = False
        with pytest.raises(IncompleteSourceError, match="no longer current"):
            first.advance()
        with pytest.raises(RuntimeError, match="terminal result"):
            first.result()
    source.current = True
    with open_session(path, source, catalog, epoch=1) as resumed:
        assert resumed.advance().complete
        resumed.result().close()


def test_source_commitment_is_checked_after_native_validation(tmp_path, journal, catalog):
    path = directory(tmp_path)
    source = OwnedSource(journal, catalog)
    source.binding = replace(source.binding, source_sha256="0" * 64)
    with open_session(path, source, catalog, create=True) as session:
        with pytest.raises(IncompleteSourceError, match="differs from its exact retained binding"):
            finish(session)
        assert session._ledger.load().checkpoint.phase == "terminal"


@pytest.mark.parametrize("cut", ["source", "pending", "publish"])
def test_actual_process_exit_retains_source_and_parent_cursor(tmp_path, journal, catalog, cut):
    path = directory(tmp_path)
    raw = tmp_path / "retained-source"
    raw.write_bytes(journal)
    code = """import os,sys
sys.path.insert(0,sys.argv[1])
from pathlib import Path
from riverhog_provenance_contracts import ContractCatalog
from riverhog_provenance.journal_session import JournalValidationBudget
from test_retained_journal_session import OwnedSource,open_session
catalog=ContractCatalog()
source=OwnedSource(Path(sys.argv[3]).read_bytes(),catalog)
session=open_session(Path(sys.argv[2]),source,catalog,create=True)
cut=sys.argv[4]
for _ in range(100000):
 progress=session.advance(JournalValidationBudget(rows=1,source_octets=7))
 if cut=='source' and progress.source_octets==7:break
 if cut=='pending' and session._input.pending:break
 if cut=='publish' and progress.accepted_frames==1:break
else:raise AssertionError('fixture cut was not reached')
os._exit(82)
"""
    process = subprocess.run(
        [sys.executable, "-c", code, str(Path(__file__).parent), str(path), str(raw), cut],
        capture_output=True,
    )
    assert process.returncode == 82, process.stderr.decode()
    source = OwnedSource(journal, catalog)
    with open_session(path, source, catalog, epoch=1) as resumed:
        cursor = resumed._input.cursor.byte_offset
        finish(resumed)
        with resumed.result() as result:
            assert result.journal_sha256 == hashlib.sha256(journal).hexdigest()
    if cut == "source":
        assert source.calls[0][0] == cursor == 7
    elif cut == "publish":
        assert not source.calls


def test_retained_session_transfers_resource_to_its_last_result_reader(tmp_path, journal, catalog):
    source = OwnedSource(journal, catalog)
    released = []
    current = JournalValidationSession.open_retained(
        directory(tmp_path),
        source,
        attempt_id=ATTEMPT,
        worker_epoch=0,
        create=True,
        catalog=catalog,
        on_storage_close=lambda: released.append("closed"),
    )
    finish(current)
    first = current.result()
    last = first.borrow()
    current.close()
    first.close()
    assert not released
    with last:
        assert last.journal_sha256 == hashlib.sha256(journal).hexdigest()
        assert last.frames[0].json_bytes
    current.close()
    last.close()
    assert released == ["closed"]


def test_unfinished_retained_session_releases_owner_without_removing_files(
    tmp_path, journal, catalog
):
    path = directory(tmp_path)
    released = []
    current = JournalValidationSession.open_retained(
        path,
        OwnedSource(journal, catalog),
        attempt_id=ATTEMPT,
        worker_epoch=0,
        create=True,
        catalog=catalog,
        on_storage_close=lambda: released.append("closed"),
    )
    current.advance(JournalValidationBudget(source_octets=1))
    current.close()
    assert released == ["closed"]
    assert (path / "snapshot.sqlite3").is_file()


@pytest.mark.parametrize("failure", ["source", "database", "initialization"])
def test_failed_retained_session_construction_leaves_resource_with_caller(
    tmp_path, journal, catalog, failure
):
    source = OwnedSource(journal, catalog)
    path = directory(tmp_path)
    if failure == "source":
        source.current = False
    released = []
    options = {"attempt_id": ATTEMPT if failure != "initialization" else "bad"}
    with pytest.raises((IncompleteSourceError, RuntimeError, ValueError)):
        JournalValidationSession.open_retained(
            path,
            source,
            worker_epoch=0,
            create=failure != "database",
            catalog=catalog,
            on_storage_close=lambda: released.append("closed"),
            **options,
        )
    assert not released


def test_retained_source_may_supply_short_closed_windows(tmp_path, journal, catalog):
    source = OwnedSource(journal, catalog)
    source.read_window = lambda offset, maximum: source.read_exact(offset, min(maximum, 127))
    with open_session(directory(tmp_path), source, catalog, create=True) as current:
        finish(current)
        with current.result() as result:
            assert result.journal_sha256 == hashlib.sha256(journal).hexdigest()
    assert any(octets == 127 for _, octets in source.calls)


@pytest.mark.parametrize("bad", [b"", b"overrun", memoryview(b"x")])
def test_invalid_source_window_cannot_publish_progress(tmp_path, journal, catalog, bad):
    source = OwnedSource(journal, catalog)
    source.read_window = lambda offset, maximum: bad
    with open_session(directory(tmp_path), source, catalog, create=True) as current:
        before = current._ledger.load()
        with pytest.raises(IncompleteSourceError, match="supply its window"):
            current.advance(JournalValidationBudget(source_octets=1))
        assert current._ledger.load() == before


def test_each_retained_unit_checks_current_source_before_local_progress(tmp_path, journal, catalog):
    source = OwnedSource(journal, catalog)
    with open_session(directory(tmp_path), source, catalog, create=True) as current:
        current.advance(JournalValidationBudget(source_octets=7))
        before = current._ledger.load()
        source.current = False
        with pytest.raises(IncompleteSourceError, match="no longer current"):
            current.advance()
        assert current._ledger.load() == before


@pytest.mark.parametrize("phase", ["source", "entry", "done"])
def test_progress_reads_no_payload_and_never_advances_the_checkpoint(
    tmp_path, journal, catalog, phase
):
    import sqlite3

    source = OwnedSource(journal, catalog)
    with open_session(directory(tmp_path), source, catalog, create=True) as current:
        if phase == "source":
            current.advance(JournalValidationBudget(source_octets=7))
        elif phase == "entry":
            while current.advance().phase != "entry":
                pass
        else:
            finish(current)
        previous = current.progress()
        calls = list(source.calls)
        db = current._store.db
        changed = db.total_changes

        def authorize(action, table, column, *_rest):
            return (
                sqlite3.SQLITE_DENY
                if action in {sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_DELETE}
                or action == sqlite3.SQLITE_READ
                and table in {"frames", "assertions", "journal_validation_frame"}
                and column == "value"
                else sqlite3.SQLITE_OK
            )

        db.set_authorizer(authorize)
        try:
            assert current.progress() == current.progress() == previous
        finally:
            db.set_authorizer(None)
        assert source.calls == calls and db.total_changes == changed
    with pytest.raises(RuntimeError, match="closed"):
        current.progress()
