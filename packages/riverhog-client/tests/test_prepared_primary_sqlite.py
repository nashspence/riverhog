from __future__ import annotations

import hashlib
import os
import sqlite3
from pathlib import Path

import pytest
from riverhog_client import PrimaryStateBusy, PrimaryStateError
from riverhog_client import _prepared_primary_sqlite as owner
from riverhog_client._prepared_primary_types import PrimaryByteFile

from tests.support.prepared_primary import SqlitePrimaryTestGuard as _SQLGuard
from tests.support.prepared_primary import ordered_primary_rows

pytestmark = pytest.mark.skipif(os.name == "nt", reason="SQL core isolation uses the Unix VFS")


LIMITS = owner.SqlitePrimaryLimits(16 * 1024**2, 32 * 1024**2, 128 * 1024)
ALLOCATION = b"original application/allocation/scope"


def _open(path: Path, *, initialize: bool, allocation=ALLOCATION):
    if initialize:
        path.touch(exist_ok=False)
    return owner.TransactionalPrimaryFiles(
        _SQLGuard(path), allocation_descriptor=allocation, limits=LIMITS, initialize=initialize
    )


def test_chunk_publication_never_holds_a_transaction_while_asking_for_input(tmp_path):
    with _open(tmp_path / "state.sqlite3", initialize=True) as files:
        parts = [b"a", b"b" * 11000, b"c" * 4096, b"", b"d" * 17]

        def chunks():
            for part in parts:
                assert not files._connection.in_transaction
                yield part

        expected = b"".join(parts)
        result = files.put_bytes("journal", chunks(), byte_limit=len(expected))
        with files.verified(result) as stream:
            assert stream.read() == expected
            stream.seek(4094)
            assert stream.read(7) == expected[4094:4101]
        assert files._one("SELECT max(length(body)) FROM primary_chunks") == (4096,)
        assert files._one("SELECT generation FROM primary_operation") is None


@pytest.mark.parametrize("content", [b"", b"a", b"x" * 4096, b"x" * 8193])
def test_original_winner_is_immutable_and_survives_cold_reopen(tmp_path, content):
    path = tmp_path / "state.sqlite3"
    with _open(path, initialize=True) as files:
        result = files.put_bytes("journal", [content], byte_limit=len(content))
        with pytest.raises(FileExistsError):
            files.put_bytes("journal", [b"replacement"], byte_limit=100)
    with _open(path, initialize=False) as files:
        with files.verified(result) as stream:
            assert stream.read() == content


def test_namespaces_are_indexed_and_root_close_invalidates_children_and_readers(tmp_path):
    files = _open(tmp_path / "state.sqlite3", initialize=True)
    child = files.child("member", initialize=True)
    other = files.child("other", initialize=True)
    try:
        result = child.put_bytes("journal", [b"original"], byte_limit=8)
        assert not other.exists("journal")
        with pytest.raises(PrimaryStateError, match="closed"):
            with child.verified(result) as stream:
                files.close()
                with pytest.raises((ValueError, PrimaryStateError), match="closed"):
                    stream.read(1)
                with pytest.raises(PrimaryStateError, match="closed"):
                    child.exists("journal")
    finally:
        files.close()


@pytest.mark.parametrize("mutation", ["bytes", "missing", "extent"])
def test_replay_validates_every_selected_chunk_before_exposing_a_reader(tmp_path, mutation):
    path = tmp_path / "state.sqlite3"
    with _open(path, initialize=True) as files:
        result = files.put_bytes("journal", [b"x" * 10000], byte_limit=10000)
    with sqlite3.connect(path) as db:
        if mutation == "bytes":
            db.execute("UPDATE primary_chunks SET body=? WHERE ordinal=1", (b"y" * 4096,))
        elif mutation == "missing":
            db.execute("DELETE FROM primary_chunks WHERE ordinal=1")
        else:
            db.execute("UPDATE primary_chunks SET body=? WHERE ordinal=1", (b"x",))
    with _open(path, initialize=False) as files:
        with pytest.raises(PrimaryStateError, match="chunk|commitment"):
            with files.verified(result):
                pytest.fail("corrupt bytes must not expose a reader")


def test_wrong_commitment_cannot_create_a_selected_name_and_cleanup_is_bounded(tmp_path):
    path = tmp_path / "state.sqlite3"
    with _open(path, initialize=True) as files:
        with pytest.raises(PrimaryStateError, match="expected identity"):
            files.put_bytes("journal", [b"x" * 10000], byte_limit=10000, expected=(10000, "0" * 64))
        with pytest.raises(PrimaryStateError, match="uncertain"):
            files.exists("journal")
    with _open(path, initialize=False) as files:
        assert not files.exists("journal")
        assert files._one("SELECT count(*) FROM primary_chunks") == (3,)
        with pytest.raises(PrimaryStateBusy, match="cleanup"):
            files.put_bytes("other", [b"y"], byte_limit=1)
        assert files.recover_step() is False
        assert files._one("SELECT count(*) FROM primary_chunks") == (2,)
        assert files.recover_step() is False
        assert files.recover_step() is False
        assert files.recover_step() is True
        assert files.recover_step() is True
        files.put_bytes("journal", [b"new"], byte_limit=3)


class _CommitFailure:
    def __init__(self, connection, *, committed):
        self.connection = connection
        self.committed = committed

    @property
    def in_transaction(self):
        return self.connection.in_transaction

    def execute(self, sql, *args):
        if sql == "COMMIT":
            if self.committed:
                self.connection.execute(sql).close()
            raise sqlite3.OperationalError("injected commit response uncertainty")
        return self.connection.execute(sql, *args)

    def blobopen(self, *args, **kwargs):
        return self.connection.blobopen(*args, **kwargs)

    def rollback(self):
        self.connection.rollback()

    def close(self):
        self.connection.close()


@pytest.mark.parametrize("committed", [False, True])
def test_uncertain_selection_requires_reopen_and_keeps_the_committed_winner(
    tmp_path, monkeypatch, committed
):
    path = tmp_path / "state.sqlite3"
    original_select = owner.TransactionalPrimaryFiles._select

    def uncertain(files, generation, file):
        files._connection = _CommitFailure(files._connection, committed=committed)
        return original_select(files, generation, file)

    with _open(path, initialize=True) as files:
        monkeypatch.setattr(
            files, "_select", lambda generation, file: uncertain(files, generation, file)
        )
        with pytest.raises(PrimaryStateError, match="mutation failed"):
            files.put_bytes("journal", [b"retained"], byte_limit=8)
        with pytest.raises(PrimaryStateError, match="uncertain"):
            files.exists("journal")
    with _open(path, initialize=False) as files:
        assert files.exists("journal") is committed
        if committed:
            result = PrimaryByteFile("journal", 8, hashlib.sha256(b"retained").hexdigest())
            with files.verified(result) as stream:
                assert stream.read() == b"retained"
            assert files.recover_step() is True
        else:
            assert files.recover_step() is False


@pytest.mark.parametrize("change", ["allocation", "revision", "shape", "missing-format"])
def test_cold_open_rejects_unknown_state_without_reinitializing_it(tmp_path, change):
    path = tmp_path / "state.sqlite3"
    with _open(path, initialize=True) as files:
        files.put_bytes("journal", [b"keep"], byte_limit=4)
    with sqlite3.connect(path) as db:
        if change == "revision":
            db.execute("UPDATE primary_schema_revision SET version_num='unknown'")
        elif change == "shape":
            db.execute("CREATE TABLE unexpected (id INTEGER)")
        elif change == "missing-format":
            db.execute("DELETE FROM primary_format")
    with pytest.raises((PrimaryStateError, RuntimeError), match="schema|allocation|unexpected"):
        _open(
            path, initialize=False, allocation=b"changed" if change == "allocation" else ALLOCATION
        )
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT body FROM primary_chunks").fetchall() == [(b"keep",)]


def test_cold_open_has_no_whole_database_integrity_or_payload_scan(tmp_path, monkeypatch):
    path = tmp_path / "state.sqlite3"
    with _open(path, initialize=True) as files:
        files.put_bytes("journal", [b"x" * 50000], byte_limit=50000)
        list(ordered_primary_rows(files, [("a", 1, "a" * 64)], byte_limit=128 * 1024))
    original = owner.sqlite3.connect
    observed = []

    def connect(*args, **kwargs):
        db = original(*args, **kwargs)
        db.set_trace_callback(observed.append)

        def authorize(action, table, column, *_rest):
            if action == sqlite3.SQLITE_READ and table in ("primary_chunks", "primary_order_rows"):
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        db.set_authorizer(authorize)
        return db

    monkeypatch.setattr(owner.sqlite3, "connect", connect)
    with _open(path, initialize=False) as files:
        assert files.exists("journal")
    assert not any("quick_check" in sql or "foreign_key_check" in sql for sql in observed)


def test_receipt_controls_read_and_compare_original_bytes_without_pending_chunk_reads(tmp_path):
    path = tmp_path / "state.sqlite3"
    with _open(path, initialize=True) as files:
        files.prepare_controls(("prepared", "usage"), byte_limit=100)
        files.put_bytes("journal", [b"pending" * 2000], byte_limit=14000)

        def authorize(action, table, column, *_rest):
            if action == sqlite3.SQLITE_READ and table == "primary_chunks":
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        files._connection.set_authorizer(authorize)
        files.put_control("prepared", b'{"original":"manifest"}', byte_limit=100)
        files.put_control("usage", b'{"generation":0}', byte_limit=100)
        assert files.exists("prepared")
        assert files.read_control("prepared", byte_limit=100) == b'{"original":"manifest"}'
        with pytest.raises(FileExistsError):
            files.put_control("prepared", b"different", byte_limit=100)
        with pytest.raises(PrimaryStateError, match="comparison"):
            files.put_control("usage", b'{"generation":1}', byte_limit=100, expected=b"wrong")
        files.put_control(
            "usage", b'{"generation":1}', byte_limit=100, expected=b'{"generation":0}'
        )
        assert files.read_control("usage", byte_limit=100) == b'{"generation":1}'
        files.remove_control("usage", expected=b'{"generation":1}')
        files.remove_control("usage", expected=b'{"generation":1}')
        assert not files.exists("usage")
        files.sync()
    with _open(path, initialize=False) as files:
        assert files.read_control("prepared", byte_limit=100) == b'{"original":"manifest"}'


def test_control_admission_and_role_conflicts_never_replace_original_bytes(tmp_path):
    with _open(tmp_path / "state.sqlite3", initialize=True) as files:
        with pytest.raises(owner.PrimaryStateCapacity):
            files.put_control("prepared", b"oversized", byte_limit=3)
        assert not files.exists("prepared")
        files.prepare_controls(("prepared",), byte_limit=8)
        files.put_control("prepared", b"original", byte_limit=8)
        with pytest.raises(owner.PrimaryStateCapacity):
            files.read_control("prepared", byte_limit=7)
        with pytest.raises(FileExistsError):
            files.put_bytes("prepared", [b"replacement"], byte_limit=11)
        files.put_bytes("journal", [b"pending"], byte_limit=7)
        with pytest.raises(PrimaryStateError, match="byte role"):
            files.put_control("journal", b"replacement", byte_limit=11)
        with pytest.raises(PrimaryStateError, match="comparison"):
            files.remove_control("prepared", expected=b"wrong")
        assert files.read_control("prepared", byte_limit=8) == b"original"


def test_existing_namespace_is_an_openable_immutable_winner(tmp_path):
    with _open(tmp_path / "state.sqlite3", initialize=True) as files:
        with files.child("member", initialize=True) as child:
            child.prepare_controls(("prepared",), byte_limit=8)
            child.put_control("prepared", b"original", byte_limit=8)
        with pytest.raises(FileExistsError):
            files.child("member", initialize=True)
        with files.child("member", initialize=False) as child:
            assert child.read_control("prepared", byte_limit=8) == b"original"


def test_selected_generation_reclaim_is_pinned_bounded_and_cold_resumable(tmp_path):
    path = tmp_path / "state.sqlite3"
    with _open(path, initialize=True) as files:
        files.prepare_controls(("prepared", "custody"), byte_limit=100)
        journal = files.put_bytes("journal", [b"x" * 10000], byte_limit=10000)
        files.put_control("prepared", b"retained-original-authority", byte_limit=100)
        with files.verified(journal) as stream:
            with pytest.raises(PrimaryStateBusy, match="pins"):
                files.remove_step("journal")
            assert stream.read(3) == b"xxx"
        assert files.remove_step("journal") is False
        assert not files.exists("journal")
        assert files._one("SELECT count(*) FROM primary_chunks") == (3,)
        assert files.read_control("prepared", byte_limit=100) == b"retained-original-authority"
        files.put_control("custody", b"proof", byte_limit=100)
        assert files.read_control("custody", byte_limit=100) == b"proof"
        changes = files._connection.total_changes
        assert files.remove_step("other")
        assert files._connection.total_changes == changes
    for remaining in (2, 1, 0):
        with _open(path, initialize=False) as files:
            assert files.remove_step("journal") is False
            assert files._one("SELECT count(*) FROM primary_chunks") == (remaining,)
            assert files.read_control("prepared", byte_limit=100) == b"retained-original-authority"
    with _open(path, initialize=False) as files:
        assert files.remove_step("journal") is True
        assert files.remove_step("journal") is True
        assert files._one("SELECT count(*) FROM primary_objects") == (0,)
        with pytest.raises(FileExistsError):
            files.put_control("custody", b"proof", byte_limit=100)
        assert files.read_control("custody", byte_limit=100) == b"proof"


@pytest.mark.parametrize("committed", [False, True])
def test_uncertain_chunk_delete_recovers_the_exact_cleanup_cursor(tmp_path, committed):
    path = tmp_path / "state.sqlite3"
    with _open(path, initialize=True) as files:
        files.put_bytes("journal", [b"x" * 10000], byte_limit=10000)
        assert files.remove_step("journal") is False
        files._connection = _CommitFailure(files._connection, committed=committed)
        with pytest.raises(PrimaryStateError, match="mutation failed"):
            files.remove_step("journal")
    with _open(path, initialize=False) as files:
        assert not files.exists("journal")
        assert files._one("SELECT cursor FROM primary_operation") == (1 if committed else 0,)
        assert files._one("SELECT count(*) FROM primary_chunks") == (2 if committed else 3,)
        while not files.remove_step("journal"):
            pass
        assert files._one("SELECT count(*) FROM primary_objects") == (0,)


@pytest.mark.parametrize("committed", [False, True])
def test_uncertain_control_commit_keeps_the_original_visible_outcome(tmp_path, committed):
    path = tmp_path / "state.sqlite3"
    with _open(path, initialize=True) as files:
        files.prepare_controls(("prepared",), byte_limit=8)
        files._connection = _CommitFailure(files._connection, committed=committed)
        with pytest.raises(PrimaryStateError, match="mutation failed"):
            files.put_control("prepared", b"original", byte_limit=8)
    with _open(path, initialize=False) as files:
        assert files.exists("prepared") is committed
        if committed:
            assert files.read_control("prepared", byte_limit=8) == b"original"
            with pytest.raises(FileExistsError):
                files.put_control("prepared", b"replacement", byte_limit=11)


def test_extent_reader_opens_without_payload_reads_and_retains_exact_framing(tmp_path):
    lines = [b"x\n", b"a" * 4093 + b"\n", b"b" * 4097 + b"\n", b"tail"]
    body = b"".join(lines)
    with _open(tmp_path / "state.sqlite3", initialize=True) as files:
        files.put_bytes("inventory", [body], byte_limit=len(body))

        def no_payload(action, table, _column, *_rest):
            if action == sqlite3.SQLITE_READ and table == "primary_chunks":
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        files._connection.set_authorizer(no_payload)
        with files.extent_reader("inventory", byte_count=len(body)) as stream:
            assert not files._connection.in_transaction
            assert stream.tell() == 0
            files._connection.set_authorizer(None)
            assert stream.readline(0) == b""
            for line in lines:
                assert stream.readline(len(line) + 1) == line
                assert not files._connection.in_transaction
            assert stream.readline() == b""
            assert stream.tell() == len(body)
            stream.seek(4094)
            assert stream.readline(10) == b"a\n"
            stream.seek(4096)
            assert stream.readline(3) == b"bbb"
            assert stream.tell() == 4099
            with pytest.raises(PrimaryStateBusy, match="pins"):
                files.remove_step("inventory")


@pytest.mark.parametrize("members", [1, 257, 1025])
def test_framed_extent_queries_grow_with_rows_and_chunk_boundaries(tmp_path, members):
    lines = [f"member-{number:08d} original binding\n".encode() for number in range(members)]
    body = b"".join(lines)
    with _open(tmp_path / "state.sqlite3", initialize=True) as files:
        files.put_bytes("inventory", [body], byte_limit=len(body))
        statements = []
        files._connection.set_trace_callback(statements.append)
        with files.extent_reader("inventory", byte_count=len(body)) as stream:
            assert not any("FROM primary_chunks" in statement for statement in statements)
            for line in lines:
                assert stream.readline(128) == line
            assert stream.readline(128) == b""
        reads = [statement for statement in statements if "FROM primary_chunks" in statement]
        assert members <= len(reads) <= members + (len(body) + 4095) // 4096
        assert all("generation=" in statement and "ordinal=" in statement for statement in reads)


@pytest.mark.parametrize("close", ["root", "child"])
def test_extent_readers_release_generation_pins_and_obey_owner_lifetime(tmp_path, close):
    with _open(tmp_path / "state.sqlite3", initialize=True) as files:
        child = files.child("member", initialize=True)
        body = b"first\nsecond\n"
        child.put_bytes("inventory", [body], byte_limit=len(body))
        with pytest.raises(PrimaryStateError, match="closed"):
            with child.extent_reader("inventory", byte_count=len(body)) as stream:
                assert stream.readline(6) == b"first\n"
                (files if close == "root" else child).close()
                with pytest.raises((ValueError, PrimaryStateError), match="closed"):
                    stream.readline(1)
        assert not files._root._pins
        assert not files._root._readers


def test_extent_reader_checks_selection_length_and_every_accessed_chunk(tmp_path):
    with _open(tmp_path / "state.sqlite3", initialize=True) as files:
        files.put_bytes("inventory", [b"x" * 5000], byte_limit=5000)
        with pytest.raises(PrimaryStateError, match="extent"):
            with files.extent_reader("inventory", byte_count=5001):
                pytest.fail("wrong extent exposed a reader")
        assert not files._pins
        with files.extent_reader("inventory", byte_count=5000) as stream:
            for invalid in [1.5, -0.5, "1"]:
                with pytest.raises(TypeError):
                    stream.readline(invalid)
            assert stream.tell() == 0
            files._connection.execute(
                "UPDATE primary_chunks SET body=? WHERE ordinal=1", (b"short",)
            ).close()
            stream.seek(4096)
            with pytest.raises(PrimaryStateError, match="chunk extent"):
                stream.readline(10)
        assert not files._pins


def test_pending_cleanup_allows_only_existing_namespace_and_other_absent_role_reads(tmp_path):
    path = tmp_path / "state.sqlite3"
    with _open(path, initialize=True) as files:
        with files.child("member", initialize=True) as member:
            member.put_bytes("journal", [b"x" * 8193], byte_limit=8193)
            assert not member.remove_step("journal")
    with _open(path, initialize=False) as files:
        with pytest.raises(FileExistsError):
            files.child("member", initialize=True)
        with files.child("member", initialize=False) as member:
            before = files._connection.total_changes
            assert member.remove_step("request")
            assert files._connection.total_changes == before
            with pytest.raises(PrimaryStateBusy):
                files.child("new-member", initialize=True)
            with pytest.raises(PrimaryStateBusy):
                member.put_bytes("request", [b"new"], byte_limit=3)
            assert member.remove_step("journal") is False
            assert files._one("SELECT count(*) FROM primary_chunks") == (2,)
            while not member.remove_step("journal"):
                pass
            assert member.remove_step("journal")


def test_unselected_candidate_cannot_be_reported_as_completed_role_cleanup(tmp_path):
    path = tmp_path / "state.sqlite3"

    def interrupted():
        yield b"x" * 4096
        raise RuntimeError("input interrupted")

    with _open(path, initialize=True) as files:
        with pytest.raises(RuntimeError, match="input interrupted"):
            files.put_bytes("journal", interrupted(), byte_limit=8192)
    with _open(path, initialize=False) as files:
        with pytest.raises(PrimaryStateBusy):
            files.remove_step("journal")
        assert files.remove_step("request")
        assert files.recover_step() is False
        assert files.recover_step() is True
        assert files.remove_step("journal")


def test_candidate_disposition_is_named_bounded_and_does_not_drive_selected_reclaim(tmp_path):
    path = tmp_path / "state.sqlite3"
    with _open(path, initialize=True) as files:
        selected = files.put_bytes("journal", [b"j" * 9000], byte_limit=9000)
        assert not files.remove_step("journal")
        before = files._connection.total_changes
        assert files.discard_candidates(("journal",))
        assert files._connection.total_changes == before
        assert files._one("SELECT count(*) FROM primary_chunks") == (3,)
        while not files.remove_step("journal"):
            pass
        assert not files.exists(selected.name)


def test_unselected_candidate_disposition_preserves_other_namespace_and_winner(tmp_path):
    path = tmp_path / "state.sqlite3"

    def interrupted():
        yield b"x" * 9000
        raise RuntimeError("candidate input interrupted")

    with _open(path, initialize=True) as files:
        with files.child("other", initialize=True):
            pass
        winner = files.put_bytes("winner", [b"original"], byte_limit=8)
        with pytest.raises(RuntimeError, match="candidate input"):
            files.put_bytes("journal", interrupted(), byte_limit=10000)
    with _open(path, initialize=False) as files:
        with files.child("other", initialize=False) as other:
            before = files._connection.total_changes
            assert other.discard_candidates(("journal",))
            assert files._connection.total_changes == before
        with pytest.raises(PrimaryStateError, match="logical owner"):
            files.discard_candidates(("different",))
        for remaining in (1, 0):
            assert not files.discard_candidates(("journal",))
            assert files._one(
                "SELECT count(*) FROM primary_chunks WHERE generation IN "
                "(SELECT generation FROM primary_operation)"
            ) == (remaining,)
        assert files.discard_candidates(("journal",))
        with files.verified(winner) as stream:
            assert stream.read() == b"original"


@pytest.mark.parametrize("count", [1, 257, 1025])
def test_same_container_ordering_has_point_growth_and_no_transaction_across_yields(tmp_path, count):
    path = tmp_path / "state.sqlite3"
    with _open(path, initialize=True) as files:
        files.prepare_controls(("marker",), byte_limit=8)
        files.put_control("marker", b"original", byte_limit=8)
        selects = 0
        steps = 0
        selecting = False

        def trace(statement):
            nonlocal selects, selecting
            selecting = statement.startswith("SELECT artifact_id,bytes,sha256")
            if selecting:
                selects += 1

        def progress():
            nonlocal steps
            if selecting:
                steps += 1
            return 0

        files._connection.set_trace_callback(trace)
        files._connection.set_progress_handler(progress, 1)

        def rows():
            for number in reversed(range(count)):
                assert not files._connection.in_transaction
                if number == count - 1:
                    with pytest.raises(PrimaryStateBusy, match="ordering"):
                        files.put_bytes("new", [b"new"], byte_limit=3)
                yield f"opaque-{number:08}", number, "a" * 64

        for number, row in enumerate(ordered_primary_rows(files, rows(), byte_limit=1024 * 1024)):
            assert row == (f"opaque-{number:08}", number, "a" * 64)
            assert not files._connection.in_transaction
        assert number == count - 1
        assert selects == count + 1 and steps <= 80 * (count + 1)
        assert files._one("PRAGMA journal_mode") == ("persist",)
        assert files._one("PRAGMA synchronous") == (3,)
        assert files.read_control("marker", byte_limit=8) == b"original"
        assert files._one("SELECT phase FROM primary_ordering") == ("ready",)


def test_ordering_reader_excludes_replacement_and_cleanup_but_not_ready_payload_work(tmp_path):
    path = tmp_path / "state.sqlite3"
    rows = [("b", 2, "b" * 64), ("a", 1, "a" * 64)]
    with _open(path, initialize=True) as files:
        with files.child("other", initialize=True) as other:
            ordered = ordered_primary_rows(files, rows, byte_limit=128 * 1024)
            assert next(ordered) == rows[1]
            with pytest.raises(PrimaryStateBusy, match="live reader"):
                next(ordered_primary_rows(other, rows, byte_limit=128 * 1024))
            with pytest.raises(PrimaryStateBusy, match="live reader"):
                files.ordering_cleanup_step()
            retained = files.put_bytes("original", [b"original"], byte_limit=8)
            assert next(ordered) == rows[0]
            ordered.close()
            assert list(ordered_primary_rows(other, rows, byte_limit=128 * 1024)) == rows[::-1]
            with files.verified(retained) as stream:
                assert stream.read() == b"original"


@pytest.mark.parametrize("failure", ["input", "duplicate"])
def test_ordering_failure_retains_only_derived_generation_and_preserves_primary(tmp_path, failure):
    path = tmp_path / "state.sqlite3"
    with _open(path, initialize=True) as files:
        original = files.put_bytes("original", [b"original"], byte_limit=8)

        def rows():
            yield "a", 1, "a" * 64
            if failure == "input":
                raise RuntimeError("ordering input interrupted")
            yield "a", 1, "a" * 64

        error = RuntimeError if failure == "input" else PrimaryStateError
        with pytest.raises(error):
            list(ordered_primary_rows(files, rows(), byte_limit=128 * 1024))
        with pytest.raises(PrimaryStateBusy, match="ordering"):
            files.put_control("new", b"new", byte_limit=3)
    with _open(path, initialize=False) as files:
        assert list(ordered_primary_rows(files, [("b", 2, "b" * 64)], byte_limit=128 * 1024)) == [
            ("b", 2, "b" * 64)
        ]
        with files.verified(original) as stream:
            assert stream.read() == b"original"


def test_ordering_page_admission_rejects_and_cold_cleanup_preserves_primary(tmp_path):
    from riverhog_client import PrimaryStateCapacity

    path = tmp_path / "state.sqlite3"
    with _open(path, initialize=True) as files:
        original = files.put_bytes("original", [b"original"], byte_limit=8)
        with pytest.raises(PrimaryStateCapacity, match="scratch admission"):
            list(
                ordered_primary_rows(
                    files, ((f"{n:064}", n, "a" * 64) for n in range(257)), byte_limit=3 * 4096
                )
            )
    with _open(path, initialize=False) as files:
        for _ in range(128):
            if files.ordering_cleanup_step():
                break
        else:
            pytest.fail("bounded scratch fixture cleanup did not finish")
        assert files._one("SELECT generation FROM primary_ordering") is None
        assert list(ordered_primary_rows(files, [("b", 2, "b" * 64)], byte_limit=128 * 1024)) == [
            ("b", 2, "b" * 64)
        ]
        with files.verified(original) as stream:
            assert stream.read() == b"original"


def test_ordering_cleanup_cursor_survives_cold_steps_and_deletes_one_row_at_a_time(tmp_path):
    path = tmp_path / "state.sqlite3"
    with _open(path, initialize=True) as files:
        list(
            ordered_primary_rows(
                files, ((f"{n:08}", n, "a" * 64) for n in range(17)), byte_limit=128 * 1024
            )
        )
        assert not files.ordering_cleanup_step()
        assert not files.ordering_cleanup_step()
        assert files._one("SELECT cursor FROM primary_ordering") == ("00000000",)
    with _open(path, initialize=False) as files:
        for remaining in range(15, -1, -1):
            assert not files.ordering_cleanup_step()
            assert files._one("SELECT count(*) FROM primary_order_rows") == (remaining,)
        assert files.ordering_cleanup_step()
        assert files.ordering_cleanup_step()


def test_ordering_schema_is_required_without_erasing_original_selected_bytes(tmp_path):
    path = tmp_path / "state.sqlite3"
    with _open(path, initialize=True) as files:
        files.put_bytes("original", [b"original"], byte_limit=8)
    with sqlite3.connect(path) as db:
        db.execute("DROP TABLE primary_order_rows")
    with pytest.raises((PrimaryStateError, RuntimeError)):
        _open(path, initialize=False)
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT body FROM primary_chunks").fetchall() == [(b"original",)]
        assert (
            db.execute("SELECT name FROM sqlite_schema WHERE name='primary_order_rows'").fetchone()
            is None
        )


def test_atomic_control_progress_does_not_advance_or_displace_pending_byte_cleanup(tmp_path):
    path = tmp_path / "state.sqlite3"
    with _open(path, initialize=True) as files:
        files.prepare_controls(("cursor", "receipt"), byte_limit=100)
        files.put_bytes("journal", [b"j" * 9000], byte_limit=9000)
        files.put_control("cursor", b"before", byte_limit=100)
        assert not files.remove_step("journal")
        operation = files._one("SELECT generation,cursor,phase FROM primary_operation")
        files.put_control("cursor", b"after", byte_limit=100, expected=b"before")
        files.put_control("receipt", b"original proof", byte_limit=100)
        assert files.read_control("cursor", byte_limit=100) == b"after"
        files.remove_control("cursor", expected=b"after")
        assert files._one("SELECT generation,cursor,phase FROM primary_operation") == operation
        assert files._one("SELECT count(*) FROM primary_chunks") == (3,)
        with pytest.raises(PrimaryStateBusy, match="byte candidate"):
            files.put_control("journal", b"replacement", byte_limit=100)
        with pytest.raises(PrimaryStateBusy):
            files.put_bytes("new", [b"new"], byte_limit=3)
    with _open(path, initialize=False) as files:
        assert files.read_control("receipt", byte_limit=100) == b"original proof"
        assert files._one("SELECT generation,cursor,phase FROM primary_operation") == operation


def test_ordering_rechecks_byte_idle_after_finishing_old_scratch(tmp_path, monkeypatch):
    from riverhog_client import _prepared_primary_sqlite_ordering as ordering

    path = tmp_path / "state.sqlite3"
    with _open(path, initialize=True) as files:
        files.put_bytes("journal", [b"j" * 9000], byte_limit=9000)
        cleanup = ordering.cleanup_step
        raced = False

        def interleaved(owner):
            nonlocal raced
            ready = cleanup(owner)
            if ready and not raced:
                raced = True
                assert not owner.remove_step("journal")
            return ready

        with monkeypatch.context() as patch:
            patch.setattr(ordering, "cleanup_step", interleaved)
            with pytest.raises(PrimaryStateBusy, match="byte mutation"):
                list(ordered_primary_rows(files, [("a", 1, "a" * 64)], byte_limit=128 * 1024))
        assert files._one("SELECT generation FROM primary_ordering") is None
        while not files.remove_step("journal"):
            pass
        assert list(ordered_primary_rows(files, [("a", 1, "a" * 64)], byte_limit=128 * 1024)) == [
            ("a", 1, "a" * 64)
        ]


@pytest.mark.parametrize("members", [1, 257])
def test_retained_scratch_grant_is_a_control_only_point_read_until_final_cleanup(tmp_path, members):
    path = tmp_path / "state.sqlite3"
    rows = [(f"{number:064x}", number, "a" * 64) for number in range(members)]
    with _open(path, initialize=True) as files:
        assert files.ordering_reserved_bytes() == 0
        assert list(ordered_primary_rows(files, rows, byte_limit=128 * 1024)) == rows
    with _open(path, initialize=False) as files:
        statements = []

        def controls_only(action, table, _column, *_rest):
            if action == sqlite3.SQLITE_READ and table in ("primary_chunks", "primary_order_rows"):
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        files._connection.set_authorizer(controls_only)
        files._connection.set_trace_callback(statements.append)
        for _ in range(3):
            assert files.ordering_reserved_bytes() == 128 * 1024
        assert statements == ["SELECT byte_limit FROM primary_ordering WHERE id=1"] * 3
        files._connection.set_authorizer(None)
        files._connection.set_trace_callback(None)
        for _ in range(members + 2):
            if files.ordering_cleanup_step():
                break
            assert files.ordering_reserved_bytes() == 128 * 1024
        else:
            pytest.fail("bounded derived cleanup did not finish")
        assert files.ordering_reserved_bytes() == 0


@pytest.mark.parametrize("damaged", ["01", "-1", "unknown"])
def test_invalid_retained_scratch_grant_is_not_replaced_by_current_configuration(tmp_path, damaged):
    path = tmp_path / "state.sqlite3"
    with _open(path, initialize=True) as files:
        list(ordered_primary_rows(files, [("a", 1, "a" * 64)], byte_limit=128 * 1024))
    with sqlite3.connect(path) as db:
        db.execute("UPDATE primary_ordering SET byte_limit=?", (damaged,))
    with _open(path, initialize=False) as files:
        with pytest.raises(PrimaryStateError, match="scratch admission is invalid"):
            files.ordering_reserved_bytes()
        assert files._one("SELECT byte_limit FROM primary_ordering") == (damaged,)


def test_control_capacity_is_prepared_explicitly_and_cannot_change_or_become_a_byte_role(tmp_path):
    with _open(tmp_path / "state.sqlite3", initialize=True) as files:
        with pytest.raises(PrimaryStateError, match="not prepared"):
            files.put_control("receipt", b"original", byte_limit=8)
        files.prepare_controls(("receipt",), byte_limit=8)
        assert not files.exists("receipt")
        with pytest.raises(FileNotFoundError):
            files.read_control("receipt", byte_limit=8)
        with pytest.raises(PrimaryStateError, match="reserved control"):
            files.put_bytes("receipt", (b"replacement",), byte_limit=11)
        with pytest.raises(PrimaryStateError, match="capacity changed"):
            files.prepare_controls(("receipt",), byte_limit=9)
        with pytest.raises(owner.PrimaryStateCapacity, match="prepared capacity"):
            files.put_control("receipt", b"too large", byte_limit=9)
        files.put_control("receipt", b"original", byte_limit=8)
        slot = files._one("SELECT rowid,length(body) FROM primary_controls")
        files.remove_control("receipt", expected=b"original")
        files.put_control("receipt", b"", byte_limit=8)
        assert files.exists("receipt") and files.read_control("receipt", byte_limit=0) == b""
        assert files._one("SELECT rowid,length(body) FROM primary_controls") == slot
        files.put_bytes("journal", (b"selected",), byte_limit=8)
        with pytest.raises(PrimaryStateError, match="byte role"):
            files.prepare_controls(("journal",), byte_limit=8)


@pytest.mark.parametrize("limit", [-1, True, 1.5, None])
def test_control_preparation_requires_explicit_extent_admission(tmp_path, limit):
    with _open(tmp_path / "state.sqlite3", initialize=True) as files:
        with pytest.raises(ValueError, match="admission"):
            files.prepare_controls(("receipt",), byte_limit=limit)
        with pytest.raises(owner.PrimaryStateCapacity, match="SQLite extent"):
            files.prepare_controls(
                ("receipt",), byte_limit=files._connection.getlimit(sqlite3.SQLITE_LIMIT_LENGTH)
            )
        assert not files.exists("receipt")
        files.sync()


@pytest.mark.parametrize(
    "damaged",
    [
        b"short",
        b"\x03" + bytes(8),
        b"\x00" + (1).to_bytes(8, "big"),
        b"\x01" + (9).to_bytes(8, "big") + bytes(8),
        "text is not an extent",
    ],
)
def test_invalid_control_extent_fails_closed_without_replacing_original_state(tmp_path, damaged):
    path = tmp_path / "state.sqlite3"
    with _open(path, initialize=True) as files:
        files.prepare_controls(("receipt",), byte_limit=8)
        files.put_control("receipt", b"original", byte_limit=8)
    with sqlite3.connect(path) as db:
        db.execute("UPDATE primary_controls SET body=?", (damaged,))
    with _open(path, initialize=False) as files:
        with pytest.raises(PrimaryStateError, match="control"):
            files.read_control("receipt", byte_limit=8)
        with pytest.raises(PrimaryStateError, match="control"):
            files.exists("receipt")
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT body FROM primary_controls").fetchone() == (damaged,)


def test_unknown_storage_geometry_is_rejected_without_erasing_original_bytes(tmp_path):
    path = tmp_path / "state.sqlite3"
    with _open(path, initialize=True) as files:
        files.put_bytes("original", (b"original",), byte_limit=8)
    with sqlite3.connect(path) as db:
        db.execute("UPDATE primary_format SET geometry='unsupported geometry'")
    with pytest.raises(PrimaryStateError, match="geometry"):
        _open(path, initialize=False)
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT body FROM primary_chunks").fetchall() == [(b"original",)]
        assert db.execute("SELECT geometry FROM primary_format").fetchone() == (
            "unsupported geometry",
        )


@pytest.mark.parametrize("phase", ["warming", "selection"])
@pytest.mark.parametrize("committed", [False, True])
def test_uncertain_journal_preparation_resumes_without_claiming_unprepared_capacity(
    tmp_path, monkeypatch, phase, committed
):
    from riverhog_client import _prepared_primary_sqlite_journal as journal

    path = tmp_path / "state.sqlite3"
    warm = journal._warm

    def uncertain(files, identifier):
        if phase == "warming":
            files._root._connection = _CommitFailure(files._root._connection, committed=committed)
        warm(files, identifier)
        if phase == "selection":
            files._root._connection = _CommitFailure(files._root._connection, committed=committed)

    with _open(path, initialize=True) as files:
        with monkeypatch.context() as patch:
            patch.setattr(journal, "_warm", uncertain)
            with pytest.raises(PrimaryStateError, match="mutation failed"):
                files.prepare_controls(("receipt",), byte_limit=65536)
        with pytest.raises(PrimaryStateError, match="uncertain"):
            files.sync()
    with _open(path, initialize=False) as files:
        assert not files.exists("receipt")
        selected = phase == "selection" and committed
        assert bool(files.journal_reserved_bytes()) is selected
        assert files._one("SELECT hex(substr(body,1,1)) FROM primary_controls") == (
            "00" if selected else "02",
        )
        if not selected:
            with pytest.raises(PrimaryStateBusy, match="preparation"):
                files.put_control("receipt", b"original", byte_limit=65536)
        files.prepare_controls(("receipt",), byte_limit=65536)
        assert files.journal_reserved_bytes() > 65536
        files.put_control("receipt", b"original", byte_limit=65536)
        assert files.read_control("receipt", byte_limit=65536) == b"original"


def test_lost_journal_reserve_blocks_payload_before_input_and_execution_restores_exact_controls(
    tmp_path,
):
    path = tmp_path / "state.sqlite3"
    body = b"original" * 8192
    with _open(path, initialize=True) as files:
        files.prepare_controls(("receipt",), byte_limit=65536)
        files.put_control("receipt", body, byte_limit=65536)
        reserved = files.journal_reserved_bytes()
    # Test-only damage to a closed, committed, non-hot journal fixture.
    path.with_name(path.name + "-journal").write_bytes(b"")
    with _open(path, initialize=False) as files:
        assert files.read_control("receipt", byte_limit=65536) == body
        assert files.space_usage().journal.end_bytes < reserved
        consumed = False

        def payload():
            nonlocal consumed
            consumed = True
            yield b"new payload"

        with pytest.raises(owner.PrimaryStateCapacity, match="journal backing"):
            files.put_bytes("new", payload(), byte_limit=11)
        assert not consumed and files._one("SELECT generation FROM primary_operation") is None
        with pytest.raises(owner.PrimaryStateCapacity, match="journal backing"):
            files.put_control("receipt", b"changed", byte_limit=65536, expected=body)
        files.prepare_controls(("receipt",), byte_limit=65536)
        space = files.space_usage()
        assert space.journal.end_bytes >= reserved
        assert space.journal.allocated_bytes >= reserved
        assert files.read_control("receipt", byte_limit=65536) == body
        files.put_control("receipt", b"after", byte_limit=65536, expected=body)


def test_journal_restore_during_pending_cleanup_does_not_read_preimages_or_advance_its_cursor(
    tmp_path,
):
    path = tmp_path / "state.sqlite3"
    with _open(path, initialize=True) as files:
        files.prepare_controls(("receipt",), byte_limit=65536)
        files.put_control("receipt", b"original", byte_limit=65536)
        files.put_bytes("journal", (b"x" * 9000,), byte_limit=9000)
        assert not files.remove_step("journal")
        operation = files._one("SELECT generation,cursor,phase FROM primary_operation")
    path.with_name(path.name + "-journal").write_bytes(b"")
    with _open(path, initialize=False) as files:

        def no_preimages(action, table, *_rest):
            return (
                sqlite3.SQLITE_DENY
                if action == sqlite3.SQLITE_READ
                and table in ("primary_chunks", "primary_order_rows")
                else sqlite3.SQLITE_OK
            )

        files._connection.set_authorizer(no_preimages)
        try:
            files.prepare_controls(("receipt",), byte_limit=65536)
            files.put_control("receipt", b"after", byte_limit=65536, expected=b"original")
        finally:
            files._connection.set_authorizer(None)
        assert files._one("SELECT generation,cursor,phase FROM primary_operation") == operation
        assert files._one("SELECT count(*) FROM primary_chunks") == (3,)
        assert files.read_control("receipt", byte_limit=65536) == b"after"


def test_small_retention_rejects_before_ready_and_larger_grant_can_resume_the_original_slot(
    tmp_path,
):
    from tests.support.prepared_primary import SqlitePrimaryTestGuard

    path = tmp_path / "state.sqlite3"
    path.touch()
    limits = owner.SqlitePrimaryLimits(2 * 1024**2, 4096, 128 * 1024)
    with owner.TransactionalPrimaryFiles(
        SqlitePrimaryTestGuard(path),
        allocation_descriptor=ALLOCATION,
        limits=limits,
        initialize=True,
    ) as files:
        with pytest.raises(owner.PrimaryStateCapacity, match="cannot admit"):
            files.prepare_controls(("receipt",), byte_limit=65536)
        assert files._one("PRAGMA journal_size_limit") == (4096,)
        assert files.journal_reserved_bytes() == 0
        assert not files.exists("receipt")
        slot = files._one("SELECT rowid,length(body) FROM primary_controls")
        assert files.space_usage().journal.end_bytes <= 4096
    with _open(path, initialize=False) as files:
        files.increase_resource_grants(
            database_bytes=LIMITS.database_bytes, journal_bytes=LIMITS.journal_bytes
        )
        files.prepare_controls(("receipt",), byte_limit=65536)
        assert files._one("SELECT rowid,length(body) FROM primary_controls") == slot
        files.put_control("receipt", b"original", byte_limit=65536)
        assert files.read_control("receipt", byte_limit=65536) == b"original"


def test_ready_control_preparation_is_read_only_and_capacity_is_fixed_for_the_construction(
    tmp_path, monkeypatch
):
    from riverhog_client import _prepared_primary_sqlite_journal as journal

    with _open(tmp_path / "state.sqlite3", initialize=True) as files:
        files.prepare_controls(("receipt",), byte_limit=65536)
        changes = files._connection.total_changes
        statements = []
        files._connection.set_trace_callback(statements.append)

        def forbidden(*_args):
            raise AssertionError("ready capacity must not be warmed again")

        monkeypatch.setattr(journal, "_warm", forbidden)
        for _ in range(3):
            files.prepare_controls(("receipt",), byte_limit=65536)
        with pytest.raises(PrimaryStateError, match="construction control capacity"):
            files.prepare_controls(("other",), byte_limit=65537)
        files._connection.set_trace_callback(None)
        assert files._connection.total_changes == changes
        assert not any(s.startswith(("BEGIN", "UPDATE", "INSERT", "DELETE")) for s in statements)


def test_control_preparation_never_releases_existing_reserved_journal_backing(
    tmp_path, monkeypatch
):
    from riverhog_client import _prepared_primary_sqlite_journal as journal

    with _open(tmp_path / "state.sqlite3", initialize=True) as files:
        limit = journal._limit

        def preserve(db, value):
            assert value != 0 or files.journal_reserved_bytes() == 0, (
                "preparation released an existing journal reservation"
            )
            return limit(db, value)

        monkeypatch.setattr(journal, "_limit", preserve)
        files.prepare_controls(("receipt",), byte_limit=65536)
        files.put_control("receipt", b"original", byte_limit=65536)
        before = files.space_usage()
        files.prepare_controls(("cursor",), byte_limit=65536)
        after = files.space_usage()
        assert after.journal.end_bytes >= before.journal_reserved_bytes
        assert after.journal.allocated_bytes >= before.journal_reserved_bytes
        assert files.read_control("receipt", byte_limit=65536) == b"original"


@pytest.mark.parametrize("change", ["install", "replace", "remove"])
@pytest.mark.parametrize("committed", [False, True])
def test_uncertain_large_control_mutation_retries_coldly_with_original_capacity(
    tmp_path, change, committed
):
    from tests.support.prepared_primary_control_faults import witness
    from tests.support.prepared_primary_resource_recovery import _CommitResponseFailure

    path = tmp_path / "state.sqlite3"

    def opened(*, initialize):
        return _open(path, initialize=initialize)

    def inject(files):
        files._connection = _CommitResponseFailure(files._connection, committed=committed)

    report = witness(opened, change=change, inject=inject)
    assert report["visible_outcome"] == ("after" if committed else "before")


@pytest.mark.parametrize("start", [0, 127, 32767, (1 << 32) - 1, (1 << 63) - 1, (1 << 64) - 3])
def test_barrier_counter_keeps_fixed_capacity_across_integer_encoding_boundaries(tmp_path, start):
    from tests.support.prepared_primary_barrier import progress

    def opened(*, initialize):
        return _open(tmp_path / "state.sqlite3", initialize=initialize)

    result = progress(opened, start)
    assert result["fixed_extent_bytes"] == 8 and not result["database_and_journal_growth"]


@pytest.mark.parametrize("damage", ["exhausted", "short", "text", "integer"])
def test_barrier_invalid_extent_or_exhaustion_preserves_originals_and_rejects_cold_open(
    tmp_path, damage
):
    from tests.support.prepared_primary_barrier import reject

    def opened(*, initialize):
        return _open(tmp_path / "state.sqlite3", initialize=initialize)

    result = reject(opened, damage)
    assert result["cold_open_rejected"] and result["original_payload_exact"]


@pytest.mark.parametrize("value", [b"", b"x" * 7, b"x" * 9, "00000000", 1])
def test_barrier_schema_requires_exact_blob_extent(tmp_path, value):
    with _open(tmp_path / "state.sqlite3", initialize=True) as files:
        previous = files._one("SELECT barrier FROM primary_format")
        with pytest.raises(sqlite3.IntegrityError, match="ck_primary_format_barrier"):
            files._db().execute("UPDATE primary_format SET barrier=? WHERE id=1", (value,))
        assert files._one("SELECT barrier FROM primary_format") == previous


@pytest.mark.parametrize("kind", ["binding", "sqlite", "native-io", "io-error"])
@pytest.mark.parametrize("stage", ["begin", "append", "select-before", "select-after"])
@pytest.mark.parametrize("rollback_failure", [False, True])
def test_transaction_refusal_preserves_originals_and_requires_cold_reconciliation(
    tmp_path, monkeypatch, kind, stage, rollback_failure
):
    path = tmp_path / "state.sqlite3"
    if kind == "binding":
        failure = MemoryError("SQLite allocation refused")
    else:
        failure = sqlite3.OperationalError("SQLite allocation refused")
        failure.sqlite_errorcode = {
            "sqlite": sqlite3.SQLITE_NOMEM,
            "native-io": sqlite3.SQLITE_IOERR_NOMEM,
            "io-error": sqlite3.SQLITE_IOERR,
        }[kind]
    expected_error = PrimaryStateError if kind == "io-error" else owner.PrimaryStateCapacity
    expected_message = "mutation failed" if kind == "io-error" else "memory admission"
    rollback_attempts = []

    class FailedConnection:
        def __init__(self, connection):
            self.connection = connection

        @property
        def in_transaction(self):
            return self.connection.in_transaction

        def execute(self, sql, *args):
            if stage == "begin" and sql == "BEGIN IMMEDIATE":
                raise failure
            if stage == "select-before" and sql == "COMMIT":
                raise failure
            result = self.connection.execute(sql, *args)
            if (stage == "append" and sql.startswith("INSERT INTO primary_chunks")) or (
                stage == "select-after" and sql == "COMMIT"
            ):
                result.close()
                raise failure
            return result

        def rollback(self):
            rollback_attempts.append(True)
            if rollback_failure:
                raise MemoryError("cleanup allocation refused")
            self.connection.rollback()

        def close(self):
            self.connection.close()

    with _open(path, initialize=True) as files:
        original = files.put_bytes("original", [b"exact original"], byte_limit=14)
        raw = files._connection
        wrapped = FailedConnection(raw)
        if stage.startswith("select"):
            select = files._select

            def selected(generation, file):
                files._connection = wrapped
                return select(generation, file)

            monkeypatch.setattr(files, "_select", selected)
        else:
            files._connection = wrapped
        with pytest.raises(expected_error, match=expected_message) as caught:
            files.put_bytes("candidate", [b"new candidate"], byte_limit=13)
        assert type(caught.value) is expected_error
        assert caught.value.__cause__ is failure
        assert bool(rollback_attempts) == (stage in ("append", "select-before"))
        assert files._poisoned
        with pytest.raises(PrimaryStateError, match="uncertain"):
            files.exists("original")
    with _open(path, initialize=False) as reopened:
        with reopened.verified(original) as stream:
            assert stream.read() == b"exact original"
        assert reopened.exists("candidate") is (stage == "select-after")
        # The interrupted singleton is retained until bounded cleanup settles it.
        steps = 0
        while not reopened.recover_step():
            steps += 1
            assert steps <= 4
        if stage == "select-after":
            candidate = PrimaryByteFile(
                "candidate", 13, hashlib.sha256(b"new candidate").hexdigest()
            )
            with reopened.verified(candidate) as stream:
                assert stream.read() == b"new candidate"
        else:
            assert not reopened.exists("candidate")
        reopened.put_bytes("later", [b"progress"], byte_limit=8)


def test_actual_sqlite_heap_exhaustion_defers_without_disabling_the_grant(tmp_path):
    import json
    import subprocess
    import sys

    code = r"""
import json,sqlite3,sys
from contextlib import closing
from pathlib import Path
from riverhog_client import PrimaryStateCapacity,PrimaryStateError
from riverhog_client._prepared_primary_sqlite import SqlitePrimaryLimits,TransactionalPrimaryFiles
from tests.support.prepared_primary import SqlitePrimaryTestGuard
path=Path(sys.argv[1]);path.touch()
limits=SqlitePrimaryLimits(16*1024**2,32*1024**2,128*1024)
def open_store(initialize):
    return TransactionalPrimaryFiles(SqlitePrimaryTestGuard(path),
        allocation_descriptor=b"heap deferral fixture",limits=limits,initialize=initialize)
def heap_limit(db,setting=None):
    statement="PRAGMA hard_heap_limit"+("" if setting is None else "="+str(setting))
    with closing(db.execute(statement)) as cursor:return cursor.fetchone()[0]
with open_store(True) as files:
    original=files.put_bytes("original",[b"exact original"],byte_limit=14)
    grant=2*1024*1024
    assert heap_limit(files._connection)==0
    assert heap_limit(files._connection,grant)==grant
    try:
        with files._transaction() as db:
            db.execute(
                "INSERT INTO primary_namespaces(parent,name) VALUES (1,'uncommitted')"
            ).close()
            with closing(db.execute("SELECT zeroblob(?)",(2*grant,))) as cursor:
                cursor.fetchone()
    except PrimaryStateCapacity as exc:
        assert isinstance(exc.__cause__,(MemoryError,sqlite3.Error))
    else:raise AssertionError("actual exhausted SQLite heap admitted an oversized allocation")
    assert files._poisoned
    try:files.exists("original")
    except PrimaryStateError as exc:assert "uncertain" in str(exc)
    else:raise AssertionError("memory refusal did not poison the operation")
    assert heap_limit(files._connection)==grant
with open_store(False) as reopened:
    with reopened.verified(original) as stream:assert stream.read()==b"exact original"
    assert reopened._one("SELECT id FROM primary_namespaces WHERE name='uncommitted'") is None
    reopened.put_bytes("later",[b"progress"],byte_limit=8)
    assert heap_limit(reopened._connection)==grant
print(json.dumps({"passed":True,"heap_limit_bytes":grant,"original_exact":True}))
"""
    result = subprocess.run(
        [sys.executable, "-c", code, str(tmp_path / "state.sqlite3")],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, (result.stdout, result.stderr)
    assert json.loads(result.stdout) == {
        "passed": True,
        "heap_limit_bytes": 2 * 1024**2,
        "original_exact": True,
    }


@pytest.mark.parametrize("refused", ["database", "journal"])
def test_hot_journal_cold_admission_is_nondestructive_and_retries_with_sufficient_grants(
    tmp_path, monkeypatch, refused
):
    import subprocess
    import sys
    from dataclasses import replace

    from riverhog_client import _prepared_primary_windows_native as windows

    path = windows.windows_container_path(tmp_path, ALLOCATION)
    guards = []

    def guarded(location, *, initialize):
        assert location == path and not initialize
        guard = _SQLGuard(path)
        guards.append(guard)
        return guard

    monkeypatch.setattr(windows, "WindowsContainer", guarded)

    def cold(limits=LIMITS):
        return owner.TransactionalPrimaryFiles.windows(
            path, allocation_descriptor=ALLOCATION, limits=limits, initialize=False
        )

    original = b"original primary" * 16384
    with _open(path, initialize=True) as files:
        selected = files.put_bytes("journal", (original,), byte_limit=len(original))

    # This isolated crash fixture spills dirty pages to produce a genuinely hot
    # journal. The maintained owner continues to disable cache spilling.
    code = r"""
import os,sqlite3,sys
db=sqlite3.connect(sys.argv[1],isolation_level=None)
assert db.execute("PRAGMA journal_mode=PERSIST").fetchone()==("persist",)
db.execute("PRAGMA synchronous=EXTRA")
db.execute("PRAGMA cache_size=1")
db.execute("PRAGMA cache_spill=ON")
db.execute("BEGIN IMMEDIATE")
db.execute("UPDATE primary_chunks SET body=zeroblob(length(body))")
os._exit(73)
"""
    crashed = subprocess.run(
        [sys.executable, "-I", "-c", code, str(path)], capture_output=True, timeout=30
    )
    assert crashed.returncode == 73, crashed.stderr
    journal = Path(str(path) + "-journal")
    assert journal.stat().st_size > 4096
    assert journal.read_bytes()[:8] == bytes.fromhex("d9d505f920a163d7")
    retained = {file: hashlib.sha256(file.read_bytes()).hexdigest() for file in (path, journal)}
    offered = replace(LIMITS, **{refused + "_bytes": 4096})

    def forbidden(*_args, **_kwargs):
        pytest.fail("undersized cold grant opened SQLite before rejecting hot recovery")

    with monkeypatch.context() as patch:
        patch.setattr(owner.sqlite3, "connect", forbidden)
        with pytest.raises(owner.PrimaryStateCapacity, match="cold recovery " + refused):
            cold(offered)
    assert guards[-1].closed
    assert all(
        hashlib.sha256(file.read_bytes()).hexdigest() == sha for file, sha in retained.items()
    )

    with cold() as files:
        with files.verified(selected) as stream:
            assert stream.read() == original
        assert files._connection.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        files.put_bytes("later", (b"progress",), byte_limit=8)
    with cold() as files:
        assert files.exists("later")
        with files.verified(selected) as stream:
            assert stream.read() == original


@pytest.mark.parametrize("operation", ["query", "body", "unselected", "selected"])
def test_recorded_callback_failure_blocks_returning_query_and_control_results(
    tmp_path, monkeypatch, operation
):
    from contextlib import contextmanager

    from riverhog_client import PrimaryStateCapacity

    failure = PrimaryStateCapacity("recorded native callback refusal")
    refused = False

    def checked():
        if refused:
            raise failure

    with _open(tmp_path / "state.sqlite3", initialize=True) as files:
        files.prepare_controls(("control",), byte_limit=32)
        if operation != "unselected":
            files.put_control("control", b"original", byte_limit=32)
        connection = files._connection
        monkeypatch.setattr(files._guard, "check_operation", checked)
        if operation == "query":

            def trace(query):
                nonlocal refused
                if query == "SELECT 7":
                    refused = True

            connection.set_trace_callback(trace)
        else:

            class Connection:
                def __getattr__(self, name):
                    return getattr(connection, name)

                @contextmanager
                def blobopen(self, *args, **kwargs):
                    nonlocal refused
                    with connection.blobopen(*args, **kwargs) as blob:
                        yield blob
                    refused = True

            files._connection = Connection()

        def run():
            if operation == "query":
                return files._one("SELECT 7")
            if operation == "selected":
                return files.exists("control")
            return files.read_control("control", byte_limit=32)

        try:
            with pytest.raises(PrimaryStateCapacity) as caught:
                run()
            assert caught.value is failure and refused
        finally:
            refused = False
            connection.set_trace_callback(None)


@pytest.mark.parametrize("backend", ["posix", "sqlite"])
@pytest.mark.parametrize("count", [0, 1, 17, 257])
def test_scoped_ordering_builds_once_and_replays_independent_cursors(tmp_path, backend, count):
    from riverhog_client._prepared_primary_posix import PrimaryFiles

    opened = (
        PrimaryFiles(tmp_path.resolve() / "state", initialize=True)
        if backend == "posix"
        else _open(tmp_path / "state.sqlite3", initialize=True)
    )
    expected = [(f"key-{number:08}", number, "a" * 64) for number in range(count)]
    observed = []

    def rows():
        for row in reversed(expected):
            observed.append(row[0])
            yield row

    with opened as files:
        original = files.put_bytes("original", (b"original",), byte_limit=8)
        with files.child("child", initialize=True) as child:
            with files.ordered_rows(rows(), byte_limit=128 * 1024) as selected:
                assert len(observed) == count and selected.count == count
                first, second = selected.rows(), selected.rows()
                if count:
                    assert next(first) == next(second) == expected[0]
                    first.close()
                    assert list(second) == expected[1:]
                else:
                    assert list(first) == list(second) == []
                for contender in [files, child]:
                    with pytest.raises(PrimaryStateBusy, match="live reader"):
                        with contender.ordered_rows(expected, byte_limit=128 * 1024):
                            pytest.fail("another order displaced a live selection")
                    with pytest.raises(PrimaryStateBusy, match="live reader"):
                        contender.ordering_cleanup_step()
                assert list(selected.rows()) == expected
                assert list(selected.rows()) == expected
                assert len(observed) == count
                escaped = selected.rows()
                with files.verified(original) as stream:
                    assert stream.read() == b"original"
            with pytest.raises(PrimaryStateError, match="view is closed"):
                next(escaped)
            with child.ordered_rows(reversed(expected), byte_limit=128 * 1024) as replacement:
                assert replacement.count == count and list(replacement.rows()) == expected
        with files.verified(original) as stream:
            assert stream.read() == b"original"


@pytest.mark.parametrize("backend", ["posix", "sqlite"])
@pytest.mark.parametrize("failure", ["ordinary", "sqlite-full"])
def test_scoped_ordering_preserves_consumer_failure_and_releases_its_lease(
    tmp_path, backend, failure
):
    from riverhog_client._prepared_primary_posix import PrimaryFiles

    opened = (
        PrimaryFiles(tmp_path.resolve() / "state", initialize=True)
        if backend == "posix"
        else _open(tmp_path / "state.sqlite3", initialize=True)
    )
    error = RuntimeError("consumer interrupted")
    if failure == "sqlite-full":
        error = sqlite3.OperationalError("consumer database full")
        error.sqlite_errorcode = sqlite3.SQLITE_FULL
    rows = [("a", 1, "a" * 64), ("b", 2, "b" * 64)]
    with opened as files:
        with pytest.raises(type(error)) as raised:
            with files.ordered_rows(rows, byte_limit=128 * 1024) as selected:
                iterator = selected.rows()
                assert next(iterator) == rows[0]
                raise error
        assert raised.value is error
        with pytest.raises(PrimaryStateError, match="view is closed"):
            next(iterator)
        with files.ordered_rows(rows, byte_limit=128 * 1024) as replayed:
            assert replayed.count == 2 and list(replayed.rows()) == rows


@pytest.mark.parametrize("mutation", ["missing", "additional"])
def test_scoped_ordering_refuses_a_changed_row_count_instead_of_partial_success(tmp_path, mutation):
    rows = [("a", 1, "a" * 64), ("b", 2, "b" * 64)]
    with _open(tmp_path / "state.sqlite3", initialize=True) as files:
        with files.ordered_rows(rows, byte_limit=128 * 1024) as selected:
            if mutation == "missing":
                files._connection.execute(
                    "DELETE FROM primary_order_rows WHERE artifact_id='b'"
                ).close()
            else:
                files._connection.execute(
                    "INSERT INTO primary_order_rows SELECT generation,'c','3',? "
                    "FROM primary_order_rows LIMIT 1",
                    ("c" * 64,),
                ).close()
            with pytest.raises(PrimaryStateError, match="count changed|rows changed"):
                list(selected.rows())


@pytest.mark.parametrize("backend", ["posix", "sqlite"])
@pytest.mark.parametrize("count", [0, 1, 17, 257])
def test_retained_ordering_reopens_without_input_mutation_or_a_live_lease(
    tmp_path, monkeypatch, backend, count
):
    from riverhog_client._prepared_primary_posix import PrimaryFiles

    opened = (
        PrimaryFiles(tmp_path.resolve() / "state", initialize=True)
        if backend == "posix"
        else _open(tmp_path / "state.sqlite3", initialize=True)
    )
    expected = [(f"{number:064x}", number, "a" * 64) for number in range(count)]
    consumed = 0

    def rows():
        nonlocal consumed
        for row in reversed(expected):
            consumed += 1
            yield row

    with opened as files:
        original = files.put_bytes("original", [b"original"], byte_limit=8)
        with files.ordered_rows(rows(), byte_limit=128 * 1024) as selected:
            retained = selected.retain()
            assert retained.count == selected.count == consumed == count
            escaped = selected.rows()
        with pytest.raises(PrimaryStateError, match="closed"):
            next(escaped)
        assert files.ordering_reserved_bytes() > 0
        assert files.recover_ordering_step()
        assert files.ordering_reserved_bytes() > 0
        independent = files.put_bytes("independent", [b"work"], byte_limit=4)
        statements = []
        connections = []
        if backend == "sqlite":
            files._connection.set_trace_callback(statements.append)
        else:
            from riverhog_client import _prepared_primary_posix_ordering as owner

            connect = owner.sqlite3.connect

            def read_only(*args, **kwargs):
                assert kwargs.get("uri") is True and str(args[0]).endswith("?mode=ro")
                result = connect(*args, **kwargs)
                result.set_trace_callback(statements.append)
                connections.append(result)
                return result

            monkeypatch.setattr(owner.sqlite3, "connect", read_only)
        for _ in range(2):
            with retained.open() as replay:
                assert replay.count == count
                first, second = replay.rows(), replay.rows()
                if count:
                    assert next(first) == next(second) == expected[0]
                    first.close()
                    assert list(second) == expected[1:]
                else:
                    assert list(first) == list(second) == []
                assert list(replay.rows()) == expected
                with pytest.raises(PrimaryStateBusy, match="live reader"):
                    with retained.open():
                        pytest.fail("another replay displaced its live view")
                with pytest.raises(PrimaryStateBusy, match="live reader"):
                    files.ordering_cleanup_step()
                escaped = replay.rows()
            with pytest.raises(PrimaryStateError, match="closed"):
                next(escaped)
        assert consumed == count
        assert sum(statement.startswith("SELECT artifact_id") for statement in statements) <= 6 * (
            count + 1
        )
        assert not any(
            statement.startswith(("INSERT", "UPDATE", "DELETE", "BEGIN", "COMMIT"))
            for statement in statements
        )
        for connection in connections:
            with pytest.raises(sqlite3.ProgrammingError, match="closed"):
                connection.execute("SELECT 1")
        for selected_file, body in [(original, b"original"), (independent, b"work")]:
            with files.verified(selected_file) as reader:
                assert reader.read() == body


@pytest.mark.parametrize("backend", ["posix", "sqlite"])
@pytest.mark.parametrize("action", ["cleanup", "replacement", "namespace-close", "root-close"])
def test_retained_ordering_refuses_lost_selection_without_rebuilding_or_losing_primary(
    tmp_path, backend, action
):
    from riverhog_client._prepared_primary_posix import PrimaryFiles

    opened = (
        PrimaryFiles(tmp_path.resolve() / "state", initialize=True)
        if backend == "posix"
        else _open(tmp_path / "state.sqlite3", initialize=True)
    )
    rows = [("a", 1, "a" * 64), ("b", 2, "b" * 64)]
    with opened as files:
        original = files.put_bytes("original", [b"original"], byte_limit=8)
        with files.child("scope", initialize=True) as child:
            with child.ordered_rows(rows, byte_limit=128 * 1024) as selected:
                retained = selected.retain()
            assert files.ordering_reserved_bytes() > 0
            if action == "cleanup":
                files.ordering_cleanup_step()
            elif action == "replacement":
                with files.ordered_rows([("new", 3, "c" * 64)], byte_limit=128 * 1024):
                    pass
            elif action == "namespace-close":
                child.close()
            else:
                files.close()
            with pytest.raises(PrimaryStateError):
                with retained.open():
                    pytest.fail("lost derivation was silently recreated")
            if action != "root-close":
                with files.verified(original) as reader:
                    assert reader.read() == b"original"
                for _ in range(len(rows) + 2):
                    if files.ordering_cleanup_step():
                        break
                else:
                    pytest.fail("bounded retained cleanup did not finish")
                assert files.ordering_reserved_bytes() == 0


@pytest.mark.parametrize("backend", ["posix", "sqlite"])
@pytest.mark.parametrize("failure", ["ordinary", "sqlite-full"])
def test_retained_ordering_preserves_consumer_failure_and_can_reopen_after_partial_use(
    tmp_path, backend, failure
):
    from riverhog_client._prepared_primary_posix import PrimaryFiles

    opened = (
        PrimaryFiles(tmp_path.resolve() / "state", initialize=True)
        if backend == "posix"
        else _open(tmp_path / "state.sqlite3", initialize=True)
    )
    error = RuntimeError("consumer interrupted")
    if failure == "sqlite-full":
        error = sqlite3.OperationalError("consumer database full")
        error.sqlite_errorcode = sqlite3.SQLITE_FULL
    rows = [("a", 1, "a" * 64), ("b", 2, "b" * 64)]
    with opened as files:
        with files.ordered_rows(rows, byte_limit=128 * 1024) as selected:
            retained = selected.retain()
        before = files.ordering_reserved_bytes()
        with pytest.raises(type(error)) as caught:
            with retained.open() as selected:
                iterator = selected.rows()
                assert next(iterator) == rows[0]
                raise error
        assert caught.value is error
        assert files.ordering_reserved_bytes() == before
        with retained.open() as selected:
            assert list(selected.rows()) == rows
        with pytest.raises(PrimaryStateError, match="closed"):
            next(iterator)


@pytest.mark.parametrize("mutation", ["missing", "replaced", "symlink"])
def test_retained_posix_ordering_refuses_changed_physical_scratch_without_recreating_it(
    tmp_path, mutation
):
    from riverhog_client._prepared_primary_posix import PrimaryFiles

    with PrimaryFiles(tmp_path.resolve() / "state", initialize=True) as files:
        original = files.put_bytes("original", [b"original"], byte_limit=8)
        with files.ordered_rows([("a", 1, "a" * 64)], byte_limit=128 * 1024) as selected:
            retained = selected.retain()
        path = files.root / "inventory-ordering" / "selection.sqlite3"
        path.unlink()
        if mutation == "replaced":
            path.write_bytes(b"unrelated derived bytes")
            path.chmod(0o600)
        elif mutation == "symlink":
            path.symlink_to(tmp_path / "absent-unrelated")
        with pytest.raises((OSError, PrimaryStateError)):
            with retained.open():
                pytest.fail("changed retained file exposed a reader")
        assert (
            path.is_symlink() if mutation == "symlink" else path.exists() is (mutation != "missing")
        )
        with files.verified(original) as reader:
            assert reader.read() == b"original"
        if mutation == "symlink":
            with pytest.raises(OSError):
                files.ordering_cleanup_step()
            assert path.is_symlink() and not (tmp_path / "absent-unrelated").exists()
        else:
            assert files.ordering_cleanup_step()


@pytest.mark.parametrize("backend", ["posix", "sqlite"])
def test_retained_ordering_never_adopts_an_old_owner_after_cold_reopen(tmp_path, backend):
    from riverhog_client._prepared_primary_posix import PrimaryFiles

    root = tmp_path.resolve() / ("state" if backend == "posix" else "state.sqlite3")

    def opened(initialize):
        return (
            PrimaryFiles(root, initialize=initialize)
            if backend == "posix"
            else _open(root, initialize=initialize)
        )

    with opened(True) as files:
        original = files.put_bytes("original", [b"original"], byte_limit=8)
        with files.ordered_rows([("a", 1, "a" * 64)], byte_limit=128 * 1024) as selected:
            retained = selected.retain()
    with opened(False) as files:
        assert files.ordering_reserved_bytes() > 0
        with pytest.raises(PrimaryStateError, match="closed"):
            with retained.open():
                pytest.fail("cold open adopted the old derivation handle")
        with files.ordered_rows([("b", 2, "b" * 64)], byte_limit=128 * 1024) as selected:
            current = selected.retain()
        with current.open() as selected:
            assert list(selected.rows()) == [("b", 2, "b" * 64)]
        with files.verified(original) as reader:
            assert reader.read() == b"original"
