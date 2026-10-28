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
        list(files.ordered_rows([("a", 1, "a" * 64)], byte_limit=128 * 1024))
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

        for number, row in enumerate(files.ordered_rows(rows(), byte_limit=1024 * 1024)):
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
            ordered = files.ordered_rows(rows, byte_limit=128 * 1024)
            assert next(ordered) == rows[1]
            with pytest.raises(PrimaryStateBusy, match="live reader"):
                next(other.ordered_rows(rows, byte_limit=128 * 1024))
            with pytest.raises(PrimaryStateBusy, match="live reader"):
                files.ordering_cleanup_step()
            retained = files.put_bytes("original", [b"original"], byte_limit=8)
            assert next(ordered) == rows[0]
            ordered.close()
            assert list(other.ordered_rows(rows, byte_limit=128 * 1024)) == rows[::-1]
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
            list(files.ordered_rows(rows(), byte_limit=128 * 1024))
        with pytest.raises(PrimaryStateBusy, match="ordering"):
            files.put_control("new", b"new", byte_limit=3)
    with _open(path, initialize=False) as files:
        assert list(files.ordered_rows([("b", 2, "b" * 64)], byte_limit=128 * 1024)) == [
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
                files.ordered_rows(
                    ((f"{n:064}", n, "a" * 64) for n in range(257)), byte_limit=3 * 4096
                )
            )
    with _open(path, initialize=False) as files:
        for _ in range(128):
            if files.ordering_cleanup_step():
                break
        else:
            pytest.fail("bounded scratch fixture cleanup did not finish")
        assert files._one("SELECT generation FROM primary_ordering") is None
        assert list(files.ordered_rows([("b", 2, "b" * 64)], byte_limit=128 * 1024)) == [
            ("b", 2, "b" * 64)
        ]
        with files.verified(original) as stream:
            assert stream.read() == b"original"


def test_ordering_cleanup_cursor_survives_cold_steps_and_deletes_one_row_at_a_time(tmp_path):
    path = tmp_path / "state.sqlite3"
    with _open(path, initialize=True) as files:
        list(
            files.ordered_rows(((f"{n:08}", n, "a" * 64) for n in range(17)), byte_limit=128 * 1024)
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
                list(files.ordered_rows([("a", 1, "a" * 64)], byte_limit=128 * 1024))
        assert files._one("SELECT generation FROM primary_ordering") is None
        while not files.remove_step("journal"):
            pass
        assert list(files.ordered_rows([("a", 1, "a" * 64)], byte_limit=128 * 1024)) == [
            ("a", 1, "a" * 64)
        ]


@pytest.mark.parametrize("members", [1, 257])
def test_retained_scratch_grant_is_a_control_only_point_read_until_final_cleanup(tmp_path, members):
    path = tmp_path / "state.sqlite3"
    rows = [(f"{number:064x}", number, "a" * 64) for number in range(members)]
    with _open(path, initialize=True) as files:
        assert files.ordering_reserved_bytes() == 0
        assert list(files.ordered_rows(rows, byte_limit=128 * 1024)) == rows
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
        list(files.ordered_rows([("a", 1, "a" * 64)], byte_limit=128 * 1024))
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
