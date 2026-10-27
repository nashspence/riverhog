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
            child.put_control("prepared", b"original", byte_limit=8)
        with pytest.raises(FileExistsError):
            files.child("member", initialize=True)
        with files.child("member", initialize=False) as child:
            assert child.read_control("prepared", byte_limit=8) == b"original"


def test_selected_generation_reclaim_is_pinned_bounded_and_cold_resumable(tmp_path):
    path = tmp_path / "state.sqlite3"
    with _open(path, initialize=True) as files:
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
