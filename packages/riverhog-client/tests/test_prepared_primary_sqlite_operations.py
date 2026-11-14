"""Original forward/restoration grant retention, monotonicity and cold refusal."""

from __future__ import annotations

import hashlib
import os
import sqlite3
from dataclasses import fields, replace

import pytest
from riverhog_client import PrimaryStateCapacity, PrimaryStateError
from riverhog_client._prepared_primary_sqlite import SqlitePrimaryLimits, TransactionalPrimaryFiles
from riverhog_client._prepared_primary_sqlite_operations import (
    SqliteOperationBank,
    SqliteOperationGrants,
    native_bank,
)

from tests.support.prepared_primary import SqlitePrimaryTestGuard

pytestmark = pytest.mark.skipif(os.name == "nt", reason="SQL core isolation uses the Unix VFS")
LIMITS = SqlitePrimaryLimits(2 * 1024 * 1024, 1024 * 1024, 128 * 1024)
BANK = SqliteOperationBank(
    128, 8 * 1024 * 1024, 65536, 8 * 1024 * 1024, 65536, 262144, 1000, 1 << 40, 1 << 60, 1 << 40
)
GRANTS = SqliteOperationGrants(BANK, BANK)
LARGER = SqliteOperationGrants(
    replace(BANK, write_bytes=2 * BANK.write_bytes), replace(BANK, read_bytes=2 * BANK.read_bytes)
)
BANK_FIELDS = tuple(field.name for field in fields(SqliteOperationBank))


def opened(path, *, initialize=False, operations=GRANTS):
    if initialize:
        path.touch()
    return TransactionalPrimaryFiles(
        SqlitePrimaryTestGuard(path),
        allocation_descriptor=b"owned original operations",
        limits=LIMITS,
        initialize=initialize,
        operations=operations,
    )


def test_original_banks_are_fixed_and_larger_cold_offers_do_not_change_them(tmp_path):
    path = tmp_path / "state.sqlite3"
    with opened(path, initialize=True) as files:
        assert files.operation_grants().grants == GRANTS
        assert len(files.operation_grants().encode()) == 184
    with opened(path, operations=LARGER) as files:
        assert files.operation_grants().grants == GRANTS
        assert files.operation_grants().revision == files.resource_grants().revision == 0


def test_missing_original_offer_stops_cold_open_before_any_barrier(tmp_path, monkeypatch):
    path = tmp_path / "state.sqlite3"
    with opened(path, initialize=True) as files:
        files.put_bytes("original", (b"original",), byte_limit=8)
    before = hashlib.sha256(path.read_bytes()).digest()
    barriers = []
    monkeypatch.setattr(TransactionalPrimaryFiles, "sync", lambda self: barriers.append(True))
    with pytest.raises(PrimaryStateCapacity, match="original operation grants"):
        opened(path, operations=None)
    assert not barriers and hashlib.sha256(path.read_bytes()).digest() == before


@pytest.mark.parametrize("field", BANK_FIELDS)
@pytest.mark.parametrize("bank", ["forward", "recovery"])
def test_each_lowered_cold_bank_field_is_rejected_without_changing_original_bytes(
    tmp_path, bank, field
):
    path = tmp_path / "state.sqlite3"
    with opened(path, initialize=True) as files:
        files.put_bytes("original", (b"original",), byte_limit=8)
    lower = replace(GRANTS, **{bank: replace(BANK, **{field: getattr(BANK, field) - 1})})
    before = hashlib.sha256(path.read_bytes()).digest()
    with pytest.raises(PrimaryStateCapacity, match="original operation grants"):
        opened(path, operations=lower)
    assert hashlib.sha256(path.read_bytes()).digest() == before


@pytest.mark.parametrize("field", BANK_FIELDS)
@pytest.mark.parametrize("bank", ["forward", "recovery"])
def test_explicit_increase_cannot_lower_any_original_bank_dimension(tmp_path, bank, field):
    with opened(tmp_path / "state.sqlite3", initialize=True) as files:
        lower = replace(GRANTS, **{bank: replace(BANK, **{field: getattr(BANK, field) - 1})})
        before = files._db().total_changes
        with pytest.raises(ValueError, match="cannot lower"):
            files.increase_resource_grants(
                database_bytes=LIMITS.database_bytes,
                journal_bytes=LIMITS.journal_bytes,
                operations=lower,
            )
        assert files._db().total_changes == before
        assert files.operation_grants().grants == GRANTS


def test_explicit_increase_is_independent_of_pending_candidate_and_updates_child_view(tmp_path):
    path = tmp_path / "state.sqlite3"
    with opened(path, initialize=True) as files:
        files.child("child", initialize=True)
        original = files.put_bytes("original", (b"original",), byte_limit=8)

        def interrupted():
            yield b"pending"
            raise RuntimeError("source stopped")

        with pytest.raises(RuntimeError, match="source stopped"):
            files.put_bytes("candidate", interrupted(), byte_limit=8192)
    with opened(path, operations=LARGER) as files:
        child = files.child("child", initialize=False)
        cursor = files._one("SELECT generation,cursor,phase FROM primary_operation")
        chunks = files._one("SELECT count(*) FROM primary_chunks")
        selected = files.increase_resource_grants(
            database_bytes=LIMITS.database_bytes,
            journal_bytes=LIMITS.journal_bytes,
            operations=LARGER,
        )
        assert selected.revision == files.operation_grants().revision == 1
        assert child.operation_grants() == files.operation_grants()
        assert files.operation_grants().grants == LARGER
        assert files._one("SELECT generation,cursor,phase FROM primary_operation") == cursor
        assert files._one("SELECT count(*) FROM primary_chunks") == chunks
        with files.verified(original) as stream:
            assert stream.read() == b"original"


def test_unconfigured_profile_can_be_selected_explicitly_and_cannot_be_cleared(tmp_path):
    path = tmp_path / "state.sqlite3"
    with opened(path, initialize=True, operations=None) as files:
        assert files.operation_grants().grants is None
        files.increase_resource_grants(
            database_bytes=LIMITS.database_bytes,
            journal_bytes=LIMITS.journal_bytes,
            operations=GRANTS,
        )
        assert files.operation_grants().grants == GRANTS
        changes = files._db().total_changes
        files.increase_resource_grants(
            database_bytes=LIMITS.database_bytes, journal_bytes=LIMITS.journal_bytes
        )
        assert files.operation_grants().grants == GRANTS
        assert files._db().total_changes == changes
    with pytest.raises(PrimaryStateCapacity):
        opened(path, operations=None)


@pytest.mark.parametrize("committed", [False, True])
def test_uncertain_increase_reconciles_operation_and_resource_revision_together(
    tmp_path, committed
):
    class Uncertain:
        def __init__(self, db):
            self.db = db

        def __getattr__(self, name):
            return getattr(self.db, name)

        def execute(self, sql, *args):
            if sql == "COMMIT":
                if committed:
                    self.db.execute(sql).close()
                raise sqlite3.OperationalError("uncertain operation increase")
            return self.db.execute(sql, *args)

    path = tmp_path / "state.sqlite3"
    with opened(path, initialize=True) as files:
        files._connection = Uncertain(files._connection)
        with pytest.raises(PrimaryStateError, match="mutation failed"):
            files.increase_resource_grants(
                database_bytes=LIMITS.database_bytes,
                journal_bytes=LIMITS.journal_bytes,
                operations=LARGER,
            )
    with opened(path, operations=LARGER) as files:
        retained = files.operation_grants()
        assert retained.revision == files.resource_grants().revision == int(committed)
        assert retained.grants == (LARGER if committed else GRANTS)
        assert files.database_reserve().grant_revision == int(committed)
        assert files.progress_journal().grant_revision == int(committed)
        assert files.physical_readiness().grant_revision == int(committed)


@pytest.mark.parametrize(
    "corruption", ["tag", "revision", "selection", "unselected-values", "overflow"]
)
def test_invalid_profile_rejects_cold_open_without_recreating_state(tmp_path, corruption):
    path = tmp_path / "state.sqlite3"
    with opened(path, initialize=True) as files:
        raw = bytearray(files.operation_grants().encode())
    if corruption == "tag":
        raw[0] ^= 1
    elif corruption == "revision":
        raw[8:16] = (1).to_bytes(8, "big")
    elif corruption == "selection":
        raw[16:24] = (2).to_bytes(8, "big")
    elif corruption == "unselected-values":
        raw[16:24] = b"\0" * 8
    else:
        raw[24:32] = (1 << 63).to_bytes(8, "big")
    with sqlite3.connect(path) as db:
        db.execute("UPDATE primary_format SET operation_grants=?", (bytes(raw),))
    before = hashlib.sha256(path.read_bytes()).digest()
    with pytest.raises(PrimaryStateError, match="operation grant"):
        opened(path)
    assert hashlib.sha256(path.read_bytes()).digest() == before


@pytest.mark.parametrize("field", BANK_FIELDS)
@pytest.mark.parametrize("value", [True, -1, 1 << 63, 1.5])
def test_unrepresentable_bank_fields_are_rejected_before_native_construction(field, value):
    with pytest.raises(ValueError, match="signed-64-bit"):
        replace(BANK, **{field: value})


@pytest.mark.parametrize(
    "override",
    [
        {"database_pages": 0},
        {"database_pages": True},
        {"database_pages": 513},
        {"journal_bytes": 0},
        {"journal_bytes": True},
        {"journal_bytes": 1048577},
    ],
)
def test_forward_caps_cannot_exceed_original_resources_before_importing_native_module(override):
    with pytest.raises(ValueError, match="physical caps"):
        native_bank(GRANTS, LIMITS, **override)


def test_invalid_bank_value_is_rejected_without_copying_or_traversing_it():
    class Unadmitted:
        def __deepcopy__(self, memo):
            raise AssertionError("invalid configuration was traversed before admission")

    with pytest.raises(ValueError, match="signed-64-bit"):
        replace(BANK, buffer_bytes=Unadmitted())
