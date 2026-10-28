"""Compiled fixed-capacity admission banks; storage callbacks are qualified separately."""

from __future__ import annotations

import importlib
import os
import random

import pytest

from tests.support.sqlite_vfs_faults import WriteBank, WriteLimits, WriteQuota

try:
    native = importlib.import_module("riverhog_client._prepared_primary_native")
except ImportError:
    if os.name == "nt":
        raise
    pytest.skip(
        "compiled Windows accounting is exercised by the native Windows gate",
        allow_module_level=True,
    )


def grant(**changes):
    values = {
        "page_bytes": 4096,
        "database_pages": 4,
        "database_end": 32 * 4096,
        "journal_end": 32 * 4096,
        "write_bytes": 64 * 4096,
        "calls": 64,
    }
    values.update(changes)
    return tuple(values.values())


@pytest.mark.parametrize("field", range(6))
@pytest.mark.parametrize("value", [0, -1, True, 1 << 63])
def test_native_grants_reject_invalid_values_before_allocation(field, value):
    values = list(grant())
    values[field] = value
    with pytest.raises((TypeError, ValueError, OverflowError)):
        native.WriteQuota(tuple(values), grant())


def test_native_grants_reject_invalid_geometry_and_unaddressable_tracking():
    with pytest.raises(ValueError, match="power of two"):
        native.WriteQuota(grant(page_bytes=3000), grant())
    with pytest.raises(OverflowError, match="tracking"):
        native.WriteQuota(grant(database_pages=1 << 62), grant())
    with pytest.raises(TypeError):
        native.WriteQuota(list(grant()), grant())
    with pytest.raises(TypeError):
        native.WriteQuota(grant()[:-1], grant())


@pytest.mark.parametrize(
    "changes,kind,amount,offset,expected,reason",
    [
        ({"database_pages": 2}, "database", 2, 4095, True, None),
        ({"database_pages": 1}, "database", 2, 4095, False, "database page grant"),
        ({"database_end": 4096}, "database", 1, 4095, True, None),
        ({"database_end": 4096}, "database", 2, 4095, False, "database extent grant"),
        ({"journal_end": 4096}, "journal", 1, 4095, True, None),
        ({"journal_end": 4096}, "journal", 2, 4095, False, "journal extent grant"),
        ({"write_bytes": 8}, "database", 8, 0, True, None),
        ({"write_bytes": 8}, "database", 9, 0, False, "write byte grant"),
        ({}, "database", 2, (1 << 63) - 2, False, "invalid write range"),
        ({}, "journal", -1, 0, False, "invalid write range"),
        ({}, "database", 1, -1, False, "invalid write range"),
        ({}, "other", 0, (1 << 63) - 1, False, "unadmitted file"),
    ],
)
def test_native_whole_attempt_is_admitted_or_refused_without_partial_charge(
    changes, kind, amount, offset, expected, reason
):
    quota = native.WriteQuota(grant(**changes), grant())
    assert quota.admit(kind, amount, offset) is expected
    report = quota.report()
    assert report["recovering"] is (not expected)
    assert report["forward"]["write_calls"] == int(expected)
    assert report["forward"]["write_bytes"] == (amount if expected else 0)
    if not expected:
        assert report["first_refusal"]["reason"] == reason
        assert report["first_refusal"]["bank"] == "forward"
        assert report["forward"]["database_pages_attempted"] == 0


def test_native_rewrites_and_zero_writes_spend_calls_without_manufacturing_extents():
    quota = native.WriteQuota(grant(database_pages=1, calls=4), grant())
    assert quota.admit("database", 3, 0)
    assert quota.admit("database", 3, 1)
    assert quota.admit("journal", 0, (1 << 63) - 1)
    assert quota.admit("database", 0, (1 << 63) - 1)
    assert not quota.admit("journal", 0, 0)
    report = quota.report()
    assert report["forward"] == {
        "database_pages_attempted": 1,
        "database_write_bytes": 6,
        "journal_write_bytes": 0,
        "journal_write_extent_bytes": 0,
        "write_bytes": 6,
        "write_calls": 4,
    }


def test_native_page_refusal_undoes_only_unsubmitted_tracking_and_latches_restoration():
    quota = native.WriteQuota(grant(database_pages=2), grant(database_pages=5))
    assert quota.admit("database", 1, 0)
    # One already charged page plus two new pages: none of this range may be delegated.
    assert not quota.admit("database", 3 * 4096, 0)
    report = quota.report()
    assert report["forward"]["database_pages_attempted"] == 1
    assert report["forward"]["write_bytes"] == 1
    assert report["forward"]["write_calls"] == 1
    assert report["recovery"]["write_calls"] == 0
    assert quota.admit("database", 5 * 4096, 0)
    assert quota.report()["recovery"]["database_pages_attempted"] == 5


def test_native_failed_delegation_keeps_full_charge_and_separate_recovery_capacity():
    quota = native.WriteQuota(grant(database_pages=1), grant(database_pages=5))
    assert quota.admit("database", 4096, 0)
    quota.failed()
    assert quota.recovering
    assert quota.admit("database", 5 * 4096, 0)
    report = quota.report()
    assert report["native_failures"] == 1
    assert report["forward"]["database_pages_attempted"] == 1
    assert report["forward"]["write_bytes"] == 4096
    assert report["recovery"]["database_pages_attempted"] == 5
    assert report["refusals"] == 0


def test_native_recovery_refusal_never_rearms_or_erases_the_first_refusal():
    quota = native.WriteQuota(grant(database_pages=1), grant(database_pages=1))
    assert not quota.admit("database", 8192, 0)
    first = quota.report()["first_refusal"]
    assert quota.admit("database", 1, 0)
    assert not quota.admit("database", 1, 4096)
    report = quota.report()
    assert report["refusals"] == 2 and report["first_refusal"] == first
    assert report["recovery"]["database_pages_attempted"] == 1
    with pytest.raises(RuntimeError, match="reinitialized"):
        quota.__init__(grant(), grant())


def test_native_tracking_is_preallocated_readonly_and_independent():
    quota = native.WriteQuota(grant(database_pages=1), grant(database_pages=1))
    assert quota.tracking_bytes == 48
    initial = quota.tracking_bytes
    for _ in range(32):
        assert quota.admit("database", 1, 0)
    quota.failed()
    assert quota.admit("database", 1, 4096)
    assert quota.report()["recovery"]["database_pages_attempted"] == 1
    assert quota.tracking_bytes == initial
    with pytest.raises(AttributeError):
        quota.tracking_bytes = 0


@pytest.mark.parametrize(
    "method,arguments", [("report", ()), ("failed", ()), ("admit", ("database", 1, 0))]
)
def test_native_uninitialized_object_cannot_access_storage_or_report_success(method, arguments):
    quota = native.WriteQuota.__new__(native.WriteQuota)
    with pytest.raises(RuntimeError, match="not initialized"):
        getattr(quota, method)(*arguments)


@pytest.mark.parametrize("seed", range(16))
def test_compiled_prefixes_match_the_independent_python_bank(seed):
    rng = random.Random(seed)
    forward = grant(database_pages=8, calls=40)
    recovery = grant(database_pages=16, calls=80)
    quota = native.WriteQuota(forward, recovery)
    model = WriteQuota(WriteBank(WriteLimits(*forward)), WriteBank(WriteLimits(*recovery)))
    tracking = quota.tracking_bytes
    for step in range(160):
        kind = rng.choice(["database", "journal", "database", "database"])
        amount = rng.choice([0, 1, 7, 4095, 4096, 4097])
        offset = rng.randrange(24) * 4096 + rng.choice([0, 1, 4095])
        admitted = quota.admit(kind, amount, offset)
        assert admitted is model.admit(kind, amount, offset)
        if admitted and step % 17 == 0:
            quota.failed()
            model.failed()
        actual = quota.report()
        expected = model.report()
        for field in ["forward", "recovery", "recovering", "refusals", "native_failures"]:
            assert actual[field] == expected[field], (seed, step, field, actual, expected)
        assert quota.tracking_bytes == tracking


@pytest.mark.parametrize("limits", [(0, 1), (1, 0), (-1, 2), (True, 2), (2, 1 << 63), [1, 2], (1,)])
@pytest.mark.parametrize("bank", ["forward", "recovery"])
def test_native_read_grants_reject_invalid_values(limits, bank):
    writes = native.WriteQuota(grant(), grant())
    values = {"forward": (1024, 16), "recovery": (8192, 32)}
    values[bank] = limits
    with pytest.raises((TypeError, ValueError, OverflowError)):
        native.ReadQuota(writes, **values)
    assert not writes.recovering


@pytest.mark.parametrize(
    "kind,amount,offset,allowed,reason",
    [
        ("database", 8, 0, True, None),
        ("journal", 8, 4096, True, None),
        ("database", 9, 0, False, "read byte grant"),
        ("database", -1, 0, False, "invalid read range"),
        ("database", 1, -1, False, "invalid read range"),
        ("journal", 2, (1 << 63) - 2, False, "invalid read range"),
        ("database", 0, (1 << 63) - 1, True, None),
        ("other", 1, 0, False, "unadmitted file"),
    ],
)
def test_native_read_whole_attempt_and_recovery_latch(kind, amount, offset, allowed, reason):
    writes = native.WriteQuota(grant(), grant())
    reads = native.ReadQuota(writes, (8, 2), (16, 4))
    assert reads.admit(kind, amount, offset) is allowed
    assert reads.report()["forward"] == {
        "read_bytes": amount if allowed else 0,
        "read_calls": int(allowed),
    }
    assert writes.recovering is (not allowed)
    if not allowed:
        assert writes.report()["first_refusal"]["reason"] == reason
        assert writes.report()["first_refusal"]["bank"] == "forward"
        assert reads.admit("journal", 16, 0)
        assert reads.report()["recovery"] == {"read_bytes": 16, "read_calls": 1}
        assert writes.admit("database", 1, 0)
        assert writes.report()["recovery"]["write_calls"] == 1


def test_native_read_call_refusal_and_write_failure_share_reserved_bank():
    writes = native.WriteQuota(grant(), grant())
    reads = native.ReadQuota(writes, (16, 2), (32, 2))
    assert reads.admit("database", 0, (1 << 63) - 1)
    assert reads.admit("database", 8, 0)
    assert not reads.admit("database", 0, 0)
    assert writes.report()["first_refusal"]["reason"] == "read call grant"
    assert reads.report()["forward"] == {"read_bytes": 8, "read_calls": 2}
    assert reads.admit("journal", 16, 0)
    assert not reads.admit("journal", 17, 0)
    assert reads.report()["recovery"] == {"read_bytes": 16, "read_calls": 1}
    assert writes.report()["first_refusal"]["reason"] == "read call grant"
    assert writes.report()["refusals"] == 2
    assert writes.report()["native_failures"] == 0

    other_writes = native.WriteQuota(grant(), grant())
    other_reads = native.ReadQuota(other_writes, (1, 1), (32, 2))
    # Retain the complete admitted read if the native delegate fails.
    assert other_reads.admit("database", 1, 0)
    other_writes.failed()
    assert other_reads.admit("journal", 32, 0)
    assert other_reads.report()["forward"] == {"read_bytes": 1, "read_calls": 1}
    assert other_reads.report()["recovery"] == {"read_bytes": 32, "read_calls": 1}
    assert other_writes.report()["native_failures"] == 1

    refused_writes = native.WriteQuota(grant(database_pages=1), grant())
    reserved_reads = native.ReadQuota(refused_writes, (1, 1), (16, 2))
    assert not refused_writes.admit("database", 8192, 0)
    assert reserved_reads.admit("journal", 16, 0)
    assert reserved_reads.report()["forward"] == {"read_bytes": 0, "read_calls": 0}
    assert reserved_reads.report()["recovery"] == {"read_bytes": 16, "read_calls": 1}


def test_native_read_bank_cannot_reset_or_use_uninitialized_objects():
    writes = native.WriteQuota(grant(), grant())
    reads = native.ReadQuota(writes, (8, 2), (16, 4))
    with pytest.raises(RuntimeError, match="reinitialized"):
        reads.__init__(writes, (8, 2), (16, 4))
    uninitialized = native.ReadQuota.__new__(native.ReadQuota)
    with pytest.raises(RuntimeError, match="not initialized"):
        uninitialized.report()
    with pytest.raises(RuntimeError, match="not initialized"):
        uninitialized.admit("database", 1, 0)
    with pytest.raises(RuntimeError, match="not initialized"):
        native.ReadQuota(native.WriteQuota.__new__(native.WriteQuota), (8, 2), (16, 4))
    with pytest.raises((TypeError, OverflowError)):
        reads.admit("database", True, 0)
    with pytest.raises(OverflowError):
        reads.admit("database", 1 << 63, 0)
    assert not writes.recovering


@pytest.mark.parametrize(
    "kind,size,allowed,reason",
    [
        ("database", 8192, True, None),
        ("database", 8193, False, "database extent grant"),
        ("journal", 4096, True, None),
        ("journal", 4097, False, "journal extent grant"),
        ("database", 0, True, None),
        ("journal", -1, False, "invalid resize extent"),
        ("database", (1 << 63) - 1, False, "database extent grant"),
        ("other", 0, False, "unadmitted file"),
    ],
)
def test_native_resize_prefix_enforces_absolute_extent_before_delegation(
    kind, size, allowed, reason
):
    quota = native.WriteQuota(grant(database_end=8192, journal_end=4096), grant())
    assert quota.resize(kind, size) is allowed
    report = quota.resize_report()
    assert report["forward"]["resize_calls"] == int(allowed)
    assert report["forward"]["database_resize_extent"] == (
        size if allowed and kind == "database" else 0
    )
    assert report["forward"]["journal_resize_extent"] == (
        size if allowed and kind == "journal" else 0
    )
    assert quota.report()["forward"]["write_calls"] == 0
    if not allowed:
        assert quota.recovering
        assert quota.report()["first_refusal"]["reason"] == reason
        assert quota.resize("database", 8192)
        assert quota.resize_report()["recovery"]["database_resize_extent"] == 8192


def test_native_resize_calls_shrink_failure_and_recovery_have_independent_capacity():
    quota = native.WriteQuota(grant(calls=2), grant(calls=2))
    assert quota.resize("database", 8192)
    assert quota.resize("database", 0)
    assert not quota.resize("database", 0)
    first = quota.report()["first_refusal"]
    assert first["reason"] == "resize call grant"
    assert quota.resize("database", 4096)
    quota.failed()
    assert quota.resize("journal", 1024)
    assert not quota.resize("journal", 0)
    assert quota.report()["first_refusal"] == first
    assert quota.resize_report()["forward"]["resize_calls"] == 2
    assert quota.resize_report()["recovery"]["resize_calls"] == 2
    assert quota.report()["native_failures"] == 1
    with pytest.raises(TypeError):
        quota.resize("database", True)
    with pytest.raises(OverflowError):
        quota.resize("database", 1 << 63)
    uninitialized = native.WriteQuota.__new__(native.WriteQuota)
    with pytest.raises(RuntimeError, match="not initialized"):
        uninitialized.resize_report()
    with pytest.raises(RuntimeError, match="not initialized"):
        uninitialized.resize("database", 1)


@pytest.mark.parametrize("value", [0, -1, True, 1 << 63, "4"])
@pytest.mark.parametrize("bank", ["forward", "recovery"])
def test_native_callback_limits_are_explicit_positive_signed_integers(value, bank):
    writes = native.WriteQuota(grant(), grant())
    limits = {"forward": 16, "recovery": 32}
    limits[bank] = value
    with pytest.raises((TypeError, ValueError, OverflowError)):
        native.CallbackQuota(writes, **limits)


def test_native_callback_close_reservation_survives_exhaustion_and_failure():
    writes = native.WriteQuota(grant(), grant())
    calls = native.CallbackQuota(writes, 2, 2)
    assert calls.opened() is True
    assert calls.report()["forward"]["callback_slots_charged"] == 2
    assert calls.report()["reserved_recovery_closes"] == 1
    assert not calls.admit()
    assert writes.report()["first_refusal"]["reason"] == "VFS callback grant"
    assert calls.admit()
    assert not calls.admit()
    assert calls.report()["recovery"]["callback_slots_charged"] == 1
    calls.closed(True)
    assert calls.report()["recovery"]["callback_slots_charged"] == 2
    assert calls.report()["reserved_recovery_closes"] == 0
    with pytest.raises(ValueError, match="reserved"):
        calls.closed(True)
    assert calls.report()["recovery"]["callback_slots_charged"] == 2


def test_native_callback_abandoned_open_keeps_its_charge_and_releases_only_close_reserve():
    writes = native.WriteQuota(grant(), grant())
    calls = native.CallbackQuota(writes, 4, 1)
    assert calls.opened() is True
    calls.abandoned(True)
    assert calls.report()["forward"]["callback_slots_charged"] == 2
    assert calls.opened() is True
    assert calls.opened() is None
    assert writes.report()["first_refusal"]["reason"] == "recovery file-close grant"
    calls.closed(True)
    assert calls.report()["recovery"]["callback_slots_charged"] == 1


def test_native_callback_infallible_refusal_must_return_error_before_recovery_delegation():
    writes = native.WriteQuota(grant(), grant())
    calls = native.CallbackQuota(writes, 1, 4)
    assert calls.admit()
    assert not calls.admit(False)
    assert writes.recovering and calls.report()["pending_error"]
    assert not calls.admit(False)
    assert calls.report()["recovery"]["callback_slots_charged"] == 0
    assert not calls.admit(True)
    assert not calls.report()["pending_error"]
    assert calls.report()["recovery"]["callback_slots_charged"] == 0
    assert calls.admit()
    assert calls.report()["recovery"]["callback_slots_charged"] == 1


def test_native_callback_recovery_open_prepays_close_and_never_refunds_failed_native_work():
    writes = native.WriteQuota(grant(), grant())
    calls = native.CallbackQuota(writes, 4, 2)
    writes.failed()
    assert calls.opened() is False
    assert calls.report()["recovery"]["callback_slots_charged"] == 2
    assert not calls.admit()
    calls.closed(False)
    assert calls.report()["recovery"]["callback_slots_charged"] == 2
    assert calls.report()["prepaid_recovery_closes"] == 0


def test_native_callback_banks_are_sealed_and_reject_uninitialized_authority():
    writes = native.WriteQuota(grant(), grant())
    calls = native.CallbackQuota(writes, 4, 4)
    with pytest.raises(RuntimeError, match="reinitialized"):
        calls.__init__(writes, 4, 4)
    uninitialized = native.CallbackQuota.__new__(native.CallbackQuota)
    for method in (uninitialized.report, uninitialized.admit, uninitialized.opened):
        with pytest.raises(RuntimeError, match="not initialized"):
            method()
    with pytest.raises(RuntimeError, match="not initialized"):
        native.CallbackQuota(native.WriteQuota.__new__(native.WriteQuota), 4, 4)
    with pytest.raises(TypeError):
        calls.admit(1)


@pytest.mark.parametrize("seed", range(16))
def test_native_callback_prefixes_match_independent_reservation_model(seed):
    import random

    rng = random.Random(seed)
    writes = native.WriteQuota(grant(), grant())
    calls = native.CallbackQuota(writes, 17, 23)
    charged = [0, 0]
    files = []
    recovering = pending = False
    for _step in range(96):
        event = rng.choice(["call", "open", "close", "abandon", "failure"])
        if event == "failure":
            writes.failed()
            recovering = True
        elif event in ("close", "abandon"):
            if not files:
                continue
            bank = files.pop(rng.randrange(len(files)))
            if event == "close":
                calls.closed(bank)
                if bank and recovering:
                    charged[1] += 1
            else:
                calls.abandoned(bank)
        elif event == "open":
            if pending:
                expected = None
                pending = False
            else:
                available = [17 - charged[0], 23 - charged[1] - files.count(True)]
                if available[int(recovering)] < 2 or (not recovering and available[1] < 1):
                    expected = None
                    recovering = True
                else:
                    expected = not recovering
                    charged[int(recovering)] += 2
                    files.append(expected)
            assert calls.opened() is expected
        else:
            can_error = rng.choice([False, True])
            if pending:
                expected = False
                pending = not can_error
            else:
                room = (23 - charged[1] - files.count(True)) if recovering else (17 - charged[0])
                expected = room > 0
                if expected:
                    charged[int(recovering)] += 1
                else:
                    recovering = True
                    pending = not can_error
            assert calls.admit(can_error) is expected
        report = calls.report()
        assert writes.recovering is recovering
        assert report["pending_error"] is pending
        assert report["forward"]["callback_slots_charged"] == charged[0]
        assert report["recovery"]["callback_slots_charged"] == charged[1]
        assert report["reserved_recovery_closes"] == files.count(True)
        assert report["prepaid_recovery_closes"] == files.count(False)
        assert charged[0] <= 17 and charged[1] + files.count(True) <= 23


@pytest.mark.parametrize("value", [0, -1, True, 1 << 63, "4"])
def test_native_process_heap_grant_rejects_invalid_values_before_runtime_admission(value):
    with pytest.raises((TypeError, ValueError, OverflowError)):
        native.SqliteProcessBudget("not-used", value)


def test_native_process_heap_owner_rejects_uninitialized_observation():
    owner = native.SqliteProcessBudget.__new__(native.SqliteProcessBudget)
    with pytest.raises(RuntimeError, match="not initialized"):
        owner.report()
    with pytest.raises(RuntimeError, match="not initialized"):
        owner.increase(1024)
