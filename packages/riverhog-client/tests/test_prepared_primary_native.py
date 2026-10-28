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
    limits = {"forward": (16, 0), "recovery": (32, 0)}
    limits[bank] = (value, 0)
    with pytest.raises((TypeError, ValueError, OverflowError)):
        native.CallbackQuota(
            writes,
            **limits,
            work=native.NativeWorkQuota(writes, (0, 0), (0, 0)),
            buffers=(0, 0),
        )


def test_native_callback_close_reservation_survives_exhaustion_and_failure():
    writes = native.WriteQuota(grant(), grant())
    calls = native.CallbackQuota(
        writes,
        (2, 0),
        (2, 0),
        work=native.NativeWorkQuota(writes, (0, 0), (0, 0)),
        buffers=(0, 0),
    )
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
    calls = native.CallbackQuota(
        writes,
        (4, 0),
        (1, 0),
        work=native.NativeWorkQuota(writes, (0, 0), (0, 0)),
        buffers=(0, 0),
    )
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
    calls = native.CallbackQuota(
        writes,
        (1, 0),
        (4, 0),
        work=native.NativeWorkQuota(writes, (0, 0), (0, 0)),
        buffers=(0, 0),
    )
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
    calls = native.CallbackQuota(
        writes,
        (4, 0),
        (2, 0),
        work=native.NativeWorkQuota(writes, (0, 0), (0, 0)),
        buffers=(0, 0),
    )
    writes.failed()
    assert calls.opened() is False
    assert calls.report()["recovery"]["callback_slots_charged"] == 2
    assert not calls.admit()
    calls.closed(False)
    assert calls.report()["recovery"]["callback_slots_charged"] == 2
    assert calls.report()["prepaid_recovery_closes"] == 0


def test_native_callback_banks_are_sealed_and_reject_uninitialized_authority():
    writes = native.WriteQuota(grant(), grant())
    calls = native.CallbackQuota(
        writes,
        (4, 0),
        (4, 0),
        work=native.NativeWorkQuota(writes, (0, 0), (0, 0)),
        buffers=(0, 0),
    )
    with pytest.raises(RuntimeError, match="reinitialized"):
        calls.__init__(writes, (4, 0), (4, 0))
    uninitialized = native.CallbackQuota.__new__(native.CallbackQuota)
    for method in (
        uninitialized.report,
        uninitialized.admit,
        uninitialized.opened,
        lambda: uninitialized.admit_sleep(0),
    ):
        with pytest.raises(RuntimeError, match="not initialized"):
            method()
    with pytest.raises(RuntimeError, match="not initialized"):
        native.CallbackQuota(
            native.WriteQuota.__new__(native.WriteQuota),
            (4, 0),
            (4, 0),
            work=native.NativeWorkQuota(
                native.WriteQuota.__new__(native.WriteQuota), (0, 0), (0, 0)
            ),
            buffers=(0, 0),
        )
    with pytest.raises(TypeError):
        calls.admit(1)


@pytest.mark.parametrize("seed", range(16))
def test_native_callback_prefixes_match_independent_reservation_model(seed):
    import random

    rng = random.Random(seed)
    writes = native.WriteQuota(grant(), grant())
    calls = native.CallbackQuota(
        writes,
        (17, 0),
        (23, 0),
        work=native.NativeWorkQuota(writes, (0, 0), (0, 0)),
        buffers=(0, 0),
    )
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
        native.SqliteProcessBudget("not-used", value, (128, 512))


def test_native_process_heap_owner_rejects_uninitialized_observation():
    owner = native.SqliteProcessBudget.__new__(native.SqliteProcessBudget)
    with pytest.raises(RuntimeError, match="not initialized"):
        owner.report()
    with pytest.raises(RuntimeError, match="not initialized"):
        owner.increase(1024)


def test_native_uninitialized_vfs_cannot_report_successful_teardown():
    owner = native.WriteVfs.__new__(native.WriteVfs)
    with pytest.raises(RuntimeError, match="uninitialized"):
        _ = owner.quarantined


@pytest.mark.parametrize("bank", ["forward", "recovery"])
@pytest.mark.parametrize("value", [-1, True, 1 << 63, "4", None])
def test_native_callback_sleep_grants_are_explicit_nonnegative_signed_integers(bank, value):
    writes = native.WriteQuota(grant(), grant())
    limits = {"forward": (16, 0), "recovery": (32, 0)}
    limits[bank] = (16, value)
    with pytest.raises((TypeError, ValueError, OverflowError)):
        native.CallbackQuota(
            writes,
            **limits,
            work=native.NativeWorkQuota(writes, (0, 0), (0, 0)),
            buffers=(0, 0),
        )


@pytest.mark.parametrize("bank", ["forward", "recovery"])
@pytest.mark.parametrize("value", [16, [16, 0], (16,), (16, 0, 0), None])
def test_native_callback_grants_require_exact_complete_tuples(bank, value):
    writes = native.WriteQuota(grant(), grant())
    limits = {"forward": (16, 0), "recovery": (32, 0)}
    limits[bank] = value
    with pytest.raises(TypeError, match="tuple"):
        native.CallbackQuota(
            writes,
            **limits,
            work=native.NativeWorkQuota(writes, (0, 0), (0, 0)),
            buffers=(0, 0),
        )


@pytest.mark.parametrize("requested", [0, 1, 999, 1000, 1001, 2**31 - 1 - 999])
def test_native_sleep_precharges_whole_rounded_delay_and_independent_recovery(requested):
    writes = native.WriteQuota(grant(), grant())
    rounded = ((requested + 999) // 1000) * 1000
    calls = native.CallbackQuota(
        writes,
        (8, rounded),
        (8, rounded),
        work=native.NativeWorkQuota(writes, (0, 0), (0, 0)),
        buffers=(0, 0),
    )
    assert calls.admit_sleep(requested)
    assert calls.report()["forward"]["sleep_microseconds_charged"] == rounded
    assert not writes.recovering
    assert not calls.admit_sleep(1)
    assert writes.recovering and calls.report()["pending_error"]
    assert writes.report()["first_refusal"]["reason"] == "VFS sleep grant"
    assert calls.report()["forward"]["sleep_microseconds_charged"] == rounded
    assert not calls.admit_sleep(requested)
    assert calls.report()["recovery"]["sleep_microseconds_charged"] == 0
    assert not calls.admit()
    assert calls.admit_sleep(requested)
    assert calls.report()["recovery"]["sleep_microseconds_charged"] == rounded


@pytest.mark.parametrize("requested", [-1, 2**31 - 999, 2**31 - 1, 2**31, 2**63 - 1])
def test_native_sleep_rejects_invalid_native_arithmetic_before_charging_duration(requested):
    writes = native.WriteQuota(grant(), grant())
    calls = native.CallbackQuota(
        writes,
        (8, 2**63 - 1),
        (8, 2**63 - 1),
        work=native.NativeWorkQuota(writes, (0, 0), (0, 0)),
        buffers=(0, 0),
    )
    assert not calls.admit_sleep(requested)
    assert writes.recovering and calls.report()["pending_error"]
    assert writes.report()["first_refusal"]["reason"] == "invalid VFS sleep range"
    assert calls.report()["forward"]["sleep_microseconds_charged"] == 0


@pytest.mark.parametrize("requested", [True, "1", 1.0, 2**63])
def test_native_sleep_rejects_nonexact_or_unrepresentable_input_without_mutating_grants(requested):
    writes = native.WriteQuota(grant(), grant())
    calls = native.CallbackQuota(
        writes,
        (8, 1000),
        (8, 1000),
        work=native.NativeWorkQuota(writes, (0, 0), (0, 0)),
        buffers=(0, 0),
    )
    before = calls.report()
    with pytest.raises((TypeError, OverflowError)):
        calls.admit_sleep(requested)
    assert calls.report() == before and not writes.recovering


def test_native_sleep_refusal_preserves_independent_file_close_capacity():
    writes = native.WriteQuota(grant(), grant())
    calls = native.CallbackQuota(
        writes,
        (4, 0),
        (4, 1000),
        work=native.NativeWorkQuota(writes, (0, 0), (0, 0)),
        buffers=(0, 0),
    )
    assert calls.opened() is True
    assert not calls.admit_sleep(1)
    assert calls.report()["reserved_recovery_closes"] == 1
    assert not calls.admit()
    assert calls.admit_sleep(1)
    calls.closed(True)
    assert calls.report()["reserved_recovery_closes"] == 0
    assert calls.report()["recovery"]["sleep_microseconds_charged"] == 1000


@pytest.mark.parametrize("seed", range(8))
def test_native_sleep_prefixes_match_independent_rounded_wait_model(seed):
    rng = random.Random(seed)
    writes = native.WriteQuota(grant(), grant())
    slots, duration = [0, 0], [0, 0]
    capacities, allowances = (17, 23), (3000, 5000)
    calls = native.CallbackQuota(
        writes,
        (17, 3000),
        (23, 5000),
        work=native.NativeWorkQuota(writes, (0, 0), (0, 0)),
        buffers=(0, 0),
    )
    recovering = pending = False
    for _ in range(96):
        action = rng.choice(["sleep", "fallible", "value", "failure"])
        if action == "failure":
            writes.failed()
            recovering = True
        else:
            index = int(recovering)
            can_error = action == "fallible"
            requested = rng.choice([-1, 0, 1, 999, 1000, 1001, 2048, 2**31 - 1])
            expected = False
            if pending:
                pending = not can_error
            elif slots[index] == capacities[index]:
                recovering, pending = True, not can_error
            else:
                slots[index] += 1
                expected = True
                if action == "sleep":
                    cost = ((requested + 999) // 1000) * 1000
                    if requested < 0 or requested > 2**31 - 1 - 999:
                        expected = False
                    elif cost > allowances[index] - duration[index]:
                        expected = False
                    else:
                        duration[index] += cost
                    if not expected:
                        recovering = pending = True
            actual = calls.admit_sleep(requested) if action == "sleep" else calls.admit(can_error)
            assert actual is expected
        report = calls.report()
        assert writes.recovering is recovering
        assert report["pending_error"] is pending
        for index, bank in enumerate(("forward", "recovery")):
            assert report[bank]["callback_slots_charged"] == slots[index]
            assert report[bank]["sleep_microseconds_charged"] == duration[index]
            assert slots[index] <= capacities[index] and duration[index] <= allowances[index]


@pytest.mark.parametrize(
    "count,delay",
    [(1, 1), (2, 3), (10, 25), (46340, 1), (1, 1073741823), (2000, 500)],
)
def test_native_retry_bootstrap_recipe_prepays_complete_open_identity_and_close(count, delay):
    calls, microseconds = native.retry_bootstrap_cost((count, delay))
    directory_attributes = 2 * (count + 1)
    nested_attributes = sum(count + 1 for _ in range(count + 1))
    create_attempts = 2 * (count + 1)
    assert calls == directory_attributes + nested_attributes + create_attempts + 3 + 3
    one_loop = sum(delay * step for step in range(1, count + 1))
    assert microseconds == (2 * one_loop + (count + 1) * one_loop + 2 * one_loop + 200) * 1000


@pytest.mark.parametrize(
    "profile",
    [
        (0, 25),
        (10, 0),
        (-1, 25),
        (10, -1),
        (2**31 - 1, 1),
        (2**31 - 2, 1),
        (46341, 1),
        (46340, 2),
        (1, 1073741824),
        (10, (2**31 - 1) // 110 + 1),
        (2**31, 1),
        (1, 2**31),
        (2**63 - 1, 2**63 - 1),
    ],
)
def test_native_retry_profile_rejects_signed_native_intermediate_overflow(profile):
    with pytest.raises(ValueError):
        native.retry_bootstrap_cost(profile)


@pytest.mark.parametrize(
    "profile",
    [
        (),
        (10,),
        (10, 25, 1),
        [10, 25],
        None,
        (True, 25),
        (10, False),
        (1.0, 25),
        (10, "25"),
        (2**63, 1),
        (1, 2**63),
    ],
)
def test_native_retry_recipe_requires_an_exact_signed_integer_pair(profile):
    with pytest.raises((TypeError, OverflowError)):
        native.retry_bootstrap_cost(profile)


def test_native_retry_bootstrap_recipe_matches_independent_bounded_loop_model():
    randomizer = random.Random(991823)
    for _ in range(512):
        count = randomizer.randint(1, 1000)
        delay = randomizer.randint(1, (2**31 - 1) // (count * (count + 1)))
        calls, microseconds = native.retry_bootstrap_cost((count, delay))
        loop_wait = sum(delay * step for step in range(1, count + 1))
        expected_calls = expected_wait = 0
        for readonly in (False, True):
            expected_calls += count + 1  # winIsDir
            expected_wait += loop_wait
            expected_calls += count + 1  # CreateFile
            expected_wait += loop_wait
            if not readonly:
                for _ in range(count + 1):
                    expected_calls += count + 1  # nested winAccess
                    expected_wait += loop_wait
        assert (calls, microseconds) == (expected_calls + 6, (expected_wait + 200) * 1000)


def native_work_snapshot(calls=0, sleep=0, close_calls=0, close_sleep=0):
    return {
        "filesystem_api_attempts_charged": calls,
        "internal_sleep_microseconds_charged": sleep,
        "close_filesystem_api_attempts_prepaid": close_calls,
        "close_internal_sleep_microseconds_prepaid": close_sleep,
    }


@pytest.mark.parametrize(
    "limits",
    [
        True,
        1,
        [1, 2],
        (),
        (1,),
        (1, 2, 3),
        (True, 1),
        (1, False),
        (-1, 1),
        (1, -1),
        (1.0, 1),
        (1, "2"),
        (2**63, 0),
        (0, 2**63),
    ],
)
@pytest.mark.parametrize("bank", ["forward", "recovery"])
def test_native_work_grants_reject_nonexact_or_unrepresentable_values(limits, bank):
    writes = native.WriteQuota(grant(), grant())
    values = {"forward": (32, 200000), "recovery": (16, 200000)}
    values[bank] = limits
    with pytest.raises((TypeError, ValueError, OverflowError)):
        native.NativeWorkQuota(writes, **values)
    assert writes.report()["refusals"] == 0


@pytest.mark.parametrize(
    "cost,expected,reason",
    [
        ((4, 10), True, None),
        ((5, 0), False, "native filesystem attempt grant"),
        ((0, 11), False, "native internal sleep grant"),
        ((5, 11), False, "native filesystem attempt grant"),
    ],
)
def test_native_work_whole_cost_is_precharged_or_refused_without_partial_debit(
    cost, expected, reason
):
    writes = native.WriteQuota(grant(), grant())
    work = native.NativeWorkQuota(writes, (4, 10), (8, 20))
    assert work.admit(cost) is expected
    assert work.report()["forward"] == native_work_snapshot(*(cost if expected else (0, 0)))
    assert work.report()["recovery"] == native_work_snapshot()
    assert writes.recovering is (not expected)
    if reason:
        assert writes.report()["first_refusal"]["reason"] == reason
        assert writes.report()["first_refusal"]["bank"] == "forward"
        assert work.admit((8, 20))
        assert work.report()["recovery"] == native_work_snapshot(8, 20)


@pytest.mark.parametrize(
    "forward,recovery,reason",
    [
        ((6, 300000), (3, 200000), "native filesystem attempt grant"),
        ((7, 299999), (3, 200000), "native internal sleep grant"),
        ((7, 300000), (2, 200000), "native filesystem attempt grant"),
        ((7, 300000), (3, 199999), "native internal sleep grant"),
    ],
)
def test_native_acquisition_prepays_both_complete_close_paths_atomically(forward, recovery, reason):
    writes = native.WriteQuota(grant(), grant())
    work = native.NativeWorkQuota(writes, forward, recovery)
    assert work.acquire((4, 100000), (3, 200000)) is None
    assert work.report() == {"forward": native_work_snapshot(), "recovery": native_work_snapshot()}
    report = writes.report()
    assert report["recovering"] and report["refusals"] == 1
    assert report["first_refusal"]["reason"] == reason


def test_native_work_prepaid_close_cannot_be_spent_by_ordinary_recovery():
    writes = native.WriteQuota(grant(), grant())
    work = native.NativeWorkQuota(writes, (7, 300000), (5, 200001))
    credit = work.acquire((4, 100000), (3, 200000))
    assert credit.report() == {
        "filesystem_api_attempts_prepaid": 3,
        "internal_sleep_microseconds_prepaid": 200000,
        "pending": True,
    }
    assert work.report() == {
        "forward": native_work_snapshot(7, 300000, 3, 200000),
        "recovery": native_work_snapshot(3, 200000, 3, 200000),
    }
    writes.failed()
    assert work.admit((2, 1))
    assert not work.admit((1, 0))
    before = work.report()
    credit.consume()
    assert not credit.report()["pending"]
    assert work.report() == before
    with pytest.raises(ValueError, match="already consumed"):
        credit.consume()
    assert work.report() == before
    # A second acquisition cannot borrow the already prepaid close path.
    assert work.acquire((0, 0), (1, 0)) is None


def test_native_recovery_acquisition_pays_its_close_once_and_never_touches_forward():
    writes = native.WriteQuota(grant(), grant())
    work = native.NativeWorkQuota(writes, (0, 0), (7, 300000))
    writes.failed()
    credit = work.acquire((4, 100000), (3, 200000))
    assert credit is not None
    assert work.report() == {
        "forward": native_work_snapshot(),
        "recovery": native_work_snapshot(7, 300000, 3, 200000),
    }
    credit.consume()
    assert not work.admit((1, 0))
    assert writes.report()["first_refusal"]["bank"] == "recovery"


def test_native_close_credit_survives_bank_replacement_without_retaining_old_tracking():
    import sys

    writes = native.WriteQuota(grant(database_pages=65536), grant(database_pages=65536))
    original_refs = sys.getrefcount(writes)
    work = native.NativeWorkQuota(writes, (4, 200000), (3, 200000))
    credit = work.acquire((1, 0), (3, 200000))
    assert credit is not None
    before = work.report()
    assert sys.getrefcount(writes) == original_refs + 1
    del work
    # The detached fixed credit holds neither the old work bank nor its large
    # WriteQuota tracking arrays. A replacement grant cannot revoke prepayment.
    assert sys.getrefcount(writes) == original_refs
    replacement_writes = native.WriteQuota(grant(), grant())
    replacement = native.NativeWorkQuota(replacement_writes, (0, 0), (0, 0))
    del writes
    credit.consume()
    assert replacement.report() == {
        "forward": native_work_snapshot(),
        "recovery": native_work_snapshot(),
    }
    assert before["forward"] == native_work_snapshot(4, 200000, 3, 200000)
    assert before["recovery"] == native_work_snapshot(3, 200000, 3, 200000)


def test_native_failed_or_unused_acquisition_never_refunds_prepaid_work():
    writes = native.WriteQuota(grant(), grant())
    work = native.NativeWorkQuota(writes, (7, 300000), (3, 200000))
    credit = work.acquire((4, 100000), (3, 200000))
    before = work.report()
    # A failed underlying open can leave no native file. Discarding/consuming
    # its unused close record does not refund either complete attempted cost.
    credit.consume()
    del credit
    assert work.report() == before
    writes.failed()
    assert not work.admit((1, 0))
    assert work.report() == before


@pytest.mark.parametrize("method", ["admit", "acquire"])
@pytest.mark.parametrize(
    "cost", [True, [1, 0], (1,), (False, 0), (1, 0.5), (-1, 0), (0, -1), (2**63, 0)]
)
def test_native_work_invalid_cost_does_not_change_or_poison_banks(method, cost):
    writes = native.WriteQuota(grant(), grant())
    work = native.NativeWorkQuota(writes, (32, 200000), (32, 200000))
    before = work.report()
    with pytest.raises((TypeError, ValueError, OverflowError)):
        if method == "admit":
            work.admit(cost)
        else:
            work.acquire(cost, (3, 200000))
    assert work.report() == before
    assert not writes.recovering


@pytest.mark.parametrize("close", [(0, 0), (0, 1), (True, 0), (1, False), (1, -1), (1, 2**63)])
def test_native_close_cost_requires_exact_positive_calls_before_any_bank_change(close):
    writes = native.WriteQuota(grant(), grant())
    work = native.NativeWorkQuota(writes, (32, 200000), (32, 200000))
    before = work.report()
    with pytest.raises((TypeError, ValueError, OverflowError)):
        work.acquire((1, 0), close)
    assert work.report() == before and not writes.recovering


def test_native_work_signed64_edge_costs_do_not_wrap_or_partially_debit():
    maximum = 2**63 - 1
    writes = native.WriteQuota(grant(), grant())
    work = native.NativeWorkQuota(writes, (maximum, maximum), (maximum, maximum))
    assert work.acquire((maximum, maximum), (maximum, maximum)) is None
    assert work.report() == {"forward": native_work_snapshot(), "recovery": native_work_snapshot()}
    credit = work.acquire((0, 0), (maximum, maximum))
    assert credit is not None
    assert work.report()["forward"] == native_work_snapshot()
    assert work.report()["recovery"] == native_work_snapshot(maximum, maximum, maximum, maximum)
    credit.consume()
    assert not work.admit((0, 1))


def test_native_work_and_close_uninitialized_or_reinitialized_objects_have_no_authority():
    writes = native.WriteQuota(grant(), grant())
    work = native.NativeWorkQuota(writes, (0, 0), (0, 0))
    with pytest.raises(RuntimeError, match="reinitialized"):
        work.__init__(writes, (32, 200000), (32, 200000))
    uninitialized = native.NativeWorkQuota.__new__(native.NativeWorkQuota)
    for method, args in (("admit", ((0, 0),)), ("acquire", ((0, 0), (1, 0))), ("report", ())):
        with pytest.raises(RuntimeError, match="not initialized"):
            getattr(uninitialized, method)(*args)
    with pytest.raises(RuntimeError, match="not initialized"):
        native.NativeWorkQuota(native.WriteQuota.__new__(native.WriteQuota), (0, 0), (0, 0))
    with pytest.raises(TypeError, match="successful native work acquisition"):
        native.NativeCloseCredit()
    empty = native.NativeCloseCredit.__new__(native.NativeCloseCredit)
    for method in ("consume", "report"):
        with pytest.raises(RuntimeError, match="not initialized"):
            getattr(empty, method)()


def test_native_close_credit_cannot_move_threads_or_be_forged_or_reset():
    from concurrent.futures import ThreadPoolExecutor

    writes = native.WriteQuota(grant(), grant())
    work = native.NativeWorkQuota(writes, (1, 0), (1, 0))
    credit = work.acquire((0, 0), (1, 0))
    for method in ("consume", "report"):
        with ThreadPoolExecutor(max_workers=1) as pool:
            with pytest.raises(RuntimeError, match="owning interpreter and thread"):
                pool.submit(getattr(credit, method)).result()
    for field, value in (("pending", False), ("cost", (0, 0))):
        with pytest.raises(AttributeError):
            setattr(credit, field, value)
    with pytest.raises(TypeError, match="successful native work acquisition"):
        credit.__init__()
    assert credit.report()["pending"]
    credit.consume()
    with pytest.raises(TypeError, match="successful native work acquisition"):
        credit.__init__()
    assert not credit.report()["pending"]


@pytest.mark.parametrize("seed", range(16))
def test_native_work_prefixes_match_independent_whole_cost_and_close_models(seed):
    rng = random.Random(seed)
    writes = native.WriteQuota(grant(), grant())
    limits = ((160, 720000), (180, 960000))
    work = native.NativeWorkQuota(writes, *limits)
    charged = [[0, 0], [0, 0]]
    prepaid = [[0, 0], [0, 0]]
    recovering = False
    refusals = 0
    credits = []
    first = None
    for step in range(160):
        action = rng.choice(("work", "acquire", "consume", "fail"))
        if action == "fail":
            writes.failed()
            recovering = True
        elif action == "consume" and credits:
            credit = credits.pop(rng.randrange(len(credits)))
            credit.consume()
            assert not credit.report()["pending"]
        elif action in ("work", "acquire"):
            current = int(recovering)
            cost = (rng.randrange(10), rng.randrange(5) * 1000)
            close = (
                (rng.randrange(1, 4), rng.choice((0, 2000, 200000)))
                if action == "acquire"
                else (0, 0)
            )
            debits = [((current), (cost[0] + close[0], cost[1] + close[1]))]
            if action == "acquire" and not recovering:
                debits.append((1, close))
            # Independent arbitrary-precision model; no native formula helpers.
            reason = None
            for bank, debit in debits:
                if charged[bank][0] + debit[0] > limits[bank][0]:
                    reason = "native filesystem attempt grant"
                elif charged[bank][1] + debit[1] > limits[bank][1]:
                    reason = "native internal sleep grant"
                if reason:
                    break
            actual = work.acquire(cost, close) if action == "acquire" else work.admit(cost)
            if reason:
                assert actual is (None if action == "acquire" else False)
                refusals += 1
                if first is None:
                    first = (reason, "recovery" if recovering else "forward")
                recovering = True
            else:
                assert actual is not None if action == "acquire" else actual is True
                if action == "acquire":
                    credits.append(actual)
                for bank, debit in debits:
                    for field in (0, 1):
                        charged[bank][field] += debit[field]
                if action == "acquire":
                    for bank in {current, 1} if not recovering else {1}:
                        for field in (0, 1):
                            prepaid[bank][field] += close[field]
        assert work.report() == {
            name: native_work_snapshot(*charged[bank], *prepaid[bank])
            for bank, name in enumerate(("forward", "recovery"))
        }, (seed, step, action)
        report = writes.report()
        assert report["recovering"] is recovering
        assert report["refusals"] == refusals
        if first:
            assert (report["first_refusal"]["reason"], report["first_refusal"]["bank"]) == first
    for credit in credits:
        credit.consume()


@pytest.mark.parametrize("profile", [(1, 1), (2, 3), (10, 25), (46340, 1), (1, 1073741823)])
@pytest.mark.parametrize(
    "method,amount,calls,nominal",
    [
        ("read", 7, "read", "retry"),
        ("write", 7, "write", "retry"),
        ("truncate", 0, 2, 0),
        ("sync", 0, 1, 0),
        ("size", 0, 1, 0),
        ("lock", 0, 6, 2000),
        ("unlock", 0, 4, 0),
        ("reserved", 0, 2, 0),
        ("control", 0, 3, 0),
        ("scalar", 0, 0, 0),
        ("open_database", 0, "database", "open"),
        ("open_journal_pinned", 0, "pinned", "open"),
        ("open_journal_new", 0, "new", "open"),
        ("delete_journal", 0, "delete", "retry"),
        ("access", 0, "access", "retry"),
        ("fullpath", 0, 2, 0),
        ("bind_database", 0, 7, 0),
        ("bind_journal", 0, 10, 0),
        ("close", 0, 3, 200000),
        ("pin_close", 0, 1, 0),
    ],
)
def test_native_filesystem_recipes_cover_qualified_method_families(
    profile, method, amount, calls, nominal
):
    count, delay = profile
    base_open = (count + 1) * (count + 5)
    expected_calls = {
        "read": count + 2,
        "write": amount + count + 1,
        "database": base_open + 3,
        "pinned": base_open + 9,
        "new": base_open + 11,
        "delete": 2 * (count + 1) + 3,
        "access": count + 1,
    }.get(calls, calls)
    wait = delay * count * (count + 1) // 2 * 1000
    expected_wait = {"retry": wait, "open": wait * (count + 5)}.get(nominal, nominal)
    assert native.filesystem_cost(method, profile, amount) == (expected_calls, expected_wait)


@pytest.mark.parametrize("method", ["read", "write"])
@pytest.mark.parametrize("amount", [0, -1, 2**31, True, 1.0, "1", 2**63, -(2**63) - 1])
def test_native_filesystem_transfer_recipes_require_positive_native_int_amounts(method, amount):
    with pytest.raises((TypeError, ValueError, OverflowError)):
        native.filesystem_cost(method, (2, 3), amount)


@pytest.mark.parametrize("method", ["close", "open_database", "bind_journal", "scalar"])
@pytest.mark.parametrize("amount", [-1, 1, 2**63 - 1])
def test_native_filesystem_nontransfer_recipes_refuse_unused_byte_operands(method, amount):
    with pytest.raises(ValueError):
        native.filesystem_cost(method, (2, 3), amount)


@pytest.mark.parametrize("method", [True, b"read", None, "", "read\0extra", "other"])
def test_native_filesystem_recipe_method_is_exact_and_known(method):
    with pytest.raises((TypeError, ValueError)):
        native.filesystem_cost(method, (2, 3), 1)


@pytest.mark.parametrize(
    "profile", [(0, 1), (1, 0), (-1, 1), (2, 2**31), (46341, 1), (2, True), [2, 3]]
)
def test_native_filesystem_recipe_requires_a_qualified_safe_profile(profile):
    with pytest.raises((TypeError, ValueError, OverflowError)):
        native.filesystem_cost("close", profile)


def test_native_filesystem_recipes_match_independent_retry_and_wrapper_composition():
    rng = random.Random(19171028)
    for _ in range(512):
        count, delay = rng.randrange(1, 25), rng.randrange(1, 101)
        profile = (count, delay)
        # Compose source loops, rather than using the compiled polynomial.
        attempts = list(range(count + 1))
        sleeps = [delay * index * 1000 for index in range(1, count + 1)]
        directories = sum(1 for _mode in ("rw", "ro") for _attempt in attempts)
        creates = sum(1 for _mode in ("rw", "ro") for _attempt in attempts)
        nested_access = sum(1 for _outer in attempts for _inner in attempts)
        open_waits = (
            sum(sleeps) * 2  # both directory loops
            + sum(sleeps) * 2  # RW and fallback RO CreateFile loops
            + sum(sum(sleeps) for _outer in attempts)
        )
        metadata = {
            "open_database": ["information", "standard", "final_path"],
            "open_journal_pinned": [
                "pre_information",
                "pre_standard",
                "pre_final_path",
                "information",
                "standard",
                "final_path",
                "post_information",
                "post_standard",
                "post_final_path",
            ],
            "open_journal_new": [
                "absence",
                "information",
                "standard",
                "final_path",
                "pin_create",
                "pin_information",
                "pin_standard",
                "pin_final_path",
                "post_information",
                "post_standard",
                "post_final_path",
            ],
        }
        for method, wrapper_calls in metadata.items():
            assert native.filesystem_cost(method, profile) == (
                directories + creates + nested_access + len(wrapper_calls),
                open_waits,
            )
        database = native.filesystem_cost("open_database", profile)
        close = native.filesystem_cost("close", profile)
        assert native.retry_bootstrap_cost(profile) == tuple(
            left + right for left, right in zip(database, close, strict=True)
        )
        assert native.filesystem_cost("read", profile, 1) == (1 + len(attempts), sum(sleeps))
        # Every successful partial write must consume a positive byte; retries
        # are not reset by intervening progress. Include the optional seek.
        size = rng.randrange(1, 100)
        assert native.filesystem_cost("write", profile, size) == (
            1 + sum(1 for _byte in range(size)) + count,
            sum(sleeps),
        )
        assert native.filesystem_cost("delete_journal", profile) == (
            3 + sum(2 for _attempt in attempts),
            sum(sleeps),
        )
        assert native.filesystem_cost("access", profile) == (len(attempts), sum(sleeps))


def test_native_callback_bundle_requires_an_explicit_matching_initialized_work_owner():
    writes = native.WriteQuota(grant(), grant())
    other = native.WriteQuota(grant(), grant())
    work = native.NativeWorkQuota(writes, (0, 0), (0, 0))
    foreign = native.NativeWorkQuota(other, (0, 0), (0, 0))
    with pytest.raises(TypeError):
        native.CallbackQuota(
            writes,
            (4, 0),
            (4, 0),
            buffers=(0, 0),
        )
    with pytest.raises(TypeError):
        native.CallbackQuota(
            writes,
            (4, 0),
            (4, 0),
            object(),
            buffers=(0, 0),
        )
    with pytest.raises(ValueError, match="same write owner"):
        native.CallbackQuota(
            writes,
            (4, 0),
            (4, 0),
            foreign,
            buffers=(0, 0),
        )
    with pytest.raises(RuntimeError, match="not initialized"):
        native.CallbackQuota(
            writes,
            (4, 0),
            (4, 0),
            native.NativeWorkQuota.__new__(native.NativeWorkQuota),
            buffers=(0, 0),
        )
    calls = native.CallbackQuota(
        writes,
        (4, 0),
        (4, 0),
        work,
        buffers=(0, 0),
    )
    assert calls.work is work
    with pytest.raises(AttributeError):
        calls.work = foreign
    assert calls.work.report() == {
        "forward": native_work_snapshot(),
        "recovery": native_work_snapshot(),
    }
    assert writes.report()["refusals"] == other.report()["refusals"] == 0


def test_native_callback_output_grants_are_required_even_with_a_matching_work_owner():
    writes = native.WriteQuota(grant(), grant())
    work = native.NativeWorkQuota(writes, (0, 0), (0, 0))
    with pytest.raises(TypeError, match="buffers"):
        native.CallbackQuota(writes, (8, 0), (8, 0), work)


@pytest.mark.parametrize(
    "buffers",
    [None, [0, 0], (0,), (0, 0, 0), (True, 0), (0, 1.0), (-1, 0), (0, 2**63)],
)
def test_native_callback_output_grants_reject_ambiguous_or_unrepresentable_values(buffers):
    writes = native.WriteQuota(grant(), grant())
    with pytest.raises((TypeError, ValueError, OverflowError)):
        native.CallbackQuota(
            writes,
            (8, 0),
            (8, 0),
            work=native.NativeWorkQuota(writes, (0, 0), (0, 0)),
            buffers=buffers,
        )
    assert not writes.recovering


def test_native_output_capacity_is_prepaid_whole_and_cannot_consume_reserved_closes():
    writes = native.WriteQuota(grant(), grant())
    calls = native.CallbackQuota(
        writes,
        (8, 0),
        (8, 0),
        work=native.NativeWorkQuota(writes, (0, 0), (0, 0)),
        buffers=(7, 11),
    )
    assert calls.opened() is True
    assert calls.admit_buffer(7, False)
    assert not calls.admit_buffer(1, False)
    assert writes.recovering and calls.report()["pending_error"]
    assert calls.report()["forward"]["output_buffer_bytes_charged"] == 7
    assert calls.report()["reserved_recovery_closes"] == 1
    assert not calls.admit_buffer(1, False)
    assert not calls.admit_buffer(11, True)
    assert calls.report()["recovery"]["output_buffer_bytes_charged"] == 0
    assert calls.admit_buffer(11, True)
    assert not calls.admit_buffer(1, True)
    assert calls.report()["recovery"]["output_buffer_bytes_charged"] == 11
    calls.closed(True)
    assert calls.report()["reserved_recovery_closes"] == 0
    assert writes.report()["first_refusal"]["reason"] == "VFS output-buffer grant"


@pytest.mark.parametrize("requested", [-1, 2**31, 2**63 - 1])
@pytest.mark.parametrize("can_error", [False, True])
def test_native_output_capacity_checks_the_signed_native_abi_before_byte_charge(
    requested, can_error
):
    writes = native.WriteQuota(grant(), grant())
    calls = native.CallbackQuota(
        writes,
        (8, 0),
        (8, 0),
        work=native.NativeWorkQuota(writes, (0, 0), (0, 0)),
        buffers=(2**63 - 1, 2**63 - 1),
    )
    assert not calls.admit_buffer(requested, can_error)
    assert calls.report()["forward"]["callback_slots_charged"] == 1
    assert calls.report()["forward"]["output_buffer_bytes_charged"] == 0
    assert calls.report()["pending_error"] is (not can_error)
    assert writes.report()["first_refusal"]["reason"] == "invalid VFS buffer range"


@pytest.mark.parametrize("requested,can_error", [(True, True), (1.0, True), ("1", True), (1, 1)])
def test_native_output_api_rejects_nonexact_input_without_changing_either_bank(
    requested, can_error
):
    writes = native.WriteQuota(grant(), grant())
    calls = native.CallbackQuota(
        writes,
        (8, 0),
        (8, 0),
        work=native.NativeWorkQuota(writes, (0, 0), (0, 0)),
        buffers=(7, 11),
    )
    before = calls.report()
    with pytest.raises(TypeError):
        calls.admit_buffer(requested, can_error)
    assert calls.report() == before and not writes.recovering


def test_native_zero_output_capacity_uses_a_slot_without_manufacturing_output():
    writes = native.WriteQuota(grant(), grant())
    calls = native.CallbackQuota(
        writes,
        (8, 0),
        (8, 0),
        work=native.NativeWorkQuota(writes, (0, 0), (0, 0)),
        buffers=(0, 0),
    )
    assert calls.admit_buffer(0)
    assert calls.report()["forward"]["callback_slots_charged"] == 1
    assert calls.report()["forward"]["output_buffer_bytes_charged"] == 0


@pytest.mark.parametrize("seed", range(8))
def test_native_output_prefixes_match_independent_slot_byte_and_error_model(seed):
    rng = random.Random(seed)
    writes = native.WriteQuota(grant(), grant())
    calls = native.CallbackQuota(
        writes,
        (13, 0),
        (19, 0),
        work=native.NativeWorkQuota(writes, (0, 0), (0, 0)),
        buffers=(17, 23),
    )
    slots, output = [0, 0], [0, 0]
    capacities, grants = (13, 19), (17, 23)
    recovering = pending = False
    for _ in range(96):
        action = rng.choice(["buffer", "fallible", "value", "failure"])
        if action == "failure":
            writes.failed()
            recovering = True
        else:
            index = int(recovering)
            can_error = rng.choice([False, True]) if action == "buffer" else action == "fallible"
            amount = rng.choice([-1, 0, 1, 7, 19, 2**31])
            expected = False
            if pending:
                pending = not can_error
            elif slots[index] == capacities[index]:
                recovering, pending = True, not can_error
            else:
                slots[index] += 1
                expected = True
                if action == "buffer":
                    if amount < 0 or amount > 2**31 - 1 or amount > grants[index] - output[index]:
                        expected = False
                        recovering, pending = True, not can_error
                    else:
                        output[index] += amount
            actual = (
                calls.admit_buffer(amount, can_error)
                if action == "buffer"
                else calls.admit(can_error)
            )
            assert actual is expected
        report = calls.report()
        assert writes.recovering is recovering and report["pending_error"] is pending
        for index, bank in enumerate(("forward", "recovery")):
            assert report[bank]["callback_slots_charged"] == slots[index]
            assert report[bank]["output_buffer_bytes_charged"] == output[index]
            assert slots[index] <= capacities[index] and output[index] <= grants[index]


@pytest.mark.parametrize(
    "limits",
    [
        None,
        [],
        [1, 2],
        (),
        (1,),
        (1, 2, 3),
        (True, 2),
        (1, False),
        (-1, 2),
        (1, -1),
        (1 << 31, 2),
        (1, 1 << 31),
        (1 << 63, 2),
        (1, 1 << 63),
        ("1", 2),
        (1, 2.0),
    ],
)
def test_native_population_grants_reject_invalid_values_before_runtime_admission(limits):
    with pytest.raises((TypeError, ValueError, OverflowError)):
        native.SqliteProcessBudget("not-used", 1048576, limits)


def test_native_population_grants_are_required_before_runtime_admission():
    with pytest.raises(TypeError):
        native.SqliteProcessBudget("not-used", 1048576)


def test_native_population_owner_rejects_uninitialized_observation_and_increase():
    owner = native.SqliteProcessBudget.__new__(native.SqliteProcessBudget)
    with pytest.raises(RuntimeError, match="not initialized"):
        owner.native_report()
    with pytest.raises(RuntimeError, match="not initialized"):
        owner.increase_native((1, 2))
