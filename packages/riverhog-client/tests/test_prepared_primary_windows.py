from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest
from riverhog_client import PrimaryStateError
from riverhog_client._prepared_primary_windows_native import (
    WindowsContainer,
    windows_container_path,
)


def test_windows_container_filename_commits_the_original_framed_descriptor():
    parent = Path("state")
    first = windows_container_path(parent, b"app\0allocation\0scope")
    assert first.parent == parent
    assert first != windows_container_path(parent, b"app\0allocation\0scope\0")
    assert first.name.startswith("primary-") and first.name.endswith(".sqlite3")
    assert first.name.isascii()


@pytest.mark.skipif(os.name != "nt", reason="requires native unprivileged Windows and NTFS")
def test_native_unprivileged_owner_and_transaction_process_boundaries(tmp_path):
    from tests.platform.windows_primary_process import run_unprivileged

    root = Path(__file__).resolve().parents[3]
    helper = root / "tests/platform/windows_primary_process.py"
    evidence_root = Path(os.environ.get("RIVERHOG_CI_TIMING_DIR", str(tmp_path)))
    evidence_root.mkdir(parents=True, exist_ok=True)
    report = evidence_root / "windows-prepared-primary.json"
    try:
        code = run_unprivileged(
            [sys.executable, str(helper), "--witness", str(tmp_path), "--report", str(report)],
            cwd=root,
            timeout_seconds=900,
            writable_directory=tmp_path,
            report=report,
        )
    except BaseException as exc:
        report.write_text(json.dumps({"failed": True, "launcher_error": str(exc)}))
        raise
    raw = report.read_text() if report.is_file() else ""
    evidence = json.loads(raw) if raw else {"missing_report": True, "exit_code": code}
    assert code == 0, evidence
    assert evidence["passed"] == 157 and evidence["elevated"] is False, evidence
    assert evidence["vfs"] == "win32"
    assert evidence["vfs_faults"]["runtime"]["base_vfs"] == "win32"
    assert len(evidence["vfs_faults"]["faults"]) == 8
    assert all(fault["injected"] for fault in evidence["vfs_faults"]["faults"])
    assert evidence["fixed_barrier"]["runtime"]["base_vfs"] == "win32"
    barrier_cases = evidence["fixed_barrier"]["cases"]
    assert len(barrier_cases) == 10
    assert all(
        case["original_control_exact"] and case["original_payload_exact"] for case in barrier_cases
    )
    progress_cases = [case for case in barrier_cases if case["kind"] == "progress"]
    assert len(progress_cases) == 6
    assert all(
        case["fixed_extent_bytes"] == 8
        and not case["database_and_journal_growth"]
        and case["cold_backing_preserved"]
        and case["quota"]["refusals"] == case["quota"]["native_failures"] == 0
        and case["quota"]["forward"]["database_pages_attempted"]
        <= case["derived_bound"]["database_pages"]
        and case["quota"]["forward"]["journal_write_extent_bytes"]
        <= case["derived_bound"]["journal_write_extent_bytes"]
        for case in progress_cases
    )
    for case in progress_cases:
        for kind in ("database", "journal"):
            before = case["space_before"][kind]
            after = case["space_after_cold_barrier"][kind]
            assert after["end_bytes"] == before["end_bytes"]
            assert after["end_bytes"] <= after["allocated_bytes"] <= before["allocated_bytes"]
        assert (
            case["space_after_cold_barrier"]["journal_reserved_bytes"]
            <= case["space_after_cold_barrier"]["journal"]["end_bytes"]
        )
    rejected = [case for case in barrier_cases if case["kind"] == "reject"]
    assert len(rejected) == 4
    assert all(
        case["rejected_without_replacement"]
        and case["owner_poisoned"]
        and case["cold_open_rejected"]
        for case in rejected
    )
    assert evidence["write_quota"]["runtime"]["base_vfs"] == "win32"
    quota_cases = evidence["write_quota"]["cases"]
    assert len(quota_cases) == 2
    assert all(
        case["quota"]["refusals"] == 1
        and case["original_control_exact"]
        and case["original_payload_exact"]
        and case["survivor_control_exact"]
        and case["capacity_retained"]
        and case["larger_grant_retry_coldly_verified"]
        and not case["production_resource_gate_certified"]
        and not case["all_allocating_file_methods_covered"]
        and not case["initialization_quota_covered"]
        for case in quota_cases
    )
    database_quota = next(case for case in quota_cases if case["refusal"] == "database")
    assert database_quota["quota"]["forward"]["database_pages_attempted"] == 2
    assert database_quota["automatic_rollback_pages_before_driver_error"] > 2
    assert database_quota["explicit_rollback_calls"] == 0
    journal_quota = next(case for case in quota_cases if case["refusal"] == "journal")
    assert journal_quota["quota"]["forward"]["database_pages_attempted"] == 0
    compiled = evidence["compiled_write_quota"]
    assert compiled["runtime"]["base_vfs"] == "win32"
    assert compiled["compiled_bank"]["compiler"].startswith("MSVC ")
    assert len(compiled["compiled_bank"]["module_sha256"]) == 64
    assert compiled["compiled_bank"]["storage_callbacks_native"] is False
    assert len(compiled["cases"]) == 2
    for case in compiled["cases"]:
        assert case["bank_implementation"] == "compiled-native"
        assert case["tracking_bytes"] > 0
        assert case["quota"]["refusals"] == 1
        assert case["original_control_exact"] and case["original_payload_exact"]
        assert case["survivor_control_exact"] and case["capacity_retained"]
        assert case["larger_grant_retry_coldly_verified"]
        assert not case["production_resource_gate_certified"]
        assert not case["all_allocating_file_methods_covered"]
        assert not case["initialization_quota_covered"]
        if case["refusal"] == "database":
            assert case["quota"]["forward"]["database_pages_attempted"] == 2
            assert case["automatic_rollback_pages_before_driver_error"] > 2
            assert case["explicit_rollback_calls"] == 0
        else:
            assert case["quota"]["forward"]["database_pages_attempted"] == 0
    native_vfs = evidence["native_write_vfs"]
    assert native_vfs["runtime"]["base_vfs"] == "win32"
    assert native_vfs["runtime"]["interposer"] == "compiled-write-vfs-version-1"
    assert (
        native_vfs["runtime"]["sqlite_library_sha256"]
        == compiled["runtime"]["sqlite_library_sha256"]
    )
    assert native_vfs["compiled_bank"]["storage_callbacks_native"] is True
    assert native_vfs["compiled_bank"]["compiler"].startswith("MSVC ")
    assert len(native_vfs["cases"]) == 2
    for case in native_vfs["cases"]:
        assert case["read_quota"]["forward"]["read_calls"] > 0
        assert case["read_quota"]["forward"]["read_bytes"] > 0
        if case["refusal"] == "database":
            assert case["read_quota"]["recovery"]["read_calls"] > 0
            assert case["read_quota"]["recovery"]["read_bytes"] > 0
        assert case["bank_implementation"] == "compiled-native"
        assert case["quota"]["refusals"] == 1
        assert case["original_control_exact"] and case["original_payload_exact"]
        assert case["survivor_control_exact"] and case["capacity_retained"]
        assert case["larger_grant_retry_coldly_verified"]
        assert not case["production_resource_gate_certified"]
        assert not case["all_allocating_file_methods_covered"]
        if case["refusal"] == "database":
            assert case["quota"]["forward"]["database_pages_attempted"] == 2
            assert case["automatic_rollback_pages_before_driver_error"] > 2
            assert case["explicit_rollback_calls"] == 0
        else:
            assert case["quota"]["forward"]["database_pages_attempted"] == 0
    assert len(evidence["space"]["cycles"]) == 3
    assert len(evidence["resource_recovery"]) == 16
    assert all(
        case["survivor_exact"]
        and not case["unsealed_primary_adopted"]
        and case["retry_reused_capacity"]
        and case["member_grant_charged_once"]
        for case in evidence["resource_recovery"]
    )
    assert evidence["resource_vfs"]["runtime"]["base_vfs"] == "win32"
    assert len(evidence["resource_vfs"]["cases"]) == 64
    assert all(
        case["vfs_fault"]["injected"]
        and case["survivor_exact"]
        and case["retry_reused_capacity"]
        and case["member_grant_charged_once"]
        for case in evidence["resource_vfs"]["cases"]
    )
    assert evidence["control_vfs"]["runtime"]["base_vfs"] == "win32"
    assert len(evidence["control_vfs"]["cases"]) == 24
    assert all(
        case["vfs_fault"]["injected"]
        and case["original_control_exact"]
        and case["original_payload_exact"]
        and case["capacity_retained"]
        and case["retry_coldly_verified"]
        for case in evidence["control_vfs"]["cases"]
    )
    measured = [
        evidence["space"]["transactions"],
        evidence["controls"]["transactions"],
        *(case["transactions"] for case in evidence["journals"]["cases"]),
    ]
    assert all(
        result["completed_transactions"] > 0
        and result["maxima"]["database_pages_written"] > 0
        and result["maxima"]["journal_write_extent_bytes"] > 0
        and result["maxima"]["other_write_bytes"] == 0
        and not result["cache_peak_certified"]
        and not result["dirty_page_admission_certified"]
        for result in measured
    )
    assert evidence["controls"]["transactions"]["failed_transactions"] > 0
    assert all(
        case["transactions"]["failed_transactions"] > 0 for case in evidence["journals"]["cases"]
    )
    print(json.dumps(evidence, sort_keys=True))


class _JournalAPI:
    """Journal lifecycle fixture; native handle and ACL behavior runs separately."""

    identity = None
    private_error = False

    def open(self, _path, *, journal):
        assert journal
        if self.identity is None:
            raise FileNotFoundError
        return 1

    def information(self, _handle, _path, *, directory):
        assert not directory
        return self.identity

    def private(self, _handle, *, inherited):
        assert inherited
        if self.private_error:
            raise PrimaryStateError("fixture sidecar is not private")

    def close(self, _handle):
        pass


def _journal_guard(monkeypatch):
    guard = object.__new__(WindowsContainer)
    guard.path = Path("state.sqlite3")
    guard._api = _JournalAPI()
    guard._journal_identity = (7, 0, 1)
    monkeypatch.setattr(guard, "check_current", lambda: None)
    return guard


def test_journal_connection_recovery_rebinds_an_absent_old_sidecar(monkeypatch):
    guard = _journal_guard(monkeypatch)
    guard.check_journal(required=False, recovering=True)
    assert guard._journal_identity is None
    guard._api.identity = (7, 0, 2)
    guard.check_journal(required=True)
    assert guard._journal_identity == (7, 0, 2)


def test_journal_replacement_outside_connection_recovery_is_rejected(monkeypatch):
    guard = _journal_guard(monkeypatch)
    guard.check_journal(required=False)
    assert guard._journal_identity == (7, 0, 1)
    guard._api.identity = (7, 0, 2)
    with pytest.raises(PrimaryStateError, match="journal identity changed"):
        guard.check_journal(required=True)
    assert guard._journal_identity == (7, 0, 1)
    guard.check_journal(required=True, recovering=True)
    assert guard._journal_identity == (7, 0, 2)


@pytest.mark.parametrize("failure", ["missing", "unprivate"])
def test_journal_connection_recovery_preserves_presence_and_private_checks(monkeypatch, failure):
    guard = _journal_guard(monkeypatch)
    if failure == "unprivate":
        guard._api.identity = (7, 0, 2)
        guard._api.private_error = True
    with pytest.raises(PrimaryStateError, match="missing|not private"):
        guard.check_journal(required=True, recovering=True)
    assert guard._journal_identity == (7, 0, 1)


def test_native_allocation_keeps_end_of_file_and_allocated_size_distinct():
    import ctypes

    from riverhog_client._prepared_primary_windows_native import _FileStandardInfo, _WindowsAPI

    calls = []

    class Kernel:
        def GetFileInformationByHandleEx(self, handle, kind, pointer, size):
            calls.append((handle, kind, size))
            info = ctypes.cast(pointer, ctypes.POINTER(_FileStandardInfo)).contents
            info.allocated, info.end, info.links = 16384, 4097, 1
            return 1

    api = object.__new__(_WindowsAPI)
    api.kernel = Kernel()
    space = api.file_space(7)
    assert calls == [(7, 1, ctypes.sizeof(_FileStandardInfo))]
    assert space.end_bytes == 4097 and space.allocated_bytes == 16384


@pytest.mark.parametrize("attributes", [0, 0x200, 0x800])
@pytest.mark.parametrize("directory", [False, True])
def test_native_identity_admission_rejects_sparse_or_compressed_state(attributes, directory):
    import ctypes

    from riverhog_client import _prepared_primary_windows_native as native

    path = Path("state")
    final = chr(92) * 2 + "?" + chr(92) + str(path)

    class Kernel:
        @staticmethod
        def GetFileInformationByHandle(_handle, pointer):
            info = ctypes.cast(pointer, ctypes.POINTER(native._FileInformation)).contents
            info.attributes = attributes | (0x10 if directory else 0)
            info.links = 1
            info.volume, info.index_high, info.index_low = 1, 0, 42
            return 1

        @staticmethod
        def GetFinalPathNameByHandleW(_handle, buffer, _size, _flags):
            if buffer is not None:
                buffer.value = final
            return len(final)

    api = object.__new__(native._WindowsAPI)
    api.kernel = Kernel()
    if attributes:
        with pytest.raises(PrimaryStateError, match="unsupported file kind"):
            api.information(1, path, directory=directory)
    else:
        assert api.information(1, path, directory=directory) == (1, 0, 42)


@pytest.mark.parametrize("failed_handle", [1, 2, 20, 30])
@pytest.mark.parametrize("delegated", [False, True])
@pytest.mark.parametrize("failure_type", [PrimaryStateError, KeyboardInterrupt])
def test_native_guard_uncertain_close_retains_unattempted_pins_without_retry(
    failed_handle, delegated, failure_type
):
    guard = object.__new__(WindowsContainer)
    guard._closed = False
    guard._close_failure = None
    handles = [1, 2, 20, 30]
    guard._handles = [(handle, Path(str(handle)), False, (1, 0, handle)) for handle in handles]
    live = set(handles)
    attempts = []
    failure = failure_type("injected close outcome uncertainty")

    class API:
        def close(self, handle):
            attempts.append(handle)
            if handle != failed_handle or delegated:
                live.remove(handle)
            if handle == failed_handle:
                raise failure

    guard._api = API()
    with pytest.raises(failure_type) as first:
        guard.close()
    assert first.value is failure
    assert guard.quarantined and guard._closed
    index = handles.index(failed_handle)
    assert attempts == list(reversed(handles[index:]))
    assert [item[0] for item in guard._handles] == handles[: index + 1]
    assert live == set(handles[:index]) | (set() if delegated else {failed_handle})
    if failed_handle == 30:
        assert 20 in live and 20 not in attempts
    with pytest.raises(PrimaryStateError, match="construction owner is closed"):
        guard.check_current()
    before = list(attempts)
    with pytest.raises(PrimaryStateError, match="quarantined") as repeated:
        guard.close()
    assert repeated.value.__cause__ is failure
    assert attempts == before


def test_native_guard_successful_close_releases_each_pin_once():
    guard = object.__new__(WindowsContainer)
    guard._closed = False
    guard._close_failure = None
    guard._handles = [(value, Path(str(value)), False, (1, 0, value)) for value in [1, 2, 20, 30]]
    closed = []

    class API:
        def close(self, handle):
            closed.append(handle)

    guard._api = API()
    guard.close()
    guard.close()
    assert closed == [30, 20, 2, 1]
    assert guard._handles == [] and not guard.quarantined


@pytest.mark.skipif(os.name != "nt", reason="requires native Windows ownership handles")
@pytest.mark.parametrize("delegated", [False, True])
def test_native_guard_failed_database_close_keeps_exclusive_owner_after_gc(tmp_path, delegated):
    import subprocess

    code = r"""
import gc,json,runpy,sys
from pathlib import Path
from riverhog_client import PrimaryStateBusy,PrimaryStateError
from riverhog_client._prepared_primary_windows_native import WindowsContainer,windows_container_path
helpers=runpy.run_path(sys.argv[1])
parent=Path(sys.argv[2])/"guard-private"
helpers["_parent"](parent)
path=windows_container_path(parent,b"uncertain metadata close")
guard=WindowsContainer(path,initialize=True)
owner_path=Path(str(path)+".owner")
api=guard._api
real_close=api.close
database=guard._database
owner=guard._owner
attempts=[]
delegated=sys.argv[3]=="True"
def uncertain(handle):
    attempts.append(handle)
    if handle==database:
        if delegated:real_close(handle)
        raise PrimaryStateError("injected database handle close response loss")
    real_close(handle)
api.close=uncertain
try:guard.close()
except PrimaryStateError as exc:assert "response loss" in str(exc)
else:raise AssertionError("uncertain data close reported complete")
assert guard.quarantined and attempts==[database]
assert owner not in attempts
api.information(owner,owner_path,directory=False)
try:guard.close()
except PrimaryStateError as exc:assert "quarantined" in str(exc)
else:raise AssertionError("quarantined metadata handle was retried")
assert attempts==[database]
# No Python owner object or test cleanup releases the unattempted native owner.
api.close=real_close
del guard
gc.collect()
probe=r'''
import json,sys
from pathlib import Path
from riverhog_client import PrimaryStateBusy
from riverhog_client._prepared_primary_windows_native import WindowsContainer
try:WindowsContainer(Path(sys.argv[1]),initialize=False)
except PrimaryStateBusy:print(json.dumps({"exclusive":True}))
else:raise AssertionError("uncertain database close released the exclusive owner")
'''
import subprocess
result=subprocess.run([sys.executable,"-I","-c",probe,str(path)],
    capture_output=True,text=True,timeout=30)
assert result.returncode==0,(result.stdout,result.stderr)
assert json.loads(result.stdout)=={"exclusive":True}
print(json.dumps({"passed":True,"delegated":delegated,"exclusive_owner_retained":True,
    "close_attempts":len(attempts)}))
"""
    helper = Path(__file__).resolve().parents[3] / "tests/platform/windows_primary_process.py"
    result = subprocess.run(
        [sys.executable, "-I", "-c", code, str(helper), str(tmp_path), str(delegated)],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, (result.stdout, result.stderr)
    assert json.loads(result.stdout) == {
        "passed": True,
        "delegated": delegated,
        "exclusive_owner_retained": True,
        "close_attempts": 1,
    }


@pytest.mark.parametrize(
    "failure", ["none", "live", "native", "metadata", "interrupt", "complete", "unregister"]
)
def test_composed_metadata_teardown_preserves_custody_until_every_owner_finishes(failure):
    from riverhog_client import PrimaryStateBusy
    from riverhog_client._prepared_primary_windows_native import NativeWindowsContainer

    events = []
    guard = object.__new__(NativeWindowsContainer)
    guard._handles = [(value, Path(str(value)), False, (1, 0, value)) for value in [1, 20, 30]]
    guard._closed = False
    guard._close_failure = None
    guard._native_released = False
    guard._native_constructing = False
    guard._native_failure = None
    error = (
        KeyboardInterrupt("metadata close interrupted")
        if failure == "interrupt"
        else PrimaryStateError("injected teardown failure")
    )

    class Gate:
        open_files = int(failure == "live")
        complete = False
        quarantined = False
        released = False
        stopped = False

        def unbind(self):
            events.append("unbind")
            if failure == "native":
                raise error

        def quarantine(self):
            events.append("quarantine")
            self.quarantined = True

        def complete_custody(self, custodian):
            assert custodian is guard and not guard._handles
            events.append("complete")
            if failure == "complete" and not self.stopped:
                self.stopped = True
                raise error
            self.complete = True

        def close(self):
            assert self.complete and not self.quarantined
            events.append("unregister")
            if failure == "unregister" and not self.stopped:
                self.stopped = True
                raise error
            self.released = True

    class API:
        def close(self, handle):
            events.append(handle)
            if handle == 30 and failure in ("metadata", "interrupt"):
                raise error

    gate = Gate()
    guard._native_gate = gate
    guard._api = API()
    if failure == "none":
        guard.close()
    elif failure == "live":
        with pytest.raises(PrimaryStateBusy, match="live native"):
            guard.close()
        assert not events and not guard._closed
        gate.open_files = 0
        guard.close()
    else:
        with pytest.raises(type(error)) as caught:
            guard.close()
        assert caught.value is error and not gate.released
        if failure == "native":
            assert events == ["unbind"] and len(guard._handles) == 3 and not guard._closed
            return
        if failure in ("metadata", "interrupt"):
            assert events == ["unbind", 30, "quarantine"]
            assert gate.quarantined and len(guard._handles) == 3
            before = list(events)
            with pytest.raises(PrimaryStateError, match="custody is quarantined") as repeated:
                guard.close()
            assert repeated.value.__cause__ is error and events == before
            return
        assert not guard._handles and guard._closed
        guard.close()
    assert gate.released and guard._native_released
    assert events[:4] == ["unbind", 30, 20, 1]
    before = list(events)
    guard.close()
    assert events == before


@pytest.mark.parametrize("stage", ["placement", "admission", "cleanup", "binding", "success"])
def test_composed_construction_keeps_uncertain_admission_charged(tmp_path, monkeypatch, stage):
    from riverhog_client._prepared_primary_windows_native import NativeWindowsContainer

    events = []
    failure = PrimaryStateError("uncertain native admission")
    path = windows_container_path(tmp_path, b"composed custody")
    if stage == "placement":
        path = tmp_path / "unsupported.sqlite3"

    class API:
        def close(self, handle):
            events.append(handle)

    class Gate:
        name = "compiled-custody"
        open_files = 0
        custodian = None
        quarantined = False
        complete = False
        charged = True

        def retain_custodian(self, custodian):
            self.custodian = custodian

        def bind(self, *args):
            events.append("bind")
            if stage == "binding":
                raise failure

        def unbind(self):
            events.append("unbind")

        def quarantine(self):
            events.append("quarantine")
            self.quarantined = True

        def complete_custody(self, custodian):
            assert custodian is self.custodian and not custodian._handles
            events.append("complete")
            self.complete = True

        def close(self):
            assert self.complete and not self.quarantined
            events.append("unregister")
            self.charged = False
            self.custodian = None

    def admitted(guard, path, *, initialize):
        assert initialize and gate.custodian is guard and gate.charged
        events.append("admission")
        guard._api = API()
        guard._handles = [(20, path, False, (1, 0, 20))]
        guard._database = 30
        guard._journal_identity = None
        if stage in ("admission", "cleanup"):
            try:
                raise failure
            except PrimaryStateError:
                if stage == "cleanup":
                    guard.close()
                raise

    monkeypatch.setattr(WindowsContainer, "__init__", admitted)
    gate = Gate()
    if stage == "success":
        guard = NativeWindowsContainer(path, initialize=True, gate=gate)
        assert guard.vfs == gate.name and gate.custodian is guard
        guard.close()
        assert events == ["admission", "bind", "unbind", 20, "complete", "unregister"]
        assert not gate.charged
    else:
        with pytest.raises(PrimaryStateError):
            NativeWindowsContainer(path, initialize=True, gate=gate)
        if stage == "placement":
            assert events == ["unbind", "complete", "unregister"] and not gate.charged
        else:
            assert gate.quarantined and gate.charged
            assert events == ["admission"] + (["bind"] if stage == "binding" else []) + [
                "quarantine"
            ]
            guard = gate.custodian
            assert guard._native_failure is failure and len(guard._handles) == 1
            before = list(events)
            with pytest.raises(PrimaryStateError, match="custody is quarantined"):
                guard.close()
            assert events == before


@pytest.mark.parametrize("failure", [None, "capacity", "native"])
def test_composed_operation_check_retains_first_failure_without_releasing_custody(failure):
    from riverhog_client import PrimaryStateCapacity
    from riverhog_client._prepared_primary_windows_native import NativeWindowsContainer

    class Gate:
        current = failure
        calls = 0

        def operation_failure(self):
            self.calls += 1
            return self.current

    guard = object.__new__(NativeWindowsContainer)
    gate = Gate()
    guard._native_gate = gate
    guard._native_operation_failure = None
    guard._closed = False
    if failure is None:
        guard.check_operation()
        assert gate.calls == 1 and not guard._closed
        gate.current = "capacity"
        failure = "capacity"
    error = PrimaryStateCapacity if failure == "capacity" else PrimaryStateError
    with pytest.raises(error) as first:
        guard.check_operation()
    gate.current = None
    before = gate.calls
    with pytest.raises(error) as repeated:
        guard.check_operation()
    assert repeated.value is first.value
    assert gate.calls == before and not guard._closed


@pytest.mark.parametrize("length,expected", [(8192 + 9, (9, 45144)), (262144 + 9, (71, 299592))])
def test_fixed_control_envelope_matches_approved_extents(length, expected):
    from riverhog_client._prepared_primary_sqlite_journal import control_envelope

    result = control_envelope((length, 16), page_bytes=4096, sector_bytes=4096, reserved_bytes=0)
    assert (result.database_pages, result.journal_bytes) == expected


@pytest.mark.parametrize("page_bytes", [512, 4096, 65536])
@pytest.mark.parametrize("sector_bytes", [512, 4096, 8192, 65536])
@pytest.mark.parametrize("reserved_bytes", [0, 16, 255])
def test_fixed_control_envelope_covers_offset_and_sector_rounding(
    page_bytes, sector_bytes, reserved_bytes
):
    import math
    from fractions import Fraction

    from riverhog_client._prepared_primary_sqlite_journal import control_envelope

    lengths = (1, 9, page_bytes - 4, page_bytes - 3, 8193, 1 << 30)
    peers = max(1, math.ceil(Fraction(sector_bytes, page_bytes)))
    records = 1 + sum(
        2 + math.ceil(Fraction(length, page_bytes - reserved_bytes - 4)) for length in lengths
    )
    result = control_envelope(
        lengths, page_bytes=page_bytes, sector_bytes=sector_bytes, reserved_bytes=reserved_bytes
    )
    assert result.database_pages == peers * records
    assert result.journal_bytes == 2 * sector_bytes + result.database_pages * (page_bytes + 8) + 16


@pytest.mark.parametrize(
    "field,value",
    [
        ("page_bytes", 0),
        ("page_bytes", True),
        ("page_bytes", 3000),
        ("page_bytes", 128),
        ("page_bytes", 1 << 17),
        ("sector_bytes", 0),
        ("sector_bytes", True),
        ("sector_bytes", 3000),
        ("sector_bytes", 1 << 63),
        ("reserved_bytes", -1),
        ("reserved_bytes", True),
        ("reserved_bytes", 256),
    ],
)
def test_fixed_control_envelope_rejects_unqualified_geometry(field, value):
    from riverhog_client._prepared_primary_sqlite_journal import control_envelope

    geometry = dict(page_bytes=4096, sector_bytes=4096, reserved_bytes=0)
    geometry[field] = value
    with pytest.raises(ValueError, match="geometry"):
        control_envelope((9,), **geometry)


@pytest.mark.parametrize("length", [0, -1, True, 1 << 63, "4", 1.5])
def test_fixed_control_envelope_rejects_invalid_prefix_without_native_work(length):
    from riverhog_client._prepared_primary_windows_native import NativeWindowsContainer

    guard = object.__new__(NativeWindowsContainer)
    with pytest.raises(ValueError, match="prefixes"):
        guard.control_envelope((length,))


def test_fixed_control_envelope_requires_an_exact_prefix_tuple_before_native_work():
    from riverhog_client._prepared_primary_windows_native import NativeWindowsContainer

    guard = object.__new__(NativeWindowsContainer)
    with pytest.raises(TypeError, match="tuple"):
        guard.control_envelope([9])


@pytest.mark.parametrize(
    "length,sector_bytes", [((1 << 63) - 1, 4096), (1, 1 << 62), ((1 << 63) - 1, 1 << 62)]
)
def test_fixed_control_envelope_rejects_unrepresentable_page_and_journal_bounds(
    length, sector_bytes
):
    from riverhog_client._prepared_primary_sqlite_journal import control_envelope

    with pytest.raises(OverflowError, match="signed-64-bit admission"):
        control_envelope((length,), page_bytes=4096, sector_bytes=sector_bytes, reserved_bytes=0)


@pytest.mark.parametrize("fault", [None, "busy", "capacity", "native", "page", "sector"])
def test_composed_control_envelope_uses_observed_geometry_or_defers(monkeypatch, fault):
    from riverhog_client import PrimaryStateBusy, PrimaryStateCapacity
    from riverhog_client._prepared_primary_windows_native import NativeWindowsContainer

    class Gate:
        failure = None

        def operation_failure(self):
            return self.failure

        def geometry(self):
            if fault == "busy":
                raise BlockingIOError("active transaction")
            if fault in ("capacity", "native"):
                self.failure = fault
                raise RuntimeError("metadata read failed")
            return {
                "page_bytes": 2048 if fault == "page" else 4096,
                "reserved_bytes": 0,
                "sector_bytes": 512,
                "logical_sector_bytes": 512,
                "atomic_sector_bytes": 4096,
                "filesystem_atomic_sector_bytes": 3000 if fault == "sector" else 4096,
            }

    guard = object.__new__(NativeWindowsContainer)
    guard._native_gate = Gate()
    guard._native_operation_failure = None
    monkeypatch.setattr(WindowsContainer, "check_current", lambda self: None)
    if fault is None:
        envelope = guard.control_envelope((8192 + 9, 16))
        assert (envelope.database_pages, envelope.journal_bytes) == (9, 45144)
    else:
        errors = {"busy": PrimaryStateBusy, "capacity": PrimaryStateCapacity}
        error = errors.get(fault, PrimaryStateError)
        with pytest.raises(error):
            guard.control_envelope((9,))
