from __future__ import annotations

import json
from contextlib import closing

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_provenance import _generic_semantics as generic
from riverhog_provenance._native_work import NativeWorkCapacity, WorkAmount, WorkQuantum
from sqlite_admission import SqliteProgressConnection
from test_generic_semantics import connect, create, identity, subject, work


def quantum(octets):
    return WorkQuantum(WorkAmount(rows=128, octets=octets, source_octets=1, transitions=1))


@pytest.fixture
def owner(tmp_path, catalog):
    path = tmp_path / "semantic.sqlite3"
    row = subject(0)
    create(path, row)
    with closing(connect(path, factory=SqliteProgressConnection)) as db:
        current = work(db, row, catalog)
        yield current
        assert current.epoch.quiescent


def projection(owner, kind, pending, raw=None):
    key = identity(70)
    expected = {"activity_id": identity(71), "at": "2025-01-01T00:00:00Z"}
    name = "generation"
    if kind == "capture":
        name, expected = "captures", identity(71)
    if raw is None:
        raw = canonical_json_bytes(expected)
    encoded_key = b"j" + canonical_json_bytes(key)
    writes = {(name, encoded_key): raw} if pending else {}
    if not pending:
        owner.db.execute("INSERT INTO maps VALUES (?,?,?)", (name, encoded_key, raw)).close()
        owner.db.commit()
    meter = generic._Meter(65536)
    view = (
        generic._CaptureTargets(owner, meter, writes)
        if kind == "capture"
        else generic._Lifecycle(owner, name, meter, writes)
    )
    return view, key, expected, raw


@pytest.mark.parametrize("kind", ["lifecycle", "capture"])
@pytest.mark.parametrize("pending", [False, True], ids=["stored", "pending"])
def test_original_metadata_read_charges_decode_and_canonical_input_separately(
    owner, kind, pending, monkeypatch
):
    view, key, expected, raw = projection(owner, kind, pending)
    calls = []
    decode, canonical = json.loads, generic.canonical_json_bytes
    document = generic.canonical_document

    def decoded(value, *args, **kwargs):
        calls.append(("decode", value))
        return decode(value, *args, **kwargs)

    def verified(value):
        calls.append(("verify", value))
        return canonical(value) if kind == "capture" else document(value)

    monkeypatch.setattr(json, "loads", decoded)
    monkeypatch.setattr(
        generic, "canonical_json_bytes" if kind == "capture" else "canonical_document", verified
    )
    original = quantum(2 * len(raw))
    before = owner.db.total_changes
    with owner.epoch.execution(None, work=original):
        assert view[key] == expected
    # Capture key encoding is distinct from the retained value's verification.
    value_calls = [
        call for call in calls if call == ("decode", raw) or call == ("verify", expected)
    ]
    assert value_calls == [("decode", raw), ("verify", expected)]
    rows = 0 if pending else 1 if kind == "capture" else 2
    assert original.consumed == WorkAmount(rows=rows, octets=2 * len(raw))
    assert original.held == WorkAmount() and owner.db.total_changes == before
    original.require_received()


@pytest.mark.parametrize("kind", ["lifecycle", "capture"])
@pytest.mark.parametrize("pass_number", [1, 2], ids=["before-decode", "before-verification"])
def test_refusal_precedes_original_value_pass_and_same_owner_can_retry(
    owner, kind, pass_number, monkeypatch
):
    view, key, expected, raw = projection(owner, kind, False)
    calls, statements = [], []
    decode, document = json.loads, generic.canonical_document
    canonical = generic.canonical_json_bytes

    def decoded(value, *args, **kwargs):
        calls.append(("decode", value))
        return decode(value, *args, **kwargs)

    def verified(value):
        calls.append(("verify", value))
        return canonical(value) if kind == "capture" else document(value)

    monkeypatch.setattr(json, "loads", decoded)
    monkeypatch.setattr(
        generic, "canonical_json_bytes" if kind == "capture" else "canonical_document", verified
    )
    small = quantum(pass_number * len(raw) - 1)
    changes = owner.db.total_changes
    owner.db.set_trace_callback(statements.append)
    try:
        with pytest.raises(NativeWorkCapacity):
            with owner.epoch.execution(None, work=small):
                view[key]
    finally:
        owner.db.set_trace_callback(None)
    assert sum(call == ("decode", raw) for call in calls) == pass_number - 1
    assert ("verify", expected) not in calls
    assert small.consumed.octets == (pass_number - 1) * len(raw)
    assert small.held == WorkAmount() and owner.epoch.quiescent
    assert owner.db.total_changes == changes
    assert not any(sql.startswith(("BEGIN", "UPDATE", "INSERT", "DELETE")) for sql in statements)
    retry = quantum(2 * len(raw))
    with owner.epoch.execution(None, work=retry):
        assert view[key] == expected
    assert retry.consumed.octets == 2 * len(raw)
    retry.require_received()
    small.require_received()


@pytest.mark.parametrize("kind", ["lifecycle", "capture"])
@pytest.mark.parametrize("failure", ["decode", "shape", "canonical"])
def test_original_corrupt_metadata_keeps_attempted_input_passes_charged(owner, kind, failure):
    expected = {"activity_id": identity(71)} if kind == "lifecycle" else identity(71)
    raw = (
        b"{"
        if failure == "decode"
        else b"[]"
        if failure == "shape"
        else b" " + canonical_json_bytes(expected)
    )
    view, key, _, _ = projection(owner, kind, True, raw)
    passes = 2 if failure == "canonical" else 1
    original = quantum(passes * len(raw))
    changes = owner.db.total_changes
    with pytest.raises(RuntimeError, match="invalid retained"):
        with owner.epoch.execution(None, work=original):
            view[key]
    assert original.consumed == WorkAmount(octets=passes * len(raw))
    assert original.held == WorkAmount() and owner.db.total_changes == changes
    original.require_received()


def test_fresh_lifecycle_write_validates_typed_metadata_without_decode_round_trip(
    owner, monkeypatch
):
    writes, encodes = {}, []
    view = generic._Lifecycle(owner, "generation", generic._Meter(65536), writes)
    key = identity(70)
    value = {
        "activity_id": identity(71),
        "at": "2025-01-01T00:00:00Z",
        "state": {"object_id": key},
        "documentary_payload": "x" * 65536,
    }
    canonical = generic.canonical_document

    def encoded(metadata):
        encodes.append(metadata)
        return canonical(metadata)

    def no_decode(*args, **kwargs):
        pytest.fail("fresh lifecycle metadata must not be decoded again")

    monkeypatch.setattr(generic, "canonical_document", encoded)
    monkeypatch.setattr(json, "loads", no_decode)
    before = owner.db.total_changes
    view[key] = value
    expected = {"activity_id": value["activity_id"], "at": value["at"]}
    assert encodes == [expected]
    assert writes == {("generation", b"j" + canonical_json_bytes(key)): canonical(expected)}
    assert owner.db.total_changes == before


@pytest.mark.parametrize(
    "changed",
    [
        {},
        {"activity_id": "invalid"},
        {"activity_id": identity(71), "at": 0},
        {"activity_id": identity(71), "at": "x" * 31},
        {"activity_id": identity(71), "at": "\U0001f600" * 30},
    ],
    ids=[
        "missing-activity",
        "invalid-uuid",
        "invalid-timestamp-type",
        "long-timestamp",
        "wide-value",
    ],
)
def test_fresh_lifecycle_metadata_keeps_shape_and_encoded_extent_guards(owner, changed):
    writes = {}
    view = generic._Lifecycle(owner, "generation", generic._Meter(65536), writes)
    key = identity(70)
    with pytest.raises(RuntimeError, match="invalid retained lifecycle metadata"):
        view[key] = {**changed, "state": {"object_id": key}}
    assert writes == {} and owner.epoch.quiescent


@pytest.mark.parametrize("kind", ["lifecycle", "capture"])
@pytest.mark.parametrize("pass_number", [1, 2], ids=["before-decode", "before-verification"])
@pytest.mark.parametrize("cause_type", [InterruptedError, ValueError, TypeError])
def test_original_memory_cancellation_stops_before_each_value_pass(
    owner, kind, pass_number, cause_type, monkeypatch
):
    view, key, _, raw = projection(owner, kind, True)
    calls, checks = [], []
    cause = cause_type("original metadata execution stopped")
    decode = json.loads

    def decoded(value, *args, **kwargs):
        calls.append(value)
        return decode(value, *args, **kwargs)

    def check():
        checks.append(None)
        # The execution entry check precedes the two original value passes.
        if len(checks) == 1 + pass_number:
            raise cause

    monkeypatch.setattr(json, "loads", decoded)
    original = quantum(2 * len(raw))
    with pytest.raises(cause_type) as raised:
        with owner.epoch.execution(check, work=original):
            view[key]
    assert raised.value is cause
    assert calls == [raw] * (pass_number - 1)
    assert original.consumed == WorkAmount(octets=(pass_number - 1) * len(raw))
    assert original.held == WorkAmount() and owner.epoch.quiescent
    original.require_received()


@pytest.mark.parametrize("cause_type", [InterruptedError, ValueError, TypeError])
def test_python_semantic_units_observe_original_cancellation_before_next_step(
    owner, cause_type, monkeypatch
):
    owner.initialize()
    before, steps, statements = owner.load(), [], []
    changes = owner.db.total_changes
    cause = cause_type("original semantic execution stopped")
    step = generic.Program.step

    def stepped(program):
        steps.append(program)
        return step(program)

    def check():
        if len(steps) == 2:
            raise cause

    monkeypatch.setattr(generic.Program, "step", stepped)
    original = quantum(65536)
    owner.db.set_trace_callback(statements.append)
    try:
        with pytest.raises(cause_type) as raised:
            with owner.epoch.execution(check, work=original):
                owner.prepare(step_budget=64)
    finally:
        owner.db.set_trace_callback(None)
    assert raised.value is cause and len(steps) == 2
    assert owner.epoch.quiescent and owner.db.total_changes == changes
    assert not any(sql.startswith(("BEGIN", "UPDATE", "INSERT", "DELETE")) for sql in statements)
    assert owner.load() == before and original.held == WorkAmount()
    original.require_received()
    retry = quantum(65536)
    with owner.epoch.execution(None, work=retry):
        batch = owner.prepare(step_budget=2)
    assert batch.before == before and len(steps) == 4
    assert owner.db.total_changes == changes
    retry.require_received()


@pytest.mark.parametrize("value", [None, "\ufdd0"])
def test_fresh_lifecycle_write_preserves_original_portable_json_error(owner, value):
    writes = {}
    view = generic._Lifecycle(owner, "generation", generic._Meter(65536), writes)
    key = identity(70)
    with pytest.raises(ValueError):
        view[key] = {"activity_id": identity(71), "at": value, "state": {"object_id": key}}
    assert writes == {} and owner.epoch.quiescent


@pytest.mark.parametrize("kind", ["lifecycle", "capture"])
@pytest.mark.parametrize("cause_type", [ValueError, TypeError, generic.GenericSemanticsCapacity])
def test_original_nested_value_check_is_not_a_semantic_rejection(
    owner, kind, cause_type, monkeypatch
):
    owner.initialize()
    view, key, _, raw = projection(owner, kind, True)
    before, calls, parsed = owner.load(), [], []
    changes = owner.db.total_changes
    cause = cause_type("original nested execution stopped")
    step, decode = generic.Program.step, json.loads

    def decoded(value, *args, **kwargs):
        result = decode(value, *args, **kwargs)
        if value == raw:
            parsed.append(value)
        return result

    def stepped(program):
        calls.append(program)
        if len(calls) == 3:
            view[key]
        return step(program)

    def check():
        if parsed:
            raise cause

    monkeypatch.setattr(json, "loads", decoded)
    monkeypatch.setattr(generic.Program, "step", stepped)
    original = quantum(65536)
    with pytest.raises(cause_type) as raised:
        with owner.epoch.execution(check, work=original):
            try:
                owner.prepare(step_budget=64)
            except cause_type as error:
                assert owner.epoch.raised_by_check(error)
                assert not owner.epoch.raised_by_check(cause_type(str(error)))
                raise
    assert raised.value is cause and len(calls) == 3 and parsed == [raw]
    assert owner.epoch.quiescent and not owner.epoch.raised_by_check(cause)
    assert owner.db.total_changes == changes and owner.load() == before
    assert original.consumed.octets == len(raw) and original.held == WorkAmount()
    original.require_received()


@pytest.mark.parametrize("cause_type", [ValueError, generic.GenericSemanticsCapacity])
def test_original_sqlite_check_inside_semantic_step_is_not_a_rejection(
    owner, cause_type, monkeypatch
):
    owner.initialize()
    before, calls, executing, statements = owner.load(), [], [], []
    changes = owner.db.total_changes
    cause = cause_type("original SQL execution stopped")
    step = generic.Program.step

    def stepped(program):
        calls.append(program)
        if len(calls) == 3:
            executing.append(True)
            owner._one(
                "WITH RECURSIVE numbers(n) AS (VALUES(1) UNION ALL "
                "SELECT n+1 FROM numbers WHERE n<100000) SELECT sum(n) FROM numbers"
            )
        return step(program)

    def check():
        if executing:
            raise cause

    monkeypatch.setattr(generic.Program, "step", stepped)
    original = quantum(65536)
    owner.db.set_trace_callback(statements.append)
    try:
        with pytest.raises(cause_type) as raised:
            with owner.epoch.execution(check, work=original):
                owner.prepare(step_budget=64)
    finally:
        owner.db.set_trace_callback(None)
    assert raised.value is cause and len(calls) == 3
    assert any(sql.startswith("WITH RECURSIVE") for sql in statements)
    assert not any(sql.startswith(("BEGIN", "INSERT", "UPDATE", "DELETE")) for sql in statements)
    assert owner.db.total_changes == changes and owner.load() == before
    assert owner.epoch.quiescent and not owner.epoch.raised_by_check(cause)
    assert original.held == WorkAmount()
    original.require_received()


def test_semantic_value_error_remains_a_rejection_without_original_check_failure(
    owner, monkeypatch
):
    owner.initialize()
    before = owner.load()
    cause = ValueError("semantic subject is invalid")

    def rejected(program):
        raise cause

    monkeypatch.setattr(generic.Program, "step", rejected)
    original = quantum(65536)
    with owner.epoch.execution(lambda: None, work=original):
        batch = owner.prepare(step_budget=1)
        assert not owner.epoch.raised_by_check(cause)
    assert batch.before == before and batch.after.phase == "reject-frames"
    assert batch.after.error_sha256 == generic._error_identity(cause)
    assert owner.load() == before and original.held == WorkAmount()
    original.require_received()
