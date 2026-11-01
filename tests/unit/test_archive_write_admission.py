from __future__ import annotations

import json
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest
from riverhog_core.catalog_archive_write_models import (
    ArchiveUploadTerminalRecord,
    ArchiveWriteControlRecord,
    ArchiveWriteEffectRecord,
    ArchiveWritePreparationRecord,
)
from riverhog_core.catalog_db import dispose_session_factory, session_scope, validate_db
from riverhog_core.catalog_models import CollectionArchiveObjectUploadRecord, CollectionUploadRecord
from riverhog_core.collection_plan import CollectionVolumePolicy
from riverhog_core.services.archive_write_admission import (
    ArchiveWriteAdmission,
    ArchiveWriteGrants,
    ClosedArchiveWrite,
)
from riverhog_protocol.errors import Conflict, NotFound
from riverhog_protocol.pack_ingress import canonical_json_bytes
from sqlalchemy import event, func, select
from sqlalchemy.exc import IntegrityError

from tests.unit.test_collection_registration_operations import PART, _member
from tests.unit.test_collection_uploads import _CREATOR, _service_with_archive_objects

GRANTS = ArchiveWriteGrants(4, 20000, 10000, 10000)


def _guard(session):
    assert not any(isinstance(row, CollectionUploadRecord) for row in session.identity_map.values())


@contextmanager
def _case(tmp_path, *, database_url=None):
    policy = CollectionVolumePolicy(
        pack_member_bytes=8, raw_volume_plaintext_bytes=PART, raw_part_plaintext_bytes=PART
    )
    service, config, provider, roots = _service_with_archive_objects(
        tmp_path, policy=policy, database_url=database_url
    )
    try:
        opened = service.create_or_resume(
            idempotency_key="write-admission",
            ingest_source="fixture",
            archive_store=None,
            initiator=_CREATOR,
            event_context=None,
        )
        collection_id = int(opened["collection_id"])
        service.register_artifacts(collection_id, [_member("1", 2)])
        volume = service.list_volumes(collection_id)["volumes"][0]
        with session_scope(service._session_factory) as session:
            record = session.get(
                CollectionArchiveObjectUploadRecord, (collection_id, volume["volume_id"])
            )
            identity = {
                "kind": record.kind,
                "object_path": record.object_path,
                "plan_sha256": record.plan_sha256,
            }
            upload = session.get(CollectionUploadRecord, collection_id)
            incarnation = upload.archive_incarnation_id
        owner = ArchiveWriteAdmission(service._session_factory)
        epoch = owner.bind_epoch(collection_id, guard=_guard)
        yield SimpleNamespace(
            service=service,
            config=config,
            provider=provider,
            factory=service._session_factory,
            owner=owner,
            epoch=epoch,
            collection_id=collection_id,
            object_id=volume["volume_id"],
            identity=identity,
            incarnation=incarnation,
        )
    finally:
        service.close()
        dispose_session_factory(service._session_factory)


@pytest.fixture
def case(tmp_path):
    with _case(tmp_path) as current:
        yield current


def _prepare(case, *, preparation=None, grants=GRANTS):
    return case.owner.prepare(
        case.collection_id,
        case.object_id,
        epoch=case.epoch,
        identity=case.identity,
        preparation=preparation or {"original_preparation": "retained-before-provider"},
        grants=grants,
        guard=_guard,
    )


def _arm(case, *, kind="begin", resource="begin", grants=GRANTS, arguments=None):
    return case.owner.arm(
        case.collection_id,
        case.object_id,
        epoch=case.epoch,
        kind=kind,
        resource=resource,
        arguments=arguments
        or ({"number": int(resource.split(":")[1])} if kind == "part" else {"original": "request"}),
        executor_id="e" * 64,
        result_capacity_bytes=1000,
        grants=grants,
        guard=_guard,
    )


def _selected(case, *, kind="cancel", request=None):
    return case.owner.select_terminal(
        case.collection_id,
        kind=kind,
        request=request or {"selected": "exact-original"},
        principal_id=_CREATOR.id,
        grants=GRANTS,
        guard=_guard,
        validate_selection=lambda session, upload: None,
    )


def _closure(case, ticket, result=None):
    with session_scope(case.factory) as session:
        effect = session.get(ArchiveWriteEffectRecord, ticket.call_id)
        return ClosedArchiveWrite(
            call_id=ticket.call_id,
            request_sha256=effect.request_sha256,
            executor_id=effect.executor_id,
            target_incarnation=case.incarnation,
            result=result or {"marker": "exact-returned-result"},
        )


def _apply(session, upload, volume, result):
    volume.failure = result["marker"]


def test_original_preparation_is_retained_before_arm_and_cold_retry(case):
    original = _prepare(case)
    cold = ArchiveWriteAdmission(case.factory)
    assert (
        cold.prepare(
            case.collection_id,
            case.object_id,
            epoch=case.epoch,
            identity=case.identity,
            preparation={"different_candidate_crypto": "must-not-replace-original"},
            grants=GRANTS,
            guard=_guard,
        )
        == original
    )
    assert cold.unresolved(case.collection_id, limit=1) == ()
    with session_scope(case.factory) as session:
        assert session.scalar(select(func.count()).select_from(ArchiveWritePreparationRecord)) == 1
    assert validate_db(case.config.database_url).condition == "current"


def test_preparation_identity_and_authority_are_checked_before_new_effect(case):
    _prepare(case)
    with pytest.raises(Conflict, match="identity changed"):
        case.owner.prepare(
            case.collection_id,
            case.object_id,
            epoch=case.epoch,
            identity={**case.identity, "object_path": "different"},
            preparation={},
            grants=GRANTS,
            guard=_guard,
        )

    def deny(session):
        raise NotFound("authority revoked")

    with pytest.raises(NotFound, match="authority revoked"):
        case.owner.arm(
            case.collection_id,
            case.object_id,
            epoch=case.epoch,
            kind="begin",
            resource="begin",
            arguments={},
            executor_id="e" * 64,
            result_capacity_bytes=100,
            grants=GRANTS,
            guard=deny,
        )
    assert case.owner.unresolved(case.collection_id, limit=1) == ()


def test_no_dispatch_capability_exists_without_original_preparation(case):
    with pytest.raises(Conflict, match="preparation is missing"):
        _arm(case)
    assert case.owner.unresolved(case.collection_id, limit=1) == ()


def test_live_capability_dispatches_once_and_cold_observation_cannot_close_it(case):
    _prepare(case)
    ticket = _arm(case)
    calls = []
    assert ticket.dispatch(lambda: calls.append("sent") or "reply") == "reply"
    with pytest.raises(Conflict, match="already used"):
        ticket.dispatch(lambda: calls.append("sent-again"))
    assert calls == ["sent"]
    cold = ArchiveWriteAdmission(case.factory)
    for _ in range(3):
        cold.observe(
            case.collection_id,
            ticket.call_id,
            None,
            apply_observation=lambda session, upload, volume, result: None,
        )
    assert cold.unresolved(case.collection_id, limit=1) == (ticket.call_id,)


def test_terminal_selection_keeps_armed_begin_and_original_volume(case):
    _prepare(case)
    ticket = _arm(case)
    selected = _selected(case)
    with session_scope(case.factory) as session:
        control = session.get(ArchiveWriteControlRecord, case.collection_id)
        effect = session.get(ArchiveWriteEffectRecord, ticket.call_id)
        terminal = session.get(ArchiveUploadTerminalRecord, selected)
        assert control.gate == "closed" and control.epoch != effect.epoch
        assert terminal.phase == "draining" and terminal.frontier == "1"
        assert session.get(CollectionUploadRecord, case.collection_id) is not None
        assert (
            session.get(CollectionArchiveObjectUploadRecord, (case.collection_id, case.object_id))
            is not None
        )
    for kind in ("part", "complete", "lookup"):
        with pytest.raises(Conflict, match="admission is closed"):
            _arm(case, kind=kind, resource="part:1" if kind == "part" else kind)
    ticket.dispatch(lambda: None)
    case.owner.settle_closed(ticket, _closure(case, ticket), apply_result=_apply)
    assert case.owner.unresolved(case.collection_id, limit=1) == ()
    with session_scope(case.factory) as session:
        assert session.get(ArchiveUploadTerminalRecord, selected).phase == "draining"


def test_returned_result_and_catalog_change_commit_together_and_retry_exactly(case):
    _prepare(case)
    ticket = _arm(case)
    ticket.dispatch(lambda: None)
    evidence = _closure(case, ticket)

    def fail_after_apply(session, upload, volume, result):
        _apply(session, upload, volume, result)
        session.flush()
        raise OSError("injected checkpoint commit failure")

    with pytest.raises(OSError, match="checkpoint commit failure"):
        case.owner.settle_closed(ticket, evidence, apply_result=fail_after_apply)
    with session_scope(case.factory) as session:
        volume = session.get(
            CollectionArchiveObjectUploadRecord, (case.collection_id, case.object_id)
        )
        assert volume.failure is None
        assert session.get(ArchiveWriteEffectRecord, ticket.call_id).unresolved
    cold = ArchiveWriteAdmission(case.factory)
    cold.settle_closed(ticket, evidence, apply_result=_apply)
    cold.settle_closed(
        ticket,
        evidence,
        apply_result=lambda *args: pytest.fail("confirmed report must not be applied twice"),
    )
    with session_scope(case.factory) as session:
        assert (
            session.get(
                CollectionArchiveObjectUploadRecord, (case.collection_id, case.object_id)
            ).failure
            == evidence.result["marker"]
        )
    with pytest.raises(Conflict, match="confirmed outcome changed"):
        cold.settle_closed(
            ticket, replace(evidence, result={"marker": "different"}), apply_result=_apply
        )


@pytest.mark.parametrize(
    "field", ["call_id", "request_sha256", "executor_id", "target_incarnation"]
)
def test_closure_must_match_exact_original_call_and_target(case, field):
    _prepare(case)
    ticket = _arm(case)
    evidence = replace(_closure(case, ticket), **{field: "f" * 64})
    with pytest.raises(Conflict, match="evidence identity changed"):
        case.owner.settle_closed(ticket, evidence, apply_result=_apply)
    assert case.owner.unresolved(case.collection_id, limit=1) == (ticket.call_id,)


def test_positive_observation_keeps_execution_uncertainty(case):
    _prepare(case)
    ticket = _arm(case, kind="complete", resource="complete")
    _selected(case)
    case.owner.observe(
        case.collection_id,
        ticket.call_id,
        {"marker": "observed-return"},
        apply_observation=_apply,
    )
    case.owner.record_unknown(ticket)
    assert case.owner.unresolved(case.collection_id, limit=1) == (ticket.call_id,)
    with pytest.raises(Conflict, match="cannot release"):
        case.owner.acknowledge_report(ticket)
    with session_scope(case.factory) as session:
        assert session.get(ArchiveWriteEffectRecord, ticket.call_id).state == "uncertain"
        assert (
            session.get(
                CollectionArchiveObjectUploadRecord, (case.collection_id, case.object_id)
            ).failure
            == "observed-return"
        )


def test_part_lanes_keep_identity_exclusion_and_completion_single_flight(case):
    _prepare(case)
    first = _arm(case, kind="part", resource="part:1")
    second = _arm(case, kind="part", resource="part:2")
    assert set(case.owner.unresolved(case.collection_id, limit=4)) == {
        first.call_id,
        second.call_id,
    }
    for kind, resource in [("part", "part:1"), ("begin", "begin"), ("complete", "complete")]:
        with pytest.raises(Conflict, match="already unresolved"):
            _arm(case, kind=kind, resource=resource)


def test_retained_effect_reservations_cover_calls_and_future_result_bytes(case):
    _prepare(case)
    ticket = _arm(case, grants=replace(GRANTS, calls=1))
    case.owner.settle_closed(ticket, _closure(case, ticket), apply_result=_apply)
    with pytest.raises(Conflict, match="grant is exhausted"):
        _arm(case, kind="part", resource="part:2", grants=replace(GRANTS, calls=1))
    case.owner.acknowledge_report(ticket)
    next_ticket = _arm(case, grants=replace(GRANTS, calls=1))
    assert next_ticket.call_id != ticket.call_id
    with pytest.raises(Conflict, match="grant is exhausted"):
        _arm(
            case,
            kind="part",
            resource="part:3",
            grants=replace(GRANTS, control_bytes=1),
        )


def test_oversize_preparation_terminal_and_result_keep_original_state(case):
    with pytest.raises(Conflict, match="preparation exceeds"):
        _prepare(case, grants=replace(GRANTS, preparation_bytes=1))
    _prepare(case)
    with pytest.raises(Conflict, match="terminal request exceeds"):
        case.owner.select_terminal(
            case.collection_id,
            kind="cancel",
            request={},
            principal_id=_CREATOR.id,
            grants=replace(GRANTS, terminal_bytes=1),
            guard=_guard,
            validate_selection=lambda session, upload: None,
        )
    ticket = _arm(case)
    with pytest.raises(Conflict, match="reserved byte grant"):
        case.owner.settle_closed(
            ticket, _closure(case, ticket, {"marker": "x" * 1001}), apply_result=_apply
        )
    assert case.owner.unresolved(case.collection_id, limit=1) == (ticket.call_id,)


def test_terminal_exact_selection_replays_and_changed_kind_or_request_conflicts(case):
    _prepare(case)
    operation = _selected(case, kind="discard", request={"challenge": "original"})
    assert _selected(case, kind="discard", request={"challenge": "original"}) == operation
    for kind, request in [("cancel", {"challenge": "original"}), ("discard", {"challenge": "new"})]:
        with pytest.raises(Conflict, match="different"):
            _selected(case, kind=kind, request=request)


def test_concurrent_terminal_requests_have_one_durable_selection(case):
    _prepare(case)
    ready = threading.Barrier(2)

    def select_kind(kind):
        ready.wait(10)
        try:
            return "selected", _selected(case, kind=kind)
        except Conflict:
            return "conflict", None

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(select_kind, ("cancel", "discard")))
    assert sorted(row[0] for row in results) == ["conflict", "selected"]
    with session_scope(case.factory) as session:
        assert session.scalar(select(func.count()).select_from(ArchiveUploadTerminalRecord)) == 1


def test_cold_owner_retains_effect_after_actual_dispatch_process_exit(case):
    _prepare(case)
    code = """
import os,sys
from riverhog_core.catalog_db import make_session_factory
from riverhog_core.services.archive_write_admission import ArchiveWriteAdmission,ArchiveWriteGrants
owner=ArchiveWriteAdmission(make_session_factory(sys.argv[1]))
owner.arm(int(sys.argv[2]),sys.argv[3],epoch=sys.argv[4],kind="begin",resource="begin",arguments={},
 executor_id="e"*64,result_capacity_bytes=1000,
 grants=ArchiveWriteGrants(4,20000,10000,10000),guard=lambda session: None)
os._exit(71)
"""
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            code,
            case.config.database_url,
            str(case.collection_id),
            case.object_id,
            case.epoch,
        ],
        timeout=15,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 71
    cold = ArchiveWriteAdmission(case.factory)
    unresolved = cold.unresolved(case.collection_id, limit=1)
    assert len(unresolved) == 1
    _selected(case)
    assert cold.unresolved(case.collection_id, limit=1) == unresolved
    assert case.provider.uploads == {}


def test_pending_lookup_is_indexed_and_settled_reports_compact_without_retry_history(case):
    _prepare(case)
    for _ in range(17):
        ticket = _arm(case)
        ticket.dispatch(lambda: None)
        case.owner.settle_closed(ticket, _closure(case, ticket), apply_result=_apply)
        case.owner.acknowledge_report(ticket)
    with session_scope(case.factory) as session:
        assert session.scalar(select(func.count()).select_from(ArchiveWriteEffectRecord)) == 0
    pending = _arm(case)
    statements = []
    engine = case.factory.kw["bind"]

    def record(connection, cursor, statement, parameters, context, executemany):
        if statement.lstrip().upper().startswith("SELECT"):
            statements.append((statement, parameters))

    event.listen(engine, "before_cursor_execute", record)
    try:
        assert case.owner.unresolved(case.collection_id, limit=1) == (pending.call_id,)
    finally:
        event.remove(engine, "before_cursor_execute", record)
    assert len(statements) == 1
    with engine.connect() as connection:
        rows = connection.exec_driver_sql(
            "EXPLAIN QUERY PLAN " + statements[0][0], statements[0][1]
        ).all()
    assert any("ix_archive_write_effects_pending" in row[-1] for row in rows)
    assert all("SCAN " not in row[-1] and "TEMP B-TREE" not in row[-1] for row in rows)


def test_preparation_and_terminal_records_do_not_cascade_away_with_upload(case):
    _prepare(case)
    selected = _selected(case)
    with session_scope(case.factory) as session:
        upload = session.get(CollectionUploadRecord, case.collection_id)
        session.delete(upload)
    with session_scope(case.factory) as session:
        assert session.get(ArchiveWritePreparationRecord, (case.collection_id, case.object_id))
        assert session.get(ArchiveUploadTerminalRecord, selected)
    with pytest.raises(IntegrityError), session_scope(case.factory) as session:
        session.delete(session.get(ArchiveWriteControlRecord, case.collection_id))
        session.flush()
    assert _selected(case) == selected


def test_absent_control_never_synthetically_settles_legacy_untracked_execution(case):
    with session_scope(case.factory) as session:
        session.delete(session.get(ArchiveWriteControlRecord, case.collection_id))
    with pytest.raises(Conflict, match="owner is uninitialized"):
        _prepare(case)
    assert case.owner.unresolved(case.collection_id, limit=1) == ()


def test_call_frontier_does_not_wrap_at_database_integer_width(case):
    _prepare(case)
    with session_scope(case.factory) as session:
        session.get(ArchiveWriteControlRecord, case.collection_id).next_call = str(2**127)
    ticket = _arm(case)
    with session_scope(case.factory) as session:
        assert session.get(ArchiveWriteEffectRecord, ticket.call_id).sequence == str(2**127)
        assert session.get(ArchiveWriteControlRecord, case.collection_id).next_call == str(
            2**127 + 1
        )


def test_admission_does_not_load_unrelated_planner_or_checkpoint_history(case):
    _prepare(case)
    statements = []
    engine = case.factory.kw["bind"]

    def record(connection, cursor, statement, parameters, context, executemany):
        if statement.lstrip().upper().startswith("SELECT"):
            statements.append(statement)

    event.listen(engine, "before_cursor_execute", record)
    try:
        _arm(case)
        _selected(case)
    finally:
        event.remove(engine, "before_cursor_execute", record)
    construction_reads = [s for s in statements if "FROM collection_uploads " in s]
    volume_reads = [s for s in statements if "FROM collection_archive_object_uploads " in s]
    assert construction_reads and volume_reads
    assert all("planner_checkpoint_json" not in s for s in construction_reads)
    assert all("completion_receipt_json" not in s for s in construction_reads)
    assert all("checkpoint_json" not in s and ".plan_json" not in s for s in volume_reads)


def test_selected_exact_terminal_retry_keeps_original_plan_but_checks_current_access(case):
    _prepare(case)
    state = {"expired": False, "revoked": False, "selections": 0}

    def current_access(session):
        if state["revoked"]:
            raise NotFound("current terminal access is unavailable")

    def new_selection(session, upload):
        if state["expired"]:
            raise Conflict("new selection plan expired")
        state["selections"] += 1

    def select():
        return case.owner.select_terminal(
            case.collection_id,
            kind="discard",
            request={"challenge": "original"},
            principal_id=_CREATOR.id,
            grants=GRANTS,
            guard=current_access,
            validate_selection=new_selection,
        )

    operation = select()
    state["expired"] = True
    assert select() == operation
    assert state["selections"] == 1
    state["revoked"] = True
    with pytest.raises(NotFound, match="current terminal access"):
        select()


def test_changed_volume_cannot_reuse_original_preparation_for_dispatch(case):
    _prepare(case)
    with session_scope(case.factory) as session:
        session.get(
            CollectionArchiveObjectUploadRecord, (case.collection_id, case.object_id)
        ).plan_sha256 = "f" * 64
    with pytest.raises(Conflict, match="prepared volume identity changed"):
        _arm(case)
    assert case.owner.unresolved(case.collection_id, limit=1) == ()


def test_closed_unused_dispatch_cannot_invoke_a_late_provider_effect(case):
    _prepare(case)
    ticket = _arm(case)
    case.owner.settle_closed(ticket, _closure(case, ticket), apply_result=_apply)
    with pytest.raises(Conflict, match="already used"):
        ticket.dispatch(lambda: pytest.fail("settled dispatch cannot mutate a provider"))


def test_running_dispatch_keeps_its_reservation_until_actual_return(case):
    _prepare(case)
    ticket = _arm(case)
    entered = threading.Event()
    release = threading.Event()

    def held():
        entered.set()
        if not release.wait(10):
            raise AssertionError("dispatch fixture release deadline")

    with ThreadPoolExecutor(max_workers=1) as pool:
        running = pool.submit(ticket.dispatch, held)
        try:
            assert entered.wait(10)
            with pytest.raises(Conflict, match="executor is still running"):
                case.owner.settle_closed(ticket, _closure(case, ticket), apply_result=_apply)
            assert case.owner.unresolved(case.collection_id, limit=1) == (ticket.call_id,)
            _selected(case)
        finally:
            release.set()
        running.result(timeout=10)
    case.owner.settle_closed(ticket, _closure(case, ticket), apply_result=_apply)
    assert case.owner.unresolved(case.collection_id, limit=1) == ()


def _assert_actor_epoch_is_pinned(case):
    original = _prepare(case)
    old_epoch = case.epoch
    with session_scope(case.factory) as session:
        control = session.get(ArchiveWriteControlRecord, case.collection_id)
        control.epoch = "f" * 64 if old_epoch != "f" * 64 else "d" * 64
    fresh = case.owner.bind_epoch(case.collection_id, guard=_guard)
    assert fresh != old_epoch
    with pytest.raises(Conflict, match="epoch changed"):
        _prepare(case, preparation={"replacement": "must-not-be-retained"})
    with pytest.raises(Conflict, match="epoch changed"):
        _arm(case)
    with pytest.raises(Conflict, match="epoch changed"):
        case.owner.load_preparation(
            case.collection_id,
            case.object_id,
            epoch=old_epoch,
            identity=case.identity,
            guard=_guard,
        )
    assert case.owner.unresolved(case.collection_id, limit=1) == ()
    case.epoch = fresh
    assert _prepare(case, preparation={"replacement": "must-not-be-retained"}) == original
    ticket = _arm(case)
    with session_scope(case.factory) as session:
        effect = session.get(ArchiveWriteEffectRecord, ticket.call_id)
        assert effect.epoch == fresh
        assert (
            session.get(
                ArchiveWritePreparationRecord, (case.collection_id, case.object_id)
            ).preparation_json
            == original
        )


def test_actor_cannot_gain_new_epoch_by_reusing_original_preparation(case):
    _assert_actor_epoch_is_pinned(case)


def test_epoch_binding_requires_current_authority_and_open_gate(case):
    def deny(session):
        raise NotFound("authority revoked")

    with pytest.raises(NotFound, match="authority revoked"):
        case.owner.bind_epoch(case.collection_id, guard=deny)
    assert case.owner.bind_epoch(case.collection_id, guard=_guard) == case.epoch
    _selected(case)
    with pytest.raises(Conflict, match="admission is closed"):
        case.owner.bind_epoch(case.collection_id, guard=_guard)
    with pytest.raises(Conflict, match="admission is closed"):
        _prepare(case)


def test_epoch_binding_never_synthesizes_missing_control(case):
    with session_scope(case.factory) as session:
        session.delete(session.get(ArchiveWriteControlRecord, case.collection_id))
    with pytest.raises(Conflict, match="owner is uninitialized"):
        case.owner.bind_epoch(case.collection_id, guard=_guard)


def _assert_original_crypto_precedes_begin(case, monkeypatch):
    from riverhog_age import UploadState
    from riverhog_core.pack_volume import parse_pack_volume_plan
    from riverhog_core.raw_volume import parse_raw_volume_plan

    with session_scope(case.factory) as session:
        record = session.get(
            CollectionArchiveObjectUploadRecord, (case.collection_id, case.object_id)
        )
        upload = session.get(CollectionUploadRecord, case.collection_id)
        kind = record.kind
        relative_path = record.relative_path
        plan_json = record.plan_json
        passphrase_id = upload.passphrase_id
        part_bytes = record.unit_plaintext_bytes
    if kind == "pack":
        plan = parse_pack_volume_plan(plan_json)
        uploader = case.service._pack_uploader(case.provider, passphrase_id=passphrase_id)
        extra = {}
    else:
        assert kind == "segment"
        plan = parse_raw_volume_plan(plan_json)
        uploader = case.service._raw_uploader(case.provider, passphrase_id=passphrase_id)
        extra = {"target_part_plaintext_bytes": part_bytes}
    original = uploader.prepare(plan=plan).to_json_bytes()
    retained = case.owner.prepare(
        case.collection_id,
        case.object_id,
        epoch=case.epoch,
        identity=case.identity,
        preparation={"age_state": json.loads(original)},
        grants=GRANTS,
        guard=_guard,
    )
    assert not case.provider.uploads
    cold = ArchiveWriteAdmission(case.factory)
    loaded = cold.load_preparation(
        case.collection_id,
        case.object_id,
        epoch=case.epoch,
        identity=case.identity,
        guard=_guard,
    )
    unchanged = loaded == retained
    assert unchanged
    begin = case.provider.begin_write
    observed = []

    def inspect_committed_preparation(**arguments):
        with session_scope(case.factory) as session:
            prepared = session.get(
                ArchiveWritePreparationRecord, (case.collection_id, case.object_id)
            )
            unchanged = prepared.preparation_json == retained
            assert unchanged
            record = session.get(
                CollectionArchiveObjectUploadRecord, (case.collection_id, case.object_id)
            )
            assert record.checkpoint_json is None
        observed.append("preparation-visible-before-begin")
        return begin(**arguments)

    monkeypatch.setattr(case.provider, "begin_write", inspect_committed_preparation)
    restored = UploadState.from_json_bytes(canonical_json_bytes(json.loads(retained)["age_state"]))
    checkpoint = uploader.open(
        collection_id=case.collection_id,
        plan=plan,
        object_path=case.identity["object_path"],
        relative_path=relative_path,
        prepared_state=restored,
        **extra,
    )
    unchanged = checkpoint.age_state_json.encode() == original
    assert unchanged and observed == ["preparation-visible-before-begin"]


def test_original_crypto_is_committed_before_provider_begin(case, monkeypatch):
    _assert_original_crypto_precedes_begin(case, monkeypatch)


def test_preparation_lookup_creates_neither_preparation_nor_dispatch_right(case):
    def load(*, identity=None, guard=_guard):
        return case.owner.load_preparation(
            case.collection_id,
            case.object_id,
            epoch=case.epoch,
            identity=case.identity if identity is None else identity,
            guard=guard,
        )

    assert load() is None
    with session_scope(case.factory) as session:
        assert session.scalar(select(func.count()).select_from(ArchiveWritePreparationRecord)) == 0
    retained = _prepare(case)
    assert load() == retained
    assert case.owner.unresolved(case.collection_id, limit=1) == ()
    assert not case.provider.uploads
    with pytest.raises(Conflict, match="identity changed"):
        load(identity={**case.identity, "object_path": "different"})

    def deny(session):
        raise NotFound("current preparation authority unavailable")

    with pytest.raises(NotFound, match="current preparation authority"):
        load(guard=deny)
    _selected(case)
    with pytest.raises(Conflict, match="admission is closed"):
        load()
