from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from threading import Barrier

import pytest
from riverhog_canonical_json import format_scalar
from riverhog_core.catalog_archive_write_models import ArchiveWriteControlRecord as Control
from riverhog_core.catalog_db import session_scope
from riverhog_core.catalog_models import CollectionUploadRecord
from riverhog_core.collection_recovery_state import RecoveryRootBinding
from riverhog_core.services.collection_recovery import CollectionRecoveryAccounting
from riverhog_protocol.errors import Conflict, NotFound
from riverhog_protocol.pack_ingress import canonical_json_bytes
from riverhog_recovery_policy import CheckpointCandidate, RecoveryPolicy
from sqlalchemy import delete, text

from tests.unit.test_archive_write_admission import _case, _guard
from tests.unit.test_recovery_policy_admission import _open, _snapshot

NS = 1_000_000_000


@contextmanager
def _root_case(tmp_path, *, policy=None, active_limit=2, control_bytes=20000, database_url=None):
    with _case(tmp_path, database_url=database_url) as case:
        case.service._config = replace(case.config, recovery=policy or RecoveryPolicy())
        case.root_id = _open(case.service, "root-recovery-accounting")
        original = _snapshot(case.factory, case.root_id)
        case.binding = RecoveryRootBinding.original(case.root_id, original[0])
        case.root_epoch = original[1]
        case.recovery = CollectionRecoveryAccounting(
            case.factory, active_grant_limit=active_limit, control_bytes=control_bytes
        )
        # This is a domain-owned selector/immutable receiving fixture. The root
        # cannot manufacture either fact from its own grant row.
        with session_scope(case.factory) as session:
            session.execute(
                text(
                    "CREATE TABLE recovery_domain_fixture "
                    "(domain TEXT PRIMARY KEY, generation TEXT)"
                )
            )
        yield case


def _candidate(*, cost=10 * NS):
    return CheckpointCandidate(
        replay_ns=200 * NS,
        restore_ns=NS,
        cost_ns=cost,
        remaining_ns=None,
        replay_provenance="fresh-useful-union",
        restore_provenance="admitted-restore-profile",
        cost_provenance="admitted-generation-profile",
        remaining_provenance="unknown-workload-extent",
    )


def _read(case):
    return case.recovery.snapshot(case.binding, guard=_guard)


def _reserve(case, *, domain="native", revision=None, useful=100 * NS, cost=10 * NS):
    return case.recovery.reserve(
        case.binding,
        epoch=case.root_epoch,
        revision=_read(case).revision if revision is None else revision,
        domain=domain,
        owner_id="workspace-identity",
        owner_fence="worker-and-exact-source-fence",
        useful_ns=useful,
        candidate=_candidate(cost=cost),
        guard=_guard,
        retain_domain=_guard,
        retain_grant=lambda session, grant: _retain_original(session, grant),
    )


def _selector(session, grant, generation):
    selected = session.execute(
        text("SELECT generation FROM recovery_domain_fixture WHERE domain=:domain"),
        {"domain": grant.domain},
    ).scalar_one_or_none()
    if selected is None:
        session.execute(
            text(
                "INSERT INTO recovery_domain_fixture (domain,generation) "
                "VALUES (:domain,:generation)"
            ),
            {"domain": grant.domain, "generation": generation},
        )
    elif selected != generation:
        raise Conflict("original selected descriptor changed")


def _select(case, grant, *, revision=None, generation="generation-a", actual=8 * NS, tail=2 * NS):
    return case.recovery.select_generation(
        case.binding,
        epoch=case.root_epoch,
        revision=_read(case).revision if revision is None else revision,
        identity=grant.identity,
        generation=generation,
        total_actual_ns=actual,
        remaining_reserved_ns=tail,
        guard=_guard,
        select_frontier=_selector,
    )


def _retain_original(session, grant):
    _guard(session)
    if grant is not None:
        assert grant.owner_id == "workspace-identity"
        assert grant.owner_fence == "worker-and-exact-source-fence"


def _receipt(*, actual, outcome):
    def receive_original(session, identity, total_actual_ns):
        assert session.in_transaction()
        assert identity
        if total_actual_ns != actual:
            raise Conflict("contradictory immutable original receipt")
        return outcome

    return receive_original


def _receive(case, grant, *, actual, outcome, revision=None):
    return case.recovery.receive_cost(
        case.binding,
        revision=_read(case).revision if revision is None else revision,
        identity=grant.identity,
        total_actual_ns=actual,
        remaining_reserved_ns=0,
        guard=_guard,
        retain_original=_retain_original,
        receive_original=_receipt(actual=actual, outcome=outcome),
    )


def _close_gate(case):
    with session_scope(case.factory) as session:
        control = session.get(Control, case.root_id)
        control.gate, control.epoch = "closed", "f" * 64


def test_original_accounting_is_bound_and_needs_no_optional_storage(tmp_path):
    with _root_case(tmp_path) as case:
        root = _read(case)
        assert root.binding == case.binding
        assert root.policy == RecoveryPolicy()
        assert root.revision == root.next_grant_sequence == 0
        assert root.accounting.reconciled
        assert root.cost_epoch_open
        assert root.grants == ()
        raw = json.loads(_snapshot(case.factory, case.root_id)[3])
        assert raw["root"] == case.binding.document()
        assert raw["held_count_slots"] == "0"


def test_original_grant_cas_competitors_cannot_spend_the_same_budget(tmp_path):
    with _root_case(tmp_path) as case:
        barrier = Barrier(2)

        def attempt(domain):
            barrier.wait(timeout=5)
            try:
                return _reserve(case, domain=domain, revision=0)[0].reason
            except Conflict:
                return "stale-root"

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(attempt, ("native", "corpus")))
        assert sorted(results) == ["checkpoint-admitted", "stale-root"]
        root = _read(case)
        assert root.revision == root.next_grant_sequence == 1
        assert root.accounting.reserved_ns == 10 * NS
        assert root.accounting.held_count_slots == 1


def test_selection_tail_overrun_and_original_receiving_are_distinct(tmp_path):
    with _root_case(tmp_path) as case:
        decision, grant, admitted = _reserve(case)
        assert decision.admitted and grant is not None
        selected = _select(case, grant)
        assert selected.accounting.successful_checkpoints == 1
        assert selected.accounting.held_count_slots == 0
        assert selected.accounting.charged_ns == 8 * NS
        assert selected.accounting.reserved_ns == 2 * NS
        assert _select(case, grant, revision=0) == selected
        with pytest.raises(Conflict, match="different generation"):
            _select(case, grant, generation="generation-b")
        with pytest.raises(Conflict, match="reread"):
            _reserve(case, domain="corpus", revision=admitted.revision)
        status, partial = case.recovery.receive_cost(
            case.binding,
            revision=selected.revision,
            identity=grant.identity,
            total_actual_ns=15 * NS,
            remaining_reserved_ns=NS,
            guard=_guard,
            retain_original=_retain_original,
        )
        assert status == "updated"
        assert partial.accounting.charged_ns == 15 * NS
        assert partial.accounting.reserved_ns == NS
        assert _reserve(case, domain="corpus", cost=NS)[0].reason == "overrun-debt"
        status, received = _receive(case, grant, actual=16 * NS, outcome="published")
        assert status == "received"
        assert received.grants == ()
        assert received.accounting.charged_ns == 16 * NS
        assert received.accounting.reserved_ns == received.accounting.held_count_slots == 0
        assert received.accounting.successful_checkpoints == 1
        with session_scope(case.factory) as session:
            assert (
                session.execute(text("SELECT generation FROM recovery_domain_fixture")).scalar_one()
                == "generation-a"
            )
        assert _receive(case, grant, actual=16 * NS, outcome="published", revision=0) == (
            "already-received",
            received,
        )
        with pytest.raises(Conflict, match="already received"):
            case.recovery.receive_cost(
                case.binding,
                revision=0,
                identity=grant.identity,
                total_actual_ns=0,
                remaining_reserved_ns=0,
                guard=_guard,
                retain_original=_retain_original,
            )
        with pytest.raises(Conflict, match="reconcile the domain"):
            _select(case, grant)


@pytest.mark.parametrize("failure_after_root_write", [False, True])
def test_domain_selector_and_root_cas_roll_back_together(
    tmp_path, monkeypatch, failure_after_root_write
):
    with _root_case(tmp_path) as case:
        _, grant, before = _reserve(case)
        original = case.recovery._save

        def interrupted(*args):
            if failure_after_root_write:
                original(*args)
            raise RuntimeError("selection interrupted before acknowledgment")

        with monkeypatch.context() as patch:
            patch.setattr(case.recovery, "_save", interrupted)
            with pytest.raises(RuntimeError, match="interrupted"):
                _select(case, grant)
        assert _read(case) == before
        with session_scope(case.factory) as session:
            assert (
                session.execute(text("SELECT count(*) FROM recovery_domain_fixture")).scalar_one()
                == 0
            )
        assert _select(case, grant).accounting.successful_checkpoints == 1


def test_count_slot_remains_until_exact_nonpublication_receiving(tmp_path):
    with _root_case(tmp_path, policy=RecoveryPolicy(max_optional_checkpoints=1)) as case:
        _, grant, root = _reserve(case)
        assert _reserve(case, domain="corpus")[0].reason == "count-limit"
        _, partial = case.recovery.receive_cost(
            case.binding,
            revision=root.revision,
            identity=grant.identity,
            total_actual_ns=3 * NS,
            remaining_reserved_ns=0,
            guard=_guard,
            retain_original=_retain_original,
        )
        assert partial.accounting.held_count_slots == 1
        assert partial.grants
        assert _reserve(case, domain="corpus")[0].reason == "count-limit"
        with pytest.raises(Conflict, match="contradicts"):
            _receive(case, grant, actual=3 * NS, outcome="published")
        _, received = _receive(case, grant, actual=3 * NS, outcome="not-published")
        assert received.accounting.charged_ns == 3 * NS
        assert (
            received.accounting.held_count_slots == received.accounting.successful_checkpoints == 0
        )
        assert _reserve(case, domain="corpus", cost=NS)[0].admitted


def test_success_count_survives_fully_received_generation_retirement(tmp_path):
    with _root_case(tmp_path, policy=RecoveryPolicy(max_optional_checkpoints=1)) as case:
        _, grant, _ = _reserve(case)
        _select(case, grant)
        _receive(case, grant, actual=10 * NS, outcome="published")
        assert _reserve(case, domain="corpus")[0].reason == "count-limit"


def test_gate_and_worker_changes_do_not_erase_original_cost_or_selection(tmp_path):
    with _root_case(tmp_path) as case:
        _, grant, _ = _reserve(case)
        _close_gate(case)
        with pytest.raises(Conflict, match="closed or changed"):
            _select(case, grant)
        with pytest.raises(Conflict, match="closed or changed"):
            _reserve(case, domain="corpus")
        status, received = _receive(case, grant, actual=4 * NS, outcome="not-published")
        assert status == "received" and received.accounting.charged_ns == 4 * NS
        # The control remains the root after the upload source is retired.
        with session_scope(case.factory) as session:
            session.execute(
                delete(CollectionUploadRecord).where(
                    CollectionUploadRecord.collection_id == case.root_id
                )
            )
        assert _read(case) == received


def test_lost_selection_reply_can_reconcile_after_gate_close_without_counting_again(tmp_path):
    with _root_case(tmp_path) as case:
        _, grant, _ = _reserve(case)
        selected = _select(case, grant)
        _close_gate(case)
        assert _select(case, grant, revision=0) == selected
        assert (
            _receive(case, grant, actual=10 * NS, outcome="published")[
                1
            ].accounting.successful_checkpoints
            == 1
        )


def test_final_duplicate_still_requires_the_same_immutable_original_receipt(tmp_path):
    with _root_case(tmp_path) as case:
        _, grant, _ = _reserve(case)
        _, received = _receive(case, grant, actual=3 * NS, outcome="not-published")
        with pytest.raises(Conflict, match="contradictory immutable"):
            case.recovery.receive_cost(
                case.binding,
                revision=0,
                identity=grant.identity,
                total_actual_ns=4 * NS,
                remaining_reserved_ns=0,
                guard=_guard,
                retain_original=_retain_original,
                receive_original=_receipt(actual=3 * NS, outcome="not-published"),
            )
        assert _read(case) == received


def test_open_epoch_restart_preserves_liabilities_and_seals_optional_work(tmp_path):
    with _root_case(tmp_path) as case:
        _, grant, admitted = _reserve(case)
        case.recovery = CollectionRecoveryAccounting(
            case.factory, active_grant_limit=2, control_bytes=20000
        )
        assert _read(case) == admitted
        sealed = case.recovery.seal_interrupted(
            case.binding, revision=admitted.revision, guard=_guard
        )
        assert sealed.grants[0].uncertain
        assert sealed.accounting.reserved_ns == 10 * NS
        assert sealed.accounting.held_count_slots == 1
        assert not sealed.accounting.reconciled
        assert _reserve(case, domain="corpus")[0].reason == "unreconciled-accounting"
        with pytest.raises(ValueError, match="fresh frontier"):
            _reserve(case, domain="corpus", useful=101 * NS)
        with pytest.raises(Conflict, match="uncertain"):
            _select(case, grant)
        _receive(case, grant, actual=4 * NS, outcome="not-published")
        _close_gate(case)
        with pytest.raises(Conflict, match="all original"):
            case.recovery.close_cost_epoch(
                case.binding, revision=_read(case).revision, guard=_guard, receive_terminal=_guard
            )
        reconciled = case.recovery.reconcile_unreserved(
            case.binding,
            revision=_read(case).revision,
            additional_actual_ns=2 * NS,
            guard=_guard,
            receive_attempts=_guard,
        )
        assert reconciled.accounting.charged_ns == 6 * NS
        assert reconciled.optional_admission_sealed and not reconciled.credit_certain
        closed = case.recovery.close_cost_epoch(
            case.binding,
            revision=reconciled.revision,
            guard=_guard,
            receive_terminal=_guard,
        )
        assert not closed.cost_epoch_open and closed.grants == ()
        assert (
            case.recovery.seal_interrupted(case.binding, revision=closed.revision, guard=_guard)
            == closed
        )


def test_failed_pregrant_work_is_charged_and_unknown_cost_cannot_be_refunded(tmp_path):
    with _root_case(tmp_path) as case:
        charged = case.recovery.charge_unreserved(
            case.binding, revision=0, actual_ns=2 * NS, guard=_guard
        )
        assert charged.accounting.charged_ns == 2 * NS
        assert charged.next_grant_sequence == 0
        sealed = case.recovery.charge_unreserved(
            case.binding, revision=charged.revision, actual_ns=None, guard=_guard
        )
        assert sealed.unreserved_uncertain and sealed.optional_admission_sealed
        assert sealed.accounting.charged_ns == 2 * NS
        assert _reserve(case)[0].reason == "unreconciled-accounting"


@pytest.mark.parametrize("capacity", ["slots", "bytes"])
def test_optional_admission_cannot_overdraw_existing_owner_capacity(tmp_path, capacity):
    with _root_case(tmp_path, active_limit=1) as case:
        if capacity == "slots":
            _reserve(case)
        else:
            raw = _snapshot(case.factory, case.root_id)[3]
            case.recovery = CollectionRecoveryAccounting(
                case.factory, active_grant_limit=1, control_bytes=len(raw.encode()) + 1
            )
        before = _read(case)
        decision, grant, _ = _reserve(case, domain="corpus", cost=NS)
        assert decision.reason == "storage-not-admitted" and grant is None
        assert _read(case) == before


@pytest.mark.parametrize(
    "corruption", ["binding", "total", "flag", "extra", "noncanonical", "format", "revision"]
)
def test_invalid_mandatory_accounting_is_never_replaced_by_replay(tmp_path, corruption):
    with _root_case(tmp_path) as case:
        before = _snapshot(case.factory, case.root_id)
        value = json.loads(before[3])
        if corruption == "binding":
            value["root"]["construction_sha256"] = "0" * 64
        elif corruption == "total":
            value["reserved_ns"] = "1"
        elif corruption == "flag":
            value["credit_certain"] = 1
        elif corruption == "extra":
            value["unowned"] = True
        elif corruption == "format":
            value["format"] = "unknown"
        raw = (
            json.dumps(value)
            if corruption == "noncanonical"
            else canonical_json_bytes(value).decode()
        )
        with session_scope(case.factory) as session:
            control = session.get(Control, case.root_id)
            control.recovery_accounting_json = raw
            if corruption == "revision":
                control.recovery_revision = "01"
        with pytest.raises(RuntimeError, match="mandatory recovery accounting"):
            _read(case)
        after = _snapshot(case.factory, case.root_id)
        assert after[0:3] == before[0:3]
        assert after[3] == raw


def test_lifetime_counters_are_exact_decimal_text_above_machine_integer_width(tmp_path):
    with _root_case(tmp_path) as case:
        huge = 10**400
        with session_scope(case.factory) as session:
            control = session.get(Control, case.root_id)
            value = json.loads(control.recovery_accounting_json)
            for name, count in {
                "useful_ns": huge * 100,
                "charged_ns": huge,
                "successful_checkpoints": huge,
                "next_grant_sequence": huge,
            }.items():
                value[name] = format_scalar("nonnegative", count)
            control.recovery_accounting_json = canonical_json_bytes(value).decode()
            control.recovery_revision = format_scalar("nonnegative", huge)
        decision, grant, admitted = _reserve(case, useful=huge * 100, cost=0)
        assert decision.admitted and grant is not None
        assert admitted.revision == admitted.next_grant_sequence == huge + 1
        _, received = _receive(case, grant, actual=0, outcome="not-published")
        assert received.accounting.charged_ns == received.accounting.successful_checkpoints == huge
        assert received.revision == huge + 2


@pytest.mark.parametrize("suffix", ["0", "01", "2", "+1", "١"])
def test_never_issued_and_noncanonical_grants_cannot_be_received(tmp_path, suffix):
    with _root_case(tmp_path) as case:
        _reserve(case)
        with pytest.raises(ValueError):
            case.recovery.receive_cost(
                case.binding,
                revision=0,
                identity=case.binding.grant_prefix + suffix,
                total_actual_ns=0,
                remaining_reserved_ns=0,
                guard=_guard,
                retain_original=_retain_original,
                receive_original=_receipt(actual=0, outcome="not-published"),
            )


def test_fresh_domain_or_permission_fence_failure_cannot_select_or_spend(tmp_path):
    with _root_case(tmp_path) as case:
        _, grant, before = _reserve(case)

        def revoked(_session, *_args):
            raise NotFound("original owner no longer authorized")

        with pytest.raises(NotFound, match="no longer authorized"):
            case.recovery.select_generation(
                case.binding,
                epoch=case.root_epoch,
                revision=before.revision,
                identity=grant.identity,
                generation="generation-a",
                total_actual_ns=0,
                remaining_reserved_ns=10 * NS,
                guard=_guard,
                select_frontier=revoked,
            )
        with pytest.raises(NotFound, match="no longer authorized"):
            case.recovery.charge_unreserved(
                case.binding, revision=before.revision, actual_ns=NS, guard=revoked
            )
        assert _read(case) == before


def test_selection_retry_rejects_a_different_original_cost_receipt(tmp_path):
    with _root_case(tmp_path) as case:
        _, grant, _ = _reserve(case)
        selected = _select(case, grant)
        with pytest.raises(Conflict, match="original cost receipt"):
            _select(case, grant, actual=9 * NS, tail=NS)
        assert _read(case) == selected


def test_concurrent_final_receiving_charges_original_once(tmp_path):
    with _root_case(tmp_path) as case:
        _, grant, admitted = _reserve(case)
        barrier = Barrier(2)

        def receive(_index):
            barrier.wait(timeout=5)
            return _receive(
                case, grant, actual=3 * NS, outcome="not-published", revision=admitted.revision
            )[0]

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(receive, range(2)))
        assert sorted(results) == ["already-received", "received"]
        root = _read(case)
        assert root.accounting.charged_ns == 3 * NS
        assert root.accounting.reserved_ns == root.accounting.held_count_slots == 0
        assert root.revision == admitted.revision + 1


def test_grant_admission_retains_domain_fence_after_control_before_spending(tmp_path):
    with _root_case(tmp_path) as case:
        before = _read(case)

        def stale(session):
            assert session.get(Control, case.root_id) is not None
            raise Conflict("domain worker no longer current")

        with pytest.raises(Conflict, match="no longer current"):
            case.recovery.reserve(
                case.binding,
                epoch=case.root_epoch,
                revision=before.revision,
                domain="native",
                owner_id="workspace-identity",
                owner_fence="obsolete",
                useful_ns=100 * NS,
                candidate=_candidate(),
                guard=_guard,
                retain_domain=stale,
                retain_grant=lambda session, grant: _retain_original(session, grant),
            )
        assert _read(case) == before


def test_partial_cost_report_requires_the_live_original_owner(tmp_path):
    with _root_case(tmp_path) as case:
        _, grant, before = _reserve(case)

        def wrong_original(_session, _grant):
            raise Conflict("not the original execution")

        with pytest.raises(Conflict, match="original execution"):
            case.recovery.receive_cost(
                case.binding,
                revision=before.revision,
                identity=grant.identity,
                total_actual_ns=2 * NS,
                remaining_reserved_ns=8 * NS,
                guard=_guard,
                retain_original=wrong_original,
            )
        assert _read(case) == before


def test_current_root_cannot_be_rebound_to_a_different_construction(tmp_path):
    with _root_case(tmp_path) as case:
        wrong = replace(case.binding, construction_sha256="0" * 64)
        with pytest.raises(Conflict, match="identity changed"):
            case.recovery.snapshot(wrong, guard=_guard)
        _, grant, before = _reserve(case)
        with pytest.raises(ValueError, match="another construction"):
            case.recovery.receive_cost(
                case.binding,
                revision=before.revision,
                identity=wrong.grant_prefix + "1",
                total_actual_ns=0,
                remaining_reserved_ns=0,
                guard=_guard,
                retain_original=_retain_original,
                receive_original=_receipt(actual=0, outcome="not-published"),
            )
        assert _read(case) == before


@pytest.mark.parametrize(
    "policy", [RecoveryPolicy(recovery_mode="restart"), RecoveryPolicy(max_optional_checkpoints=0)]
)
def test_snapshotted_policy_suppresses_optional_work_without_allocating(tmp_path, policy):
    with _root_case(tmp_path, policy=policy) as case:
        before = _read(case)
        decision, grant, root = _reserve(case)
        assert not decision.admitted and grant is None
        assert root == before == _read(case)


def test_decreasing_original_cost_and_incomplete_final_receiving_are_refused(tmp_path):
    with _root_case(tmp_path) as case:
        _, grant, _ = _reserve(case)
        selected = _select(case, grant)
        with pytest.raises(ValueError, match="cannot decrease"):
            _receive(case, grant, actual=NS, outcome="published")
        with pytest.raises(ValueError, match="whole reservation"):
            case.recovery.receive_cost(
                case.binding,
                revision=selected.revision,
                identity=grant.identity,
                total_actual_ns=8 * NS,
                remaining_reserved_ns=NS,
                guard=_guard,
                retain_original=_retain_original,
                receive_original=_receipt(actual=8 * NS, outcome="published"),
            )
        assert _read(case) == selected
