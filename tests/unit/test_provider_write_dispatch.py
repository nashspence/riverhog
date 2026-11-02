from __future__ import annotations

import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

import pytest
from riverhog_core.provider_write_dispatch import ArmedProviderWrite
from riverhog_protocol.errors import Conflict


def test_provider_dispatch_imports_without_persistent_or_service_owners():
    code = """
import importlib.abc
import sys

class IndependentDispatch(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "sqlalchemy" or fullname.startswith(
            ("sqlalchemy.", "riverhog_core.catalog", "riverhog_core.services")
        ):
            raise AssertionError("dispatch imported a persistent owner")

sys.meta_path.insert(0, IndependentDispatch())
from riverhog_core.provider_write_dispatch import ArmedProviderWrite
from riverhog_protocol.errors import Conflict

ticket = ArmedProviderWrite(
    call_id="a" * 64, collection_id=1, object_id="object", nonce="private-nonce",
    request_sha256="b" * 64, executor_id="c" * 64, target_incarnation="target",
)
withdrawn = ArmedProviderWrite(
    call_id="d" * 64, collection_id=1, object_id="other", nonce="other-nonce",
    request_sha256="e" * 64, executor_id="f" * 64, target_incarnation="target",
)
proof = withdrawn.withdraw()
withdrawn._require_withdrawal(proof)
assert withdrawn.withdraw() is proof
assert ticket.dispatch(lambda: "completed") == "completed"
try:
    ticket.dispatch(lambda: "repeated")
except Conflict:
    pass
else:
    raise AssertionError("one-use dispatch was reconstructed")
"""
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def _ticket():
    return ArmedProviderWrite(
        call_id="a" * 64,
        collection_id=1,
        object_id="object",
        nonce="original-private-nonce",
        request_sha256="b" * 64,
        executor_id="c" * 64,
        target_incarnation="original-target",
    )


def test_unused_dispatch_withdrawal_retains_one_proof_and_cannot_invoke():
    ticket = _ticket()
    proof = ticket.withdraw()
    assert ticket.withdraw() is proof
    ticket._require_withdrawal(proof)
    with pytest.raises(Conflict, match="withdrawal evidence changed"):
        ticket._require_withdrawal(replace(proof))
    with pytest.raises(Conflict, match="already used"):
        ticket.dispatch(lambda: pytest.fail("withdrawn executor cannot start"))
    assert "original-private-nonce" not in repr(proof)


@pytest.mark.parametrize("fails", [False, True])
def test_running_returned_and_failed_dispatch_cannot_be_withdrawn(fails):
    ticket = _ticket()
    entered, release = threading.Event(), threading.Event()

    def invoke():
        entered.set()
        assert release.wait(timeout=5)
        if fails:
            raise OSError("injected executor failure")
        return "executed"

    with ThreadPoolExecutor(max_workers=1) as pool:
        running = pool.submit(ticket.dispatch, invoke)
        try:
            assert entered.wait(timeout=5)
            with pytest.raises(Conflict, match="already started"):
                ticket.withdraw()
        finally:
            release.set()
        if fails:
            with pytest.raises(OSError, match="executor failure"):
                running.result(timeout=5)
        else:
            assert running.result(timeout=5) == "executed"
    with pytest.raises(Conflict, match="already started"):
        ticket.withdraw()


def test_closed_capability_without_live_withdrawal_cannot_supply_never_started_proof():
    ticket = _ticket()
    ticket._close_dispatch()
    with pytest.raises(Conflict, match="already started or closed"):
        ticket.withdraw()


def test_dispatch_and_withdrawal_race_never_grants_both_owners():
    with ThreadPoolExecutor(max_workers=2) as pool:
        for _ in range(32):
            ticket = _ticket()
            barrier = threading.Barrier(3)
            calls = []

            def invoke(ticket, barrier, calls):
                barrier.wait(timeout=5)
                try:
                    ticket.dispatch(lambda: calls.append("physical-call"))
                except Conflict:
                    return "withdrawn"
                return "dispatched"

            def withdraw(ticket, barrier):
                barrier.wait(timeout=5)
                try:
                    return ticket.withdraw()
                except Conflict:
                    return None

            dispatch = pool.submit(invoke, ticket, barrier, calls)
            withdrawal = pool.submit(withdraw, ticket, barrier)
            barrier.wait(timeout=5)
            outcome, proof = dispatch.result(timeout=5), withdrawal.result(timeout=5)
            if proof is None:
                assert outcome == "dispatched" and calls == ["physical-call"]
                with pytest.raises(Conflict, match="already started"):
                    ticket.withdraw()
            else:
                assert outcome == "withdrawn" and calls == []
                ticket._require_withdrawal(proof)
                assert ticket.withdraw() is proof
