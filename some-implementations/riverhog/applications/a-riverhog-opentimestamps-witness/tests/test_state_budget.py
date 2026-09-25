# SPDX-License-Identifier: Apache-2.0
"""Persistence budgets are checked before effects and before accepting merged evidence."""

from dataclasses import replace

import pytest
from a_riverhog_opentimestamps_witness import proof as core
from opentimestamps.core.notary import PendingAttestation
from opentimestamps.core.op import OpAppend
from opentimestamps.core.timestamp import Timestamp
from test_proof import ALLOW, DIGEST, A, Calendar, initial, raw


def test_merged_proof_must_fit_serialized_state_budget(monkeypatch):
    job = initial()
    # The wire proof is small enough, but its hex-encoded persisted form is not.
    node = Timestamp(job.submission_commitment)
    node.ops.add(OpAppend(b"x" * 1000)).attestations.add(PendingAttestation(A))
    monkeypatch.setattr(core, "MAX_STATE", len(core.dump_job(job)) + 500)
    failed = core.step(job, Calendar(raw(node)), now=100, allow=ALLOW)
    assert failed.proof is None
    assert failed.work[0].error == "bad_proof"
    assert failed.work[0].attempts == 1
    assert failed.work[0].due == 160
    assert core.load_job(core.dump_job(failed), DIGEST) == failed


def test_oversized_input_is_rejected_before_network(monkeypatch):
    job = initial()
    monkeypatch.setattr(core, "MAX_STATE", len(core.dump_job(job)) - 1)
    calendar = Calendar(b"must not be used")
    with pytest.raises(core.StateError):
        core.step(job, calendar, now=100, allow=ALLOW)
    assert not calendar.calls


def test_allowlist_discovery_must_fit_before_network(monkeypatch):
    node = Timestamp(DIGEST)
    node.ops.add(OpAppend(b"x" * 1000)).attestations.add(PendingAttestation(A))
    job = replace(initial(), proof=core.write_proof(node), work=())
    monkeypatch.setattr(core, "MAX_STATE", len(core.dump_job(job)) + 100)
    calendar = Calendar(b"must not be used")
    with pytest.raises(core.StateError):
        core.step(job, calendar, now=100, allow=ALLOW)
    assert not calendar.calls
