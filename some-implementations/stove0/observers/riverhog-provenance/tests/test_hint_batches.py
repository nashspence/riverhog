"""Batch only independent hints; retain per-subject provenance/failure semantics."""

from types import SimpleNamespace
from typing import cast

import pytest
from a_stove0_materialization_hint_evidence_contract_lib import (
    MATERIALIZATION_HINT_INTERFACE,
    MATERIALIZATION_HINT_OBSERVER_CONTRACT,
)
from a_stove0_riverhog_provenance_evidence_contract_lib import CORE_PROVENANCE_INTERFACE
from a_stove0_riverhog_provenance_observer import RiverhogProvenanceObserver
from stove0_observer_protocol import ContentObservationRequest
from stove0_observer_support import ContentObservationRuntime
from stove0_protocol import CollectionRootIdentityRef
from test_core_facts import _fixture

from tests.stove0_observation_fixtures import observation_payload


def _request(observer, subjects):
    contract = MATERIALIZATION_HINT_OBSERVER_CONTRACT
    return ContentObservationRequest.seal(
        observation_payload(
            contract=contract,
            interface=MATERIALIZATION_HINT_INTERFACE,
            work_id="e" * 64,
            observer_registration_id="canonical-hint",
            observer_descriptor_sha256=observer.descriptor().descriptor_sha256,
            observer_contract_id=contract.id,
            observer_contract_sha256=contract.contract_sha256,
            read_actions=("read-provenance",),
            subjects=subjects,
        )
    )


def test_batched_hints_equal_single_subject_observations_and_fail_closed():
    observer = RiverhogProvenanceObserver(image_id="sha256:" + "d" * 64)
    support = observer.descriptor().support_for(MATERIALIZATION_HINT_OBSERVER_CONTRACT.id)
    assert support.preferred_subject_batch_size == 16
    assert MATERIALIZATION_HINT_INTERFACE.partitioning == "independent-subjects"
    assert CORE_PROVENANCE_INTERFACE.partitioning == "whole-scope"
    subjects, provenance = [], {}
    for index in range(16):
        subject, binding, summary = _fixture(
            name=f"/source/{index}.wav",
            view_id="urn:uuid:11111111-1111-4111-8111-111111111111",
            hint={"components": [f"{index}.wav"]} if index % 2 else None,
        )
        subject = subject.model_copy(
            update={
                "id": f"subject-{index:04}",
                "collection": CollectionRootIdentityRef(
                    collection_id=str(index + 1),
                    archive_root_sha256="a" * 64,
                    artifact_set_identity="b" * 64,
                ),
            }
        )
        subjects.append(subject)
        provenance[subject.id] = binding, summary

    class Runtime:
        def __init__(self):
            self.read = []
            self.heartbeats = 0

        def heartbeat(self):
            self.heartbeats += 1

        def open_provenance(self, subject):
            self.read.append(subject)
            binding, summary = provenance[subject.id]
            return SimpleNamespace(binding=binding, bound_summary=lambda: summary)

    runtime = Runtime()
    batch = observer.observe(
        _request(observer, tuple(subjects)), cast(ContentObservationRuntime, runtime)
    )
    assert batch.state == "observed"
    assert sorted(runtime.read, key=lambda item: item.id) == subjects
    assert runtime.heartbeats >= 16
    single_facts = []
    for subject in subjects:
        single = observer.observe(
            _request(observer, (subject,)), cast(ContentObservationRuntime, Runtime())
        )
        assert single.state == "observed"
        single_facts.extend(single.facts["artifacts"])
    assert batch.facts["artifacts"] == single_facts

    class MissingLast(Runtime):
        def open_provenance(self, subject):
            if subject == subjects[-1]:
                raise RuntimeError("live exact primary is unavailable")
            return super().open_provenance(subject)

    failed_runtime = MissingLast()
    failed = observer.observe(
        _request(observer, tuple(subjects)), cast(ContentObservationRuntime, failed_runtime)
    )
    assert sorted(failed_runtime.read, key=lambda item: item.id) == subjects[:-1]
    assert failed.state == "failed" and failed.facts is None
    assert failed.failure.code == "canonical-occurrence-unavailable"


def test_hint_batch_checks_cancellation_before_each_read():
    observer = RiverhogProvenanceObserver(image_id="sha256:" + "d" * 64)
    subject, _, _ = _fixture(
        name="/source.wav", view_id="urn:uuid:11111111-1111-4111-8111-111111111111"
    )

    class Canceled(Exception):
        pass

    class Runtime:
        def heartbeat(self):
            raise Canceled("canceled before a protected read")

        def open_provenance(self, subject):
            pytest.fail("read bypassed live cancellation")

    with pytest.raises(Canceled):
        observer.observe(_request(observer, (subject,)), cast(ContentObservationRuntime, Runtime()))


def test_independent_hint_evaluation_overlaps_with_bounded_inflight_work():
    from threading import Barrier, Lock

    observer = RiverhogProvenanceObserver(image_id="sha256:" + "d" * 64)
    subjects, provenance = [], {}
    for index in range(32):
        subject, binding, summary = _fixture(
            name=f"/source/{index}.wav",
            view_id="urn:uuid:11111111-1111-4111-8111-111111111111",
        )
        subject = subject.model_copy(
            update={
                "id": f"subject-{index:04}",
                "collection": CollectionRootIdentityRef(
                    collection_id=str(index + 1),
                    archive_root_sha256="a" * 64,
                    artifact_set_identity="b" * 64,
                ),
            }
        )
        subjects.append(subject)
        provenance[subject.id] = binding, summary
    gate, lock = Barrier(4), Lock()
    active = peak = completed = 0

    def bound_summary(summary):
        nonlocal active, peak, completed
        with lock:
            active += 1
            peak = max(peak, active)
        try:
            gate.wait(timeout=5)
            return summary
        finally:
            with lock:
                active -= 1
                completed += 1

    class Runtime:
        def heartbeat(self):
            pass

        def open_provenance(self, subject):
            binding, summary = provenance[subject.id]
            return SimpleNamespace(binding=binding, bound_summary=lambda: bound_summary(summary))

    result = observer.observe(
        _request(observer, subjects), cast(ContentObservationRuntime, Runtime())
    )
    assert result.state == "observed"
    assert completed == len(subjects)
    assert peak == 4
    assert [fact["subject_id"] for fact in result.facts["artifacts"]] == [
        subject.id for subject in subjects
    ]
