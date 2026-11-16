"""Focused real-source boundaries required by #981 and the #997 proposal."""

from __future__ import annotations

import json
import os
from dataclasses import replace
from unittest.mock import patch

import pytest
from prepared_primary_experiment import (
    PAYLOAD,
    ROLES,
    PublicationApiFixture,
    ReadLeaseNeedsRecovery,
    WarmControlReadLease,
    measure_work,
    member,
    open_store,
    prepare,
    receipt_for,
    role_path,
    run_experiment,
)
from riverhog_client._prepared_primary_documents import PreparedPrimary, primary_control_bytes
from riverhog_client._prepared_primary_publisher import PrimaryPublisher
from riverhog_client._prepared_primary_types import PrimaryStateError
from riverhog_protocol import CollectionUploadCustodyObjectDocument
from riverhog_provenance import BoundedSourceObserver, BytesSource

pytestmark = pytest.mark.skipif(os.name != "posix", reason="actual POSIX backend experiment")


@pytest.fixture(scope="module")
def observation(tmp_path_factory):
    root = tmp_path_factory.mktemp("997-observer")
    with patch.dict(os.environ, {"RIVERHOG_PROVENANCE_STATE_HOME": str(root / "identity")}):
        yield BoundedSourceObserver().observe(BytesSource(PAYLOAD))


def retained(tmp_path, observation, *, decided=False):
    root = tmp_path / "primary"
    api = PublicationApiFixture()
    with open_store(root, initialize=True) as store:
        publisher = PrimaryPublisher(api, store)
        record = prepare(publisher, observation)
        if decided:
            api.member_exists = True
            record = publisher.replay(record)
        paths = {role: role_path(store, role) for role in ROLES}
        pending = store.usage.pending_bytes
    return root, record, api, paths, pending


def assert_control_only(sample):
    # POSIX reclamation may open each role to check its private inode before unlink.
    # It must not open a content reader, read/hash any preimage, or enumerate members.
    assert sample.counters["preimage_reader_opens"] == 0
    assert sample.counters["preimage_read_bytes"] == 0
    assert sample.counters["directory_scans"] == 0
    assert sample.counters["path_iterations"] == 0


def test_real_native_retry_preserves_original_journal_decision_and_complete_receipt(tmp_path):
    report = run_experiment(tmp_path / "run")
    assert report["observer_calls"] == report["distinct_staged_journals"] == 1
    assert report["stage_calls"] == report["bind_calls"] == report["exact_decision_calls"] == 2
    assert report["original_preimage_bytes"] > len(PAYLOAD)
    assert len(report["original_journal_sha256"]) == len(report["original_manifest_sha256"]) == 64
    json.dumps(report)  # The deliverable is complete structured evidence, not a log fragment.
    phases = report["samples"]
    baseline = phases["current_warm_controls"]
    entry = phases["proposed_read_epoch_entry"]
    warm = phases["proposed_warm_controls"]
    assert baseline["fsync_calls"] > entry["fsync_calls"] > 0
    assert warm.get("fsync_calls", 0) == 0
    assert entry["fsync_calls"] + warm.get("fsync_calls", 0) < baseline["fsync_calls"]
    assert warm.get("os_write_bytes", 0) == warm.get("unlinks", 0) == 0
    assert warm["primary_read_bytes"] == baseline["primary_read_bytes"]
    settled = phases["cold_receipt_then_reclaim_and_retry"]
    assert settled["preimage_reader_opens"] == settled["preimage_read_bytes"] == 0
    assert settled["fsync_calls"] > 0  # Original receipt/reclamation barriers remain real.


@pytest.mark.parametrize("role", ROLES)
@pytest.mark.parametrize("damage", ["missing", "truncated", "changed"])
def test_receipt_only_accepts_original_controls_but_never_invents_decision_ack(
    tmp_path, observation, role, damage
):
    root, original, api, paths, pending = retained(tmp_path, observation)
    path = paths[role]
    content = path.read_bytes()
    if damage == "missing":
        path.unlink()
    else:
        path.write_bytes(content[:1] if damage == "truncated" else b"x" * len(content))
    with measure_work() as sample:
        with open_store(root) as store:
            assert store.load_controls(member()) == original
            received = store.accept_custody(original, receipt_for(original))
            assert received.custody is not None and received.decision_accepted is None
            assert not received.compact and not store.reclaim(member())
            assert store.usage.pending_bytes == pending
    assert_control_only(sample)
    with open_store(root) as store:
        assert store.load_controls(member()) == received
        with pytest.raises(PrimaryStateError):
            PrimaryPublisher(api, store).replay(received)
        assert not api.journals and not api.decision_calls
        assert not store.reclaim(member())
        assert store.usage.pending_bytes == pending


def test_two_original_acknowledgements_reclaim_original_reserved_lengths_once(
    tmp_path, observation
):
    root, original, _, paths, pending = retained(tmp_path, observation, decided=True)
    paths["journal"].write_bytes(b"short")
    with measure_work() as sample:
        with open_store(root) as store:
            received = store.accept_custody(original, receipt_for(original))
            assert received.compact and store.usage.pending_bytes == pending
            assert store.reclaim(member())
            assert store.usage.pending_bytes == 0
            assert store.reclaim(member())
            assert store.usage.pending_bytes == 0
            assert store.load_controls(member()) == received
    assert_control_only(sample)
    assert all(not path.exists() for path in paths.values())
    with open_store(root) as store:
        assert store.load_controls(member()) == received
        assert store.reclaim(member()) and store.usage.pending_bytes == 0


@pytest.mark.parametrize("changed", ["collection", "member", "primary", "completion", "self-seal"])
def test_complete_receipt_equality_and_self_commitment_cannot_be_skipped(
    tmp_path, observation, changed
):
    root, original, _, _, pending = retained(tmp_path, observation)
    if changed == "collection":
        receipt = receipt_for(original, collection_id=43)
    elif changed == "member":
        binding = original.prepared.primary.model_copy(update={"artifact_id": "2" * 64})
        receipt = receipt_for(original, artifact_id="2" * 64, primary=binding)
    elif changed == "primary":
        binding = original.prepared.primary.model_copy(
            update={"delivery_association_id": "urn:uuid:aaaabbbb-1234-4234-8234-123456789abc"}
        )
        receipt = receipt_for(original, primary=binding)
    elif changed == "completion":
        receipt = receipt_for(original, completion_requirement_sha256="e" * 64)
    else:
        # model_copy deliberately bypasses validation; the production receiver must reparse it.
        receipt = receipt_for(original).model_copy(update={"receipt_sha256": "f" * 64})
    with measure_work() as sample:
        with open_store(root) as store:
            with pytest.raises((ValueError, PrimaryStateError)):
                store.accept_custody(original, receipt)
            assert store.load_controls(member()) == original
            assert store.usage.pending_bytes == pending
    assert_control_only(sample)


def test_second_otherwise_valid_receipt_cannot_replace_original_proof(tmp_path, observation):
    root, original, _, _, _ = retained(tmp_path, observation)
    first = receipt_for(original)
    second = receipt_for(
        original,
        archive_objects=(
            CollectionUploadCustodyObjectDocument(
                volume_id="pack-" + "0" * 64, sealed_receipt_sha256="e" * 64
            ),
        ),
    )
    with open_store(root) as store:
        received = store.accept_custody(original, first)
        assert store.accept_custody(original, first) == received
        with pytest.raises(PrimaryStateError, match="conflicts"):
            store.accept_custody(original, second)
    with open_store(root) as store:
        assert store.load_controls(member()) == received
        assert received.custody.receipt_sha256 == first.receipt_sha256


def test_stale_prepared_handle_cannot_select_receipt_authority(tmp_path, observation):
    root, original, _, _, _ = retained(tmp_path, observation)
    fields = original.prepared.model_dump(mode="json", exclude={"manifest_sha256"})
    fields["request"]["sha256"] = "e" * 64
    stale = replace(original, prepared=PreparedPrimary.seal(**fields))
    with open_store(root) as store:
        with pytest.raises(PrimaryStateError, match="matching manifest"):
            store.accept_custody(stale, receipt_for(original))
        assert store.load_controls(member()) == original


def test_changed_retry_descriptor_fails_before_observer_or_remote_mutation(tmp_path, observation):
    root, original, api, _, _ = retained(tmp_path, observation)
    before = api.session_reads
    with open_store(root) as store:
        with pytest.raises(PrimaryStateError, match="inputs"):
            prepare(PrimaryPublisher(api, store), observation, version="another-version")
        assert store.load_controls(member()) == original
    assert api.session_reads == before and not api.journals and not api.decision_calls


def test_warm_lease_rejects_write_and_requires_real_barrier_for_later_receipt(
    tmp_path, observation
):
    root, original, _, _, _ = retained(tmp_path, observation)
    with open_store(root) as store:
        with WarmControlReadLease(store) as lease:
            with measure_work() as reads:
                assert lease.read() == lease.read() == original
            assert_control_only(reads)
            assert reads.counters["fsync_calls"] == 0
            with pytest.raises(ReadLeaseNeedsRecovery, match="cannot publish"):
                store.accept_custody(original, receipt_for(original))
            with pytest.raises(ReadLeaseNeedsRecovery, match="invalidated"):
                lease.read()
        with measure_work() as receipt:
            received = store.accept_custody(original, receipt_for(original))
            assert received.custody is not None
        assert receipt.counters["fsync_calls"] > 0


def test_warm_lease_refuses_candidate_recovery_instead_of_hiding_unknown_write(
    tmp_path, observation
):
    root, original, _, paths, _ = retained(tmp_path, observation)
    candidate = paths["journal"].parent / "candidate-decision-accepted"
    with open_store(root) as store:
        with WarmControlReadLease(store) as lease:
            candidate.touch(mode=0o600)
            with pytest.raises(ReadLeaseNeedsRecovery, match="candidate"):
                lease.read()
            assert candidate.exists()
        with measure_work() as recovery:
            assert store.load_controls(member()) == original
        assert not candidate.exists()
        assert recovery.counters["fsync_calls"] > 0


def test_warm_lease_preserves_original_manifest_validation(tmp_path, observation):
    root, original, _, paths, _ = retained(tmp_path, observation)
    manifest = paths["journal"].parent / "prepared"
    with open_store(root) as store:
        with WarmControlReadLease(store) as lease:
            fields = json.loads(primary_control_bytes(original.prepared))
            fields["request"]["sha256"] = "e" * 64
            manifest.write_text(json.dumps(fields))
            with pytest.raises((PrimaryStateError, ValueError)):
                lease.read()


@pytest.mark.parametrize("mutation", ["new-store", "new-child", "cleanup"])
def test_warm_lease_prevents_creation_and_cleanup_before_any_filesystem_mutation(
    tmp_path, observation, mutation
):
    root, original, _, _, _ = retained(tmp_path, observation)
    other = tmp_path / "other-primary"
    with open_store(root) as store:
        with WarmControlReadLease(store) as lease:
            with measure_work() as sample:
                with pytest.raises(ReadLeaseNeedsRecovery):
                    if mutation == "new-store":
                        open_store(other, initialize=True)
                    elif mutation == "new-child":
                        store._files.child("new-child", initialize=True)
                    else:
                        store._files.remove("session")
            assert sample.counters["fsync_calls"] == 0
            assert sample.counters["os_write_bytes"] == sample.counters["unlinks"] == 0
            with pytest.raises(ReadLeaseNeedsRecovery, match="invalidated"):
                lease.read()
        assert store.load_controls(member()) == original
    assert not other.exists() and not (root / "new-child").exists()


def test_cold_reopen_and_identical_receipt_retry_keep_original_durability_barriers(
    tmp_path, observation
):
    root, original, _, _, _ = retained(tmp_path, observation)
    with open_store(root) as store:
        received = store.accept_custody(original, receipt_for(original))
        with WarmControlReadLease(store) as lease:
            assert lease.read() == received
    with measure_work() as cold:
        store = open_store(root)
        assert store.load_controls(member()) == received
    try:
        with measure_work() as retry:
            assert store.accept_custody(original, receipt_for(original)) == received
        assert cold.counters["fsync_calls"] > 0 and retry.counters["fsync_calls"] > 0
        assert_control_only(cold)
        assert_control_only(retry)
    finally:
        store.close()
