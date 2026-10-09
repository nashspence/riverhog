"""Receipt polling must not repeatedly register an ever-growing open pack."""

from __future__ import annotations

import hashlib
from pathlib import Path
from types import SimpleNamespace

import pytest
from riverhog_client import ProducerArtifactIdentity, ProducerFile
from riverhog_protocol import ArtifactId
from riverhog_protocol.errors import Forbidden

from tests.unit.test_incremental_collection_producer import _CustodyApi, _producer


class PackApi(_CustodyApi):
    """Model independent payload sealing and later history/custody verification."""

    def __init__(self, *, pack_members: int = 10000) -> None:
        super().__init__()
        self.pack_members = pack_members
        self.pending_seals: set[str] = set()
        self.sealed: set[str] = set()
        self.ready: set[str] = set()
        self.registered_members = 0
        self.reads: list[str] = []
        self.read_failure: Exception | None = None
        self.forged_digest = False

    def _receipt(self, row):
        if row["artifact_id"] not in self.sealed & self.ready:
            return None
        return super()._receipt(row)

    def register_collection_upload_session_artifacts(self, collection_id, artifacts, **kwargs):
        before = len(self.rows)
        self.registered_members += len(artifacts)
        result = super().register_collection_upload_session_artifacts(
            collection_id, artifacts, **kwargs
        )
        if len(self.rows) != before and len(self.rows) % self.pack_members == 0:
            self.pending_seals.update(self.rows)
            result["volumes"] = ["newly-closed-pack"]
        for row in result["artifacts"]:
            row["payload_sealed"] = row["artifact_id"] in self.sealed
        return result

    def acquire_collection_upload_session_work(self, collection_id, *, limit=16):
        # A successful payload upload makes these members eligible for receipt
        # verification, but does not attest to their history or full custody.
        self.sealed.update(self.pending_seals)
        self.pending_seals.clear()
        return super().acquire_collection_upload_session_work(collection_id, limit=limit)

    def get_collection_upload_session_artifact(self, collection_id, artifact_id):
        self.reads.append(artifact_id)
        if self.read_failure:
            failure, self.read_failure = self.read_failure, None
            raise failure
        row = super().get_collection_upload_session_artifact(collection_id, artifact_id)
        row["payload_sealed"] = artifact_id in self.sealed
        if self.forged_digest:
            row["sha256"] = "f" * 64
        return row


class OpenPackApi(PackApi):
    """Counting fixture: prior primary binding is already accepted; no receipt yet."""

    def get_collection_upload_session_artifact_provenance_binding(self, collection_id, artifact_id):
        return SimpleNamespace(artifact_id=artifact_id)


def append_member(producer, root: Path, index: int):
    payload = f"output-{index}".encode()
    path = root / f"{index}.bin"
    path.write_bytes(payload)
    identity = ProducerArtifactIdentity(
        ArtifactId(f"{index:064x}"), len(payload), hashlib.sha256(payload).hexdigest()
    )
    receipts = producer.append_inputs(
        [ProducerFile(path, identity.artifact_id, allow_missing_materialization_hint=True)],
        expected_identities={identity.artifact_id: identity},
    )
    return path, identity, receipts


@pytest.mark.parametrize("count", [16, 32, 64, 128, 257])
def test_open_pack_append_and_history_polling_is_linear(tmp_path: Path, count: int) -> None:
    api = OpenPackApi()
    producer = _producer(api)
    try:
        for index in range(count):
            path, _, receipts = append_member(producer, tmp_path, index)
            assert receipts == ()
            assert producer.reconcile_custody() == ()
            assert path.exists()
        # One initial/resume upload scan, then one registration per new member.
        # In particular, no prefix of 1..count is re-registered after every append.
        assert api.registered_members == count + 1
        assert api.registration_calls == count + 1
        assert api.reads == []
    finally:
        producer.stop()


def test_pack_closure_discovers_prior_members_but_seal_is_not_safe_release(tmp_path: Path) -> None:
    api = PackApi(pack_members=2)
    producer = _producer(api)
    try:
        first, one, _ = append_member(producer, tmp_path, 0)
        second, two, receipts = append_member(producer, tmp_path, 1)
        assert receipts == ()
        assert api.sealed == {one.artifact_id, two.artifact_id}
        assert producer.reconcile_custody() == ()
        assert first.exists() and second.exists()
        # History/custody completion may happen asynchronously after upload.
        api.ready.update(api.sealed)
        registrations = api.registration_calls
        receipts = producer.reconcile_custody()
        assert {row.artifact for row in receipts} == {one, two}
        assert api.registration_calls == registrations
        for path in (first, second):
            path.unlink()
        assert producer.reconcile_custody() == ()
    finally:
        producer.stop()


def test_delayed_custody_polling_is_bounded_fair_and_eventually_complete(tmp_path: Path) -> None:
    api = PackApi(pack_members=40)
    producer = _producer(api)
    try:
        expected = {append_member(producer, tmp_path, index)[1] for index in range(40)}
        reads = len(api.reads)
        assert producer.reconcile_custody() == ()
        assert len(api.reads) - reads == 16
        first_batch = set(api.reads[reads:])
        reads = len(api.reads)
        assert producer.reconcile_custody() == ()
        assert len(api.reads) - reads == 16
        assert first_batch.isdisjoint(api.reads[reads:])
        api.ready.update(api.sealed)
        collected = set()
        registrations = api.registration_calls
        for _ in range(3):
            reads = len(api.reads)
            collected.update(row.artifact for row in producer.reconcile_custody())
            assert len(api.reads) - reads <= 16
        assert collected == expected
        assert api.registration_calls == registrations
        assert producer.reconcile_custody() == ()
    finally:
        producer.stop()


@pytest.mark.parametrize("failure", [TimeoutError("lost response"), Forbidden("revoked")])
def test_failed_poll_stays_pending_and_never_releases_source(tmp_path: Path, failure) -> None:
    api = PackApi(pack_members=1)
    producer = _producer(api)
    try:
        path, identity, _ = append_member(producer, tmp_path, 0)
        api.ready.update(api.sealed)
        api.read_failure = failure
        with pytest.raises(type(failure)):
            producer.reconcile_custody()
        assert path.exists()
        assert [row.artifact for row in producer.reconcile_custody()] == [identity]
    finally:
        producer.stop()


def test_forged_member_identity_is_rejected_before_source_release(tmp_path: Path) -> None:
    api = PackApi(pack_members=1)
    producer = _producer(api)
    try:
        path, identity, _ = append_member(producer, tmp_path, 0)
        api.ready.update(api.sealed)
        api.forged_digest = True
        with pytest.raises(RuntimeError, match="changed a registered artifact"):
            producer.reconcile_custody()
        assert path.exists()
        api.forged_digest = False
        assert [row.artifact for row in producer.reconcile_custody()] == [identity]
    finally:
        producer.stop()


def test_resume_reconstructs_custody_from_server_not_polling_memory(tmp_path: Path) -> None:
    api = PackApi(pack_members=1)
    first = _producer(api)
    try:
        path, identity, _ = append_member(first, tmp_path, 0)
    finally:
        first.stop()
    resumed = _producer(api)
    try:
        assert resumed.resume_artifact_custody(identity) is None
        assert path.exists()
        api.ready.update(api.sealed)
        assert resumed.resume_artifact_custody(identity).artifact == identity
        path.unlink()
        # A later retry needs no local bytes after a verified custody receipt.
        assert resumed.resume_artifact_custody(identity).artifact == identity
    finally:
        resumed.stop()


@pytest.mark.parametrize("count", [16, 32, 64, 128, 257])
def test_sealed_but_delayed_history_does_not_reregister_prior_members(
    tmp_path: Path, count: int
) -> None:
    api = OpenPackApi(pack_members=1)
    producer = _producer(api)
    try:
        for index in range(count):
            _, _, receipts = append_member(producer, tmp_path, index)
            assert receipts == ()
            assert producer.reconcile_custody() == ()
        # Each input is registered once and refreshed once after payload upload,
        # even if history verification lags behind every single append.
        assert api.registered_members == 2 * count
        assert api.registration_calls == 2 * count
        assert len(api.reads) <= 2 * count * 16
    finally:
        producer.stop()
