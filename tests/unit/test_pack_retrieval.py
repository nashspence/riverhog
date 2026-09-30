from __future__ import annotations

import hashlib

from riverhog_age import ResumableAgeScryptSession
from riverhog_core.domain.archive import ArchiveArtifact
from riverhog_core.pack_retrieval import (
    BILLING_MODE_WHOLE_OBJECT,
    PackMemberRangeReader,
    PackMemberRetrievalSource,
    PackRangeBatchReader,
    PackRangeRetrievalPolicy,
    PackVolumeRetrievalSource,
    plan_pack_range_retrieval,
)
from riverhog_core.pack_volume import iter_render_pack_upload_unit, plan_pack_volume


class MemoryRangeStore:
    def __init__(self, content: bytes) -> None:
        self.content = content
        self.requests: list[tuple[str, str | None, int, int]] = []

    def iter_object_range(
        self,
        *,
        object_path: str,
        revision: str | None,
        expected_bytes: int,
        offset: int,
        size: int,
    ):
        assert expected_bytes == len(self.content)
        self.requests.append((object_path, revision, offset, size))
        yield self.content[offset : offset + size]


def _artifact(artifact_id: str, content: bytes) -> ArchiveArtifact:
    return ArchiveArtifact(
        artifact_id=artifact_id,
        bytes=len(content),
        sha256=hashlib.sha256(content).hexdigest(),
    )


def _archive(
    contents: dict[str, bytes],
) -> tuple[
    object,
    PackVolumeRetrievalSource,
    dict[str, PackMemberRetrievalSource],
    bytes,
]:
    artifacts = tuple(_artifact(artifact_id, content) for artifact_id, content in contents.items())
    plan = plan_pack_volume(artifacts, sequence=0)
    plaintext = b"".join(
        chunk
        for unit in plan.units
        for chunk in iter_render_pack_upload_unit(
            plan,
            unit.unit,
            lambda artifact_id: (contents[artifact_id],),
        )
    )
    session = ResumableAgeScryptSession.create(
        "archive passphrase",
        log_n=1,
    )
    state = session.export_state(plaintext_size=len(plaintext)).to_json_bytes().decode("utf-8")
    ciphertext = session.encrypt_plaintext(plaintext)
    source = PackVolumeRetrievalSource(
        volume_id=plan.volume_id,
        object_path=f"archives/x/volumes/{plan.volume_id}.tar.age",
        revision="v1",
        plaintext_bytes=len(plaintext),
        stored_bytes=len(ciphertext),
        age_state_json=state,
    )
    members = {
        current.artifact_id: PackMemberRetrievalSource(
            artifact_id=current.artifact_id,
            bytes=current.bytes,
            sha256=current.sha256,
            data_offset=current.data_offset,
        )
        for current in plan.members
    }
    return plan, source, members, ciphertext


def test_two_mib_packed_artifact_does_not_download_its_six_mib_pack() -> None:
    contents = {
        "1" * 64: b"a" * (2 * 1024 * 1024),
        "2" * 64: b"t" * (2 * 1024 * 1024),
        "3" * 64: b"z" * (2 * 1024 * 1024),
    }
    _plan, source, members, ciphertext = _archive(contents)
    store = MemoryRangeStore(ciphertext)

    recovered = b"".join(
        PackMemberRangeReader(
            store,
            passphrase="archive passphrase",
        ).iter_member(source, members["2" * 64])
    )

    assert recovered == contents["2" * 64]
    assert len(store.requests) == 1
    assert store.requests[0][3] < source.stored_bytes // 2


def test_head_middle_and_tail_ranges_read_only_bounded_pack_framing() -> None:
    content = bytes(range(251)) * (2 * 1024 * 1024 // 251)
    content += b"tail"
    _plan, source, members, ciphertext = _archive({"4" * 64: content})
    store = MemoryRangeStore(ciphertext)
    size = 4096

    for offset in (0, len(content) // 2, len(content) - size):
        store.requests.clear()
        recovered = b"".join(
            PackMemberRangeReader(
                store,
                passphrase="archive passphrase",
            ).iter_member_range(
                source,
                members["4" * 64],
                offset=offset,
                size=size,
            )
        )

        assert recovered == content[offset : offset + size]
        assert len(store.requests) == 1
        assert store.requests[0][3] < len(content) // 4


def test_batch_reader_coalesces_nearby_members_and_verifies_each_artifact() -> None:
    contents = {
        "5" * 64: b"alpha" * 1000,
        "6" * 64: b"beta" * 1000,
        "7" * 64: b"c" * (2 * 1024 * 1024),
    }
    _plan, source, members, ciphertext = _archive(contents)
    policy = PackRangeRetrievalPolicy(
        merge_gap_ciphertext_bytes=1024 * 1024,
        max_request_ciphertext_bytes=4 * 1024 * 1024,
    )
    plan = plan_pack_range_retrieval(
        source,
        (members["5" * 64], members["6" * 64]),
        policy=policy,
    )
    store = MemoryRangeStore(ciphertext)

    recovered = PackRangeBatchReader(
        store,
        passphrase="archive passphrase",
    ).read_members(plan)

    assert recovered == {"5" * 64: contents["5" * 64], "6" * 64: contents["6" * 64]}
    assert plan.request_count == 1
    assert len(store.requests) == 1


def test_range_plan_accounts_for_cold_store_whole_object_semantics() -> None:
    contents = {"1" * 64: b"a" * 1000, "b" * 64: b"b" * 1000}
    _plan, source, members, _ciphertext = _archive(contents)
    whole_object = plan_pack_range_retrieval(
        source,
        (members["1" * 64],),
        policy=PackRangeRetrievalPolicy(billing_mode=BILLING_MODE_WHOLE_OBJECT),
    )
    assert whole_object.remote_bytes < source.stored_bytes
    assert whole_object.accounted_remote_bytes == source.stored_bytes


def test_batch_reader_verifies_zero_byte_members_without_remote_read() -> None:
    contents = {"8" * 64: b"", "9" * 64: b"data"}
    _plan, source, members, ciphertext = _archive(contents)
    plan = plan_pack_range_retrieval(source, (members["8" * 64],))
    store = MemoryRangeStore(ciphertext)

    recovered = PackRangeBatchReader(
        store,
        passphrase="archive passphrase",
    ).read_members(plan)

    assert recovered == {"8" * 64: b""}
    assert store.requests == []


def test_range_plan_rejects_a_member_larger_than_the_memory_request_bound() -> None:
    contents = {"a" * 64: b"x" * (2 * 1024 * 1024)}
    _plan, source, members, _ciphertext = _archive(contents)

    try:
        plan_pack_range_retrieval(
            source,
            (members["a" * 64],),
            policy=PackRangeRetrievalPolicy(
                max_request_ciphertext_bytes=1024 * 1024,
            ),
        )
    except ValueError as exc:
        assert "request bound" in str(exc)
    else:
        raise AssertionError("oversized selective range was accepted")


def test_batch_reader_uses_parallel_range_requests_under_a_byte_budget() -> None:
    import threading
    import time

    contents = {
        "1" * 64: b"a" * (2 * 1024 * 1024),
        "b" * 64: b"b" * (2 * 1024 * 1024),
        "7" * 64: b"c" * (2 * 1024 * 1024),
    }
    _plan, source, members, ciphertext = _archive(contents)
    policy = PackRangeRetrievalPolicy(
        merge_gap_ciphertext_bytes=0,
        max_request_ciphertext_bytes=3 * 1024 * 1024,
    )
    plan = plan_pack_range_retrieval(source, tuple(members.values()), policy=policy)
    assert plan.request_count >= 2

    class SlowRangeStore(MemoryRangeStore):
        def __init__(self, content: bytes) -> None:
            super().__init__(content)
            self.lock = threading.Lock()
            self.active = 0
            self.maximum_active = 0

        def iter_object_range(self, **kwargs):
            self.requests.append(
                (
                    kwargs["object_path"],
                    kwargs["revision"],
                    kwargs["offset"],
                    kwargs["size"],
                )
            )
            with self.lock:
                self.active += 1
                self.maximum_active = max(self.maximum_active, self.active)
            time.sleep(0.03)
            yield self.content[kwargs["offset"] : kwargs["offset"] + kwargs["size"]]
            with self.lock:
                self.active -= 1

    store = SlowRangeStore(ciphertext)
    recovered = PackRangeBatchReader(
        store,
        passphrase="archive passphrase",
        request_concurrency=3,
        max_inflight_bytes=16 * 1024 * 1024,
    ).read_members(plan)

    assert recovered == contents
    assert store.maximum_active >= 2


def test_range_policy_supports_per_store_overrides_with_global_fallback() -> None:
    policy = PackRangeRetrievalPolicy(
        merge_gap_ciphertext_bytes=4 * 1024 * 1024,
        max_request_ciphertext_bytes=16 * 1024 * 1024,
        billing_mode=BILLING_MODE_WHOLE_OBJECT,
    )

    assert policy.merge_gap_ciphertext_bytes == 4 * 1024 * 1024
    assert policy.max_request_ciphertext_bytes == 16 * 1024 * 1024
    assert policy.billing_mode == BILLING_MODE_WHOLE_OBJECT


def test_single_member_reader_reports_remote_crypto_and_downstream_phases() -> None:
    import time

    contents = {"2" * 64: b"x" * (256 * 1024)}
    _plan, source, members, ciphertext = _archive(contents)
    timings = []
    reader = PackMemberRangeReader(
        MemoryRangeStore(ciphertext),
        passphrase="archive passphrase",
        timing_observer=timings.append,
    )

    recovered = bytearray()
    for chunk in reader.iter_member(source, members["2" * 64]):
        recovered.extend(chunk)
        time.sleep(0.001)

    assert bytes(recovered) == contents["2" * 64]
    assert len(timings) == 1
    assert timings[0].remote_seconds >= 0
    assert timings[0].crypto_seconds >= 0
    assert timings[0].downstream_seconds > 0
