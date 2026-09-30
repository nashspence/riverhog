from __future__ import annotations

import hashlib
import json
import tarfile
from io import BytesIO

from riverhog_age import CHUNK_SIZE
from riverhog_core.domain.archive import ArchiveArtifact, PackVolumePlan
from riverhog_core.pack_volume import (
    PACK_INDEX_PATH,
    PACK_PADDING_PREFIX,
    iter_render_pack_upload_unit,
    iter_render_pack_upload_unit_payload,
    pack_unit_descriptors,
    pack_volume_plan_payload,
    parse_pack_volume_plan,
    plan_pack_volume,
)
from riverhog_protocol.pack_ingress import canonical_json_bytes

PART = 5 * 1024 * 1024


def _artifacts(payloads: dict[str, bytes]) -> tuple[ArchiveArtifact, ...]:
    return tuple(
        ArchiveArtifact(artifact_id, len(content), hashlib.sha256(content).hexdigest())
        for artifact_id, content in payloads.items()
    )


def _plaintext(plan: PackVolumePlan, payloads: dict[str, bytes]) -> bytes:
    return b"".join(
        chunk
        for unit in plan.units
        for chunk in iter_render_pack_upload_unit(
            plan,
            unit.unit,
            lambda artifact_id: (payloads[artifact_id],),
        )
    )


def test_pack_plan_is_deterministic_and_age_aligned() -> None:
    payloads = {f"{index + 1:064x}": bytes([index]) * (1024 * 1024) for index in range(7)}
    artifacts = _artifacts(payloads)
    first = plan_pack_volume(artifacts, sequence=0, part_plaintext_bytes=PART)
    second = plan_pack_volume(tuple(reversed(artifacts)), sequence=0, part_plaintext_bytes=PART)
    assert first == second
    assert len(first.units) >= 2
    assert all(
        unit.plaintext_bytes >= PART and unit.plaintext_end % CHUNK_SIZE == 0
        for unit in first.units[:-1]
    )
    assert first.units[-1].final and first.units[-1].includes_index
    assert first.plan_sha256 == pack_unit_descriptors(first)[0].plan_sha256


def test_rendered_pack_is_standard_tar_with_exact_opaque_member_index() -> None:
    payloads = {"1" * 64: b"alpha", "2" * 64: b"beta", "3" * 64: b""}
    plan = plan_pack_volume(_artifacts(payloads), sequence=3)
    plaintext = _plaintext(plan, payloads)
    assert len(plaintext) == plan.plaintext_bytes
    with tarfile.open(fileobj=BytesIO(plaintext), mode="r:") as archive:
        names = archive.getnames()
        assert set(names) == {"artifacts/" + artifact_id for artifact_id in payloads} | {
            PACK_INDEX_PATH
        }
        for artifact_id, content in payloads.items():
            assert archive.extractfile("artifacts/" + artifact_id).read() == content  # type: ignore[union-attr]
        index_bytes = archive.extractfile(PACK_INDEX_PATH).read()  # type: ignore[union-attr]
    assert hashlib.sha256(index_bytes).hexdigest() == plan.index_sha256
    index = json.loads(index_bytes)
    assert index["format"] == "riverhog-pack-index/v1"
    assert {row["artifact_id"] for row in index["artifacts"]} == set(payloads)
    assert all("path" not in row for row in index["artifacts"])
    assert not any(name.startswith(PACK_PADDING_PREFIX) for name in names)


def test_pack_upload_unit_payload_and_checkpoint_round_trip() -> None:
    payloads = {"1" * 64: b"abc", "2" * 64: b"defgh"}
    plan = plan_pack_volume(_artifacts(payloads), sequence=7)
    descriptor = pack_unit_descriptors(plan)[0]
    payload = b"".join(payloads[source.artifact_id] for source in descriptor.sources)
    rendered = b"".join(iter_render_pack_upload_unit_payload(plan, 0, (payload[:4], payload[4:])))
    assert len(rendered) == plan.units[0].plaintext_bytes
    with tarfile.open(fileobj=BytesIO(rendered), mode="r:") as archive:
        for artifact_id, content in payloads.items():
            assert archive.extractfile("artifacts/" + artifact_id).read() == content  # type: ignore[union-attr]
    recipe = pack_volume_plan_payload(plan)
    assert recipe["format"] == "pack-volume-plan/v1"
    assert parse_pack_volume_plan(canonical_json_bytes(recipe)) == plan
