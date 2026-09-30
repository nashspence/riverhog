from __future__ import annotations

import hashlib

from riverhog_core.domain.archive import ArchiveArtifact
from riverhog_core.raw_volume import (
    parse_raw_volume_plan,
    plan_raw_volumes,
    raw_volume_plan_bytes,
    raw_volume_plan_payload,
)


def test_raw_volume_plan_is_canonical_restart_state() -> None:
    artifact = ArchiveArtifact(
        artifact_id="a" * 64,
        bytes=17,
        sha256=hashlib.sha256(b"x" * 17).hexdigest(),
    )
    plans = plan_raw_volumes((artifact,), starting_sequence=3, max_plaintext_bytes=10)
    assert [current.volume_id for current in plans] == [
        "segment-" + f"{3:064x}",
        "segment-" + f"{4:064x}",
    ]
    for plan in plans:
        payload = raw_volume_plan_payload(plan)
        assert payload["format"] == "raw-volume-plan/v1"
        assert parse_raw_volume_plan(raw_volume_plan_bytes(plan)) == plan
