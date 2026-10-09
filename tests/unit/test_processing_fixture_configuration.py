from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest
from riverhog_canonical_json import canonical_json_bytes

ROOT = Path(__file__).resolve().parents[2]
HELPER = ROOT / "scripts/_processing_fixture.sh"


def configuration(*lanes: str, **overrides: str) -> dict[str, int]:
    script = (
        'set -euo pipefail; source "$1"; shift; '
        'for lane in "$@"; do configure_processing_fixture "$lane"; done; '
        "processing_fixture_json"
    )
    result = subprocess.run(
        ["bash", "-c", script, "fixture", str(HELPER), *lanes],
        env={**os.environ, **overrides},
        capture_output=True,
        text=True,
        check=True,
    )
    value = json.loads(result.stdout)
    assert result.stdout.strip().encode() == canonical_json_bytes(value)
    return value


@pytest.mark.parametrize("frames", ["2000", "32000"])
@pytest.mark.parametrize(
    "lane,count",
    [
        ("processing-admission", 16),
        ("processing-e2e", 4),
        ("processing-overlap", 1),
        ("processing-scale", 128),
    ],
)
def test_every_physical_fixture_setting_is_independent_of_previous_scenarios(
    lane: str, count: int, frames: str
) -> None:
    environment = {"STOVE0_SMOKE_AUDIO_FRAMES": frames, "STOVE0_SMOKE_FILE_COUNT": "128"}
    standalone = configuration(lane, **environment)
    aggregate = configuration(
        "all", "processing-admission", "processing-e2e", "processing-overlap", lane, **environment
    )
    assert standalone == aggregate
    assert standalone["smoke_file_count"] == count
    assert standalone["smoke_claim_file_count"] == count + 1
    assert standalone["smoke_observation_timeout"] == 300 + 60 * count
    assert standalone["smoke_completion_timeout"] == 600 + 480 * count
    assert standalone["smoke_audio_frames"] == int(frames)


def test_large_then_small_fixtures_reset_content_capacity_and_deadlines() -> None:
    direct = configuration("processing-overlap", STOVE0_SMOKE_FILE_COUNT="512")
    after_scale = configuration(
        "processing-scale", "processing-overlap", STOVE0_SMOKE_FILE_COUNT="512"
    )
    assert direct == after_scale
    assert direct["smoke_content_timeout"] == 300
    assert direct["smoke_download_quota_bytes"] == 16 * 1024 * 1024


@pytest.mark.parametrize(
    "name,value",
    [
        ("STOVE0_SMOKE_FILE_COUNT", "0"),
        ("STOVE0_SMOKE_FILE_COUNT", "1001"),
        ("STOVE0_SMOKE_AUDIO_FRAMES", "0"),
        ("STOVE0_SMOKE_AUDIO_FRAMES", "1;exit 0"),
    ],
)
def test_invalid_physical_fixture_configuration_fails_before_any_proof(
    name: str, value: str
) -> None:
    with pytest.raises(subprocess.CalledProcessError):
        configuration("processing-scale", **{name: value})
