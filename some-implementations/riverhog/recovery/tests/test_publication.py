from __future__ import annotations

from pathlib import Path

import pytest
from a_riverhog_recovery_tool._publication import publish_complete_recovery


def test_exclusive_completion_never_replaces_an_existing_recovery(tmp_path: Path) -> None:
    staging = tmp_path / "staging"
    staging.mkdir()
    (staging / "recovery.json").write_bytes(b"complete")
    output = tmp_path / "output"
    output.mkdir()
    (output / "sentinel").write_bytes(b"preserve")
    with pytest.raises(OSError):
        publish_complete_recovery(staging, output)
    assert (output / "sentinel").read_bytes() == b"preserve"
    assert (staging / "recovery.json").read_bytes() == b"complete"


def test_completion_rejects_a_link_in_staging(tmp_path: Path) -> None:
    staging = tmp_path / "staging"
    staging.mkdir()
    (staging / "link").symlink_to(tmp_path / "elsewhere")
    with pytest.raises(ValueError, match="symbolic link"):
        publish_complete_recovery(staging, tmp_path / "output")
    assert not (tmp_path / "output").exists()
