from __future__ import annotations

import subprocess
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from a_stove0_rclone_target.contracts import RCLONE_DELIVER_OPERATION
from a_stove0_rclone_target.target import RcloneDestination, RcloneEffectTargetService
from stove0_target_support import TargetEffectCommitUncertain


def _destination() -> RcloneDestination:
    return RcloneDestination(identity="a" * 64, remote="remote:archive")


def test_generic_rclone_descriptor_has_one_effect_operation(tmp_path: Path) -> None:
    target = RcloneEffectTargetService(
        state_root=tmp_path / "state",
        workspace_root=tmp_path / "workspace",
        destination=_destination(),
        image_id="sha256:" + "b" * 64,
        implementation_version="0.1.0",
    )
    try:
        descriptor = target.descriptor()
        assert descriptor.protocol == "stove0-effect-target/v1"
        assert descriptor.implementation_id == "a-stove0-rclone-target/v1"
        assert tuple(item.operation_id for item in descriptor.operations) == (
            RCLONE_DELIVER_OPERATION.id,
        )
        assert RCLONE_DELIVER_OPERATION.inputs[0].role == "*"
        assert RCLONE_DELIVER_OPERATION.source_collection_retirement_permitted
    finally:
        target.close()


def test_delivery_verifies_bytes_before_marker_and_reads_marker_back(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    objects = tmp_path / "objects"
    objects.mkdir()
    manifest = tmp_path / "manifest.json"
    manifest.write_bytes(b'{"delivery":"exact"}')
    calls: list[tuple[str, ...]] = []

    def run(command: list[str], **_kwargs: Any) -> SimpleNamespace:
        calls.append(tuple(command))
        return SimpleNamespace(stdout=manifest.read_bytes() if command[1] == "cat" else b"")

    monkeypatch.setattr(subprocess, "run", run)
    _destination().commit(delivery_id="c" * 64, objects_root=objects, manifest_path=manifest)
    assert [command[1] for command in calls] == ["copy", "check", "copyto", "cat"]
    assert calls[1][2] == "--download"
    assert calls[2][-1] == calls[3][-1]


def test_delivery_readback_mismatch_is_commit_uncertain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    objects = tmp_path / "objects"
    objects.mkdir()
    manifest = tmp_path / "manifest.json"
    manifest.write_bytes(b"sealed")

    def run(command: list[str], **_kwargs: Any) -> SimpleNamespace:
        return SimpleNamespace(stdout=b"different" if command[1] == "cat" else b"")

    monkeypatch.setattr(subprocess, "run", run)
    with pytest.raises(TargetEffectCommitUncertain):
        _destination().commit(delivery_id="c" * 64, objects_root=objects, manifest_path=manifest)
