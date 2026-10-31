"""An upload retry retains independently allocated member identities."""

from __future__ import annotations

from pathlib import Path

import pytest
from a_riverhog_cli.directory_upload import prepare_upload

from tests.support.cli_upload import read_upload_sources


def test_directory_upload_persists_opaque_ids_and_exact_source_view(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    root = tmp_path / "source"
    root.mkdir()
    (root / "Album").mkdir()
    (root / "Album" / "clip.mov").write_bytes(b"clip")
    (root / "Album" / "clip.xmp").write_bytes(b"metadata")

    first = prepare_upload(root, "retry-key", ordering_bytes=64 * 1024)
    second = prepare_upload(root, "retry-key", ordering_bytes=64 * 1024)
    assert first == second
    assert len(read_upload_sources(first)) == 2
    assert len({item.artifact_id for item in read_upload_sources(first)}) == 2
    assert all(item.artifact_id != item.sha256 for item in read_upload_sources(first))
    assert [item.producer_file().materialization_hint for item in read_upload_sources(first)] == [
        ("Album", "clip.mov"),
        ("Album", "clip.xmp"),
    ]

    (root / "Album" / "clip.xmp").write_bytes(b"changed")
    with pytest.raises(ValueError, match="saved upload identity differs"):
        prepare_upload(root, "retry-key", ordering_bytes=64 * 1024)


def test_directory_upload_rejects_symlink_sources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    root = tmp_path / "source"
    root.mkdir()
    (root / "original.bin").write_bytes(b"content")
    (root / "alias.bin").symlink_to(root / "original.bin")
    with pytest.raises(ValueError, match="symbolic link"):
        prepare_upload(root, "retry-key", ordering_bytes=64 * 1024)


def test_directory_upload_does_not_reserve_provenance_shaped_names(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    root = tmp_path / "source"
    root.mkdir()
    (root / "sample.fprov.jsonseq").write_bytes(b"ordinary member")
    upload = prepare_upload(root, "retry-key", ordering_bytes=64 * 1024)
    assert [item.relative_parts for item in read_upload_sources(upload)] == [
        ("sample.fprov.jsonseq",)
    ]
