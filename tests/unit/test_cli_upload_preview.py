from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest
from a_riverhog_cli import directory_upload, main
from typer.testing import CliRunner


def _track_directories(monkeypatch, *, fault=None, kind=None):
    actual = os.scandir
    counts = {"active": 0, "peak": 0, "entries": 0, "opened": 0, "closed": 0}

    class Tracked:
        def __init__(self, inner):
            self.inner, self.closed = inner, False
            counts["active"] += 1
            counts["opened"] += 1
            counts["peak"] = max(counts["peak"], counts["active"])

        def __iter__(self):
            return self

        def __next__(self):
            entry = next(self.inner)
            counts["entries"] += 1
            if kind is not None and entry.name == "member":
                return SimpleNamespace(
                    path=entry.path,
                    is_symlink=lambda: kind == "symlink",
                    is_dir=lambda **_kwargs: False,
                    is_file=lambda **_kwargs: False,
                )
            return entry

        def close(self):
            if not self.closed:
                self.inner.close()
                self.closed = True
                counts["active"] -= 1
                counts["closed"] += 1

    def scan(path):
        if fault is not None and Path(path) == fault[0]:
            raise fault[1]("fixture directory could not be read")
        return Tracked(actual(path))

    monkeypatch.setattr(directory_upload.os, "scandir", scan)
    return counts


def _forbidden(*_args, **_kwargs):
    pytest.fail("read-only summary reached whole-inventory allocation or publication")


@pytest.mark.parametrize("count", [1, 64, 257])
def test_preview_counts_every_member_with_one_walk_and_one_hash_each(tmp_path, monkeypatch, count):
    root = tmp_path / "source"
    root.mkdir()
    for index in range(count):
        (root / f"member-{index:06}").write_bytes(b"x" * (index % 4))
    actual = directory_upload._source_fixity
    calls = []

    def fixity(path):
        calls.append(path)
        return actual(path)

    with monkeypatch.context() as patch:
        counts = _track_directories(patch)
        patch.setattr(directory_upload, "_source_paths", _forbidden)
        patch.setattr(directory_upload, "_new_upload_identity", _forbidden)
        patch.setattr(Path, "rglob", _forbidden)
        patch.setattr(directory_upload, "_source_fixity", fixity)
        preview = directory_upload.summarize_upload(root)
    assert preview.artifact_count == count
    assert preview.total_bytes == sum(index % 4 for index in range(count))
    assert len(preview.sources) == min(count, 5)
    assert [row["relative_components"] for row in preview.sources] == [
        [f"member-{index:06}"] for index in range(min(count, 5))
    ]
    assert [row["sha256"] for row in preview.sources] == [
        hashlib.sha256(b"x" * (index % 4)).hexdigest() for index in range(min(count, 5))
    ]
    assert len(calls) == len(set(calls)) == count
    assert counts == {"active": 0, "peak": 1, "entries": count, "opened": 1, "closed": 1}


def test_preview_directory_handles_follow_depth_and_close_after_wide_and_nested_walk(
    tmp_path, monkeypatch
):
    root = tmp_path / "source"
    root.mkdir()
    for index in range(50):
        (root / f"empty-{index}").mkdir()
    path = root
    for _ in range(12):
        path = path / "branch"
        path.mkdir()
    (path / "deep.bin").write_bytes(b"deep")
    (root / "root.bin").write_bytes(b"root")
    with monkeypatch.context() as patch:
        counts = _track_directories(patch)
        preview = directory_upload.summarize_upload(root)
    assert preview.artifact_count == 2 and preview.total_bytes == 8
    assert counts["entries"] == 50 + 12 + 2
    assert counts["peak"] == 13
    assert counts["opened"] == counts["closed"] == 63
    assert counts["active"] == 0


@pytest.mark.parametrize("json_mode", [False, True])
def test_cli_preview_keeps_canonical_examples_and_never_opens_state_or_api(
    tmp_path, monkeypatch, json_mode
):
    root = tmp_path / "source"
    root.mkdir()
    names = ["z.bin", "m.bin", "b.bin", "aa.bin", "a.bin", "é.bin", "aa/deep.bin"]
    for name in names:
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"same")
    state = tmp_path / "uncreated-state"
    monkeypatch.setenv("XDG_STATE_HOME", str(state))
    for owner, name in [
        (directory_upload, "_new_upload_identity"),
        (directory_upload, "_state_path"),
        (main, "prepare_primary_upload"),
        (main, "UploadSqliteResources"),
        (main, "client"),
    ]:
        monkeypatch.setattr(owner, name, _forbidden)
    monkeypatch.setattr(directory_upload, "_source_paths", _forbidden)
    arguments = [
        "collection",
        "upload",
        "start",
        str(root),
        "--idempotency-key",
        "preview",
        "--dry-run",
    ]
    if json_mode:
        arguments.append("--json")
    result = CliRunner().invoke(main.app, arguments)
    assert result.exit_code == 0, result.output
    if json_mode:
        assert json.loads(result.stdout) == {
            "format": "a-riverhog-cli-upload-preview/v1",
            "idempotency_key": "preview",
            "artifact_count": 7,
            "total_bytes": 28,
            "sources": [
                {
                    "relative_components": parts,
                    "bytes": 4,
                    "sha256": hashlib.sha256(b"same").hexdigest(),
                }
                for parts in [["a.bin"], ["aa.bin"], ["aa", "deep.bin"], ["b.bin"], ["m.bin"]]
            ],
        }
    else:
        assert result.stdout.strip() == "would upload 7 artifacts"
    assert not state.exists()


@pytest.mark.parametrize("kind", ["file", "empty", "directories"])
def test_preview_rejects_roots_without_regular_members(tmp_path, kind):
    root = tmp_path / "source"
    if kind == "file":
        root.write_bytes(b"not a directory")
        message = "must be a directory"
    else:
        root.mkdir()
        if kind == "directories":
            (root / "empty").mkdir()
        message = "has no regular files"
    for inspect in (directory_upload.summarize_upload, directory_upload.preview_upload):
        with pytest.raises(ValueError, match=message):
            inspect(root)


@pytest.mark.parametrize("error", [PermissionError, FileNotFoundError])
def test_preview_never_omits_an_unreadable_or_disappeared_directory(tmp_path, monkeypatch, error):
    root = tmp_path / "source"
    blocked = root / "blocked"
    blocked.mkdir(parents=True)
    (blocked / "member").write_bytes(b"must not disappear")
    with monkeypatch.context() as patch:
        counts = _track_directories(patch, fault=(blocked, error))
        with pytest.raises(error, match="could not be read"):
            directory_upload.summarize_upload(root)
    assert counts["active"] == 0 and counts["opened"] == counts["closed"] == 1


def test_cli_preview_hash_failure_after_five_examples_never_reports_partial_success(
    tmp_path, monkeypatch
):
    root = tmp_path / "source"
    root.mkdir()
    for index in range(10):
        (root / f"member-{index}").write_bytes(b"same")
    state = tmp_path / "uncreated-state"
    monkeypatch.setenv("XDG_STATE_HOME", str(state))
    actual = directory_upload._source_fixity
    calls = []

    def interrupted(path):
        calls.append(path)
        if len(calls) == 7:
            raise ValueError("upload source changed while hashing")
        return actual(path)

    with monkeypatch.context() as patch:
        counts = _track_directories(patch)
        patch.setattr(directory_upload, "_source_fixity", interrupted)
        patch.setattr(directory_upload, "_new_upload_identity", _forbidden)
        patch.setattr(main, "client", _forbidden)
        result = CliRunner().invoke(
            main.app,
            [
                "collection",
                "upload",
                "start",
                str(root),
                "--idempotency-key",
                "preview",
                "--dry-run",
                "--json",
            ],
        )
    assert result.exit_code == 2 and result.stdout == ""
    assert len(calls) == 7 and not state.exists()
    assert counts["active"] == 0 and counts["opened"] == counts["closed"] == 1


def test_source_walk_cancellation_closes_every_actual_directory_iterator(tmp_path, monkeypatch):
    root = tmp_path / "source"
    (root / "child").mkdir(parents=True)
    (root / "child" / "member").write_bytes(b"exact")
    with monkeypatch.context() as patch:
        counts = _track_directories(patch)
        paths = directory_upload._walk_source_paths(root)
        assert next(paths) == root / "child" / "member"
        assert counts["active"] == counts["peak"] == 2
        paths.close()
    assert counts["active"] == 0 and counts["opened"] == counts["closed"] == 2


@pytest.mark.parametrize("inspect", ["summarize_upload", "preview_upload"])
@pytest.mark.parametrize("kind", ["symlink", "special"])
def test_source_walk_refuses_observed_links_and_special_members_before_hashing(
    tmp_path, monkeypatch, inspect, kind
):
    root = tmp_path / "source"
    root.mkdir()
    (root / "member").write_bytes(b"not permission to follow a link")
    with monkeypatch.context() as patch:
        counts = _track_directories(patch, kind=kind)
        patch.setattr(directory_upload, "_source_fixity", _forbidden)
        with pytest.raises(
            ValueError, match="symbolic link" if kind == "symlink" else "not a regular file"
        ):
            getattr(directory_upload, inspect)(root)
    assert counts["active"] == 0 and counts["opened"] == counts["closed"] == 1


@pytest.mark.skipif(
    os.name == "nt", reason="actual POSIX link fixture; portable observed-link refusal is separate"
)
@pytest.mark.parametrize("kind", ["file", "directory"])
def test_streamed_preview_refuses_actual_symbolic_links(tmp_path, kind):
    root = tmp_path / "source"
    root.mkdir()
    target = tmp_path / "outside"
    if kind == "file":
        target.write_bytes(b"outside")
    else:
        target.mkdir()
        (target / "member").write_bytes(b"outside")
    (root / "linked").symlink_to(target, target_is_directory=kind == "directory")
    with pytest.raises(ValueError, match="symbolic link"):
        directory_upload.summarize_upload(root)
