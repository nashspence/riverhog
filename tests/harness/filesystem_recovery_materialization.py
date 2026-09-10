"""Prepare and verify the built-artifact filesystem recovery proof."""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import os
import sys
from pathlib import Path
from typing import Any, cast


def prepare(workspace: Path) -> None:
    from riverhog_storage_adapter_filesystem import (
        FilesystemStorageAdapter,
        FilesystemStorageAdapterConfig,
    )
    from riverhog_storage_adapter_protocol import (
        SmallObjectWriteRequest,
        WriteStartRequest,
    )

    repository = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(repository))
    recovery_fixture = importlib.import_module("reference.riverhog.recovery.tests.test_recovery")
    passphrase = cast(str, recovery_fixture.PASSPHRASE)
    passphrase_id = cast(str, recovery_fixture.PASSPHRASE_ID)
    write_archive = cast(Any, recovery_fixture._write_archive)
    workspace.mkdir(mode=0o700)
    archive = workspace / "logical-archive"
    write_archive(
        archive,
        with_provenance=True,
        description="Filesystem materialization qualification",
        tags=("camera", "qualification"),
    )
    root = workspace / "adapter-root"
    adapter = FilesystemStorageAdapter(
        FilesystemStorageAdapterConfig(root=root, minimum_free_bytes=0)
    )
    prefix = "archives/recovery-proof"
    for path in sorted(item for item in archive.rglob("*") if item.is_file()):
        relative = path.relative_to(archive).as_posix()
        payload = path.read_bytes()
        object_path = f"{prefix}/{relative}"
        adapter.put_small_object(
            SmallObjectWriteRequest(
                object_path=object_path,
                content_type="application/octet-stream",
                required_identity_assertions={"riverhog-object": relative},
                placement="archive",
                mode="create_only",
                stored_bytes=len(payload),
                stored_sha256=hashlib.sha256(payload).hexdigest(),
            ),
            payload,
        )
    unrelated = b"must-not-enter-selected-materializations"
    adapter.put_small_object(
        SmallObjectWriteRequest(
            object_path="archives/unrelated/payload.age",
            content_type="application/octet-stream",
            required_identity_assertions={"riverhog-object": "unrelated"},
            placement="archive",
            mode="create_only",
            stored_bytes=len(unrelated),
            stored_sha256=hashlib.sha256(unrelated).hexdigest(),
        ),
        unrelated,
    )
    adapter.begin_write(
        WriteStartRequest(
            object_path=f"{prefix}/incomplete.age",
            expected_bytes=1024,
            content_type="application/octet-stream",
            required_identity_assertions={"riverhog-object": "incomplete"},
            placement="archive",
        )
    )
    adapter.close()
    passphrases = workspace / "passphrases.json"
    passphrases.write_text(
        json.dumps({passphrase_id: passphrase}),
        encoding="utf-8",
    )
    os.chmod(passphrases, 0o600)


def verify_full(workspace: Path) -> None:
    output = workspace / "recovered"
    expected = {
        "notes/alpha.txt": b"alpha\n",
        "notes/beta.txt": b"beta\n",
        "video.bin": b"first-second",
    }
    actual = {path: (output / path).read_bytes() for path in expected}
    if actual != expected:
        raise RuntimeError("built-artifact recovery differs from its source collection")
    if (workspace / "full" / "archives/unrelated").exists():
        raise RuntimeError("materialization included an unselected provider object")
    if (workspace / "full" / "archives/recovery-proof/incomplete.age").exists():
        raise RuntimeError("materialization included a nonterminal write")


def verify_description(workspace: Path) -> None:
    payload = json.loads((workspace / "description.json").read_text(encoding="utf-8"))
    if payload["description"] != "Filesystem materialization qualification":
        raise RuntimeError("description-only recovery returned the wrong authority")
    materialized = workspace / "description" / "archives/recovery-proof"
    files = {
        path.relative_to(materialized).as_posix()
        for path in materialized.rglob("*")
        if path.is_file()
    }
    if files != {
        "description.json.age",
        "manifest.json.age",
        "recovery.json",
    }:
        raise RuntimeError("description-only materialization read outside its exact selection")


def verify_tags(workspace: Path) -> None:
    records = [
        json.loads(line)
        for line in (workspace / "tags.json-seq").read_text(encoding="utf-8").splitlines()
    ]
    tags = [record["tag"] for record in records if record.get("record") == "tag"]
    if len(tags) != 2 or set(tags) != {"camera", "qualification"}:
        raise RuntimeError("tags-only recovery returned the wrong authority")
    if not records or records[-1] != {"record": "complete", "tag_count": 2}:
        raise RuntimeError("tags-only recovery did not prove exact completion")
    materialized = workspace / "tags" / "archives/recovery-proof"
    files = {
        path.relative_to(materialized).as_posix()
        for path in materialized.rglob("*")
        if path.is_file()
    }
    if not {"manifest.json.age", "recovery.json", "tags/head.json.age"}.issubset(files):
        raise RuntimeError("tags-only materialization omitted required authority")
    if any(path.startswith(("volumes/", "metadata/", "provenance/")) for path in files):
        raise RuntimeError("tags-only materialization read collection payload authority")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command",
        choices=("prepare", "verify-full", "verify-description", "verify-tags"),
    )
    parser.add_argument("workspace", type=Path)
    return parser


def main() -> None:
    args = _parser().parse_args()
    {
        "prepare": prepare,
        "verify-full": verify_full,
        "verify-description": verify_description,
        "verify-tags": verify_tags,
    }[args.command](args.workspace)


if __name__ == "__main__":
    main()
