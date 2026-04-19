from __future__ import annotations

import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterable

from .mock_data import MockFile


def stage_collection_files(
    harness,
    upload_path: str,
    files: Iterable[MockFile],
    *,
    directories: Iterable[str] = (),
) -> Path:
    root = harness.storage.upload_collection_root(upload_path)
    root.mkdir(parents=True, exist_ok=True)

    for rel in directories:
        (root / rel).mkdir(parents=True, exist_ok=True)

    for sample in files:
        target = root / sample.relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(sample.content)
        os.chmod(target, int(sample.mode, 8))
        unix_time = datetime.fromisoformat(sample.mtime.replace("Z", "+00:00")).timestamp()
        os.utime(target, (unix_time, unix_time))
    return root


def seal_collection(
    harness,
    upload_path: str,
    *,
    description: str,
    keep_buffer_after_archive: bool = False,
) -> dict:
    response = harness.client.post(
        "/v1/collections/seal",
        headers=harness.auth_headers(),
        json={
            "upload_path": upload_path,
            "description": description,
            "keep_buffer_after_archive": keep_buffer_after_archive,
        },
    )
    assert response.status_code == 200, response.text
    return response.json()


def flush_containers(harness) -> list[str]:
    response = harness.client.post(
        "/v1/containers/flush",
        headers=harness.auth_headers(),
    )
    assert response.status_code == 200, response.text
    return response.json()["closed_containers"]


def closed_container_roots(harness, container_ids: list[str]) -> dict[str, Path]:
    roots: dict[str, Path] = {}
    with harness.session() as session:
        for container_id in container_ids:
            container = session.get(harness.models.Container, container_id)
            assert container is not None
            roots[container_id] = Path(container.root_abs_path)
    return roots


def activation_container_from_root(
    harness,
    container_id: str,
    *,
    mutate: Callable[[str, bytes], bytes] | None = None,
) -> tuple[dict, dict]:
    create_session = harness.client.post(
        f"/v1/containers/{container_id}/activation/sessions",
        headers=harness.auth_headers(),
    )
    assert create_session.status_code == 200, create_session.text
    session_body = create_session.json()
    session_id = session_body["session_id"]

    expected = harness.client.get(
        f"/v1/containers/{container_id}/activation/sessions/{session_id}/expected",
        headers=harness.auth_headers(),
    )
    assert expected.status_code == 200, expected.text

    with harness.session() as session:
        container = session.get(harness.models.Container, container_id)
        assert container is not None
        source_root = Path(container.root_abs_path)

    staging_root = Path(expected.json()["staging_path"])
    if staging_root.exists():
        shutil.rmtree(staging_root, ignore_errors=True)
    shutil.copytree(source_root, staging_root)

    if mutate is not None:
        for path in sorted(p for p in staging_root.rglob("*") if p.is_file()):
            relpath = path.relative_to(staging_root).as_posix()
            path.write_bytes(mutate(relpath, path.read_bytes()))

    complete = harness.client.post(
        f"/v1/containers/{container_id}/activation/sessions/{session_id}/complete",
        headers=harness.auth_headers(),
    )
    return session_body, complete


def register_iso(harness, container_id: str, content: bytes) -> dict:
    source = harness.archive_root / "seed-isos" / f"{container_id}.iso"
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_bytes(content)
    response = harness.client.post(
        f"/v1/containers/{container_id}/iso/register",
        headers=harness.auth_headers(),
        json={"server_path": str(source)},
    )
    assert response.status_code == 200, response.text
    return response.json()


def create_iso(harness, container_id: str, *, overwrite: bool = False, volume_label: str | None = None) -> dict:
    payload: dict[str, object] = {"overwrite": overwrite}
    if volume_label is not None:
        payload["volume_label"] = volume_label
    response = harness.client.post(
        f"/v1/containers/{container_id}/iso/create",
        headers=harness.auth_headers(),
        json=payload,
    )
    assert response.status_code == 200, response.text
    return response.json()
