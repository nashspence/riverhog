from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pytest

from arc_core.runtime_config import RuntimeConfig
from arc_core.stores.s3_hot_store import S3HotStore


class _FakeBody:
    def __init__(self, content: bytes) -> None:
        self._content = content

    def read(self) -> bytes:
        return self._content


class _FakeS3Client:
    def __init__(self) -> None:
        self.objects: dict[str, dict[str, Any]] = {}
        self.deleted: list[str] = []
        self.copy_sources: list[dict[str, str]] = []
        self.read_sizes: list[int] = []

    def put_object(self, *, Bucket: str, Key: str, Body: object, **kwargs: Any) -> None:
        _ = Bucket
        if isinstance(Body, bytes):
            body = Body
        else:
            read = Body.read
            parts: list[bytes] = []
            while True:
                chunk = cast(bytes, read(3))
                if not chunk:
                    break
                self.read_sizes.append(len(chunk))
                parts.append(chunk)
            body = b"".join(parts)
        self.objects[Key] = {
            "Body": body,
            "ContentLength": len(body),
            "LastModified": datetime(2026, 4, 20, 4, 1, 0, tzinfo=UTC),
            **kwargs,
        }

    def copy_object(
        self,
        *,
        Bucket: str,
        Key: str,
        CopySource: dict[str, str],
    ) -> None:
        _ = Bucket
        self.copy_sources.append(CopySource)
        source = self.objects[CopySource["Key"]]
        self.objects[Key] = {**source}

    def delete_object(self, *, Bucket: str, Key: str) -> None:
        _ = Bucket
        self.deleted.append(Key)
        self.objects.pop(Key, None)

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        _ = Bucket
        return {"Body": _FakeBody(cast(bytes, self.objects[Key]["Body"]))}


def _config(tmp_path: Path) -> RuntimeConfig:
    config = RuntimeConfig(
        object_store="s3",
        s3_endpoint_url="http://example.invalid:9000",
        s3_region="us-east-1",
        s3_bucket="riverhog",
        s3_access_key_id="test-access",
        s3_secret_access_key="test-secret",
        s3_force_path_style=True,
        tusd_base_url="http://example.invalid:1080/files",
        tusd_hook_secret="hook-secret",
        sqlite_path=tmp_path / "state.sqlite3",
    )
    return config


def _store_with_client(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    client: _FakeS3Client,
) -> S3HotStore:
    monkeypatch.setattr(
        "arc_core.stores.s3_hot_store.create_s3_client",
        lambda config: client,
    )
    return S3HotStore(_config(tmp_path))


def test_put_collection_file_stream_promotes_complete_temp_object(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client = _FakeS3Client()
    store = _store_with_client(monkeypatch, tmp_path, client)

    store.put_collection_file_stream(
        "docs",
        "large.bin",
        (chunk for chunk in [b"abc", b"defg", b"hi"]),
        content_length=9,
    )

    assert store.get_collection_file("docs", "large.bin") == b"abcdefghi"
    assert client.read_sizes == [3, 3, 3]
    assert client.copy_sources == [
        {"Bucket": "riverhog", "Key": client.deleted[0]},
    ]
    assert client.deleted[0] not in client.objects


def test_put_collection_file_stream_does_not_promote_failed_stream(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client = _FakeS3Client()
    store = _store_with_client(monkeypatch, tmp_path, client)

    def chunks() -> Iterable[bytes]:
        yield b"abc"
        raise ValueError("bad stream")

    with pytest.raises(ValueError, match="bad stream"):
        store.put_collection_file_stream(
            "docs",
            "large.bin",
            chunks(),
            content_length=9,
        )

    assert "collections/docs/large.bin" not in client.objects
    assert client.copy_sources == []
    assert client.deleted == []
