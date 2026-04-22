from __future__ import annotations

import base64
import hashlib

import httpx

from arc_cli.client import ApiClient


def test_get_collection_quotes_reserved_characters_but_preserves_slashes(monkeypatch) -> None:
    captured: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(str(request.url))
        return httpx.Response(200, json={"id": "tax/2022 reports"})

    transport = httpx.MockTransport(handler)

    def fake_client(self: ApiClient) -> httpx.Client:
        return httpx.Client(base_url=self.base_url, transport=transport)

    monkeypatch.setattr(ApiClient, "_client", fake_client)

    client = ApiClient(base_url="https://api.test")
    client.get_collection("tax/2022 reports")

    assert captured == ["https://api.test/v1/collections/tax/2022%20reports"]


def test_create_or_resume_fetch_entry_upload_uses_manifest_entry_endpoint(monkeypatch) -> None:
    captured: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append((request.method, str(request.url)))
        return httpx.Response(
            200,
            json={
                "entry": "e1",
                "protocol": "tus",
                "upload_url": "https://uploads.test/fx-1/e1",
                "offset": 0,
                "length": 12,
                "checksum_algorithm": "sha256",
                "expires_at": "2026-04-23T00:00:00Z",
            },
        )

    transport = httpx.MockTransport(handler)

    def fake_client(self: ApiClient) -> httpx.Client:
        return httpx.Client(base_url=self.base_url, transport=transport)

    monkeypatch.setattr(ApiClient, "_client", fake_client)

    client = ApiClient(base_url="https://api.test")
    payload = client.create_or_resume_fetch_entry_upload("fx-1", "e1")

    assert payload["upload_url"] == "https://uploads.test/fx-1/e1"
    assert captured == [("POST", "https://api.test/v1/fetches/fx-1/entries/e1/upload")]


def test_append_upload_chunk_uses_tus_patch_headers(monkeypatch) -> None:
    captured: list[httpx.Request] = []
    content = b"invoice fixture bytes\n"

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(
            204,
            headers={
                "Upload-Offset": str(len(content)),
                "Upload-Expires": "2026-04-23T00:00:00Z",
            },
        )

    transport = httpx.MockTransport(handler)

    def fake_client(self: ApiClient) -> httpx.Client:
        return httpx.Client(base_url=self.base_url, transport=transport)

    monkeypatch.setattr(ApiClient, "_client", fake_client)

    client = ApiClient(base_url="https://api.test")
    payload = client.append_upload_chunk(
        "https://uploads.test/fx-1/e1",
        offset=0,
        checksum_algorithm="sha256",
        content=content,
    )

    checksum = base64.b64encode(hashlib.sha256(content).digest()).decode("ascii")

    assert payload == {
        "offset": len(content),
        "expires_at": "2026-04-23T00:00:00Z",
    }
    assert len(captured) == 1
    request = captured[0]
    assert request.method == "PATCH"
    assert str(request.url) == "https://uploads.test/fx-1/e1"
    assert request.headers["Content-Type"] == "application/offset+octet-stream"
    assert request.headers["Tus-Resumable"] == "1.0.0"
    assert request.headers["Upload-Offset"] == "0"
    assert request.headers["Upload-Checksum"] == f"sha256 {checksum}"
    assert request.read() == content
