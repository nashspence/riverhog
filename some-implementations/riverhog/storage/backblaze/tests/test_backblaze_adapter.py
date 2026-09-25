from __future__ import annotations

import os
from io import BytesIO
from typing import Any, cast

from a_riverhog_b2_store import app as adapter_app
from a_riverhog_s3_store_lib.incarnation import marker_document, marker_key
from fastapi import FastAPI
from fastapi.testclient import TestClient


class _Client:
    def __init__(self) -> None:
        self.ready: list[dict[str, str]] = []

    def head_bucket(self, **request: str) -> None:
        self.ready.append(request)

    def get_object(self, **request: str) -> dict[str, object]:
        assert request == {"Bucket": "fixture-bucket", "Key": marker_key("")}
        return {"Body": BytesIO(marker_document("00000000-0000-4000-8000-000000000001"))}


def test_backblaze_artifact_is_one_immediate_s3_target(
    monkeypatch: Any,
) -> None:
    values = {
        "A_RIVERHOG_B2_STORE_TOKEN": "adapter-token",
        "A_RIVERHOG_B2_STORE_ENDPOINT_URL": "https://s3.us-west.example.test",
        "A_RIVERHOG_B2_STORE_REGION": "us-west-test",
        "A_RIVERHOG_B2_STORE_BUCKET": "fixture-bucket",
        "A_RIVERHOG_B2_STORE_ACCESS_KEY_ID": "key-id",
        "A_RIVERHOG_B2_STORE_SECRET_ACCESS_KEY": "secret-key",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)
    client = _Client()
    configs: list[object] = []
    apps: list[object] = []

    def create(config: object, *, tuning: object) -> _Client:
        configs.extend((config, tuning))
        return client

    monkeypatch.setattr(adapter_app, "create_s3_client", create)
    monkeypatch.setattr(
        "a_riverhog_b2_store.app.importlib.metadata.version",
        lambda _name: "1.0.0",
    )
    monkeypatch.setattr(
        "a_riverhog_b2_store.app.uvicorn.run",
        lambda app, **_kwargs: apps.append(app),
    )

    assert adapter_app.main([]) == 0
    assert len(configs) == 2
    assert len(apps) == 1
    http = TestClient(cast(FastAPI, apps[0]))
    descriptor = http.get(
        "/v1/adapter",
        headers={"Authorization": "Bearer adapter-token"},
    )

    assert descriptor.status_code == 200
    assert descriptor.json()["implementation_id"] == "a-riverhog-b2-store/v1"
    assert descriptor.json()["read_mode"] == "immediate"
    assert http.get("/health/ready").status_code == 200
    assert client.ready == [{"Bucket": "fixture-bucket"}]
    assert "A_RIVERHOG_B2_STORE_TOKEN" not in os.environ
    assert "A_RIVERHOG_B2_STORE_SECRET_ACCESS_KEY" not in os.environ
