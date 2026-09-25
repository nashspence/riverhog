from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Any

import pytest
import yaml
from a_riverhog_b2_store import app as adapter_app
from a_riverhog_s3_store_lib.incarnation import marker_document, marker_key


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
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "b2.yaml"
    document: dict[str, object] = {
        "endpoint_url": "https://s3.us-west.example.test",
        "region": "us-west-test",
        "bucket": "fixture-bucket",
    }
    for name, value in {
        "token": "adapter-token",
        "access_key_id": "key-id",
        "secret_access_key": "secret-key",
    }.items():
        path = tmp_path / f"{name}.secret"
        path.write_text(f"{value}\n", encoding="utf-8")
        document[f"{name}_file"] = str(path)
    config_path.write_text(yaml.safe_dump(document), encoding="utf-8")
    client = _Client()
    configs: list[object] = []
    apps: list[object] = []
    bindings: list[dict[str, Any]] = []

    def create(config: object, *, tuning: object) -> _Client:
        configs.extend((config, tuning))
        return client

    monkeypatch.setattr(adapter_app, "create_s3_client", create)
    monkeypatch.setattr(
        adapter_app,
        "create_storage_adapter_app",
        lambda **kwargs: bindings.append(kwargs) or object(),
    )
    monkeypatch.setattr(
        "a_riverhog_b2_store.app.importlib.metadata.version",
        lambda _name: "1.0.0",
    )
    monkeypatch.setattr(
        "a_riverhog_b2_store.app.uvicorn.run",
        lambda app, **_kwargs: apps.append(app),
    )

    assert adapter_app.main(["--config", str(config_path)]) == 0
    assert len(configs) == 2
    assert len(apps) == 1
    assert bindings[0]["token"] == "adapter-token"
    descriptor = bindings[0]["adapter"].descriptor()
    assert descriptor.implementation_id == "a-riverhog-b2-store/v1"
    assert descriptor.read_mode == "immediate"
    bindings[0]["readiness"]()
    assert client.ready == [{"Bucket": "fixture-bucket"}]
    assert adapter_app.B2_CONFIG_SCHEMA == adapter_app.B2StoreDocument.model_json_schema()

    document["unknown_policy"] = True
    config_path.write_text(yaml.safe_dump(document), encoding="utf-8")
    with pytest.raises(ValueError, match="Additional properties are not allowed"):
        adapter_app.main(["--config", str(config_path)])
    assert len(configs) == 2
