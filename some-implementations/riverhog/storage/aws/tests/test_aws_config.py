from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from a_riverhog_aws_store import app as adapter_app


def _document(tmp_path: Path) -> tuple[Path, dict[str, object]]:
    document: dict[str, object] = {
        "endpoint_url": "https://s3.example.test",
        "region": "fixture-region",
        "bucket": "fixture-bucket",
        "root_prefix": "fixture-root",
        "read_mode": "restore_required",
        "restore_tier": "Bulk",
        "restore_days": 3,
    }
    for name, value in {
        "token": "adapter-token",
        "access_key_id": "key-id",
        "secret_access_key": "secret-key",
        "session_token": "session-token",
    }.items():
        path = tmp_path / f"{name}.secret"
        path.write_text(f"{value}\n", encoding="utf-8")
        document[f"{name}_file"] = str(path)
    config_path = tmp_path / "aws.yaml"
    config_path.write_text(yaml.safe_dump(document), encoding="utf-8")
    return config_path, document


def test_aws_document_validates_before_s3_client_and_provisions_selected_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path, document = _document(tmp_path)
    created: list[tuple[object, object]] = []
    provisioned: list[tuple[object, str, str]] = []
    client = object()

    def create(config: object, *, tuning: object) -> object:
        created.append((config, tuning))
        return client

    def provision(current: object, *, bucket: str, root_prefix: str) -> None:
        provisioned.append((current, bucket, root_prefix))

    monkeypatch.setattr(adapter_app, "create_s3_client", create)
    monkeypatch.setattr(adapter_app, "provision_storage_incarnation", provision)

    assert adapter_app.AWS_CONFIG_SCHEMA == adapter_app.AwsStoreDocument.model_json_schema()
    assert adapter_app.main(["--config", str(config_path), "--provision-root"]) == 0
    assert len(created) == 1
    assert created[0][0].access_key_id == "key-id"
    assert created[0][0].session_token == "session-token"
    assert provisioned == [(client, "fixture-bucket", "fixture-root")]

    document["cloudfront"] = {"base_url": "https://distribution.example.test"}
    config_path.write_text(yaml.safe_dump(document), encoding="utf-8")
    with pytest.raises(ValueError, match="cloudfront"):
        adapter_app.main(["--config", str(config_path), "--provision-root"])
    assert len(created) == 1


def test_aws_document_requires_mounted_secret_files(tmp_path: Path) -> None:
    config_path, document = _document(tmp_path)
    document["secret_access_key_file"] = "/missing/secret"
    config_path.write_text(yaml.safe_dump(document), encoding="utf-8")
    with pytest.raises(ValueError, match="secret_access_key_file cannot be read"):
        adapter_app.load_config(config_path).client_config()
