from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml
from stove0_core import database_url_from_config, load_stove0_config
from stove0_core.runtime_config import generated_config_schema

SCHEMA = Path(__file__).parents[1] / "server/src/stove0_core/config.schema.json"


def _config(tmp_path: Path) -> tuple[Path, dict[str, object]]:
    secrets = {
        "database_url_file": "postgresql+psycopg://stove0@postgres/stove0",
        "api_token_file": "operator-token",
        "riverhog_token_file": "role-specific-riverhog-token",
        "browse_token_signing_key_file": "stove0-test-browse-token-signing-key-v1",
    }
    document: dict[str, object] = {
        "riverhog_base_url": "https://riverhog.invalid",
        "declared_workspace_protection": "encrypted-at-rest",
        "recipes": {"format": "stove0-recipes/v1", "operations": [], "recipes": []},
    }
    for name, value in secrets.items():
        path = tmp_path / name
        path.write_text(value + "\n", encoding="utf-8")
        document[name] = str(path)
    path = tmp_path / "stove0.yaml"
    _write(path, document)
    return path, document


def _write(path: Path, document: dict[str, object]) -> None:
    path.write_text(yaml.safe_dump(document, sort_keys=True), encoding="utf-8")


def test_published_schema_matches_parser() -> None:
    assert json.loads(SCHEMA.read_text(encoding="utf-8")) == generated_config_schema()


def test_scheduler_configuration_does_not_require_operator_api_secret(tmp_path: Path) -> None:
    path, document = _config(tmp_path)
    document.pop("api_token_file")
    _write(path, document)
    config = load_stove0_config(path, require_api_token=False)
    assert config.api_token is None
    assert config.riverhog_token == "role-specific-riverhog-token"
    assert config.declared_workspace_protection == "encrypted-at-rest"
    assert config.recipes.recipes == ()


def test_runtime_configuration_connects_policy_and_registrations(tmp_path: Path) -> None:
    path, document = _config(tmp_path)
    callback_key = tmp_path / "callback-key"
    callback_key.write_text("target-callback-signing-key\n", encoding="utf-8")
    observer_token = tmp_path / "observer-token"
    observer_token.write_text("observer-secret\n", encoding="utf-8")
    document.update(
        riverhog_allow_insecure_http=True,
        declared_workspace_protection="memory-backed",
        claim_lease_seconds=240,
        capability_ttl_seconds=120,
        scheduler_interval_seconds=0.5,
        operational_state_retention_seconds=86400,
        browse_token_lifetime_seconds=7200,
        target_authority_batch_size=17,
        observers={
            "probe": {
                "base_url": "http://probe:8080",
                "token_file": str(observer_token),
                "allow_insecure_http": True,
                "semantic_validator_providers": ["fixture"],
            }
        },
        targets={"target": {"base_url": "https://target.invalid"}},
        departure_targets={"index": {"base_url": "https://index.invalid"}},
        target_callback_base_url="http://stove0.internal:8080",
        target_callback_allow_insecure_http=True,
        target_callback_signing_key_file=str(callback_key),
        admissions={"format": "stove0-admissions/v1", "policies": []},
        departures={"format": "stove0-departures/v1", "policies": []},
    )
    _write(path, document)
    config = load_stove0_config(path)
    assert config.api_token == "operator-token"
    assert config.riverhog_allow_insecure_http is True
    assert config.observers["probe"].token == "observer-secret"
    assert config.observers["probe"].semantic_validator_providers == ("fixture",)
    assert config.targets["target"].base_url == "https://target.invalid"
    assert config.departure_targets["index"].base_url == "https://index.invalid"
    assert config.departures.format == "stove0-departures/v1"
    assert config.target_callback_base_url == "http://stove0.internal:8080"
    assert config.target_callback_allow_insecure_http is True
    assert config.target_callback_signing_key == "target-callback-signing-key"
    assert config.target_authority_batch_size == 17
    assert config.claim_lease_seconds == 240
    assert config.capability_ttl_seconds == 120
    assert config.scheduler_interval_seconds == 0.5
    assert config.operational_state_retention_seconds == 86400
    assert config.browse_token_lifetime_seconds == 7200


def test_missing_api_secret_and_callback_secret_fail_before_startup(tmp_path: Path) -> None:
    path, document = _config(tmp_path)
    document.pop("api_token_file")
    _write(path, document)
    with pytest.raises(ValueError, match="api_token_file is required"):
        load_stove0_config(path)
    document["targets"] = {"target": {"base_url": "https://target.invalid"}}
    document["target_callback_base_url"] = "https://stove0.invalid"
    _write(path, document)
    with pytest.raises(ValueError, match="target_callback_signing_key_file is required"):
        load_stove0_config(path, require_api_token=False)


def test_database_url_is_postgresql_only(tmp_path: Path) -> None:
    path, document = _config(tmp_path)
    Path(str(document["database_url_file"])).write_text("sqlite+pysqlite:///:memory:")
    with pytest.raises(ValueError, match="must use postgresql"):
        database_url_from_config(path)
    with pytest.raises(ValueError, match="must use postgresql"):
        load_stove0_config(path)


def test_schema_rejects_unknown_policy_and_nonfinite_interval(tmp_path: Path) -> None:
    path, document = _config(tmp_path)
    document["STOVE0_TARGETS_JSON"] = "{}"
    _write(path, document)
    with pytest.raises(ValueError, match="Additional properties are not allowed"):
        load_stove0_config(path)
    document.pop("STOVE0_TARGETS_JSON")
    document["scheduler_interval_seconds"] = float("nan")
    _write(path, document)
    with pytest.raises(ValueError, match="non-finite"):
        load_stove0_config(path)


def test_registration_provider_constraints_and_secret_errors(tmp_path: Path) -> None:
    path, document = _config(tmp_path)
    document["observers"] = {
        "probe": {
            "base_url": "https://probe.invalid",
            "semantic_validator_providers": ["same", "same"],
        }
    }
    _write(path, document)
    with pytest.raises(ValueError, match="must be nonempty and unique"):
        load_stove0_config(path)
    document["observers"] = {}
    document["targets"] = {
        "target": {
            "base_url": "https://target.invalid",
            "semantic_validator_providers": ["fixture"],
        }
    }
    _write(path, document)
    with pytest.raises(ValueError, match="Additional properties are not allowed"):
        load_stove0_config(path)
    document["targets"] = {}
    document["riverhog_token_file"] = str(tmp_path / "missing")
    _write(path, document)
    with pytest.raises(ValueError, match="cannot be read"):
        load_stove0_config(path)


def test_runtime_repr_does_not_emit_secret_material(tmp_path: Path) -> None:
    path, _ = _config(tmp_path)
    rendered = repr(load_stove0_config(path))
    assert "role-specific-riverhog-token" not in rendered
    assert "stove0-test-browse-token-signing-key-v1" not in rendered
