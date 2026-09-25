from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from a_riverhog_ftp_spool.app import FtpSpoolComposition
from a_riverhog_ftp_spool.config import (
    FTP_SPOOL_CONFIG_SCHEMA,
    FtpSpoolConfig,
    SourceConfig,
    generated_config_schema,
    load_config,
    load_source_config,
)

REPO_ROOT = Path(__file__).parents[5]


def _write_config(path: Path, root: Path, *, host_id: str) -> None:
    (path.parent / "riverhog.token").write_text("riverhog-file-token\n", encoding="utf-8")
    (path.parent / "adapter.token").write_text("adapter-file-token\n", encoding="utf-8")
    path.write_text(
        yaml.safe_dump(
            {
                "host_id": host_id,
                "riverhog_base_url": "https://riverhog.invalid",
                "riverhog_token_file": str(path.parent / "riverhog.token"),
                "api_token_file": str(path.parent / "adapter.token"),
                "provenance_observer": "fixture-observer",
                "sources": [
                    {
                        "id": "ftp",
                        "root": str(root),
                        "ingest_source": "ftp:fixture",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )


def test_adapter_secrets_are_loaded_from_named_files(tmp_path: Path) -> None:
    config_path = tmp_path / "ftp-spool.yaml"
    _write_config(
        config_path,
        tmp_path / "ftp",
        host_id="urn:uuid:00000000-0000-4000-8000-000000000001",
    )
    config = load_config(config_path)

    assert config.riverhog_token == "riverhog-file-token"
    assert config.api_token == "adapter-file-token"
    (tmp_path / "adapter.token").write_text("\n", encoding="utf-8")
    with pytest.raises(ValueError, match="api_token_file must contain a nonempty secret"):
        load_config(config_path)


def test_listener_loads_only_its_source_without_riverhog_credentials(tmp_path: Path) -> None:
    config_path = tmp_path / "ftp-spool.yaml"
    _write_config(
        config_path,
        tmp_path / "ftp",
        host_id="urn:uuid:00000000-0000-4000-8000-000000000001",
    )

    source = load_source_config(config_path, "ftp")

    assert source.root == (tmp_path / "ftp").resolve()
    assert source.ingest_source == "ftp:fixture"


def test_source_grouping_policy_has_no_hidden_collection_ceiling(tmp_path: Path) -> None:
    source = SourceConfig(
        id="large-source",
        root=tmp_path,
        ingest_source="ftp:large-fixture",
        max_files=100_001,
    )

    assert source.max_files == 100_001


def test_supplied_compose_is_ftp_only_bounded_and_unprivileged() -> None:
    path = REPO_ROOT / "some-implementations/riverhog/ingress/ftp/compose.yaml"
    compose = yaml.safe_load(path.read_text(encoding="utf-8"))
    services = compose["services"]

    assert set(services) == {"ftp-spool", "ftp-listener", "intake-init"}
    assert services["ftp-spool"]["read_only"] is True
    assert services["ftp-spool"]["user"] == "65532:65532"
    assert services["ftp-spool"]["group_add"] == ["${A_RIVERHOG_FTP_SPOOL_SECRET_FILE_GID:-65532}"]
    assert services["ftp-spool"]["networks"] == ["default", "riverhog-control"]
    listener = services["ftp-listener"]
    assert listener["image"] == services["ftp-spool"]["image"]
    assert listener["build"] == services["ftp-spool"]["build"]
    assert listener["read_only"] is True
    assert listener["user"] == "65532:65532"
    assert listener["cap_drop"] == ["ALL"]
    assert listener["secrets"] == ["ftp_password"]
    assert listener["command"][:4] == [
        "a-riverhog-ftp-spool",
        "--config",
        "/etc/riverhog/ftp-spool.yaml",
        "listen",
    ]
    assert "/run/secrets/ftp_password" in listener["command"]
    assert "volumes" not in compose
    assert compose["networks"]["riverhog-control"] == {
        "external": True,
        "name": "${RIVERHOG_CONTROL_NETWORK:-riverhog_default}",
    }
    config_mount = next(
        item
        for item in services["ftp-spool"]["volumes"]
        if item["target"] == "/etc/riverhog/ftp-spool.yaml"
    )
    assert config_mount["source"] == (
        "${A_RIVERHOG_FTP_SPOOL_CONFIG_HOST_PATH:"
        "?A_RIVERHOG_FTP_SPOOL_CONFIG_HOST_PATH is required}"
    )
    assert "archive" not in {key.casefold() for key in compose.get("volumes", {})}


def test_completion_failure_work_and_capacity_are_explicit(tmp_path: Path) -> None:
    config = FtpSpoolConfig(
        host_id="test-host",
        riverhog_base_url="https://riverhog.invalid",
        riverhog_token="riverhog-token",
        api_token="adapter-token",
        completion_failure_capacity=23,
        completion_failure_attempt_budget=7,
        sources=(
            SourceConfig(
                id="ftp",
                root=tmp_path / "ftp",
                ingest_source="ftp:test",
                provenance="omit",
                provenance_omission_reason="Fixture intentionally omits provenance.",
            ),
        ),
    )

    assert config.completion_failure_capacity == 23
    assert config.completion_failure_attempt_budget == 7


def test_supplied_configuration_is_current_and_secret_injected(
    tmp_path: Path,
) -> None:
    riverhog_token = tmp_path / "riverhog.token"
    adapter_token = tmp_path / "adapter.token"
    riverhog_token.write_text("riverhog\n", encoding="utf-8")
    adapter_token.write_text("adapter\n", encoding="utf-8")
    fixture = REPO_ROOT / "qualification/fixtures/a-riverhog-ftp-spool/config.yaml"
    document = yaml.safe_load(fixture.read_text(encoding="utf-8"))
    document["riverhog_token_file"] = str(riverhog_token)
    document["api_token_file"] = str(adapter_token)
    local_config = tmp_path / "ftp-spool.yaml"
    local_config.write_text(yaml.safe_dump(document), encoding="utf-8")
    config = load_config(local_config)

    assert config.host_id == "urn:uuid:00000000-0000-4000-8000-000000000001"
    assert config.riverhog_base_url == "http://app:8000"
    assert config.poll_seconds == 5
    assert config.pending_claim_capacity == 128
    assert config.claim_attempt_budget == 8
    assert config.discovery_entry_budget == 4096
    assert config.completion_failure_capacity == 128
    assert config.completion_failure_attempt_budget == 8
    assert [source.id for source in config.sources] == ["ftp-intake"]
    source = config.source("ftp-intake")
    assert source.ingest_source == "ftp:example-intake"
    assert source.description == "Example FTP intake"
    assert source.tags == ("source:ftp",)
    assert source.close_mode == "stable"
    assert source.provenance_omission_reason == (
        "The FTP producer cannot observe the source host filesystem."
    )
    assert source.max_bytes > 0 and source.max_files > 0


def test_adapter_config_path_environment_is_connected(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "ftp-spool.yaml"
    _write_config(
        config_path,
        tmp_path / "ftp",
        host_id="urn:uuid:00000000-0000-4000-8000-000000000001",
    )
    monkeypatch.setenv("A_RIVERHOG_FTP_SPOOL_CONFIG", str(config_path))
    assert load_config().host_id == "urn:uuid:00000000-0000-4000-8000-000000000001"


def test_capture_configuration_requires_a_canonical_provenance_host_identity(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "ftp-spool.yaml"
    _write_config(config_path, tmp_path / "ftp", host_id="human-label")

    with pytest.raises(ValueError, match="host_id must be a lowercase UUID URN"):
        load_config(config_path)


def test_published_yaml_schema_matches_typed_document_and_rejects_extra_policy(
    tmp_path: Path,
) -> None:
    assert FTP_SPOOL_CONFIG_SCHEMA == generated_config_schema()
    config_path = tmp_path / "ftp-spool.yaml"
    _write_config(
        config_path,
        tmp_path / "ftp",
        host_id="urn:uuid:00000000-0000-4000-8000-000000000001",
    )
    document = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    document["unknown_policy"] = True
    config_path.write_text(yaml.safe_dump(document), encoding="utf-8")
    with pytest.raises(ValueError, match="Additional properties are not allowed"):
        load_config(config_path)


def test_capture_requires_one_explicit_connected_observer_provider(tmp_path: Path) -> None:
    source = SourceConfig(
        id="ftp",
        root=tmp_path / "ftp",
        ingest_source="ftp:fixture",
    )
    values = {
        "host_id": "urn:uuid:00000000-0000-4000-8000-000000000675",
        "riverhog_base_url": "https://riverhog.invalid",
        "riverhog_token": "riverhog-token",
        "api_token": "adapter-token",
        "sources": (source,),
    }

    with pytest.raises(ValueError, match="explicit observer provider"):
        FtpSpoolConfig(**values)

    config = FtpSpoolConfig(provenance_observer="a-riverhog-linux-provenance-observer", **values)
    assert config.provenance_observer == "a-riverhog-linux-provenance-observer"
    composition = FtpSpoolComposition.build(config)
    try:
        assert (
            composition.adapter.status()["provenance_observer"]
            == "a-riverhog-linux-provenance-observer"
        )
    finally:
        composition.api.close()


def test_omission_does_not_accept_an_unused_observer_setting(tmp_path: Path) -> None:
    source = SourceConfig(
        id="ftp",
        root=tmp_path / "ftp",
        ingest_source="ftp:fixture",
        provenance="omit",
        provenance_omission_reason="Fixture explicitly omits provenance.",
    )

    with pytest.raises(ValueError, match="observer is unused"):
        FtpSpoolConfig(
            host_id="fixture",
            riverhog_base_url="https://riverhog.invalid",
            riverhog_token="riverhog-token",
            api_token="adapter-token",
            provenance_observer="a-riverhog-linux-provenance-observer",
            sources=(source,),
        )
