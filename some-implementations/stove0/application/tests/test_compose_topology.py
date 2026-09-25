from __future__ import annotations

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).parents[4]
COMPOSE = REPO_ROOT / "some-implementations/stove0/application/compose.yaml"
CONFIG = REPO_ROOT / "qualification/fixtures/stove0/config.yaml"


def test_supplied_topology_uses_one_postgres_authority_and_distinct_roles() -> None:
    payload = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    services = payload["services"]

    assert set(services) == {
        "api",
        "controller",
        "a-stove0-exiftool-observer",
        "a-stove0-ffprobe-sampling-observer",
        "a-review0-nvenc-av1-opus-sampler",
        "a-stove0-nvenc-av1-opus-target",
        "a-review0-opus-sampler",
        "a-stove0-opus-target",
        "a-review0-rclone-target",
        "a-review0-materializer",
        "state",
        "worker",
    }
    assert services["controller"]["command"][-1] == "controller"
    assert services["worker"]["command"][-1] == "worker"
    for name in ("state", "api", "controller", "worker"):
        assert services[name]["environment"]["STOVE0_CONFIG"] == "/etc/stove0/config.yaml"
    assert services["controller"]["secrets"][-1] == {
        "source": "stove0_controller_riverhog_token",
        "target": "stove0_riverhog_token",
    }
    assert services["worker"]["secrets"][-1] == {
        "source": "stove0_worker_riverhog_token",
        "target": "stove0_riverhog_token",
    }
    assert "stove0_api_token" not in services["controller"]["secrets"]
    assert "stove0_api_token" not in services["worker"]["secrets"]
    configuration_mounts = services["api"]["volumes"]
    assert configuration_mounts == [
        {
            "type": "bind",
            "source": "${STOVE0_CONFIG_HOST_PATH:?STOVE0_CONFIG_HOST_PATH is required}",
            "target": "/etc/stove0/config.yaml",
            "read_only": True,
        },
    ]


def test_supplied_topology_keeps_payload_scratch_ephemeral_and_roles_private() -> None:
    payload = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    services = payload["services"]
    for name in (
        "api",
        "controller",
        "worker",
        "state",
        "a-stove0-exiftool-observer",
        "a-stove0-ffprobe-sampling-observer",
        "a-review0-nvenc-av1-opus-sampler",
        "a-stove0-nvenc-av1-opus-target",
        "a-review0-opus-sampler",
        "a-stove0-opus-target",
        "a-review0-rclone-target",
        "a-review0-materializer",
    ):
        assert services[name]["read_only"] is True
        assert services[name]["user"] == "65532:65532"
        assert services[name]["group_add"] == ["${STOVE0_SECRET_FILE_GID:-65532}"]
        assert services[name]["cap_drop"] == ["ALL"]
    for name in (
        "a-stove0-exiftool-observer",
        "a-stove0-ffprobe-sampling-observer",
        "a-review0-nvenc-av1-opus-sampler",
        "a-stove0-nvenc-av1-opus-target",
        "a-review0-opus-sampler",
        "a-stove0-opus-target",
        "a-review0-rclone-target",
        "a-review0-materializer",
    ):
        assert "ports" not in services[name]
    assert services["a-stove0-nvenc-av1-opus-target"]["profiles"] == ["nvenc"]
    assert services["a-review0-nvenc-av1-opus-sampler"]["profiles"] == ["nvenc"]
    assert services["a-stove0-ffprobe-sampling-observer"]["command"][0] == (
        "a-stove0-ffprobe-sampling-observer"
    )
    assert services["a-stove0-exiftool-observer"]["command"][0] == "a-stove0-exiftool-observer"
    assert services["a-stove0-opus-target"]["command"][0] == "a-stove0-opus-target"
    assert services["a-review0-opus-sampler"]["command"][0] == "a-review0-opus-sampler"
    assert services["a-review0-materializer"]["command"][0] == "a-review0-materializer"
    assert services["a-review0-rclone-target"]["command"][0] == "a-review0-rclone-target"
    assert (
        services["a-stove0-nvenc-av1-opus-target"]["command"][0] == "a-stove0-nvenc-av1-opus-target"
    )
    assert (
        services["a-review0-nvenc-av1-opus-sampler"]["command"][0]
        == "a-review0-nvenc-av1-opus-sampler"
    )
    assert payload["networks"]["stove0-internal"]["internal"] is True
    assert payload["networks"]["review-sampler"]["internal"] is True
    assert set(payload["volumes"]) == {
        "a-stove0-nvenc-av1-opus-target-state",
        "a-stove0-opus-target-state",
        "a-review0-materializer-state",
        "a-review0-rclone-target-state",
        "review0-effect-delivery",
        "review0-workspace",
    }


def test_sampler_containers_share_only_ephemeral_workspace_and_no_authority() -> None:
    payload = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    services = payload["services"]
    for name in ("a-review0-opus-sampler", "a-review0-nvenc-av1-opus-sampler"):
        service = services[name]
        assert service["networks"] == ["review-sampler"]
        assert service["volumes"] == ["review0-workspace:/run/review0"]
        assert all("riverhog" not in item for item in service["secrets"])
        assert all("target_token" not in item for item in service["secrets"])
        assert "STOVE0_DATABASE_URL_FILE" not in service["environment"]
    for name in ("a-review0-materializer", "a-review0-rclone-target"):
        assert set(services[name]["networks"]) == {
            "review-sampler",
            "riverhog-control",
            "stove0-internal",
        }
    workspace = payload["volumes"]["review0-workspace"]
    assert workspace["driver_opts"]["type"] == "tmpfs"


def test_paired_target_and_sampler_roles_bind_the_same_manifest_and_image_id() -> None:
    payload = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    services = payload["services"]
    assert services["a-stove0-opus-target"]["image"] == services["a-review0-opus-sampler"]["image"]
    assert services["a-stove0-opus-target"]["image"].startswith(
        "${A_STOVE0_OPUS_TARGET_IMAGE_REF:-"
    )
    assert (
        services["a-stove0-opus-target"]["environment"]["A_STOVE0_OPUS_TARGET_IMAGE_ID"]
        == services["a-review0-opus-sampler"]["environment"]["A_REVIEW0_OPUS_SAMPLER_IMAGE_ID"]
    )
    assert (
        services["a-stove0-nvenc-av1-opus-target"]["image"]
        == services["a-review0-nvenc-av1-opus-sampler"]["image"]
    )
    assert services["a-stove0-nvenc-av1-opus-target"]["image"].startswith(
        "${A_STOVE0_NVENC_AV1_OPUS_TARGET_IMAGE_REF:-"
    )
    assert (
        services["a-stove0-nvenc-av1-opus-target"]["environment"][
            "A_STOVE0_NVENC_AV1_OPUS_TARGET_IMAGE_ID"
        ]
        == services["a-review0-nvenc-av1-opus-sampler"]["environment"][
            "A_REVIEW0_NVENC_AV1_OPUS_SAMPLER_IMAGE_ID"
        ]
    )
    assert (
        services["a-stove0-opus-target"]["image"]
        != services["a-stove0-nvenc-av1-opus-target"]["image"]
    )
    assert (
        services["a-review0-materializer"]["image"] != services["a-review0-rclone-target"]["image"]
    )
    assert services["a-review0-materializer"]["build"]["dockerfile"] == (
        "some-implementations/stove0/review0/materialize-target/Dockerfile"
    )
    assert services["a-review0-rclone-target"]["build"]["dockerfile"] == (
        "some-implementations/stove0/review0/rclone-effect-target/Dockerfile"
    )
    assert services["a-stove0-opus-target"]["build"]["dockerfile"] == (
        "some-implementations/stove0/targets/opus/Dockerfile"
    )
    assert services["a-stove0-nvenc-av1-opus-target"]["build"]["dockerfile"] == (
        "some-implementations/stove0/targets/nvenc-av1-opus/Dockerfile"
    )
    assert "gpus" not in services["a-stove0-opus-target"]
    assert "profiles" not in services["a-stove0-opus-target"]


def test_supplied_topology_uses_secret_files_and_explicit_lan_http_opt_in() -> None:
    payload = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    services = payload["services"]
    config = yaml.safe_load(CONFIG.read_text(encoding="utf-8"))
    assert config["riverhog_allow_insecure_http"] is True
    assert config["target_callback_base_url"] == "http://api:8080"
    assert config["target_callback_allow_insecure_http"] is True
    assert config["target_callback_signing_key_file"] == (
        "/run/secrets/stove0_target_callback_signing_key"
    )
    assert config["riverhog_token_file"] == "/run/secrets/stove0_riverhog_token"
    for name in ("api", "controller", "worker"):
        environment = services[name]["environment"]
        assert environment["STOVE0_CONFIG"] == "/etc/stove0/config.yaml"
        assert "stove0_target_callback_signing_key" in services[name]["secrets"]
        assert any(
            isinstance(secret, dict) and secret["target"] == "stove0_riverhog_token"
            for secret in services[name]["secrets"]
        )
        assert "RIVERHOG_TOKEN" not in environment
    text = COMPOSE.read_text(encoding="utf-8")
    assert "STOVE0_API_TOKEN=" not in text
    assert "RIVERHOG_TOKEN=" not in text


def test_supplied_observer_registrations_connect_exact_one_role_services() -> None:
    payload = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    services = payload["services"]
    registrations = yaml.safe_load(CONFIG.read_text(encoding="utf-8"))["observers"]
    expected = {
        "exiftool": (
            "http://a-stove0-exiftool-observer:8080",
            "a_stove0_exiftool_observer_token",
            ["media-metadata"],
        ),
        "ffprobe-sampling": (
            "http://a-stove0-ffprobe-sampling-observer:8080",
            "a_stove0_ffprobe_sampling_observer_token",
            ["media-sampling"],
        ),
    }
    assert set(registrations) == set(expected)
    for registration, (base_url, secret, providers) in expected.items():
        assert registrations[registration] == {
            "base_url": base_url,
            "token_file": f"/run/secrets/{secret}",
            "allow_insecure_http": True,
            "semantic_validator_providers": providers,
        }
    for role in ("api", "controller", "worker"):
        service = services[role]
        for _, (_, secret, _) in expected.items():
            assert secret in service["secrets"]


def test_supplied_target_registrations_bind_fixed_review_result_modes() -> None:
    payload = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    services = payload["services"]
    registrations = yaml.safe_load(CONFIG.read_text(encoding="utf-8"))["targets"]
    assert registrations["review"] == {
        "base_url": "http://a-review0-materializer:8080",
        "token_file": "/run/secrets/a_review0_materializer_token",
        "allow_insecure_http": True,
    }
    assert registrations["review-effect"] == {
        "base_url": "http://a-review0-rclone-target:8080",
        "token_file": "/run/secrets/a_review0_rclone_target_token",
        "allow_insecure_http": True,
    }
    for role in ("api", "controller", "worker"):
        service = services[role]
        assert "a_review0_materializer_token" in service["secrets"]
        assert "a_review0_rclone_target_token" in service["secrets"]


def test_supplied_topology_connects_bounded_operational_state_retention() -> None:
    payload = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    services = payload["services"]
    from stove0_core.runtime_config import Stove0Document

    assert Stove0Document.model_fields["operational_state_retention_seconds"].default == 2592000
    for name in (
        "a-stove0-opus-target",
        "a-stove0-nvenc-av1-opus-target",
        "a-review0-materializer",
        "a-review0-rclone-target",
    ):
        assert (
            services[name]["environment"]["STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS"]
            == "${STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS:-2592000}"
        )
