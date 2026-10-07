from __future__ import annotations

from pathlib import Path

import yaml
from a_stove0_ffprobe_observer import FfprobeObserver
from a_stove0_riverhog_provenance_observer import RiverhogProvenanceObserver
from stove0_observer_client import load_semantic_validator_registry
from stove0_observer_protocol import require_semantic_validators

REPO_ROOT = Path(__file__).parents[4]
COMPOSE = REPO_ROOT / "some-implementations/stove0/application/compose.yaml"
CONFIG = REPO_ROOT / "qualification/fixtures/stove0/config.yaml"


def test_supplied_multi_contract_observers_have_controller_acceptance_profiles() -> None:
    registrations = yaml.safe_load(CONFIG.read_text(encoding="utf-8"))["observers"]
    image_id = "sha256:" + "a" * 64
    for registration, descriptor in (
        ("ffprobe-sampling", FfprobeObserver(image_id=image_id).descriptor()),
        ("ffprobe-streams", FfprobeObserver(image_id=image_id).descriptor()),
        ("canonical-hint", RiverhogProvenanceObserver(image_id=image_id).descriptor()),
        ("canonical-provenance", RiverhogProvenanceObserver(image_id=image_id).descriptor()),
    ):
        registry = load_semantic_validator_registry(
            registrations[registration]["semantic_validator_providers"]
        )
        require_semantic_validators(registry, descriptor)


def test_supplied_topology_uses_one_postgres_authority_and_distinct_roles() -> None:
    payload = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    services = payload["services"]

    assert set(services) == {
        "api",
        "controller",
        "a-stove0-exiftool-observer",
        "a-stove0-ffprobe-observer",
        "a-stove0-magic-observer",
        "a-stove0-filename-prefix-sidecar-observer",
        "a-stove0-riverhog-provenance-observer",
        "a-review0-nvenc-av1-opus-sampler",
        "a-stove0-nvenc-av1-opus-target",
        "a-review0-opus-sampler",
        "a-stove0-opus-target",
        "a-stove0-rclone-target",
        "review0",
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
        "a-stove0-ffprobe-observer",
        "a-stove0-magic-observer",
        "a-stove0-filename-prefix-sidecar-observer",
        "a-stove0-riverhog-provenance-observer",
        "a-review0-nvenc-av1-opus-sampler",
        "a-stove0-nvenc-av1-opus-target",
        "a-review0-opus-sampler",
        "a-stove0-opus-target",
        "a-stove0-rclone-target",
        "review0",
    ):
        assert services[name]["read_only"] is True
        assert services[name]["user"] == "65532:65532"
        assert services[name]["group_add"] == ["${STOVE0_SECRET_FILE_GID:-65532}"]
        assert services[name]["cap_drop"] == ["ALL"]
    for name in (
        "a-stove0-exiftool-observer",
        "a-stove0-ffprobe-observer",
        "a-stove0-magic-observer",
        "a-stove0-filename-prefix-sidecar-observer",
        "a-stove0-riverhog-provenance-observer",
        "a-review0-nvenc-av1-opus-sampler",
        "a-stove0-nvenc-av1-opus-target",
        "a-review0-opus-sampler",
        "a-stove0-opus-target",
        "a-stove0-rclone-target",
        "review0",
    ):
        assert "ports" not in services[name]
    assert services["a-stove0-nvenc-av1-opus-target"]["profiles"] == ["nvenc"]
    assert services["a-review0-nvenc-av1-opus-sampler"]["profiles"] == ["nvenc"]
    assert services["a-stove0-ffprobe-observer"]["command"][0] == ("a-stove0-ffprobe-observer")
    assert services["a-stove0-magic-observer"]["command"][0] == "a-stove0-magic-observer"
    assert services["a-stove0-filename-prefix-sidecar-observer"]["command"][0] == (
        "a-stove0-filename-prefix-sidecar-observer"
    )
    assert services["a-stove0-exiftool-observer"]["command"][0] == "a-stove0-exiftool-observer"
    assert services["a-stove0-opus-target"]["command"][0] == "a-stove0-opus-target"
    assert services["a-review0-opus-sampler"]["command"][0] == "a-review0-opus-sampler"
    assert services["review0"]["command"][0] == "review0"
    assert services["a-stove0-rclone-target"]["command"][0] == "a-stove0-rclone-target"
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
        "magic-observer-state",
        "exiftool-observer-state",
        "ffprobe-observer-state",
        "riverhog-provenance-observer-state",
        "filename-prefix-sidecar-observer-state",
        "a-stove0-nvenc-av1-opus-target-state",
        "a-stove0-opus-target-state",
        "review0-state",
        "a-stove0-rclone-target-state",
        "a-stove0-rclone-delivery",
        "review0-workspace",
        "a-stove0-rclone-target-workspace",
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
    assert set(services["review0"]["networks"]) == {
        "review-sampler",
        "riverhog-control",
        "stove0-internal",
    }
    assert set(services["a-stove0-rclone-target"]["networks"]) == {
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
    assert services["review0"]["image"] != services["a-stove0-rclone-target"]["image"]
    assert services["review0"]["build"]["dockerfile"] == (
        "some-implementations/stove0/review0/application/Dockerfile"
    )
    assert services["a-stove0-rclone-target"]["build"]["dockerfile"] == (
        "some-implementations/stove0/targets/rclone/Dockerfile"
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
            "http://a-stove0-ffprobe-observer:8080",
            "a_stove0_ffprobe_observer_token",
            ["ffprobe-streams", "media-sampling"],
        ),
        "ffprobe-streams": (
            "http://a-stove0-ffprobe-observer:8080",
            "a_stove0_ffprobe_observer_token",
            ["ffprobe-streams", "media-sampling"],
        ),
        "magic": (
            "http://a-stove0-magic-observer:8080",
            "a_stove0_magic_observer_token",
            ["magic"],
        ),
        "filename-prefix-sidecars": (
            "http://a-stove0-filename-prefix-sidecar-observer:8080",
            "a_stove0_filename_prefix_sidecar_observer_token",
            ["filename-prefix-sidecars"],
        ),
        "canonical-hint": (
            "http://a-stove0-riverhog-provenance-observer:8080",
            "a_stove0_riverhog_provenance_observer_token",
            ["materialization-hint", "riverhog-provenance"],
        ),
        "canonical-provenance": (
            "http://a-stove0-riverhog-provenance-observer:8080",
            "a_stove0_riverhog_provenance_observer_token",
            ["materialization-hint", "riverhog-provenance"],
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


def test_supplied_target_registrations_separate_review_and_generic_effect() -> None:
    payload = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    services = payload["services"]
    registrations = yaml.safe_load(CONFIG.read_text(encoding="utf-8"))["targets"]
    assert registrations["review"] == {
        "base_url": "http://review0:8080",
        "token_file": "/run/secrets/review0_token",
        "allow_insecure_http": True,
    }
    assert registrations["rclone"] == {
        "base_url": "http://a-stove0-rclone-target:8080",
        "token_file": "/run/secrets/a_stove0_rclone_target_token",
        "allow_insecure_http": True,
    }
    for role in ("api", "controller", "worker"):
        service = services[role]
        assert "review0_token" in service["secrets"]
        assert "a_stove0_rclone_target_token" in service["secrets"]


def test_supplied_topology_connects_bounded_operational_state_retention() -> None:
    payload = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    services = payload["services"]
    from stove0_core.runtime_config import Stove0Document

    assert Stove0Document.model_fields["operational_state_retention_seconds"].default == 2592000
    for name in (
        "a-stove0-opus-target",
        "a-stove0-nvenc-av1-opus-target",
        "review0",
        "a-stove0-rclone-target",
    ):
        assert (
            services[name]["environment"]["STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS"]
            == "${STOVE0_TARGET_TERMINAL_STATE_RETENTION_SECONDS:-2592000}"
        )
