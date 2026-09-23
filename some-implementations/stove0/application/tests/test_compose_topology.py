from __future__ import annotations

import json
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).parents[4]
COMPOSE = REPO_ROOT / "some-implementations/stove0/application/compose.yaml"


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
    assert (
        services["controller"]["environment"]["STOVE0_DATABASE_URL_FILE"]
        == services["worker"]["environment"]["STOVE0_DATABASE_URL_FILE"]
    )
    assert (
        services["controller"]["environment"]["RIVERHOG_TOKEN_FILE"]
        != services["worker"]["environment"]["RIVERHOG_TOKEN_FILE"]
    )
    assert "STOVE0_API_TOKEN_FILE" not in services["controller"]["environment"]
    assert "STOVE0_API_TOKEN_FILE" not in services["worker"]["environment"]
    configuration_mounts = services["api"]["volumes"][:2]
    assert configuration_mounts == [
        {
            "type": "bind",
            "source": "${STOVE0_RECIPES_HOST_PATH:?STOVE0_RECIPES_HOST_PATH is required}",
            "target": "/etc/stove0/recipes.yaml",
            "read_only": True,
        },
        {
            "type": "bind",
            "source": "${STOVE0_ADMISSIONS_HOST_PATH:?STOVE0_ADMISSIONS_HOST_PATH is required}",
            "target": "/etc/stove0/admissions.json",
            "read_only": True,
        },
    ]
    for name in ("api", "controller", "worker"):
        assert services[name]["environment"]["STOVE0_ADMISSIONS_PATH"] == (
            "/etc/stove0/admissions.json"
        )


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


def test_paired_target_and_sampler_roles_bind_the_same_image_digest() -> None:
    payload = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    services = payload["services"]
    assert services["a-stove0-opus-target"]["image"] == services["a-review0-opus-sampler"]["image"]
    assert (
        services["a-stove0-opus-target"]["environment"]["A_STOVE0_OPUS_TARGET_IMAGE_DIGEST"]
        == services["a-review0-opus-sampler"]["environment"]["A_REVIEW0_OPUS_SAMPLER_IMAGE_DIGEST"]
    )
    assert (
        services["a-stove0-nvenc-av1-opus-target"]["image"]
        == services["a-review0-nvenc-av1-opus-sampler"]["image"]
    )
    assert (
        services["a-stove0-nvenc-av1-opus-target"]["environment"][
            "A_STOVE0_NVENC_AV1_OPUS_TARGET_IMAGE_DIGEST"
        ]
        == services["a-review0-nvenc-av1-opus-sampler"]["environment"][
            "A_REVIEW0_NVENC_AV1_OPUS_SAMPLER_IMAGE_DIGEST"
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
    for name in ("api", "controller", "worker"):
        environment = services[name]["environment"]
        assert environment["RIVERHOG_ALLOW_INSECURE_HTTP"] == "true"
        assert environment["STOVE0_TARGET_CALLBACK_BASE_URL"] == "http://api:8080"
        assert environment["STOVE0_TARGET_CALLBACK_ALLOW_INSECURE_HTTP"] == "true"
        assert environment["STOVE0_TARGET_CALLBACK_SIGNING_KEY_FILE"] == (
            "/run/secrets/stove0_target_callback_signing_key"
        )
        assert "stove0_target_callback_signing_key" in services[name]["secrets"]
        assert environment["RIVERHOG_TOKEN_FILE"].startswith("/run/secrets/")
        assert "RIVERHOG_TOKEN" not in environment
    text = COMPOSE.read_text(encoding="utf-8")
    assert "STOVE0_API_TOKEN=" not in text
    assert "RIVERHOG_TOKEN=" not in text


def test_supplied_observer_registrations_connect_exact_one_role_services() -> None:
    payload = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    services = payload["services"]
    expected = {
        "exiftool": (
            "http://a-stove0-exiftool-observer:8080",
            "A_STOVE0_EXIFTOOL_OBSERVER_TOKEN",
            "a_stove0_exiftool_observer_token",
            ["media-metadata"],
        ),
        "ffprobe-sampling": (
            "http://a-stove0-ffprobe-sampling-observer:8080",
            "A_STOVE0_FFPROBE_SAMPLING_OBSERVER_TOKEN",
            "a_stove0_ffprobe_sampling_observer_token",
            ["media-sampling"],
        ),
    }
    for role in ("api", "controller", "worker"):
        service = services[role]
        registrations = json.loads(service["environment"]["STOVE0_OBSERVERS_JSON"])
        assert set(registrations) == set(expected)
        for registration, (base_url, token_env, secret, providers) in expected.items():
            assert registrations[registration] == {
                "base_url": base_url,
                "token_env": token_env,
                "allow_insecure_http": True,
                "semantic_validator_providers": providers,
            }
            assert service["environment"][f"{token_env}_FILE"] == f"/run/secrets/{secret}"
            assert secret in service["secrets"]


def test_supplied_target_registrations_bind_fixed_review_result_modes() -> None:
    payload = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    services = payload["services"]
    for role in ("api", "controller", "worker"):
        service = services[role]
        registrations = json.loads(service["environment"]["STOVE0_TARGETS_JSON"])
        assert registrations["review"] == {
            "base_url": "http://a-review0-materializer:8080",
            "token_env": "A_REVIEW0_MATERIALIZER_TOKEN",
            "allow_insecure_http": True,
        }
        assert registrations["review-effect"] == {
            "base_url": "http://a-review0-rclone-target:8080",
            "token_env": "A_REVIEW0_RCLONE_TARGET_TOKEN",
            "allow_insecure_http": True,
        }
        assert "a_review0_materializer_token" in service["secrets"]
        assert "a_review0_rclone_target_token" in service["secrets"]


def test_supplied_topology_connects_bounded_operational_state_retention() -> None:
    payload = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    services = payload["services"]
    for name in ("api", "controller", "worker"):
        assert (
            services[name]["environment"]["STOVE0_OPERATIONAL_STATE_RETENTION_SECONDS"]
            == "${STOVE0_OPERATIONAL_STATE_RETENTION_SECONDS:-2592000}"
        )
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
