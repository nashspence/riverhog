from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts/runtime_image_identity.py"


def load_script() -> Any:
    spec = importlib.util.spec_from_file_location("runtime_image_identity", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _digest(character: str) -> str:
    return f"sha256:{character * 64}"


def _index_fixture() -> tuple[dict[tuple[str, str], dict[str, object]], str, str, str]:
    repository = "ghcr.io/nashspence/a-stove0-opus-target"
    tag = "sha-" + "a" * 40
    index_digest = _digest("1")
    manifest_digest = _digest("2")
    config_digest = _digest("3")
    index = {
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.index.v1+json",
        "digest": index_digest,
        "manifests": [
            {
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "digest": manifest_digest,
                "platform": {"os": "linux", "architecture": "amd64"},
            },
            {
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "digest": _digest("4"),
                "annotations": {"vnd.docker.reference.type": "attestation-manifest"},
                "platform": {"os": "unknown", "architecture": "unknown"},
            },
        ],
    }
    manifest = {
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "digest": manifest_digest,
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": config_digest,
        },
    }
    documents: dict[tuple[str, str], dict[str, object]] = {
        (f"{repository}:{tag}", "Manifest"): index,
        (f"{repository}@{index_digest}", "Manifest"): index,
        (f"{repository}@{manifest_digest}", "Manifest"): manifest,
        (f"{repository}@{manifest_digest}", "Image"): {
            "os": "linux",
            "architecture": "amd64",
        },
    }
    return documents, repository, tag, manifest_digest


def test_resolver_selects_the_runnable_platform_beneath_an_attested_index(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_script()
    documents, repository, tag, manifest_digest = _index_fixture()
    inspected: list[tuple[str, str]] = []

    def inspect(reference: str, kind: str) -> dict[str, object]:
        inspected.append((reference, kind))
        return documents[(reference, kind)]

    monkeypatch.setattr(module, "_inspect", inspect)
    record = module.resolve_image(repository, tag)

    assert record == {
        "repository": repository,
        "platform": "linux/amd64",
        "image_index_digest": _digest("1"),
        "image_manifest_digest": manifest_digest,
        "image_id": _digest("3"),
    }
    assert inspected == [
        (f"{repository}:{tag}", "Manifest"),
        (f"{repository}@{_digest('1')}", "Manifest"),
        (f"{repository}@{manifest_digest}", "Manifest"),
        (f"{repository}@{manifest_digest}", "Image"),
    ]


def test_resolver_rejects_ambiguous_platform_and_wrong_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_script()
    documents, repository, tag, manifest_digest = _index_fixture()
    monkeypatch.setattr(module, "_inspect", lambda reference, kind: documents[(reference, kind)])
    index = documents[(f"{repository}@{_digest('1')}", "Manifest")]
    assert isinstance(index["manifests"], list)
    index["manifests"].append(dict(index["manifests"][0]))
    with pytest.raises(module.ImageIdentityError, match="exactly one runnable"):
        module.resolve_image(repository, tag)

    index["manifests"].pop()
    manifest = documents[(f"{repository}@{manifest_digest}", "Manifest")]
    assert isinstance(manifest["config"], dict)
    manifest["config"]["digest"] = "3" * 64
    with pytest.raises(module.ImageIdentityError, match="configuration digest"):
        module.resolve_image(repository, tag)


def test_resolver_checks_single_manifest_platform(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_script()
    repository = "ghcr.io/nashspence/a-review0-materializer"
    digest = _digest("5")
    manifest = {
        "schemaVersion": 2,
        "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
        "digest": digest,
        "config": {
            "mediaType": "application/vnd.docker.container.image.v1+json",
            "digest": _digest("6"),
        },
    }
    documents = {
        (f"{repository}:1.0.0", "Manifest"): manifest,
        (f"{repository}@{digest}", "Manifest"): manifest,
        (f"{repository}@{digest}", "Image"): {"os": "linux", "architecture": "amd64"},
    }
    monkeypatch.setattr(module, "_inspect", lambda reference, kind: documents[(reference, kind)])
    assert module.resolve_image(repository, "1.0.0") == {
        "repository": repository,
        "platform": "linux/amd64",
        "image_manifest_digest": digest,
        "image_id": _digest("6"),
    }
    documents[(f"{repository}@{digest}", "Image")] = {"os": "linux", "architecture": "arm64"}
    with pytest.raises(module.ImageIdentityError, match="not linux/amd64"):
        module.resolve_image(repository, "1.0.0")

    documents[(f"{repository}@{digest}", "Image")] = {
        "os": "linux",
        "architecture": "amd64",
        "variant": "v3",
    }
    with pytest.raises(module.ImageIdentityError, match="not linux/amd64"):
        module.resolve_image(repository, "1.0.0")


def test_release_resolver_covers_the_exact_runtime_inventory_and_writes_pins(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_script()
    seen: list[tuple[str, str]] = []

    def resolve(repository: str, tag: str) -> dict[str, str]:
        seen.append((repository, tag))
        return {
            "repository": repository,
            "platform": "linux/amd64",
            "image_manifest_digest": _digest("a"),
            "image_id": _digest("b"),
        }

    monkeypatch.setattr(module, "resolve_image", resolve)
    result = module.resolve_release_images(REPO_ROOT / "release.toml", "1.0.0")
    assert result["format"] == "riverhog-runtime-image-identities/v1"
    assert len(result["images"]) == 13
    assert len(seen) == 13
    env = module.compose_env(result)
    assert (
        f"A_STOVE0_OPUS_TARGET_IMAGE_REF=ghcr.io/nashspence/a-stove0-opus-target@{_digest('a')}\n"
    ) in env
    assert f"A_STOVE0_OPUS_TARGET_IMAGE_ID={_digest('b')}\n" in env
    assert "A_REVIEW0_OPUS_SAMPLER_IMAGE_ID" not in env
