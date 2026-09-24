from __future__ import annotations

import hashlib
import importlib.util
import json
import subprocess
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
OCI_MANIFEST = "application/vnd.oci.image.manifest.v1+json"
OCI_INDEX = "application/vnd.oci.image.index.v1+json"


def load_script() -> Any:
    spec = importlib.util.spec_from_file_location(
        "runtime_image_identity", ROOT / "scripts/runtime_image_identity.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _wire(value: dict[str, object]) -> tuple[bytes, str]:
    raw = json.dumps(value, separators=(",", ":")).encode()
    return raw, "sha256:" + hashlib.sha256(raw).hexdigest()


def _registry(
    attested: bool, duplicate: bool = False
) -> tuple[str, str, str, str, dict[tuple[str, str], dict[str, object]], dict[str, bytes]]:
    repository, tag = "ghcr.io/example/runtime", "sha-" + "a" * 40
    image_id = "sha256:" + "3" * 64
    manifest_raw, manifest_digest = _wire(
        {
            "schemaVersion": 2,
            "mediaType": OCI_MANIFEST,
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": image_id,
                "size": 100,
            },
            "layers": [],
        }
    )
    manifest_descriptor = {
        "mediaType": OCI_MANIFEST,
        "digest": manifest_digest,
        "size": len(manifest_raw),
        "platform": {"os": "linux", "architecture": "amd64"},
    }
    formatted: dict[tuple[str, str], dict[str, object]] = {
        (f"{repository}@{manifest_digest}", "Image"): {"os": "linux", "architecture": "amd64"}
    }
    raw = {f"{repository}@{manifest_digest}": manifest_raw}
    if attested:
        entries = [
            manifest_descriptor,
            {
                "mediaType": OCI_MANIFEST,
                "digest": "sha256:" + "4" * 64,
                "annotations": {"vnd.docker.reference.type": "attestation-manifest"},
                "platform": {"os": "unknown", "architecture": "unknown"},
            },
        ]
        if duplicate:
            entries.append(manifest_descriptor.copy())
        index_raw, index_digest = _wire(
            {
                "schemaVersion": 2,
                "mediaType": OCI_INDEX,
                "manifests": entries,
            }
        )
        formatted[(f"{repository}:{tag}", "Manifest")] = {
            "mediaType": OCI_INDEX,
            "digest": index_digest,
            "size": len(index_raw),
            "manifests": entries,
        }
        raw[f"{repository}@{index_digest}"] = index_raw
    else:
        index_digest = ""
        # Real Buildx .Manifest for a single image is only a descriptor.
        formatted[(f"{repository}:{tag}", "Manifest")] = {
            key: manifest_descriptor[key] for key in ("mediaType", "digest", "size")
        }
    return repository, tag, manifest_digest, index_digest, formatted, raw


def _mock_buildx(
    monkeypatch: pytest.MonkeyPatch,
    module: Any,
    formatted: dict[tuple[str, str], dict[str, object]],
    raw: dict[str, bytes],
) -> list[list[str]]:
    commands: list[list[str]] = []

    def run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[Any]:
        commands.append(command)
        assert command[:4] == ["docker", "buildx", "imagetools", "inspect"]
        assert kwargs["capture_output"] is True and kwargs["check"] is True
        if command[4] == "--raw":
            assert kwargs.get("text") is None
            return subprocess.CompletedProcess(command, 0, stdout=raw[command[5]])
        assert command[4] == "--format" and kwargs["text"] is True
        kind = command[5].removeprefix("{{json .").removesuffix("}}")
        return subprocess.CompletedProcess(
            command, 0, stdout=json.dumps(formatted[(command[6], kind)])
        )

    monkeypatch.setattr(module.subprocess, "run", run)
    return commands


@pytest.mark.parametrize("attested", [False, True])
def test_resolver_follows_real_descriptor_and_raw_manifest_shapes(
    monkeypatch: pytest.MonkeyPatch, attested: bool
) -> None:
    module = load_script()
    repository, tag, manifest_digest, index_digest, formatted, raw = _registry(attested)
    commands = _mock_buildx(monkeypatch, module, formatted, raw)
    assert module.resolve_image(repository, tag) == {
        "repository": repository,
        "platform": "linux/amd64",
        "image_manifest_digest": manifest_digest,
        "image_id": "sha256:" + "3" * 64,
        **({"image_index_digest": index_digest} if attested else {}),
    }
    assert [command[5] for command in commands if command[4] == "--raw"] == [
        f"{repository}@{digest}"
        for digest in ([index_digest, manifest_digest] if attested else [manifest_digest])
    ]


def test_resolver_rejects_ambiguous_platform_and_changed_manifest_bytes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_script()
    repository, tag, _, _, formatted, raw = _registry(True, duplicate=True)
    _mock_buildx(monkeypatch, module, formatted, raw)
    with pytest.raises(module.ImageIdentityError, match="exactly one runnable"):
        module.resolve_image(repository, tag)

    repository, tag, digest, _, formatted, raw = _registry(False)
    raw[f"{repository}@{digest}"] += b" "
    _mock_buildx(monkeypatch, module, formatted, raw)
    with pytest.raises(module.ImageIdentityError, match="pinned manifest differs"):
        module.resolve_image(repository, tag)


def test_resolver_checks_config_platform_and_descriptor_size(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_script()
    repository, tag, digest, _, formatted, raw = _registry(False)
    _mock_buildx(monkeypatch, module, formatted, raw)
    formatted[(f"{repository}@{digest}", "Image")] = {"os": "linux", "architecture": "arm64"}
    with pytest.raises(module.ImageIdentityError, match="not linux/amd64"):
        module.resolve_image(repository, tag)

    formatted[(f"{repository}:{tag}", "Manifest")]["size"] += 1
    with pytest.raises(module.ImageIdentityError, match="size differs"):
        module.resolve_image(repository, tag)


def test_release_resolver_covers_inventory_and_writes_pins(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = load_script()
    seen: list[tuple[str, str]] = []

    def resolve(repository: str, tag: str) -> dict[str, str]:
        seen.append((repository, tag))
        return {
            "repository": repository,
            "platform": "linux/amd64",
            "image_manifest_digest": "sha256:" + "a" * 64,
            "image_id": "sha256:" + "b" * 64,
        }

    monkeypatch.setattr(module, "resolve_image", resolve)
    result = module.resolve_release_images(ROOT / "release.toml", "1.0.0")
    assert result["format"] == "riverhog-runtime-image-identities/v1"
    assert len(result["images"]) == len(seen) == 13
    env = module.compose_env(result)
    assert (
        "A_STOVE0_OPUS_TARGET_IMAGE_REF="
        "ghcr.io/nashspence/a-stove0-opus-target@sha256:" + "a" * 64 + "\n"
    ) in env
    assert "A_STOVE0_OPUS_TARGET_IMAGE_ID=sha256:" + "b" * 64 + "\n" in env
    assert "A_REVIEW0_OPUS_SAMPLER_IMAGE_ID" not in env
