"""Resolve published runtime image manifests and ImageIDs from a registry tag."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import tomllib
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
FORMAT = "riverhog-runtime-image-identities/v1"
PLATFORM = "linux/amd64"
OCI_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
TAG = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}\Z")
INDEX_MEDIA_TYPES = {
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
}
MANIFEST_MEDIA_TYPES = {
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.v2+json",
}
CONFIG_MEDIA_TYPES = {
    "application/vnd.oci.image.config.v1+json",
    "application/vnd.docker.container.image.v1+json",
}


class ImageIdentityError(ValueError):
    """A registry image cannot satisfy the runtime image identity contract."""


def _digest(value: object, label: str) -> str:
    if not isinstance(value, str) or OCI_DIGEST.fullmatch(value) is None:
        raise ImageIdentityError(f"{label} must be a lowercase sha256 OCI digest")
    return value


def _inspect(reference: str, kind: str) -> dict[str, Any]:
    try:
        output = subprocess.run(
            [
                "docker",
                "buildx",
                "imagetools",
                "inspect",
                "--format",
                f"{{{{json .{kind}}}}}",
                reference,
            ],
            capture_output=True,
            text=True,
            check=True,
        ).stdout
        document = json.loads(output)
    except (subprocess.CalledProcessError, OSError, json.JSONDecodeError) as error:
        raise ImageIdentityError(f"could not inspect {kind.lower()} for {reference}") from error
    if not isinstance(document, dict):
        raise ImageIdentityError(f"registry returned no {kind.lower()} for {reference}")
    return document


def _manifest(repository: str, digest: str, descriptor: dict[str, Any]) -> dict[str, Any]:
    reference = f"{repository}@{digest}"
    try:
        raw = subprocess.run(
            ["docker", "buildx", "imagetools", "inspect", "--raw", reference],
            capture_output=True,
            check=True,
        ).stdout
        manifest = json.loads(raw)
    except (subprocess.CalledProcessError, OSError, json.JSONDecodeError) as error:
        raise ImageIdentityError(f"could not inspect manifest for {reference}") from error
    if hashlib.sha256(raw).hexdigest() != digest.removeprefix("sha256:"):
        raise ImageIdentityError(f"pinned manifest differs from {reference}")
    if not isinstance(manifest, dict):
        raise ImageIdentityError(f"registry returned no manifest for {reference}")
    if descriptor.get("mediaType") != manifest.get("mediaType"):
        raise ImageIdentityError(f"manifest media type differs from descriptor for {reference}")
    if "size" in descriptor and descriptor["size"] != len(raw):
        raise ImageIdentityError(f"manifest size differs from descriptor for {reference}")
    if manifest.get("schemaVersion") != 2:
        raise ImageIdentityError(f"unsupported image manifest schema for {reference}")
    return manifest


def resolve_image(repository: str, tag: str) -> dict[str, str]:
    if not repository or "@" in repository or TAG.fullmatch(tag) is None:
        raise ImageIdentityError("a repository and valid registry tag are required")
    descriptor = _inspect(f"{repository}:{tag}", "Manifest")
    top_digest = _digest(descriptor.get("digest"), "tag digest")
    top = _manifest(repository, top_digest, descriptor)
    media_type = top.get("mediaType")
    image_index_digest: str | None = None
    if media_type in INDEX_MEDIA_TYPES:
        image_index_digest = top_digest
        entries = top.get("manifests")
        if not isinstance(entries, list):
            raise ImageIdentityError("image index has no manifest descriptors")
        candidates = [
            entry
            for entry in entries
            if isinstance(entry, dict)
            and entry.get("mediaType") in MANIFEST_MEDIA_TYPES
            and isinstance(entry.get("platform"), dict)
            and entry["platform"].get("os") == "linux"
            and entry["platform"].get("architecture") == "amd64"
            and not entry["platform"].get("variant")
            and entry.get("artifactType") is None
            and (
                not isinstance(entry.get("annotations"), dict)
                or entry["annotations"].get("vnd.docker.reference.type") != "attestation-manifest"
            )
        ]
        if len(candidates) != 1:
            raise ImageIdentityError(
                "image index must contain exactly one runnable linux/amd64 manifest"
            )
        manifest_digest = _digest(candidates[0].get("digest"), "platform manifest digest")
        manifest = _manifest(repository, manifest_digest, candidates[0])
    elif media_type in MANIFEST_MEDIA_TYPES:
        manifest_digest = top_digest
        manifest = top
    else:
        raise ImageIdentityError(f"tag points to an unsupported image object: {media_type}")
    if manifest.get("mediaType") not in MANIFEST_MEDIA_TYPES:
        raise ImageIdentityError("platform descriptor does not point to a runnable image manifest")
    config = manifest.get("config")
    if not isinstance(config, dict) or config.get("mediaType") not in CONFIG_MEDIA_TYPES:
        raise ImageIdentityError("runnable image manifest has no image configuration")
    image_id = _digest(config.get("digest"), "image configuration digest")
    image = _inspect(f"{repository}@{manifest_digest}", "Image")
    if image.get("os") != "linux" or image.get("architecture") != "amd64" or image.get("variant"):
        raise ImageIdentityError("runnable image configuration is not linux/amd64")
    result = {
        "repository": repository,
        "platform": PLATFORM,
        "image_manifest_digest": manifest_digest,
        "image_id": image_id,
    }
    if image_index_digest is not None:
        result["image_index_digest"] = image_index_digest
    return result


def resolve_release_images(inventory: Path, tag: str) -> dict[str, object]:
    config = tomllib.loads(inventory.read_text(encoding="utf-8"))
    images = config["images"]
    if images["platforms"] != [PLATFORM]:
        raise ImageIdentityError("release runtime images must have one linux/amd64 platform")
    runtime = images["runtime"]
    if not isinstance(runtime, dict) or not runtime:
        raise ImageIdentityError("release inventory has no runtime images")
    records = {
        target: resolve_image(str(value["repository"]), tag)
        for target, value in sorted(runtime.items())
    }
    return {"format": FORMAT, "images": records}


def compose_env(identities: dict[str, object]) -> str:
    lines: list[str] = []
    images = identities["images"]
    assert isinstance(images, dict)
    for target, record in sorted(images.items()):
        assert isinstance(record, dict)
        prefix = target.upper().replace("-", "_")
        lines.append(f"{prefix}_IMAGE_REF={record['repository']}@{record['image_manifest_digest']}")
        lines.append(f"{prefix}_IMAGE_ID={record['image_id']}")
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--tag", required=True, help="Published release tag, for example 1.0.0 or sha-<source SHA>."
    )
    parser.add_argument("--inventory", type=Path, default=ROOT / "release.toml")
    parser.add_argument("--record", type=Path, help="Write the verified identity record as JSON.")
    parser.add_argument(
        "--compose-env",
        type=Path,
        help="Write digest-pinned Compose image references and ImageIDs.",
    )
    args = parser.parse_args(argv)
    try:
        document = resolve_release_images(args.inventory, args.tag)
    except (ImageIdentityError, KeyError, OSError, tomllib.TOMLDecodeError) as error:
        parser.error(str(error))
    if args.compose_env is not None:
        args.compose_env.write_text(compose_env(document), encoding="utf-8")
    rendered = json.dumps(document, sort_keys=True, indent=2) + "\n"
    if args.record is None:
        print(rendered, end="")
    else:
        args.record.write_text(rendered, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
