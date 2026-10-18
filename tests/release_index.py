"""Exercise the native index producer with disposable release wheel subjects."""

from __future__ import annotations

from pathlib import Path

from contract_atlas.installation_publication import unpack_installation_index
from contract_atlas.model import canonical_bytes
from contract_atlas.publication import file_sha256
from release_installation import INSTALLATION_FORMAT, write_index_snapshot


def make_index(directory: Path, tag: str, source_sha: str):
    directory.mkdir(parents=True, exist_ok=True)
    wheels = {}
    assets = {}
    for number, name in enumerate(("a-riverhog-cli", "riverhog-client"), 1):
        file = directory / (name.replace("-", "_") + f"-{tag[1:]}-py3-none-any.whl")
        file.write_bytes(b"synthetic index wheel subject " + name.encode())
        wheels[name] = {
            "asset": file.name,
            "sha256": file_sha256(file),
            "size": file.stat().st_size,
        }
        assets[file.name] = {
            "id": number,
            "name": file.name,
            "digest": "sha256:" + file_sha256(file),
            "size": file.stat().st_size,
        }
    archive = directory / f"riverhog-python-index-{tag}.tar.gz"
    prefix = f"artifacts/{tag}/simple/"
    write_index_snapshot(
        archive,
        simple_index_path=prefix,
        names=set(wheels),
        wheels=wheels,
        asset_base_url=f"https://github.com/nashspence/riverhog/releases/download/{tag}/",
        source_epoch=0,
    )
    manifest = {
        "format": INSTALLATION_FORMAT,
        "tag": tag,
        "version": tag[1:],
        "source_sha": source_sha,
        "index": {
            "path": prefix,
            "url": "https://nashspence.github.io/riverhog/" + prefix,
            "snapshot_asset": archive.name,
            "snapshot_sha256": file_sha256(archive),
            "first_party_projects": sorted(wheels),
        },
        "wheels": wheels,
    }
    manifest_path = directory / "install-manifest.json"
    manifest_path.write_bytes(canonical_bytes(manifest))
    for number, file in enumerate((manifest_path, archive), 10):
        assets[file.name] = {
            "id": number,
            "name": file.name,
            "digest": "sha256:" + file_sha256(file),
            "size": file.stat().st_size,
        }
    installation = unpack_installation_index(
        manifest_path,
        archive,
        directory / "index",
        repository="nashspence/riverhog",
        tag=tag,
        source_sha=source_sha,
        assets=assets,
        budget=900_000_000,
    )
    return installation, manifest, assets
