from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

from contract_atlas.installation_publication import unpack_installation_index  # noqa: E402
from contract_atlas.model import ContractAtlasError, canonical_bytes  # noqa: E402
from contract_atlas.publication import file_sha256  # noqa: E402
from release_installation import write_index_snapshot  # noqa: E402

from tests.release_index import make_index  # noqa: E402


@pytest.mark.parametrize("change", ["source", "tag", "path", "url", "digest", "wheel", "links"])
def test_index_consumption_requires_exact_release_identity_inventory_and_links(tmp_path, change):
    _, manifest, assets = make_index(tmp_path / "published", "v1.0.0", "1" * 40)
    folder = tmp_path / "published"
    archive = folder / manifest["index"]["snapshot_asset"]
    if change == "source":
        manifest["source_sha"] = "2" * 40
    elif change == "tag":
        manifest["tag"] = "v1.0.1"
    elif change in {"path", "url"}:
        manifest["index"][change] += "other/"
    elif change == "digest":
        manifest["index"]["snapshot_sha256"] = "0" * 64
    elif change == "wheel":
        assets[next(iter(manifest["wheels"].values()))["asset"]]["digest"] = "sha256:" + "0" * 64
    else:
        write_index_snapshot(
            archive,
            simple_index_path=manifest["index"]["path"],
            names=set(manifest["wheels"]),
            wheels=manifest["wheels"],
            asset_base_url="https://unrelated.invalid/",
            source_epoch=0,
        )
        manifest["index"]["snapshot_sha256"] = file_sha256(archive)
    manifest_path = folder / "install-manifest.json"
    manifest_path.write_bytes(canonical_bytes(manifest))
    with pytest.raises(ContractAtlasError):
        unpack_installation_index(
            manifest_path,
            archive,
            tmp_path / "reloaded",
            repository="nashspence/riverhog",
            tag="v1.0.0",
            source_sha="1" * 40,
            assets=assets,
            budget=900_000_000,
        )
