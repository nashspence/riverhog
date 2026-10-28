from __future__ import annotations

import base64
import copy
import csv
import hashlib
import io
import subprocess
import sys
import tarfile
import tomllib
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

import release  # noqa: E402
import release_installation as installation  # noqa: E402
from contract_atlas.installation_publication import unpack_installation_index  # noqa: E402
from contract_atlas.model import canonical_bytes  # noqa: E402

from tests.unit.test_release_installation import _external_lock, _wheel_records  # noqa: E402

TAGS = {
    "linux-x64": "cp312-cp312-manylinux_2_17_x86_64",
    "macos-arm64": "cp312-cp312-macosx_11_0_arm64",
    "windows-x64": "cp312-cp312-win_amd64",
}


@pytest.fixture(scope="module")
def projects():
    return release.validate_release_contract(ROOT)


@pytest.fixture
def subjects(monkeypatch, projects):
    records = [
        item for item in _wheel_records(projects) if item["distribution"] != "riverhog-client"
    ]
    for tag in TAGS.values():
        name = f"riverhog_client-0.1.0-{tag}.whl"
        records.append(
            {
                "kind": "wheel",
                "distribution": "riverhog-client",
                "name": f"python/{name}",
                "size": len(name),
                "sha256": hashlib.sha256(name.encode()).hexdigest(),
            }
        )
    monkeypatch.setattr(installation, "_export_external_lock", lambda *_args: _external_lock())
    return projects, records


def _build(tmp_path, projects, records):
    return installation.build_installation_artifacts(
        ROOT,
        tmp_path,
        projects,
        records,
        version="0.1.0",
        source_sha="1" * 40,
        source_epoch=1234567890,
        repository="nashspence/riverhog",
        simple_index_path="artifacts/v{version}/simple/",
    )[0]


def test_native_wheels_survive_locks_index_publication_and_platform_selection(tmp_path, subjects):
    projects, records = subjects
    manifest = _build(tmp_path, projects, records)
    installation.verify_installation_artifacts(tmp_path, manifest)
    client = manifest["wheels"]["riverhog-client"]
    assert len(client) == 3
    for component in manifest["components"]:
        if "riverhog-client" not in {item["name"] for item in component["resolved_packages"]}:
            continue
        lock = tomllib.loads((tmp_path / component["lock"]["path"]).read_text())
        package = next(item for item in lock["packages"] if item["name"] == "riverhog-client")
        assert {item["url"].split("/")[-1] for item in package["wheels"]} == {
            item["asset"] for item in client
        }
        assert {item["hashes"]["sha256"] for item in package["wheels"]} == {
            item["sha256"] for item in client
        }
        for platform, tag in TAGS.items():
            selected = next(
                item
                for item in component["platform_requirements"][platform]
                if item["name"] == "riverhog-client"
            )
            assert selected["url"].endswith(f"-{tag}.whl")
            assert installation.platform_wheel_assets(
                manifest, {"riverhog-client"}, platform=platform
            ) == {f"riverhog_client-0.1.0-{tag}.whl"}

    assets = {
        item["asset"]: {"digest": "sha256:" + item["sha256"], "size": item["size"]}
        for values in manifest["wheels"].values()
        for item in values
    }
    snapshot = tmp_path / manifest["index"]["snapshot_path"]
    manifest_path = tmp_path / "install-manifest.json"
    manifest_path.write_bytes(canonical_bytes(manifest))
    unpack_installation_index(
        manifest_path,
        snapshot,
        tmp_path / "index",
        repository="nashspence/riverhog",
        tag="v0.1.0",
        source_sha="1" * 40,
        assets=assets,
        budget=10_000_000,
    )
    page = tmp_path / "index/artifacts/v0.1.0/simple/riverhog-client/index.html"
    text = page.read_text()
    assert text.count("<a href=") == 3
    assert all(item["sha256"] in text and item["asset"] in text for item in client)


@pytest.mark.parametrize("platform", TAGS)
def test_missing_native_platform_is_an_installation_failure(tmp_path, subjects, platform):
    projects, records = subjects
    records = [item for item in records if not item["name"].endswith(f"-{TAGS[platform]}.whl")]
    with pytest.raises(installation.InstallationError, match=f"no {platform}"):
        _build(tmp_path, projects, records)


@pytest.mark.parametrize("replacement", ["cp311-cp311-win_amd64", "cp312-cp312-win_arm64"])
def test_native_wheel_abi_and_architecture_must_match(tmp_path, subjects, replacement):
    projects, records = subjects
    item = next(item for item in records if item["name"].endswith("-win_amd64.whl"))
    item["name"] = f"python/riverhog_client-0.1.0-{replacement}.whl"
    with pytest.raises(installation.InstallationError, match="no windows-x64"):
        _build(tmp_path, projects, records)


@pytest.mark.parametrize(
    "change",
    ["duplicate", "name", "version", "digest", "size", "bool-size", "float-size", "path"],
)
def test_native_wheel_subjects_cannot_be_dropped_coerced_or_misidentified(
    tmp_path, subjects, change
):
    projects, records = subjects
    item = next(item for item in records if item["name"].endswith("-win_amd64.whl"))
    if change == "duplicate":
        records.append({**item, "sha256": "f" * 64})
    elif change == "name":
        item["name"] = item["name"].replace("riverhog_client-", "other_client-")
    elif change == "version":
        item["name"] = item["name"].replace("-0.1.0-", "-0.2.0-")
    elif change == "digest":
        item["sha256"] = "not-a-digest"
    elif change == "size":
        item["size"] = 0
    elif change == "bool-size":
        item["size"] = True
    elif change == "float-size":
        item["size"] = 1.5
    elif change == "path":
        item["name"] = "../" + item["name"]
    with pytest.raises(installation.InstallationError):
        _build(tmp_path, projects, records)


def test_native_wheel_order_does_not_change_locks_or_index(tmp_path, subjects):
    projects, records = subjects
    first = _build(tmp_path / "one", projects, records)
    second = _build(tmp_path / "two", projects, list(reversed(records)))
    assert first == second
    assert (tmp_path / "one/install-manifest.json").read_bytes() == (
        tmp_path / "two/install-manifest.json"
    ).read_bytes()


@pytest.mark.parametrize("change", ["asset", "digest", "size", "missing-link", "duplicate"])
def test_every_native_asset_remains_bound_at_publication(tmp_path, subjects, change):
    from contract_atlas.model import ContractAtlasError

    projects, records = subjects
    manifest = _build(tmp_path, projects, records)
    assets = {
        item["asset"]: {"digest": "sha256:" + item["sha256"], "size": item["size"]}
        for values in manifest["wheels"].values()
        for item in values
    }
    item = manifest["wheels"]["riverhog-client"][-1]
    if change == "asset":
        del assets[item["asset"]]
    elif change == "digest":
        assets[item["asset"]]["digest"] = "sha256:" + "f" * 64
    elif change == "size":
        assets[item["asset"]]["size"] += 1
    elif change == "missing-link":
        manifest["wheels"]["riverhog-client"].pop()
    elif change == "duplicate":
        manifest["wheels"]["riverhog-client"].append(copy.deepcopy(item))
    manifest_path = tmp_path / "install-manifest.json"
    manifest_path.write_bytes(canonical_bytes(manifest))
    with pytest.raises(ContractAtlasError):
        unpack_installation_index(
            manifest_path,
            tmp_path / manifest["index"]["snapshot_path"],
            tmp_path / "index",
            repository="nashspence/riverhog",
            tag="v0.1.0",
            source_sha="1" * 40,
            assets=assets,
            budget=10_000_000,
        )


def _artifact_set(tmp_path, monkeypatch):
    project = SimpleNamespace(
        name="sample-lib", path="owner", requires_python=">=3.12", license_expression="Apache-2.0"
    )
    owner = tmp_path / "owner"
    owner.mkdir()
    (owner / "pyproject.toml").write_text(
        '[project]\nname="sample-lib"\nversion="0.1.0"\nlicense="Apache-2.0"\n'
    )
    monkeypatch.setattr(
        release,
        "publication_contract",
        lambda *_args: {
            "distributions": {
                "sample-lib": {
                    "artifacts": [
                        {"format": "wheel", "coordinate": "dist/sample_lib-{version}-*.whl"},
                        {"format": "sdist", "coordinate": "dist/sample_lib-{version}.tar.gz"},
                    ]
                }
            }
        },
    )
    dist = tmp_path / "dist"
    dist.mkdir()
    metadata = (
        b"Metadata-Version: 2.4\nName: sample-lib\nVersion: 0.1.0\n"
        b"Requires-Python: >=3.12\nLicense-Expression: Apache-2.0\n"
    )
    for tag in TAGS.values():
        path = dist / f"sample_lib-0.1.0-{tag}.whl"
        with zipfile.ZipFile(path, "w") as archive:
            archive.writestr("sample_lib-0.1.0.dist-info/METADATA", metadata)
            archive.writestr(
                "sample_lib-0.1.0.dist-info/WHEEL",
                f"Wheel-Version: 1.0\nRoot-Is-Purelib: false\nTag: {tag}\n",
            )
    with tarfile.open(dist / "sample_lib-0.1.0.tar.gz", "w:gz") as archive:
        info = tarfile.TarInfo("sample_lib-0.1.0/PKG-INFO")
        info.size = len(metadata)
        archive.addfile(info, io.BytesIO(metadata))
    return project, dist


def test_release_inventory_retains_all_valid_native_wheels_and_the_sdist(tmp_path, monkeypatch):
    project, dist = _artifact_set(tmp_path, monkeypatch)
    result = release._validate_distribution_artifacts(tmp_path, [project], version="0.1.0")
    assert set(result) == {path.name for path in dist.iterdir()}
    assert len(result) == 4
    assert all(item[0] is project and item[1] == set() for item in result.values())


@pytest.mark.parametrize(
    "change",
    [
        "missing-sdist",
        "missing-wheels",
        "extra",
        "metadata-name",
        "metadata-version",
        "invalid-version",
        "metadata-abi",
        "missing-tags",
        "missing-wheel-metadata",
        "missing-payload-metadata",
    ],
)
def test_native_release_inventory_requires_both_formats_and_exact_metadata(
    tmp_path, monkeypatch, change
):
    project, dist = _artifact_set(tmp_path, monkeypatch)
    if change == "missing-sdist":
        (dist / "sample_lib-0.1.0.tar.gz").unlink()
    elif change == "missing-wheels":
        for path in dist.glob("*.whl"):
            path.unlink()
    elif change == "extra":
        (dist / "unexpected.whl").write_bytes(b"unexpected")
    else:
        path = dist / f"sample_lib-0.1.0-{TAGS['windows-x64']}.whl"
        with zipfile.ZipFile(path) as archive:
            files = {name: archive.read(name) for name in archive.namelist()}
        metadata = "sample_lib-0.1.0.dist-info/METADATA"
        wheel = "sample_lib-0.1.0.dist-info/WHEEL"
        if change == "metadata-name":
            files[metadata] = files[metadata].replace(b"sample-lib", b"another-lib")
        elif change == "metadata-version":
            files[metadata] = files[metadata].replace(b"0.1.0", b"0.2.0")
        elif change == "invalid-version":
            files[metadata] = files[metadata].replace(b"0.1.0", b"invalid")
        elif change == "metadata-abi":
            files[wheel] = files[wheel].replace(b"cp312-cp312", b"cp311-cp311")
        elif change == "missing-tags":
            files[wheel] = b"Wheel-Version: 1.0\nRoot-Is-Purelib: false\n"
        elif change == "missing-wheel-metadata":
            del files[wheel]
        elif change == "missing-payload-metadata":
            del files[metadata]
        with zipfile.ZipFile(path, "w") as archive:
            for name, data in files.items():
                archive.writestr(name, data)
    with pytest.raises(release.ReleaseError):
        release._validate_distribution_artifacts(tmp_path, [project], version="0.1.0")


@pytest.mark.parametrize(
    "platform,uv_platform",
    [
        ("linux-x64", "x86_64-unknown-linux-gnu"),
        ("macos-arm64", "aarch64-apple-darwin"),
        ("windows-x64", "x86_64-pc-windows-msvc"),
    ],
)
@pytest.mark.parametrize("tamper", [False, True])
@pytest.mark.parametrize("rebuild", [False, True])
def test_locked_uv_installs_the_selected_platform_subject(
    tmp_path, platform, uv_platform, tamper, rebuild
):
    wheelhouse = tmp_path / "wheels"
    wheelhouse.mkdir()
    subjects = []
    variants = [
        (identity, tag, build)
        for identity, tag in TAGS.items()
        for build in ([0, 2, 10] if rebuild else [0])
    ]
    for identity, tag, build in variants:
        build_tag = f"-{build}" if build else ""
        build_field = f"Build: {build}\n" if build else ""
        files = {
            "sample_lib.py": f"IDENTITY = {identity!r}\nBUILD = {build}\n".encode(),
            "sample_lib-0.1.0.dist-info/METADATA": (
                b"Metadata-Version: 2.4\nName: sample-lib\nVersion: 0.1.0\n"
                b"Requires-Python: >=3.12\nLicense-Expression: Apache-2.0\n"
            ),
            "sample_lib-0.1.0.dist-info/WHEEL": (
                f"Wheel-Version: 1.0\nRoot-Is-Purelib: false\nTag: {tag}\n{build_field}".encode()
            ),
        }
        record = io.StringIO()
        writer = csv.writer(record)
        for name, content in files.items():
            digest = base64.urlsafe_b64encode(hashlib.sha256(content).digest()).rstrip(b"=")
            writer.writerow([name, "sha256=" + digest.decode(), len(content)])
        writer.writerow(["sample_lib-0.1.0.dist-info/RECORD", "", ""])
        files["sample_lib-0.1.0.dist-info/RECORD"] = record.getvalue().encode()
        path = wheelhouse / f"sample_lib-0.1.0{build_tag}-{tag}.whl"
        with zipfile.ZipFile(path, "w") as archive:
            for name, content in files.items():
                archive.writestr(name, content)
        subjects.append(
            {
                "asset": path.name,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "size": path.stat().st_size,
            }
        )
    lock = tmp_path / "pylock.fixture.toml"
    lock.write_text(
        'lock-version = "1.0"\ncreated-by = "Riverhog qualification"\n'
        'requires-python = ">=3.12"\n\n'
        + installation._internal_lock_block(
            name="sample-lib",
            version="0.1.0",
            index_url=wheelhouse.as_uri() + "/",
            wheels=subjects,
            asset_base_url=wheelhouse.as_uri() + "/",
            marker=None,
        )
    )
    expected_build = 10 if rebuild else 0
    build_tag = "-10" if rebuild else ""
    selected_asset = f"sample_lib-0.1.0{build_tag}-{TAGS[platform]}.whl"
    manifest = {"version": "0.1.0", "wheels": {"sample-lib": subjects}}
    assert installation.platform_wheel_assets(manifest, {"sample-lib"}, platform=platform) == {
        selected_asset
    }
    if tamper:
        selected = wheelhouse / selected_asset
        selected.write_bytes(selected.read_bytes() + b"changed-after-lock")
    target = tmp_path / "installed"
    result = subprocess.run(
        [
            "uv",
            "--no-config",
            "pip",
            "sync",
            str(lock),
            "--python",
            sys.executable,
            "--target",
            str(target),
            "--python-platform",
            uv_platform,
            "--python-version",
            "3.12",
            "--no-build",
            "--no-index",
        ],
        text=True,
        capture_output=True,
        timeout=60,
    )
    if tamper:
        assert result.returncode != 0, result.stdout + result.stderr
        assert not (target / "sample_lib.py").exists()
        return
    assert result.returncode == 0, result.stdout + result.stderr
    assert (target / "sample_lib.py").read_text() == (
        f"IDENTITY = {platform!r}\nBUILD = {expected_build}\n"
    )
    assert f"Tag: {TAGS[platform]}\n" in (target / "sample_lib-0.1.0.dist-info/WHEEL").read_text()
