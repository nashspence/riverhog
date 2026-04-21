from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path

import pytest
import yaml

from arc_core.planner.manifest import MANIFEST_FILENAME, README_FILENAME
from tests.fixtures.acceptance import AcceptanceSystem, acceptance_system
from tests.fixtures.data import (
    DOCS_COLLECTION_ID,
    DOCS_FILES,
    IMAGE_ID,
    SPLIT_COPY_ONE_ID,
    SPLIT_COPY_TWO_ID,
    SPLIT_FILE_PARTS,
    SPLIT_FILE_RELPATH,
    SPLIT_IMAGE_ONE_ID,
    SPLIT_IMAGE_TWO_ID,
    fixture_decrypt_bytes,
)

pytestmark = pytest.mark.integration


def _write_downloaded_iso(iso_bytes: bytes, workspace: Path) -> Path:
    iso_path = workspace / "image.iso"
    iso_path.write_bytes(iso_bytes)
    return iso_path


def _verify_iso(iso_path: Path) -> None:
    proc = subprocess.run(
        [
            "xorriso",
            "-abort_on",
            "FAILURE",
            "-for_backup",
            "-md5",
            "on",
            "-indev",
            str(iso_path),
            "-check_md5",
            "FAILURE",
            "--",
            "-check_md5_r",
            "FAILURE",
            "/",
            "--",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, "\n".join(part for part in (proc.stdout, proc.stderr) if part)


def _extract_iso(iso_path: Path, workspace: Path) -> Path:
    extract_root = workspace / "disc"
    extract_root.mkdir()
    proc = subprocess.run(
        [
            "xorriso",
            "-osirrox",
            "on",
            "-indev",
            str(iso_path),
            "-extract",
            "/",
            str(extract_root),
            "-end",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr or proc.stdout
    return extract_root


def test_split_fetch_manifest_includes_part_level_recovery_hints(
    acceptance_system: AcceptanceSystem,
) -> None:
    acceptance_system.seed_docs_archive_with_split_invoice()
    acceptance_system.seed_fetch("fx-1", "docs:/tax/2022/invoice-123.pdf")

    response = acceptance_system.request("GET", "/v1/fetches/fx-1/manifest")

    assert response.status_code == 200
    entry = response.json()["entries"][0]
    assert [part["index"] for part in entry["parts"]] == [0, 1]
    assert [part["copies"][0]["copy"] for part in entry["parts"]] == [
        SPLIT_COPY_ONE_ID,
        SPLIT_COPY_TWO_ID,
    ]
    assert [part["bytes"] for part in entry["parts"]] == [len(part) for part in SPLIT_FILE_PARTS]
    assert [part["sha256"] for part in entry["parts"]] == [
        hashlib.sha256(part).hexdigest() for part in SPLIT_FILE_PARTS
    ]


def test_ready_image_iso_uses_the_canonical_disc_layout(
    acceptance_system: AcceptanceSystem,
    tmp_path: Path,
) -> None:
    acceptance_system.seed_planner_fixtures()

    response = acceptance_system.request("GET", f"/v1/images/{IMAGE_ID}/iso")

    assert response.status_code == 200
    iso_path = _write_downloaded_iso(response.content, tmp_path)
    _verify_iso(iso_path)
    extract_root = _extract_iso(iso_path, tmp_path)
    relfiles = sorted(
        path.relative_to(extract_root).as_posix()
        for path in extract_root.rglob("*")
        if path.is_file()
    )

    assert README_FILENAME in relfiles
    assert MANIFEST_FILENAME in relfiles
    assert "collections/000001.ots.age" in relfiles
    assert "collections/000001.yml.age" in relfiles
    assert "files/000001.age" in relfiles
    assert "files/000001.yml.age" in relfiles
    assert "files/000002.age" in relfiles
    assert "files/000002.yml.age" in relfiles
    assert not any(
        "invoice-123.pdf" in relpath or "receipt-456.pdf" in relpath for relpath in relfiles
    )
    assert all(relpath == README_FILENAME or relpath.endswith(".age") for relpath in relfiles)

    readme = (extract_root / README_FILENAME).read_text(encoding="utf-8")
    assert "arc-disc" in readme
    assert "DISC.yml.age" in readme
    assert "multiple discs" in readme

    manifest = yaml.safe_load(
        fixture_decrypt_bytes((extract_root / MANIFEST_FILENAME).read_bytes()).decode("utf-8")
    )
    assert manifest["schema"] == "disc-manifest/v1"
    assert manifest["image"] == {"id": IMAGE_ID, "volume_id": "ARC-IMG-20260420-01"}
    assert [collection["id"] for collection in manifest["collections"]] == [DOCS_COLLECTION_ID]

    collection = manifest["collections"][0]
    assert collection["manifest"] == "collections/000001.yml.age"
    assert collection["proof"] == "collections/000001.ots.age"
    assert [entry["path"] for entry in collection["files"]] == [
        "/tax/2022/invoice-123.pdf",
        "/tax/2022/receipt-456.pdf",
    ]
    assert collection["files"][0]["object"] == "files/000001.age"
    assert collection["files"][0]["sidecar"] == "files/000001.yml.age"
    assert collection["files"][1]["object"] == "files/000002.age"
    assert collection["files"][1]["sidecar"] == "files/000002.yml.age"

    sidecar = yaml.safe_load(
        fixture_decrypt_bytes(
            (extract_root / collection["files"][0]["sidecar"]).read_bytes()
        ).decode("utf-8")
    )
    assert sidecar["schema"] == "file-sidecar/v1"
    assert sidecar["collection"] == DOCS_COLLECTION_ID
    assert sidecar["path"] == "/tax/2022/invoice-123.pdf"

    payload = fixture_decrypt_bytes((extract_root / collection["files"][0]["object"]).read_bytes())
    assert payload == DOCS_FILES["tax/2022/invoice-123.pdf"]

    collection_manifest = yaml.safe_load(
        fixture_decrypt_bytes((extract_root / collection["manifest"]).read_bytes()).decode("utf-8")
    )
    assert collection_manifest["schema"] == "collection-hash-manifest/v1"
    assert collection_manifest["collection"] == DOCS_COLLECTION_ID
    assert [row["relative_path"] for row in collection_manifest["files"]] == sorted(DOCS_FILES)

    proof = fixture_decrypt_bytes((extract_root / collection["proof"]).read_bytes()).decode("utf-8")
    assert "OpenTimestamps stub proof v1" in proof
    assert "file: HASHES.yml" in proof


def test_split_file_parts_are_listed_per_disc_and_reconstruct_the_original_plaintext(
    acceptance_system: AcceptanceSystem,
    tmp_path: Path,
) -> None:
    acceptance_system.seed_split_planner_fixtures()

    extracted_parts: list[bytes] = []
    for image_id, expected_index in (
        (SPLIT_IMAGE_ONE_ID, 1),
        (SPLIT_IMAGE_TWO_ID, 2),
    ):
        response = acceptance_system.request("GET", f"/v1/images/{image_id}/iso")

        assert response.status_code == 200
        image_workspace = tmp_path / image_id
        image_workspace.mkdir()
        iso_path = _write_downloaded_iso(response.content, image_workspace)
        _verify_iso(iso_path)
        extract_workspace = tmp_path / f"{image_id}-extract"
        extract_workspace.mkdir()
        extract_root = _extract_iso(iso_path, extract_workspace)
        relfiles = sorted(
            path.relative_to(extract_root).as_posix()
            for path in extract_root.rglob("*")
            if path.is_file()
        )

        assert f"files/000001.00{expected_index}.age" in relfiles
        assert f"files/000001.00{expected_index}.yml.age" in relfiles

        manifest = yaml.safe_load(
            fixture_decrypt_bytes((extract_root / MANIFEST_FILENAME).read_bytes()).decode("utf-8")
        )
        collection = manifest["collections"][0]
        file_entry = collection["files"][0]

        assert file_entry["path"] == f"/{SPLIT_FILE_RELPATH}"
        assert "object" not in file_entry
        assert "sidecar" not in file_entry
        assert file_entry["parts"] == {
            "count": 2,
            "present": [
                {
                    "index": expected_index,
                    "object": f"files/000001.00{expected_index}.age",
                    "sidecar": f"files/000001.00{expected_index}.yml.age",
                }
            ],
        }

        sidecar = yaml.safe_load(
            fixture_decrypt_bytes(
                (extract_root / file_entry["parts"]["present"][0]["sidecar"]).read_bytes()
            ).decode("utf-8")
        )
        assert sidecar["schema"] == "file-sidecar/v1"
        assert sidecar["collection"] == DOCS_COLLECTION_ID
        assert sidecar["path"] == f"/{SPLIT_FILE_RELPATH}"
        assert sidecar["part"] == {"index": expected_index, "count": 2}

        extracted_parts.append(
            fixture_decrypt_bytes(
                (extract_root / file_entry["parts"]["present"][0]["object"]).read_bytes()
            )
        )

    assert tuple(extracted_parts) == SPLIT_FILE_PARTS
    assert b"".join(extracted_parts) == DOCS_FILES[SPLIT_FILE_RELPATH]


def test_arc_disc_fetch_recovers_a_split_file_across_successive_discs(
    acceptance_system: AcceptanceSystem,
) -> None:
    acceptance_system.seed_docs_archive_with_split_invoice()
    acceptance_system.seed_pin("docs:/tax/2022/invoice-123.pdf")
    acceptance_system.seed_fetch("fx-1", "docs:/tax/2022/invoice-123.pdf")
    acceptance_system.configure_arc_disc_fixture(fetch_id="fx-1")
    state_dir = acceptance_system.workspace / "recovery-state" / "fx-1"

    result = acceptance_system.run_arc_disc(
        "fetch",
        "fx-1",
        "--state-dir",
        str(state_dir),
        "--device",
        "/dev/fake-sr0",
        "--json",
    )

    assert result.returncode == 0
    assert json.loads(result.stdout)["state"] == "done"
    assert SPLIT_COPY_ONE_ID in result.stderr
    assert SPLIT_COPY_TWO_ID in result.stderr
    assert acceptance_system.state.is_hot("docs:/tax/2022/invoice-123.pdf") is True
    assert (
        acceptance_system.uploaded_entry_content("fx-1", SPLIT_FILE_RELPATH)
        == acceptance_system.state.selected_files("docs:/tax/2022/invoice-123.pdf")[0].content
    )


def test_arc_disc_fetch_resumes_split_file_recovery_from_state_dir(
    acceptance_system: AcceptanceSystem,
) -> None:
    acceptance_system.seed_docs_archive_with_split_invoice()
    acceptance_system.seed_pin("docs:/tax/2022/invoice-123.pdf")
    acceptance_system.seed_fetch("fx-1", "docs:/tax/2022/invoice-123.pdf")
    state_dir = acceptance_system.workspace / "recovery-state" / "fx-1"

    acceptance_system.configure_arc_disc_fixture(fetch_id="fx-1", fail_copy_ids={SPLIT_COPY_TWO_ID})
    first = acceptance_system.run_arc_disc(
        "fetch",
        "fx-1",
        "--state-dir",
        str(state_dir),
        "--device",
        "/dev/fake-sr0",
    )

    assert first.returncode != 0
    assert (state_dir / "parts" / "e1" / "000000.part").is_file()
    assert acceptance_system.fetches.get("fx-1").state.value != "done"

    acceptance_system.configure_arc_disc_fixture(fetch_id="fx-1", fail_copy_ids={SPLIT_COPY_ONE_ID})
    second = acceptance_system.run_arc_disc(
        "fetch",
        "fx-1",
        "--state-dir",
        str(state_dir),
        "--device",
        "/dev/fake-sr0",
        "--json",
    )

    assert second.returncode == 0
    assert json.loads(second.stdout)["state"] == "done"
    assert SPLIT_COPY_ONE_ID not in second.stderr
    assert SPLIT_COPY_TWO_ID in second.stderr
    assert (
        acceptance_system.uploaded_entry_content("fx-1", SPLIT_FILE_RELPATH)
        == acceptance_system.state.selected_files("docs:/tax/2022/invoice-123.pdf")[0].content
    )
