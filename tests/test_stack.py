import hashlib
from dataclasses import dataclass
from pathlib import Path

from minio.error import S3Error

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


@dataclass(frozen=True)
class UploadSpec:
    path: Path
    original_name: str
    content_type: str


UPLOADS = [
    UploadSpec(
        path=FIXTURES_DIR / "camera-roll-note.txt",
        original_name="camera roll notes.txt",
        content_type="text/plain",
    ),
    UploadSpec(
        path=FIXTURES_DIR / "sensor-catalog.json",
        original_name="sensor catalog.json",
        content_type="application/json",
    ),
    UploadSpec(
        path=FIXTURES_DIR / "box-inventory.csv",
        original_name="box inventory.csv",
        content_type="text/csv",
    ),
]


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def object_bytes(stack, object_key: str) -> bytes:
    response = stack.minio().get_object(stack.bucket, object_key)
    try:
        return response.read()
    finally:
        response.close()
        response.release_conn()


def object_exists(stack, object_key: str) -> bool:
    try:
        stack.minio().stat_object(stack.bucket, object_key)
    except S3Error as exc:
        if exc.code == "NoSuchKey":
            return False
        raise
    return True


def test_real_stack_round_trip(stack):
    uploads = []

    for spec in UPLOADS:
        payload = spec.path.read_bytes()
        with spec.path.open("rb") as handle:
            response = stack.api(
                "POST",
                "/ingest",
                files={"file": (spec.path.name, handle, spec.content_type)},
                data={"original_name": spec.original_name},
            )
        assert response.status_code == 200, response.text
        item = response.json()
        assert item["status"] == "staged"
        assert item["original_name"] == spec.original_name
        assert item["size"] == len(payload)
        assert item["content_type"] == spec.content_type
        assert item["sha256"] == sha256_bytes(payload)
        uploads.append({**item, "payload": payload})

    files_response = stack.api("GET", "/files")
    assert files_response.status_code == 200, files_response.text
    files_by_hash = {row["sha256"]: row for row in files_response.json()}
    assert len(files_by_hash) == len(UPLOADS)

    for upload in uploads:
        row = files_by_hash[upload["sha256"]]
        assert row["original_name"] == upload["original_name"]
        assert row["status"] == "staged"
        assert object_exists(stack, upload["object_key"])
        assert object_bytes(stack, upload["object_key"]) == upload["payload"]

    seal_response = stack.api("POST", "/seal", timeout=180)
    assert seal_response.status_code == 200, seal_response.text
    package = seal_response.json()
    assert package["status"] == "sealed"
    assert package["bytes"] == sum(len(upload["payload"]) for upload in uploads)
    assert package["file_count"] == len(uploads)

    packages_response = stack.api("GET", "/packages")
    assert packages_response.status_code == 200, packages_response.text
    packages = packages_response.json()
    assert len(packages) == 1
    assert packages[0]["id"] == package["id"]
    assert packages[0]["disc_url"].endswith(f"/d/{package['iso_sha256']}")

    html_response = stack.api("GET", f"/d/{package['iso_sha256']}")
    assert html_response.status_code == 200, html_response.text
    assert "Archived disc" in html_response.text
    for upload in uploads:
        assert upload["original_name"] in html_response.text

    manifest_response = stack.api("GET", f"/d/{package['iso_sha256']}.json")
    assert manifest_response.status_code == 200, manifest_response.text
    manifest = manifest_response.json()
    assert manifest["disc"]["status"] == "sealed"
    assert manifest["disc"]["file_count"] == len(uploads)
    assert [item["original_name"] for item in manifest["files"]] == [
        upload["original_name"] for upload in uploads
    ]

    iso_response = stack.api("GET", f"/packages/{package['id']}/download", timeout=180)
    assert iso_response.status_code == 200, iso_response.text
    iso_bytes = iso_response.content
    assert sha256_bytes(iso_bytes) == package["iso_sha256"]

    burned_response = stack.api("POST", f"/packages/{package['id']}/burned")
    assert burned_response.status_code == 200, burned_response.text
    burned = burned_response.json()
    assert burned["status"] == "burned"
    assert burned["iso_name"] is None

    missing_iso_response = stack.api("GET", f"/packages/{package['id']}/download")
    assert missing_iso_response.status_code == 404

    for upload in uploads:
        assert not object_exists(stack, upload["object_key"])

    rehydrate_response = stack.api(
        "POST",
        "/rehydrate",
        files={
            "file": (
                f"{package['iso_sha256']}.iso",
                iso_bytes,
                "application/octet-stream",
            )
        },
        data={"passphrase": stack.passphrase},
        timeout=180,
    )
    assert rehydrate_response.status_code == 200, rehydrate_response.text
    rehydrated = rehydrate_response.json()
    assert rehydrated["package"]["status"] == "rehydrated"
    assert sorted(rehydrated["keys"]) == sorted(upload["object_key"] for upload in uploads)

    for upload in uploads:
        assert object_exists(stack, upload["object_key"])
        assert object_bytes(stack, upload["object_key"]) == upload["payload"]

    unrehydrate_response = stack.api("DELETE", f"/rehydrated/{package['id']}")
    assert unrehydrate_response.status_code == 200, unrehydrate_response.text
    unrehydrated = unrehydrate_response.json()
    assert unrehydrated["status"] == "burned"
    assert unrehydrated["rehydrated_at"] is None

    for upload in uploads:
        assert not object_exists(stack, upload["object_key"])
