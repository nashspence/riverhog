from __future__ import annotations

from pathlib import Path

from .helpers import seal_collection, stage_collection_files
from .mock_data import family_archive_files


def test_auth_and_upload_path_validation(app_factory):
    with app_factory() as harness:
        health = harness.client.get("/healthz")
        assert health.status_code == 200
        assert health.json() == {"status": "ok"}

        unauthorized = harness.client.post("/v1/collections/seal", json={"upload_path": "blocked"})
        assert unauthorized.status_code == 401

        invalid_path = harness.client.post(
            "/v1/collections/seal",
            headers=harness.auth_headers(),
            json={"upload_path": "../escape", "description": "blocked"},
        )
        assert invalid_path.status_code == 400
        assert invalid_path.json()["detail"] == "path must not escape its root"


def test_sealing_collection_claims_staged_files_and_exports_active_bytes(app_factory):
    with app_factory() as harness:
        sample = family_archive_files()[0]
        upload_path = "family-archive-root"
        upload_root = stage_collection_files(harness, upload_path, [sample])

        seal = seal_collection(harness, upload_path, description="home video ingest")
        assert not upload_root.exists()

        collection_id = seal["collection_id"]
        assert collection_id == harness.storage.collection_id_from_upload_path(upload_path)

        with harness.session() as session:
            collection = session.get(harness.models.Collection, collection_id)
            assert collection is not None
            assert collection.upload_relpath == upload_path

            collection_file = session.query(harness.models.CollectionFile).filter_by(collection_id=collection_id).one()
            assert collection_file.status == "active"
            assert collection_file.actual_sha256 == sample.sha256
            assert Path(collection_file.buffer_abs_path).read_bytes() == sample.content

        export_path = harness.storage.export_collection_root(collection_id) / sample.relative_path
        assert export_path.exists()
        assert export_path.read_bytes() == sample.content

        manifest_path = harness.storage.inactive_collection_hash_manifest_path(collection_id)
        proof_path = harness.storage.inactive_collection_hash_proof_path(collection_id)
        assert manifest_path.exists()
        assert proof_path.exists()

        manifest = manifest_path.read_text(encoding="utf-8")
        proof = proof_path.read_text(encoding="utf-8")

        assert "schema: collection-hash-manifest/v1" in manifest
        assert f"collection_id: {collection_id}" in manifest
        assert f"path: {sample.relative_path}" in manifest
        assert f"sha256: {sample.sha256}" in manifest
        assert "OpenTimestamps stub proof v1" in proof


def test_seal_rejects_symlinks_in_staged_collection(app_factory):
    with app_factory() as harness:
        upload_path = "symlink-rejection-archive"
        upload_root = harness.storage.upload_collection_root(upload_path)
        upload_root.mkdir(parents=True, exist_ok=True)
        target = upload_root / "real.txt"
        target.write_text("hello\n", encoding="utf-8")
        (upload_root / "linked.txt").symlink_to(target)

        seal = harness.client.post(
            "/v1/collections/seal",
            headers=harness.auth_headers(),
            json={"upload_path": upload_path, "description": "symlink rejection archive"},
        )
        assert seal.status_code == 400
        assert seal.json()["detail"] == "symlinks are not supported in collections: linked.txt"
        assert upload_root.exists()
