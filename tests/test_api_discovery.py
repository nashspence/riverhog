from __future__ import annotations

from .helpers import flush_containers, register_iso, seal_collection, stage_collection_files
from .mock_data import family_archive_files


def test_collection_and_container_list_endpoints_surface_summary_state(app_factory):
    with app_factory() as harness:
        sample = family_archive_files()[0]
        upload_path = "discovery-archive"
        stage_collection_files(harness, upload_path, [sample])

        sealed = seal_collection(
            harness,
            upload_path,
            description="discovery archive",
            keep_buffer_after_archive=True,
        )
        collection_id = sealed["collection_id"]

        collections = harness.client.get("/v1/collections", headers=harness.auth_headers())
        assert collections.status_code == 200, collections.text

        body = collections.json()
        assert len(body["collections"]) == 1
        assert body["collections"][0] == {
            "collection_id": collection_id,
            "status": "sealed",
            "upload_relative_path": upload_path,
            "upload_path": str(harness.storage.upload_collection_root(upload_path)),
            "buffer_path": str(harness.storage.buffered_collection_root(collection_id)),
            "description": "discovery archive",
            "keep_buffer_after_archive": True,
            "file_count": 1,
            "directory_count": len(sample.relative_path.split("/")) - 1,
            "created_at": body["collections"][0]["created_at"],
            "sealed_at": body["collections"][0]["sealed_at"],
            "export_path": str(harness.storage.export_collection_root(collection_id)),
            "hash_manifest_path": str(harness.storage.inactive_collection_hash_manifest_path(collection_id)),
            "hash_proof_path": str(harness.storage.inactive_collection_hash_proof_path(collection_id)),
        }
        assert body["collections"][0]["created_at"].endswith("Z")
        assert body["collections"][0]["sealed_at"].endswith("Z")

        container_id = (sealed["closed_containers"] or flush_containers(harness))[0]
        register_iso(harness, container_id, b"registered-iso")

        containers = harness.client.get("/v1/containers", headers=harness.auth_headers())
        assert containers.status_code == 200, containers.text

        payload = containers.json()
        assert len(payload["containers"]) == 1
        assert payload["containers"][0]["container_id"] == container_id
        assert payload["containers"][0]["status"] in {"inactive", "active"}
        assert payload["containers"][0]["entry_count"] > 0
        assert payload["containers"][0]["active_root_present"] is False
        assert payload["containers"][0]["iso_present"] is True
        assert payload["containers"][0]["iso_size_bytes"] == len(b"registered-iso")
        assert payload["containers"][0]["root_path"]
        assert payload["containers"][0]["active_root_path"] is None
        assert payload["containers"][0]["iso_path"]
        assert payload["containers"][0]["burn_confirmed_at"] is None
        assert payload["containers"][0]["created_at"].endswith("Z")

        pool = harness.client.get("/v1/containers/pool", headers=harness.auth_headers())
        assert pool.status_code == 200, pool.text
        assert pool.json()["state"] in {"empty", "ready", "waiting", "over-buffer"}
