from __future__ import annotations

from .helpers import flush_containers, register_iso, seal_collection, stage_collection_files
from .mock_data import document_archive_files, family_archive_files


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


def test_plan_endpoint_and_seal_response_surface_current_disc_allocation_plan(app_factory):
    with app_factory(
        CONTAINER_FILL_GB="0.00075",
        CONTAINER_SPILL_FILL_GB="0.00070",
        CONTAINER_TARGET_GB="0.00080",
        CONTAINER_BUFFER_MAX_GB="0.0100",
    ) as harness:
        upload_path = "planned-financial-archive"
        stage_collection_files(harness, upload_path, document_archive_files())

        sealed = seal_collection(
            harness,
            upload_path,
            description="planned financial archive",
        )
        collection_id = sealed["collection_id"]
        plan = sealed["plan"]

        assert plan["target_bytes"] > 0
        assert plan["planned_disc_count"] == 1
        assert plan["closed_disc_count"] == 0
        assert plan["buffer_planned_bytes"] > 0
        assert plan["buffer_payload_bytes"] > 0
        assert len(plan["discs"]) == 1

        disc = plan["discs"][0]
        assert disc["name"] == "PLAN001"
        assert disc["status"] == "planned_partial"
        assert disc["meets_close_threshold"] is False
        assert disc["used_bytes"] > 0
        assert disc["payload_bytes"] > 0
        assert disc["collections"][0]["collection"] == collection_id
        assert disc["collections"][0]["is_partial_collection"] is False
        assert {
            file_summary["path"]
            for item in disc["collections"][0]["items"]
            for file_summary in item["files"]
        } == {sample.relative_path for sample in document_archive_files()}

        current_plan = harness.client.get(
            "/v1/containers/plan",
            headers=harness.auth_headers(),
        )
        assert current_plan.status_code == 200, current_plan.text
        assert current_plan.json() == plan
