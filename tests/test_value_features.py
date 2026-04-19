from __future__ import annotations

from pathlib import Path

from .helpers import activation_container_from_root, create_iso, create_collection, force_flush, seal_collection, stage_collection_files
from .mock_data import MockFile, document_archive_files, patterned_bytes


def test_open_collection_tree_keeps_explicit_and_derived_directories(app_factory):
    with app_factory() as harness:
        collection_id = create_collection(harness, description="catalog coverage archive")

        sample = MockFile(
            "scans/2025/passports/alex/passport.pdf",
            patterned_bytes("passport-scan", 24_000),
        )
        stage_collection_files(
            harness,
            collection_id,
            [sample],
            directories=["scans/2025/receipts"],
        )

        tree = harness.client.get(
            f"/v1/collections/{collection_id}/tree",
            headers=harness.auth_headers(),
        )
        assert tree.status_code == 200, tree.text

        directories = {node["path"] for node in tree.json()["nodes"] if node["kind"] == "directory"}
        assert directories == {
            "scans",
            "scans/2025",
            "scans/2025/passports",
            "scans/2025/passports/alex",
            "scans/2025/receipts",
        }

        file_nodes = [node for node in tree.json()["nodes"] if node["kind"] == "file"]
        assert file_nodes == [
            {
                "path": sample.relative_path,
                "kind": "file",
                "size_bytes": sample.size_bytes,
                "active": True,
                "source": "intake",
                "container_ids": [],
                "status": "open",
                "extra": None,
            }
        ]


def test_open_collection_content_reads_directly_from_intake_path(app_factory):
    with app_factory() as harness:
        sample = document_archive_files()[0]
        collection_id = create_collection(harness, description="open intake reads")
        stage_collection_files(harness, collection_id, [sample])

        content = harness.client.get(
            f"/v1/collections/{collection_id}/content/{sample.relative_path}",
            headers=harness.auth_headers(),
        )
        assert content.status_code == 200
        assert content.content == sample.content


def test_sealed_collection_rejects_new_seal_attempt_and_exposes_hash_bundle(app_factory):
    with app_factory() as harness:
        sample = document_archive_files()[0]
        collection_id = create_collection(harness, description="sealed archive behavior")
        stage_collection_files(harness, collection_id, [sample])
        seal_collection(harness, collection_id)

        reseal = harness.client.post(
            f"/v1/collections/{collection_id}/seal",
            headers=harness.auth_headers(),
        )
        assert reseal.status_code == 409
        assert reseal.json()["detail"] == "collection already sealed"

        bundle = harness.client.get(
            f"/v1/collections/{collection_id}/hash-manifest-proof",
            headers=harness.auth_headers(),
        )
        assert bundle.status_code == 200


def test_container_activation_and_evict_toggle_container_and_collection_visibility(app_factory):
    with app_factory() as harness:
        samples = document_archive_files()
        collection_id = create_collection(harness, description="activation visibility archive")
        stage_collection_files(harness, collection_id, samples)

        sealed = seal_collection(harness, collection_id)
        container_id = (sealed["closed_containers"] or force_flush(harness))[0]

        release = harness.client.post(
            f"/v1/collections/{collection_id}/buffer/release",
            headers=harness.auth_headers(),
        )
        assert release.status_code == 200, release.text

        with harness.session() as session:
            container_entry = (
                session.query(harness.models.ContainerEntry)
                .filter_by(container_id=container_id, kind="payload")
                .order_by(harness.models.ContainerEntry.relative_path.asc())
                .first()
            )
            assert container_entry is not None
            container_relpath = container_entry.relative_path

        inactive_container = harness.client.get(
            f"/v1/containers/{container_id}/content/{container_relpath}",
            headers=harness.auth_headers(),
        )
        assert inactive_container.status_code == 409
        assert inactive_container.json()["error"] == "container_inactive"

        _, complete = activation_container_from_root(harness, container_id)
        assert complete.status_code == 200, complete.text

        active_container = harness.client.get(
            f"/v1/containers/{container_id}/content/{container_relpath}",
            headers=harness.auth_headers(),
        )
        assert active_container.status_code == 200

        active_collection = harness.client.get(
            f"/v1/collections/{collection_id}/content/{samples[0].relative_path}",
            headers=harness.auth_headers(),
        )
        assert active_collection.status_code == 200
        assert active_collection.content == samples[0].content

        evict = harness.client.delete(
            f"/v1/containers/{container_id}/activation",
            headers=harness.auth_headers(),
        )
        assert evict.status_code == 200, evict.text

        container_tree = harness.client.get(
            f"/v1/containers/{container_id}/tree",
            headers=harness.auth_headers(),
        )
        assert container_tree.status_code == 200
        assert all(node["active"] is False for node in container_tree.json()["nodes"] if node["kind"] == "file")

        inactive_again = harness.client.get(
            f"/v1/collections/{collection_id}/content/{samples[0].relative_path}",
            headers=harness.auth_headers(),
        )
        assert inactive_again.status_code == 409
        assert inactive_again.json()["error"] == "inactive_on_container"
        assert inactive_again.json()["container_ids"] == [container_id]


def test_iso_overwrite_and_burn_confirmation_are_idempotent(app_factory):
    with app_factory() as harness:
        collection_id = create_collection(harness, description="iso lifecycle archive")
        stage_collection_files(harness, collection_id, document_archive_files())

        sealed = seal_collection(harness, collection_id)
        container_id = (sealed["closed_containers"] or force_flush(harness))[0]

        first_iso = create_iso(harness, container_id, volume_label="FIRST LABEL")
        assert Path(first_iso["iso_path"]).exists()

        conflict = harness.client.post(
            f"/v1/containers/{container_id}/iso/create",
            headers=harness.auth_headers(),
            json={"overwrite": False},
        )
        assert conflict.status_code == 409
        assert conflict.json()["detail"] == "iso already exists; pass overwrite=true to replace it"

        replaced_iso = create_iso(harness, container_id, overwrite=True, volume_label="SECOND LABEL")
        assert replaced_iso["iso_path"] == first_iso["iso_path"]
        assert replaced_iso["size_bytes"] == Path(replaced_iso["iso_path"]).stat().st_size

        confirmed_once = harness.client.post(
            f"/v1/containers/{container_id}/burn/confirm",
            headers=harness.auth_headers(),
        )
        assert confirmed_once.status_code == 200, confirmed_once.text

        confirmed_twice = harness.client.post(
            f"/v1/containers/{container_id}/burn/confirm",
            headers=harness.auth_headers(),
        )
        assert confirmed_twice.status_code == 200, confirmed_twice.text
        assert confirmed_twice.json()["burn_confirmed_at"] == confirmed_once.json()["burn_confirmed_at"]
        assert confirmed_twice.json()["released_collection_ids"] == []


def test_webhook_subscription_backfills_existing_unconfirmed_containers(app_factory):
    with app_factory() as harness:
        collection_id = create_collection(harness, description="notification backfill archive")
        stage_collection_files(harness, collection_id, document_archive_files())

        sealed = seal_collection(harness, collection_id)
        container_id = (sealed["closed_containers"] or force_flush(harness))[0]

        subscribe = harness.client.post(
            "/v1/containers/finalization-webhooks",
            headers=harness.auth_headers(),
            json={"webhook_url": "http://example.test/archive-hook"},
        )
        assert subscribe.status_code == 200, subscribe.text
        assert subscribe.json()["pending_container_count"] == 1

        with harness.session() as session:
            notification = session.query(harness.models.ContainerFinalizationNotification).filter_by(container_id=container_id).one()
            assert notification.status == "pending"
            assert notification.next_attempt_at is not None
