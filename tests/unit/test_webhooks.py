from __future__ import annotations

from datetime import UTC, datetime

from arc_core.webhooks import (
    ImagesReadyBatch,
    ReadyImage,
    WebhookConfig,
    build_glacier_upload_failed_payload,
    build_images_ready_payload,
)


def test_build_images_ready_payload_supports_multiple_images() -> None:
    payload = build_images_ready_payload(
        config=WebhookConfig(url="https://example.test/hook", base_url="https://api.test"),
        batch=ImagesReadyBatch(
            batch_id="batch-1",
            images=[
                ReadyImage(
                    image_id="20260420T040001Z", filename="20260420T040001Z.iso", iso_available=True
                ),
                ReadyImage(
                    image_id="20260420T040002Z", filename="20260420T040002Z.iso", iso_available=True
                ),
            ],
        ),
        delivered_at=datetime(2026, 4, 20, tzinfo=UTC),
    )
    assert payload["event"] == "images.ready"
    assert len(payload["images"]) == 2
    assert payload["images"][0]["download_url"].endswith("/v1/images/20260420T040001Z/iso")


def test_build_glacier_upload_failed_payload_includes_error_context() -> None:
    payload = build_glacier_upload_failed_payload(
        config=WebhookConfig(url="https://example.test/hook", base_url="https://api.test"),
        image_id="20260420T040001Z",
        error="s3 timeout",
        attempts=3,
        failed_at="2026-04-20T05:00:00Z",
    )
    assert payload == {
        "event": "images.glacier_upload.failed",
        "image_id": "20260420T040001Z",
        "failed_at": "2026-04-20T05:00:00Z",
        "attempts": 3,
        "error": "s3 timeout",
        "image_url": "https://api.test/v1/images/20260420T040001Z",
        "download_url": "https://api.test/v1/images/20260420T040001Z/iso",
    }
