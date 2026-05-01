from __future__ import annotations

from datetime import UTC, datetime

from arc_core.webhooks import (
    ImagesReadyBatch,
    ReadyImage,
    WebhookConfig,
    build_images_ready_payload,
    build_recovery_ready_payload,
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
    assert payload["title"] == "Blank discs are needed"
    assert payload["urgency"] == "attention"
    assert payload["actions"] == [
        {"label": "Run arc-disc", "command": "arc-disc", "argv": ["arc-disc"]}
    ]
    assert "Run arc-disc" in str(payload["body"])
    assert len(payload["images"]) == 2
    assert payload["images"][0]["download_url"].endswith("/v1/images/20260420T040001Z/iso")


def test_build_recovery_ready_payload_includes_session_and_image_urls() -> None:
    payload = build_recovery_ready_payload(
        config=WebhookConfig(url="https://example.test/hook", base_url="https://api.test"),
        session_id="rs-20260420T040001Z-1",
        restore_expires_at="2026-04-20T06:00:00Z",
        images=[
            {
                "image_id": "20260420T040001Z",
                "filename": "20260420T040001Z.iso",
            }
        ],
        delivered_at=datetime(2026, 4, 20, 5, 0, tzinfo=UTC),
        reminder_count=0,
        reminder=False,
    )
    assert payload["event"] == "images.rebuild_ready"
    assert payload["type"] == "image_rebuild"
    assert payload["title"] == "Recovery is ready"
    assert payload["urgency"] == "time-sensitive"
    assert payload["actions"] == [
        {"label": "Run arc-disc", "command": "arc-disc", "argv": ["arc-disc"]}
    ]
    assert payload["session_id"] == "rs-20260420T040001Z-1"
    assert payload["session_url"] == "https://api.test/v1/recovery-sessions/rs-20260420T040001Z-1"
    assert payload["delivered_at"] == "2026-04-20T05:00:00Z"
    assert payload["restore_expires_at"] == "2026-04-20T06:00:00Z"
    assert payload["reminder_count"] == 0
    assert payload["reminder_interval_seconds"] == 3600.0
    assert payload["affected"] == ["20260420T040001Z"]
    assert payload["images"] == [
        {
            "image_id": "20260420T040001Z",
            "filename": "20260420T040001Z.iso",
            "image_url": "https://api.test/v1/images/20260420T040001Z",
        }
    ]
    assert "Run arc-disc" in str(payload["body"])
