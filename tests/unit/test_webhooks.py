from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import jsonschema

from arc_core.webhooks import (
    ImagesReadyBatch,
    ReadyImage,
    WebhookConfig,
    build_images_ready_payload,
    build_recovery_ready_payload,
    build_status_notification_payload,
)

ROOT = Path(__file__).resolve().parents[2]


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


def test_build_status_notification_payload_uses_statechart_status_event() -> None:
    payload = build_status_notification_payload(
        statechart="arc.upload",
        state="progress",
        operation_id="upload:photos-2024",
        workflow="collection upload",
        occurred_at=datetime(2026, 4, 20, 4, 0, 1, tzinfo=UTC),
        progress={
            "current": 3,
            "total": 3,
            "unit": "files",
            "summary": "3 files accepted",
        },
    )

    assert payload["kind"] == "status"
    assert payload["event"] == "operator.arc_upload.still_running"
    assert payload["status"] == "still_running"
    assert payload["operation_id"] == "upload:photos-2024"
    assert payload["statechart"] == "arc.upload"
    assert payload["state"] == "progress"
    assert payload["command"] == "arc"
    assert payload["urgency"] == "info"
    assert payload["occurred_at"] == "2026-04-20T04:00:01Z"


def test_build_status_notification_payload_preserves_action_needed_reference() -> None:
    payload = build_status_notification_payload(
        statechart="arc_disc.recovery",
        state="approval_required",
        operation_id="recovery:rs-20260420T040001Z-rebuild-1",
        workflow="disc recovery",
        occurred_at=datetime(2026, 4, 20, 4, 0, 1, tzinfo=UTC),
        blocked_reason="operator approval required",
    )

    assert payload["event"] == "operator.arc_disc_recovery.blocked"
    assert payload["status"] == "blocked"
    assert payload["urgency"] == "attention"
    assert payload["blocked_reason"] == "operator approval required"
    assert payload["action_needed_event"] == "operator.recovery_approval_required"


def test_build_status_notification_payload_matches_schema() -> None:
    schema = json.loads(
        (ROOT / "contracts/operator/status-notification.schema.json").read_text(
            encoding="utf-8"
        )
    )
    payload = build_status_notification_payload(
        statechart="arc.upload",
        state="cloud_backup_failed",
        operation_id="upload:photos-2024",
        workflow="collection upload",
        occurred_at=datetime(2026, 4, 20, 4, 0, 1, tzinfo=UTC),
        error="cloud backup failed after retries",
    )

    jsonschema.Draft202012Validator(
        schema,
        format_checker=jsonschema.FormatChecker(),
    ).validate(payload)
