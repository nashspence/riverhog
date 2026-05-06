from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol, cast

import httpx

from arc_core.operator_statecharts import StatechartCatalogError
from arc_core.operator_workflows import load_default_operator_workflows
from contracts.operator import copy as operator_copy

_OPERATOR_WORKFLOWS = load_default_operator_workflows()


@dataclass(frozen=True)
class ReadyImage:
    image_id: str
    filename: str
    iso_available: bool


@dataclass(frozen=True)
class ImagesReadyBatch:
    batch_id: str
    images: list[ReadyImage]
    reminder_count: int = 0
    initial_sent_at: datetime | None = None
    next_attempt_at: datetime | None = None


class ImageReadyReminderStore(Protocol):
    def list_due(self, *, now: datetime, limit: int) -> list[ImagesReadyBatch]: ...
    def mark_delivered(
        self, batch_id: str, *, delivered_at: datetime, next_attempt_at: datetime | None
    ) -> None: ...
    def mark_failed(self, batch_id: str, *, error: str, next_attempt_at: datetime) -> None: ...


@dataclass(frozen=True)
class WebhookConfig:
    url: str
    base_url: str
    timeout_seconds: float = 10.0
    retry_seconds: float = 60.0
    reminder_interval_seconds: float = 3600.0


def utcnow() -> datetime:
    return datetime.now(UTC)


def isoformat_z(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def image_iso_download_path(image_id: str) -> str:
    return f"/v1/images/{image_id}/iso"


def image_summary_path(image_id: str) -> str:
    return f"/v1/images/{image_id}"


def image_iso_download_url(base_url: str, image_id: str) -> str:
    return f"{base_url.rstrip('/')}{image_iso_download_path(image_id)}"


def image_summary_url(base_url: str, image_id: str) -> str:
    return f"{base_url.rstrip('/')}{image_summary_path(image_id)}"


def build_status_notification_payload(
    *,
    statechart: str,
    state: str,
    operation_id: str,
    workflow: str,
    occurred_at: datetime,
    blocked_reason: str | None = None,
    error: str | None = None,
    progress: Mapping[str, object] | None = None,
) -> dict[str, object]:
    state_payload = _OPERATOR_WORKFLOWS.catalog.require_state(statechart, state)
    status_event = _status_event_for_state(statechart, state, state_payload)
    status = str(status_event["status"])
    statechart_payload = _OPERATOR_WORKFLOWS.catalog.require_statechart(statechart)
    payload: dict[str, object] = {
        "kind": "status",
        "event": str(status_event["event"]),
        "status": status,
        "workflow": workflow,
        "operation_id": operation_id,
        "statechart": statechart,
        "state": state,
        "command": str(statechart_payload.get("command")),
        "title": f"{workflow} {status.replace('_', ' ')}",
        "body": f"{workflow} is {status.replace('_', ' ')}.",
        "urgency": _status_urgency(status),
        "occurred_at": isoformat_z(occurred_at),
    }
    if progress is not None:
        payload["progress"] = dict(progress)
    if status == "blocked":
        payload["blocked_reason"] = blocked_reason or "operator action is required"
    if status == "failed":
        payload["error"] = error or "operator work failed"
    if action_needed_event := status_event.get("action_needed_event"):
        payload["action_needed_event"] = str(action_needed_event)
    return payload


def emit_status_notification(
    *,
    config: WebhookConfig,
    statechart: str,
    state: str,
    operation_id: str,
    workflow: str,
    occurred_at: datetime | None = None,
    blocked_reason: str | None = None,
    error: str | None = None,
    progress: Mapping[str, object] | None = None,
) -> dict[str, object]:
    payload = build_status_notification_payload(
        statechart=statechart,
        state=state,
        operation_id=operation_id,
        workflow=workflow,
        occurred_at=occurred_at or utcnow(),
        blocked_reason=blocked_reason,
        error=error,
        progress=progress,
    )
    post_webhook(config=config, payload=payload)
    return payload


def recovery_session_path(session_id: str) -> str:
    return f"/v1/recovery-sessions/{session_id}"


def recovery_session_url(base_url: str, session_id: str) -> str:
    return f"{base_url.rstrip('/')}{recovery_session_path(session_id)}"


def build_images_ready_payload(
    *, config: WebhookConfig, batch: ImagesReadyBatch, delivered_at: datetime
) -> dict[str, object]:
    is_reminder = batch.initial_sent_at is not None
    reminder_count = batch.reminder_count + (1 if is_reminder else 0)
    base_payload: dict[str, object] = {
        "event": "images.ready.reminder" if is_reminder else "images.ready",
        "batch_id": batch.batch_id,
        "delivered_at": isoformat_z(delivered_at),
        "reminder_count": reminder_count,
        "reminder_interval_seconds": config.reminder_interval_seconds,
        "images": [
            {
                "image_id": image.image_id,
                "filename": image.filename,
                "iso_available": image.iso_available,
                "download_url": image_iso_download_url(config.base_url, image.image_id),
            }
            for image in batch.images
        ],
    }
    notification = operator_copy.push_burn_work_ready(
        disc_count=len(batch.images),
        oldest_ready_at=isoformat_z(batch.initial_sent_at),
    )
    _OPERATOR_WORKFLOWS.require_notification_view(
        str(base_payload["event"]),
        "push_burn_work_ready",
    )
    return {
        **base_payload,
        **notification.payload(
            reminder=is_reminder,
            reminder_count=reminder_count,
            delivered_at=isoformat_z(delivered_at),
        ),
    }


def build_recovery_ready_payload(
    *,
    config: WebhookConfig,
    session_id: str,
    restore_expires_at: str | None,
    images: list[dict[str, str]],
    delivered_at: datetime,
    reminder_count: int,
    reminder: bool,
) -> dict[str, object]:
    affected = [image["image_id"] for image in images]
    reminder_count_value = reminder_count + (1 if reminder else 0)
    payload: dict[str, object] = {
        "event": "images.rebuild_ready.reminder" if reminder else "images.rebuild_ready",
        "type": "image_rebuild",
        "session_id": session_id,
        "delivered_at": isoformat_z(delivered_at),
        "restore_expires_at": restore_expires_at,
        "reminder_count": reminder_count_value,
        "reminder_interval_seconds": config.reminder_interval_seconds,
        "affected": affected,
        "images": [
            {
                "image_id": image["image_id"],
                "filename": image["filename"],
                **(
                    {"image_url": image_summary_url(config.base_url, image["image_id"])}
                    if config.base_url
                    else {}
                ),
            }
            for image in images
        ],
    }
    if config.base_url:
        payload["session_url"] = recovery_session_url(config.base_url, session_id)
    notification = operator_copy.push_recovery_ready(
        affected=affected,
        expires_at=restore_expires_at,
    )
    _OPERATOR_WORKFLOWS.require_notification_view(
        str(payload["event"]),
        "push_recovery_ready",
    )
    return {
        **payload,
        **notification.payload(
            reminder=reminder,
            reminder_count=reminder_count_value,
            delivered_at=isoformat_z(delivered_at),
        ),
    }


def post_webhook(*, config: WebhookConfig, payload: dict[str, object]) -> None:
    with httpx.Client(timeout=config.timeout_seconds) as client:
        response = client.post(config.url, json=payload)
        response.raise_for_status()


def _status_event_for_state(
    statechart: str,
    state: str,
    state_payload: Mapping[str, Any],
) -> Mapping[str, Any]:
    status_events = state_payload.get("status_events")
    if not isinstance(status_events, list) or not status_events:
        raise StatechartCatalogError(f"{statechart}.{state} defines no status_events")
    status_event = status_events[0]
    if not isinstance(status_event, Mapping):
        raise StatechartCatalogError(f"{statechart}.{state} status_events[0] is not an object")
    return cast(Mapping[str, Any], status_event)


def _status_urgency(status: str) -> str:
    if status == "blocked":
        return "attention"
    if status == "failed":
        return "important"
    return "info"


class ImagesReadyReminderService:
    def __init__(self, *, store: ImageReadyReminderStore, config: WebhookConfig) -> None:
        self.store = store
        self.config = config

    def deliver_due(self, *, now: datetime | None = None, limit: int = 100) -> int:
        current = now or utcnow()
        delivered = 0
        for batch in self.store.list_due(now=current, limit=limit):
            try:
                payload = build_images_ready_payload(
                    config=self.config, batch=batch, delivered_at=current
                )
                post_webhook(config=self.config, payload=payload)
            except Exception as exc:
                self.store.mark_failed(
                    batch.batch_id,
                    error=str(exc),
                    next_attempt_at=current
                    + timedelta(seconds=max(1.0, self.config.retry_seconds)),
                )
                continue
            next_attempt = None
            if self.config.reminder_interval_seconds > 0:
                next_attempt = current + timedelta(seconds=self.config.reminder_interval_seconds)
            self.store.mark_delivered(
                batch.batch_id, delivered_at=current, next_attempt_at=next_attempt
            )
            delivered += 1
        return delivered

    async def run_forever(self, *, interval_seconds: float = 30.0) -> None:
        while True:
            self.deliver_due()
            await asyncio.sleep(interval_seconds)
