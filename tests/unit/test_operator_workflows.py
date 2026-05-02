from __future__ import annotations

import pytest

from arc_core.operator_statecharts import StatechartCatalogError
from arc_core.operator_workflows import load_default_operator_workflows


def test_arc_attention_items_resolve_through_statechart_guards() -> None:
    workflows = load_default_operator_workflows(validate_schema=True)

    expected_views = {
        "notification_health_failed": "arc_item_notification_health_failed",
        "setup_needs_attention": "arc_item_setup_needs_attention",
        "billing_needs_attention": "arc_item_billing_needs_attention",
        "cloud_backup_failed": "arc_item_cloud_backup_failed",
        "collection_upload_retry": "arc_item_upload_retry_available",
    }

    for kind, view in expected_views.items():
        decision = workflows.arc_home_attention_decision(kind)
        assert decision.statechart == "arc.home"
        workflows.catalog.require_view(decision.statechart, decision.state, view)


def test_arc_disc_attention_items_resolve_through_statechart_guards() -> None:
    workflows = load_default_operator_workflows(validate_schema=True)

    expected_views = {
        "unfinished_local_disc": "disc_item_unfinished_local_copy",
        "recovery_ready": "disc_item_recovery_ready",
        "recovery_approval_required": "disc_item_recovery_approval_required",
        "hot_recovery_needs_media": "disc_item_hot_recovery_needs_media",
        "replacement_disc_needed": "disc_item_replacement_disc_needed",
        "burn_work_ready": "disc_item_burn_work_ready",
        "recovery_expired": "disc_item_recovery_expired",
    }

    for kind, view in expected_views.items():
        decision = workflows.arc_disc_attention_decision(kind)
        assert decision.statechart == "arc_disc.guided"
        workflows.catalog.require_view(decision.statechart, decision.state, view)


def test_notification_events_resolve_through_statechart_classifier() -> None:
    workflows = load_default_operator_workflows(validate_schema=True)

    expected_views = {
        "images.ready": "push_burn_work_ready",
        "operator.disc_work_waiting_too_long": "push_disc_work_waiting_too_long",
        "operator.replacement_disc_needed": "push_replacement_disc_needed",
        "operator.recovery_approval_required": "push_recovery_approval_required",
        "images.rebuild_ready": "push_recovery_ready",
        "images.rebuild_ready.reminder": "push_recovery_ready",
        "operator.hot_recovery_needs_media": "push_hot_recovery_needs_media",
        "collections.glacier_upload.failed": "push_cloud_backup_failed",
        "operator.notification_health_failed": "push_notification_health_failed",
        "operator.billing_needs_attention": "push_billing_needs_attention",
        "operator.setup_needs_attention": "push_setup_needs_attention",
    }

    for event, view in expected_views.items():
        workflows.require_notification_view(event, view)


def test_workflow_validation_fails_when_expected_view_drifts() -> None:
    workflows = load_default_operator_workflows(validate_schema=True)

    with pytest.raises(StatechartCatalogError):
        workflows.require_notification_view("images.ready", "push_recovery_ready")
