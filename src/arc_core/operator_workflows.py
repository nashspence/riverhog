from __future__ import annotations

from dataclasses import dataclass

from arc_core.operator_statecharts import (
    OperatorDecision,
    OperatorView,
    StatechartCatalog,
    load_default_statechart_catalog,
)

ARC_HOME_ATTENTION_GUARDS: dict[str, str] = {
    "notification_health_failed": "notification_delivery_needs_attention",
    "setup_needs_attention": "setup_needs_attention",
    "billing_needs_attention": "billing_needs_attention",
    "cloud_backup_failed": "cloud_backup_failed_after_retries",
    "collection_upload_retry": "collection_upload_retry_available",
}

ARC_DISC_ATTENTION_GUARDS: dict[str, str] = {
    "unfinished_local_disc": "unfinished_local_disc",
    "recovery_ready": "recovery_ready",
    "recovery_approval_required": "recovery_approval_required",
    "hot_recovery_needs_media": "hot_recovery_needs_media",
    "replacement_disc_needed": "replacement_disc_needed",
    "burn_work_ready": "ordinary_blank_disc_work",
    "recovery_expired": "recovery_window_expired",
}


@dataclass(frozen=True, slots=True)
class OperatorWorkflows:
    catalog: StatechartCatalog

    def arc_home_attention_decision(self, kind: str) -> OperatorDecision:
        state = self.catalog.state_for_guard(
            "arc.home",
            "attention_summary",
            ARC_HOME_ATTENTION_GUARDS[kind],
        )
        return self.catalog.decision("arc.home", state)

    def arc_disc_attention_decision(self, kind: str) -> OperatorDecision:
        state = self.catalog.state_for_guard(
            "arc_disc.guided",
            "attention_summary",
            ARC_DISC_ATTENTION_GUARDS[kind],
        )
        return self.catalog.decision("arc_disc.guided", state)

    def decision(self, statechart: str, state: str) -> OperatorDecision:
        return self.catalog.decision(statechart, state)

    def view(self, statechart: str, state: str, *, text: str) -> OperatorView:
        return self.catalog.operator_view(statechart, state, text=text)

    def notification_decision(self, event: str) -> OperatorDecision:
        state = self.catalog.state_for_event(
            "operator.notifications",
            "classify_event",
            event,
        )
        return self.catalog.decision("operator.notifications", state)

    def require_notification_view(self, event: str, view: str) -> None:
        decision = self.notification_decision(event)
        self.catalog.require_view(decision.statechart, decision.state, view)


def load_default_operator_workflows(*, validate_schema: bool = False) -> OperatorWorkflows:
    return OperatorWorkflows(
        catalog=load_default_statechart_catalog(validate_schema=validate_schema)
    )
