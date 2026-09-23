"""Contract-owned error acceptance for Stove0 control operations."""

from __future__ import annotations

from http_api_contracts import HttpOperationErrorAuthority

STOVE0_HTTP_ERROR_AUTHORITY = HttpOperationErrorAuthority.from_codes(
    common=("bad_request", "unauthorized", "forbidden", "internal_error"),
    operation={
        "get_recipe": ("not_found",),
        "create_work": ("conflict", "not_found"),
        "get_work": ("not_found",),
        "inspect_work_coordination": ("not_found",),
        "get_artifact_selection": ("not_found",),
        "step_work": ("conflict", "not_found"),
        "retry_work": ("conflict", "not_found"),
        "cancel_work": ("conflict", "not_found"),
        "preview_workflow": ("not_found",),
        "create_evaluation": ("conflict",),
        "get_evaluation": ("conflict", "not_found"),
        "step_evaluation": ("conflict", "not_found"),
        "cancel_evaluation": ("conflict", "not_found"),
        "retry_evaluation_variant": ("conflict", "not_found"),
        "review_evaluation_variant": ("conflict", "not_found"),
        "run_scheduler": ("conflict",),
    },
    exact={
        "health_live": ("internal_error",),
        "health_ready": ("internal_error", "service_unavailable"),
    },
)


__all__ = ["STOVE0_HTTP_ERROR_AUTHORITY"]
