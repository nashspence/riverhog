"""Stove0 v1 HTTP client."""

from __future__ import annotations

import math
import os
import secrets
from collections.abc import Mapping, Sequence
from typing import Any, Self
from urllib.parse import quote

import httpx
from http_api_contracts import (
    HealthOut as HealthOut,
)
from http_api_contracts import (
    closed_literal_values,
    parse_operation_error_payload,
    safe_http_base_url,
)
from stove0_observer_protocol import ContentObservationEvidence
from stove0_operator_contracts import (
    STOVE0_HTTP_ERROR_AUTHORITY,
    AdmissionPage,
    AdmissionPolicyCatalogView,
    AdmissionPolicyStatus,
    AdmissionSort,
    AdmissionState,
    AdmissionView,
    DepartureEffectPage,
    DepartureEffectView,
    DeparturePolicyCatalogView,
    DeparturePolicyStatus,
    EvaluationPage,
    EvaluationPhase,
    EvaluationReviewRequest,
    EvaluationSort,
    EvaluationView,
    ObservationTaskDetail,
    ObservationTaskPage,
    OperatorWorkflowPreviewRequest,
    PlanningOwnerKind,
    RecipeCatalogView,
    RecipeView,
    SchedulerRole,
    SchedulerRun,
    SchedulerRunRequest,
    SchedulerStatus,
    SortOrder,
    Stove0EventPage,
    WorkCreateRequest,
    WorkInitiationStatus,
    WorkPage,
    WorkPhase,
    WorkSort,
    WorkView,
)
from stove0_protocol import (
    ArtifactSelectionPage,
    BranchSetEvaluation,
    CollectionRootIdentityRef,
    EvaluationDefinition,
    PlanningJobStatus,
)
from stove0_protocol.observation_evidence import AcceptedEvidencePage, AcceptedViewPage

_PLANNING_OWNER_KINDS = closed_literal_values(PlanningOwnerKind)
_SORT_ORDERS = closed_literal_values(SortOrder)
_WORK_SORTS = closed_literal_values(WorkSort)
_WORK_PHASES = closed_literal_values(WorkPhase)
_EVALUATION_SORTS = closed_literal_values(EvaluationSort)
_EVALUATION_PHASES = closed_literal_values(EvaluationPhase)
_ADMISSION_SORTS = closed_literal_values(AdmissionSort)
_ADMISSION_STATES = closed_literal_values(AdmissionState)


def _one_of(value: str, allowed: frozenset[str], label: str) -> str:
    if value not in allowed:
        choices = ", ".join(sorted(allowed))
        raise ValueError(f"{label} must be one of: {choices}")
    return value


class Stove0ApiError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        code: str = "a_stove0_client_error",
        observed_status: int | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        if observed_status is not None and not 400 <= observed_status <= 599:
            raise ValueError("observed HTTP status must be a 4xx or 5xx response")
        self.message = message
        self.code = code
        self.observed_status = observed_status
        self.details = dict(details or {})


class Stove0ApiClient:
    def __init__(
        self,
        base_url: str | None = None,
        token: str | None = None,
        *,
        allow_insecure_http: bool | None = None,
        timeout_seconds: float | None = None,
        http2: bool | None = None,
    ) -> None:
        allow = (
            _boolean_env("STOVE0_ALLOW_INSECURE_HTTP", False)
            if allow_insecure_http is None
            else allow_insecure_http
        )
        self.base_url = safe_http_base_url(
            base_url or os.getenv("STOVE0_BASE_URL") or "http://127.0.0.1:8080",
            setting="STOVE0_BASE_URL",
            allow_insecure_http=allow,
        )
        self.allow_insecure_http = allow
        self.token = token or os.getenv("STOVE0_TOKEN")
        self.http2 = _boolean_env("STOVE0_HTTP2", True) if http2 is None else http2
        self.timeout_seconds = (
            _positive_float_env("STOVE0_HTTP_TIMEOUT_SECONDS", 300.0)
            if timeout_seconds is None
            else _positive_float(timeout_seconds, "timeout_seconds")
        )
        self._client: httpx.Client | None = None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> None:
        self.close()

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    def health_live(self) -> HealthOut:
        return HealthOut.model_validate(
            self._json("health_live", "GET", "/health/live", authenticated=False)
        )

    def health_ready(self) -> HealthOut:
        return HealthOut.model_validate(
            self._json("health_ready", "GET", "/health/ready", authenticated=False)
        )

    def list_events(self, *, after: str | None = None, limit: int = 100) -> Stove0EventPage:
        return Stove0EventPage.model_validate(
            self._json("list_events", "GET", "/v1/events", params=_params(after=after, limit=limit))
        )

    def list_recipes(self) -> RecipeCatalogView:
        return RecipeCatalogView.model_validate(self._json("list_recipes", "GET", "/v1/recipes"))

    def get_recipe(self, recipe_id: str, *, revision: int | None = None) -> RecipeView:
        return RecipeView.model_validate(
            self._json(
                "get_recipe",
                "GET",
                f"/v1/recipes/{quote(recipe_id, safe='')}",
                params=_params(revision=revision),
            )
        )

    def list_admission_policies(self) -> AdmissionPolicyCatalogView:
        return AdmissionPolicyCatalogView.model_validate(
            self._json("list_admission_policies", "GET", "/v1/admission-policies")
        )

    def rebaseline_admission_policy(self, policy_id: str) -> AdmissionPolicyStatus:
        return AdmissionPolicyStatus.model_validate(
            self._json(
                "rebaseline_admission_policy",
                "POST",
                f"/v1/admission-policies/{quote(policy_id, safe='')}:rebaseline",
            )
        )

    def backfill_admission_policy(self, policy_id: str) -> AdmissionPolicyStatus:
        return AdmissionPolicyStatus.model_validate(
            self._json(
                "backfill_admission_policy",
                "POST",
                f"/v1/admission-policies/{quote(policy_id, safe='')}:backfill",
            )
        )

    def list_admissions(
        self,
        *,
        page_size: int = 25,
        page_token: str | None = None,
        policy_id: str | None = None,
        state: AdmissionState | None = None,
        query: str | None = None,
        sort: AdmissionSort = "created_at",
        order: SortOrder = "desc",
    ) -> AdmissionPage:
        return AdmissionPage.model_validate(
            self._json(
                "list_admissions",
                "GET",
                "/v1/admissions",
                params=_params(
                    **{
                        "page_size": page_size,
                        "page_token": page_token,
                        "policy_id": policy_id,
                        "state": (
                            _one_of(state, _ADMISSION_STATES, "admission state") if state else None
                        ),
                        "q": query,
                        "sort": _one_of(sort, _ADMISSION_SORTS, "admission sort"),
                        "order": _one_of(order, _SORT_ORDERS, "sort order"),
                    }
                ),
            )
        )

    def get_admission(self, admission_id: str) -> AdmissionView:
        return AdmissionView.model_validate(
            self._json("get_admission", "GET", f"/v1/admissions/{quote(admission_id, safe='')}")
        )

    def list_departure_policies(self) -> DeparturePolicyCatalogView:
        return DeparturePolicyCatalogView.model_validate(
            self._json("list_departure_policies", "GET", "/v1/departure-policies")
        )

    def rebaseline_departure_policy(self, policy_id: str) -> DeparturePolicyStatus:
        return DeparturePolicyStatus.model_validate(
            self._json(
                "rebaseline_departure_policy",
                "POST",
                f"/v1/departure-policies/{quote(policy_id, safe='')}:rebaseline",
            )
        )

    def list_departure_effects(
        self, *, page_size: int = 25, page_token: str | None = None
    ) -> DepartureEffectPage:
        return DepartureEffectPage.model_validate(
            self._json(
                "list_departure_effects",
                "GET",
                "/v1/departure-effects",
                params=_params(page_size=page_size, page_token=page_token),
            )
        )

    def get_departure_effect(self, departure_id: str) -> DepartureEffectView:
        return DepartureEffectView.model_validate(
            self._json(
                "get_departure_effect",
                "GET",
                f"/v1/departure-effects/{quote(departure_id, safe='')}",
            )
        )

    def list_work(
        self,
        *,
        page_size: int = 25,
        page_token: str | None = None,
        phase: WorkPhase | None = None,
        query: str | None = None,
        sort: WorkSort = "updated_at",
        order: SortOrder = "desc",
    ) -> WorkPage:
        return WorkPage.model_validate(
            self._json(
                "list_work",
                "GET",
                "/v1/work",
                params=_params(
                    **{
                        "page_size": page_size,
                        "page_token": page_token,
                        "phase": _one_of(phase, _WORK_PHASES, "work phase") if phase else None,
                        "q": query,
                        "sort": _one_of(
                            sort,
                            _WORK_SORTS,
                            "work sort",
                        ),
                        "order": _one_of(
                            order,
                            _SORT_ORDERS,
                            "sort order",
                        ),
                    }
                ),
            )
        )

    def create_work(
        self,
        recipe_id: str,
        inputs: Sequence[CollectionRootIdentityRef],
        *,
        preview_sha256: str,
        recipe_revision: int | None = None,
        effective_intent: Mapping[str, Any] | None = None,
        invocation_id: str | None = None,
    ) -> WorkInitiationStatus:
        request = WorkCreateRequest.model_validate(
            dict(
                recipe_id=recipe_id,
                invocation_id=invocation_id or secrets.token_hex(32),
                preview_sha256=preview_sha256,
                recipe_revision=None if recipe_revision is None else str(recipe_revision),
                inputs=tuple(inputs),
                effective_intent=dict(effective_intent or {}),
            )
        )
        return WorkInitiationStatus.model_validate(
            self._json(
                "create_work",
                "POST",
                "/v1/work",
                json=request.model_dump(mode="json", exclude_none=True),
            )
        )

    def get_work_initiation(self, job_id: str) -> WorkInitiationStatus:
        return WorkInitiationStatus.model_validate(
            self._json(
                "get_work_initiation",
                "GET",
                f"/v1/work-initiations/{quote(job_id, safe='')}",
            )
        )

    def get_work(self, work_id: str) -> WorkView:
        return WorkView.model_validate(
            self._json("get_work", "GET", f"/v1/work/{quote(work_id, safe='')}")
        )

    def inspect_work_coordination(self, work_id: str) -> BranchSetEvaluation:
        return BranchSetEvaluation.model_validate(
            self._json(
                "inspect_work_coordination",
                "GET",
                f"/v1/work/{quote(work_id, safe='')}/coordination",
            )
        )

    def list_observation_tasks(
        self,
        *,
        owner_kind: PlanningOwnerKind,
        owner_id: str,
        page_size: int = 25,
        page_token: str | None = None,
    ) -> ObservationTaskPage:
        return ObservationTaskPage.model_validate(
            self._json(
                "list_observation_tasks",
                "GET",
                "/v1/observation-tasks",
                params=_params(
                    owner_kind=_one_of(owner_kind, _PLANNING_OWNER_KINDS, "owner_kind"),
                    owner_id=owner_id,
                    page_size=page_size,
                    page_token=page_token,
                ),
            )
        )

    def get_observation_task(
        self,
        task_work_id: str,
        task_id: str,
        *,
        owner_kind: PlanningOwnerKind,
        owner_id: str,
    ) -> ObservationTaskDetail:
        return ObservationTaskDetail.model_validate(
            self._json(
                "get_observation_task",
                "GET",
                f"/v1/observation-tasks/{quote(task_work_id, safe='')}/{quote(task_id, safe='')}",
                params=_params(
                    owner_kind=_one_of(owner_kind, _PLANNING_OWNER_KINDS, "owner_kind"),
                    owner_id=owner_id,
                ),
            )
        )

    def get_observation_results(
        self,
        task_work_id: str,
        task_id: str,
        *,
        owner_kind: PlanningOwnerKind,
        owner_id: str,
        evidence_set_sha256: str,
        start_ordinal: int = 0,
        limit: int = 100,
    ) -> AcceptedEvidencePage:
        return AcceptedEvidencePage.model_validate(
            self._json(
                "get_observation_results",
                "GET",
                f"/v1/observation-tasks/{quote(task_work_id, safe='')}/"
                f"{quote(task_id, safe='')}/results",
                params=_params(
                    owner_kind=_one_of(owner_kind, _PLANNING_OWNER_KINDS, "owner_kind"),
                    owner_id=owner_id,
                    evidence_set_sha256=evidence_set_sha256,
                    start_ordinal=str(start_ordinal),
                    limit=limit,
                ),
            )
        )

    def get_observation_result(
        self, request_id: str, *, owner_kind: PlanningOwnerKind, owner_id: str
    ) -> ContentObservationEvidence:
        return ContentObservationEvidence.model_validate(
            self._json(
                "get_observation_result",
                "GET",
                f"/v1/observation-results/{quote(request_id, safe='')}",
                params=_params(
                    owner_kind=_one_of(owner_kind, _PLANNING_OWNER_KINDS, "owner_kind"),
                    owner_id=owner_id,
                ),
            )
        )

    def get_observation_view(
        self,
        task_work_id: str,
        task_id: str,
        view_id: str,
        *,
        owner_kind: PlanningOwnerKind,
        owner_id: str,
        view_sha256: str,
        start_ordinal: int = 0,
        limit: int = 100,
    ) -> AcceptedViewPage:
        return AcceptedViewPage.model_validate(
            self._json(
                "get_observation_view",
                "GET",
                f"/v1/observation-tasks/{quote(task_work_id, safe='')}/"
                f"{quote(task_id, safe='')}/views/{quote(view_id, safe='')}",
                params=_params(
                    owner_kind=_one_of(owner_kind, _PLANNING_OWNER_KINDS, "owner_kind"),
                    owner_id=owner_id,
                    view_sha256=view_sha256,
                    start_ordinal=str(start_ordinal),
                    limit=limit,
                ),
            )
        )

    def get_artifact_selection(
        self,
        selection_sha256: str,
        *,
        continuation: str | None = None,
    ) -> ArtifactSelectionPage:
        return ArtifactSelectionPage.model_validate(
            self._json(
                "get_artifact_selection",
                "GET",
                f"/v1/artifact-selections/{quote(selection_sha256, safe='')}",
                params=_params(continuation=continuation),
            )
        )

    def step_work(self, work_id: str) -> WorkView:
        return WorkView.model_validate(
            self._json("step_work", "POST", f"/v1/work/{quote(work_id, safe='')}/step")
        )

    def retry_work(self, work_id: str) -> WorkView:
        return WorkView.model_validate(
            self._json("retry_work", "POST", f"/v1/work/{quote(work_id, safe='')}/retry")
        )

    def cancel_work(self, work_id: str) -> WorkView:
        return WorkView.model_validate(
            self._json("cancel_work", "POST", f"/v1/work/{quote(work_id, safe='')}/cancel")
        )

    def preview_workflow(
        self,
        recipe_id: str,
        inputs: Sequence[CollectionRootIdentityRef],
        *,
        recipe_revision: int | None = None,
        effective_intent: Mapping[str, Any] | None = None,
        invocation_id: str | None = None,
    ) -> PlanningJobStatus:
        request = OperatorWorkflowPreviewRequest.model_validate(
            dict(
                recipe_id=recipe_id,
                invocation_id=invocation_id or secrets.token_hex(32),
                recipe_revision=None if recipe_revision is None else str(recipe_revision),
                inputs=tuple(inputs),
                effective_intent=dict(effective_intent or {}),
            )
        )
        return PlanningJobStatus.model_validate(
            self._json(
                "preview_workflow",
                "POST",
                "/v1/workflow-previews",
                json=request.model_dump(mode="json", exclude_none=True),
            )
        )

    def get_workflow_preview(self, job_id: str) -> PlanningJobStatus:
        return PlanningJobStatus.model_validate(
            self._json(
                "get_workflow_preview",
                "GET",
                f"/v1/workflow-previews/{quote(job_id, safe='')}",
            )
        )

    def cancel_workflow_preview(self, job_id: str) -> PlanningJobStatus:
        return PlanningJobStatus.model_validate(
            self._json(
                "cancel_workflow_preview",
                "POST",
                f"/v1/workflow-previews/{quote(job_id, safe='')}/cancel",
            )
        )

    def list_evaluations(
        self,
        *,
        page_size: int = 25,
        page_token: str | None = None,
        phase: EvaluationPhase | None = None,
        query: str | None = None,
        sort: EvaluationSort = "updated_at",
        order: SortOrder = "desc",
    ) -> EvaluationPage:
        return EvaluationPage.model_validate(
            self._json(
                "list_evaluations",
                "GET",
                "/v1/evaluations",
                params=_params(
                    **{
                        "page_size": page_size,
                        "page_token": page_token,
                        "phase": (
                            _one_of(phase, _EVALUATION_PHASES, "evaluation phase")
                            if phase
                            else None
                        ),
                        "q": query,
                        "sort": _one_of(
                            sort,
                            _EVALUATION_SORTS,
                            "evaluation sort",
                        ),
                        "order": _one_of(
                            order,
                            _SORT_ORDERS,
                            "sort order",
                        ),
                    }
                ),
            )
        )

    def create_evaluation(self, definition: Mapping[str, Any]) -> EvaluationView:
        request = EvaluationDefinition.model_validate(definition)
        return EvaluationView.model_validate(
            self._json(
                "create_evaluation",
                "POST",
                "/v1/evaluations",
                json=request.model_dump(mode="json", by_alias=True, exclude_none=True),
            )
        )

    def get_evaluation(self, evaluation_id: str) -> EvaluationView:
        return EvaluationView.model_validate(
            self._json("get_evaluation", "GET", f"/v1/evaluations/{quote(evaluation_id, safe='')}")
        )

    def step_evaluation(self, evaluation_id: str) -> EvaluationView:
        return EvaluationView.model_validate(
            self._json(
                "step_evaluation", "POST", f"/v1/evaluations/{quote(evaluation_id, safe='')}/step"
            )
        )

    def cancel_evaluation(self, evaluation_id: str) -> EvaluationView:
        return EvaluationView.model_validate(
            self._json(
                "cancel_evaluation",
                "POST",
                f"/v1/evaluations/{quote(evaluation_id, safe='')}/cancel",
            )
        )

    def retry_evaluation_variant(self, evaluation_id: str, variant_id: str) -> EvaluationView:
        return EvaluationView.model_validate(
            self._json(
                "retry_evaluation_variant",
                "POST",
                f"/v1/evaluations/{quote(evaluation_id, safe='')}/variants/"
                f"{quote(variant_id, safe='')}/retry",
            )
        )

    def review_evaluation_variant(
        self,
        evaluation_id: str,
        variant_id: str,
        *,
        rating: int | None = None,
        note: str | None = None,
    ) -> EvaluationView:
        request = EvaluationReviewRequest(rating=rating, note=note)
        return EvaluationView.model_validate(
            self._json(
                "review_evaluation_variant",
                "PUT",
                f"/v1/evaluations/{quote(evaluation_id, safe='')}/variants/"
                f"{quote(variant_id, safe='')}/review",
                json=request.model_dump(mode="json", exclude_none=True),
            )
        )

    def scheduler_status(self) -> SchedulerStatus:
        return SchedulerStatus.model_validate(
            self._json("scheduler_status", "GET", "/v1/admin/scheduler")
        )

    def run_scheduler(
        self,
        *,
        role: SchedulerRole = "combined",
        work_limit: int = 25,
    ) -> SchedulerRun:
        request = SchedulerRunRequest(
            role=role,
            work_limit=work_limit,
        )
        return SchedulerRun.model_validate(
            self._json(
                "run_scheduler",
                "POST",
                "/v1/admin/scheduler/run",
                json=request.model_dump(mode="json"),
            )
        )

    def _json(
        self,
        operation_id: str,
        method: str,
        path: str,
        *,
        authenticated: bool = True,
        **kwargs: Any,
    ) -> dict[str, Any]:
        if authenticated and not self.token:
            raise Stove0ApiError("STOVE0_TOKEN is required")
        headers = {"Accept": "application/json"}
        if authenticated and self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        client = self._persistent_client()
        try:
            response = client.request(method, path, headers=headers, **kwargs)
        except httpx.HTTPError as exc:
            raise Stove0ApiError(f"stove0 request failed: {exc}") from exc
        if response.status_code >= 400:
            self._raise_for_error(operation_id, response)
        value = response.json()
        if not isinstance(value, dict):
            raise Stove0ApiError("stove0 returned a non-object JSON response")
        return value

    @staticmethod
    def _raise_for_error(operation_id: str, response: httpx.Response) -> None:
        try:
            payload = response.json()
        except ValueError:
            payload = None
        try:
            code, message, details = parse_operation_error_payload(
                STOVE0_HTTP_ERROR_AUTHORITY,
                operation_id,
                status=response.status_code,
                payload=payload,
            )
        except (TypeError, ValueError) as exc:
            raise Stove0ApiError(
                "stove0 returned an undeclared or invalid operation error response",
                code="invalid_response",
                observed_status=response.status_code,
            ) from exc
        raise Stove0ApiError(
            message,
            code=code,
            observed_status=response.status_code,
            details=details,
        )

    def _persistent_client(self) -> httpx.Client:
        if self._client is None:
            self._client = httpx.Client(
                base_url=self.base_url,
                timeout=self.timeout_seconds,
                http2=self.http2,
            )
        return self._client


def _boolean_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    value = raw.strip().casefold()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be true or false")


def _params(**values: object) -> dict[str, object]:
    return {name: value for name, value in values.items() if value is not None}


def _positive_float_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return _positive_float(float(raw), name)
    except ValueError as exc:
        raise ValueError(f"{name} must be a positive number of seconds") from exc


def _positive_float(value: float, name: str) -> float:
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be a positive number of seconds")
    return value


__all__ = ["Stove0ApiClient", "Stove0ApiError"]
