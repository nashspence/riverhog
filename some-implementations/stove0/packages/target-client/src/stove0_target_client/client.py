"""Black-box HTTP client for independently deployed stove0 targets."""

from __future__ import annotations

import hashlib
import threading
from collections.abc import Callable, Iterator, Mapping
from typing import Any, Literal, TypeVar
from urllib.parse import quote

import httpx
from http_api_contracts import (
    http_operation_for_request,
    parse_declared_error_payload,
    safe_http_base_url,
)
from http_api_contracts.control import check_control_budget, control_timeout, finite_control_seconds
from http_api_contracts.metadata_contact import metadata_contact
from http_api_contracts.metadata_exchange import NativeMetadataTransport
from pydantic import BaseModel, TypeAdapter, ValidationError
from stove0_target_protocol import (
    TARGET_CALLBACK_HTTP_OPERATIONS,
    TARGET_HTTP_OPERATIONS,
    AcceptedTargetJob,
    InputArtifact,
    InputDispositionDeclaration,
    OperationContract,
    OutputArtifact,
    OutputArtifactRoleCount,
    OutputArtifactSetIdentity,
    OutputSourceEdge,
    Sha256,
    TargetCallbackAccess,
    TargetCallbackAcknowledgement,
    TargetDescriptor,
    TargetInputPage,
    TargetJobRequest,
    TargetJobStatus,
    TargetOutputPage,
    TargetPreflightRequest,
    TargetPreflightResponse,
    TargetProductionAuthority,
    TargetProductionSealResponse,
    update_output_artifact_commitment,
    validate_preflight_response_against_request,
    validate_status_against_request,
)

ModelT = TypeVar("ModelT", bound=BaseModel)
_JOB_ID = TypeAdapter(Sha256)


class TargetProtocolError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        failure_kind: Literal["remote_rejection", "transport", "invalid_response"],
        code: str | None = None,
        observed_status: int | None = None,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        if failure_kind == "remote_rejection":
            if code is None or observed_status is None or not 400 <= observed_status <= 599:
                raise ValueError("remote rejection requires a declared code and error status")
        elif code is not None or observed_status is not None:
            raise ValueError("client-local failure must not claim a remote code or status")
        self.message = message
        self.failure_kind = failure_kind
        self.code = code
        self.observed_status = observed_status
        self.details = dict(details or {})


class TargetClient:
    def __init__(
        self,
        base_url: str,
        *,
        token: str | None = None,
        timeout: float = 5.0,
        allow_insecure_http: bool = False,
        staged_metadata: bool = False,
    ) -> None:
        self.base_url = safe_http_base_url(
            base_url,
            setting="target base URL",
            allow_insecure_http=allow_insecure_http,
        )
        self.timeout = finite_control_seconds(timeout)
        self._metadata = (
            NativeMetadataTransport(
                wire=lambda method, path, model, payload=None: self._wire(
                    method, path, model, payload=payload
                ),
                operations=TARGET_HTTP_OPERATIONS,
                protocol_error=TargetProtocolError,
                timeout=self.timeout,
            )
            if staged_metadata
            else None
        )
        self.token = token.strip() if token and token.strip() else None

    def descriptor(self) -> TargetDescriptor:
        return self._request("GET", "/v1/target", TargetDescriptor)

    def preflight(self, request: TargetPreflightRequest) -> TargetPreflightResponse:
        response = self._request("POST", "/v1/preflight", TargetPreflightResponse, request)
        self._validate(lambda: validate_preflight_response_against_request(response, request))
        return response

    def put_job(
        self,
        request: TargetJobRequest,
        *,
        operation: OperationContract,
    ) -> TargetJobStatus:
        status = self._request(
            "PUT",
            f"/v1/jobs/{request.declaration.job_id}",
            TargetJobStatus,
            request,
        )
        self._validate(lambda: validate_status_against_request(status, request, operation))
        return status

    def status(
        self,
        request: TargetJobRequest | AcceptedTargetJob,
        *,
        operation: OperationContract,
    ) -> TargetJobStatus:
        if not isinstance(request, (TargetJobRequest, AcceptedTargetJob)):
            raise ValueError("target status requires exact accepted-request context")
        status = self._request(
            "GET",
            f"/v1/jobs/{_job_id(request.declaration.job_id)}",
            TargetJobStatus,
        )
        self._validate(lambda: validate_status_against_request(status, request, operation))
        return status

    def cancel(
        self,
        request: TargetJobRequest | AcceptedTargetJob,
        *,
        operation: OperationContract,
    ) -> TargetJobStatus:
        if not isinstance(request, (TargetJobRequest, AcceptedTargetJob)):
            raise ValueError("target cancellation requires exact accepted-request context")
        status = self._request(
            "POST",
            f"/v1/jobs/{_job_id(request.declaration.job_id)}/cancel",
            TargetJobStatus,
            request.accepted() if isinstance(request, TargetJobRequest) else request,
        )
        self._validate(lambda: validate_status_against_request(status, request, operation))
        return status

    @staticmethod
    def _validate(validation: Callable[[], None]) -> None:
        try:
            validation()
        except (TypeError, ValueError) as exc:
            raise TargetProtocolError(
                "target returned a response inconsistent with the request",
                failure_kind="invalid_response",
            ) from exc

    def close(self) -> None:
        if self._metadata is not None:
            self._metadata.close()

    def _request(
        self, method: str, path: str, model: type[ModelT], payload: BaseModel | None = None
    ) -> ModelT:
        if self._metadata is not None:
            return self._metadata.request(method, path, model, payload)
        return self._wire(method, path, model, payload=payload)

    @metadata_contact
    def _wire(
        self,
        method: str,
        path: str,
        model: type[ModelT],
        payload: BaseModel | None = None,
    ) -> ModelT:
        operation = http_operation_for_request(TARGET_HTTP_OPERATIONS, method, path)
        if operation is None:
            raise ValueError("target client request is absent from its HTTP contract")
        request_kwargs: dict[str, Any] = {}
        if payload is not None:
            request_kwargs["json"] = payload.model_dump(
                mode="json",
                by_alias=True,
                exclude_none=True,
            )
        try:
            with httpx.Client(timeout=control_timeout(self.timeout)) as client:
                response = client.request(
                    method,
                    f"{self.base_url}{path}",
                    headers=({"Authorization": f"Bearer {self.token}"} if self.token else None),
                    **request_kwargs,
                )
        except httpx.HTTPError as exc:
            raise TargetProtocolError(
                f"target request failed: {exc}",
                failure_kind="transport",
            ) from exc
        check_control_budget()
        if response.status_code >= 400:
            try:
                code, message, details = parse_declared_error_payload(
                    operation,
                    status=response.status_code,
                    payload=response.json(),
                )
            except (TypeError, ValueError) as exc:
                raise TargetProtocolError(
                    "target returned an undeclared or invalid error response",
                    failure_kind="invalid_response",
                ) from exc
            raise TargetProtocolError(
                message,
                failure_kind="remote_rejection",
                code=code,
                observed_status=response.status_code,
                details=details,
            )
        try:
            return model.model_validate(response.json())
        except (TypeError, ValueError) as exc:
            raise TargetProtocolError(
                "target returned an invalid protocol response",
                failure_kind="invalid_response",
            ) from exc


class TargetCallbackClient:
    """Execution-scoped bounded reads from the Stove0 callback surface."""

    def __init__(self, access: TargetCallbackAccess, *, timeout: float | None = 300.0) -> None:
        self.base_url = safe_http_base_url(
            access.stove0_base_url,
            setting="Stove0 target callback base URL",
            allow_insecure_http=access.allow_insecure_http,
        )
        self.token = access.token
        self._allow_insecure_http = access.allow_insecure_http
        self._lock = threading.RLock()
        self._client = httpx.Client(http2=True, timeout=timeout)

    def refresh_access(self, access: TargetCallbackAccess) -> None:
        """Renew transient callback credentials without changing the endpoint."""
        base_url = safe_http_base_url(
            access.stove0_base_url,
            setting="Stove0 target callback base URL",
            allow_insecure_http=access.allow_insecure_http,
        )
        if base_url != self.base_url or access.allow_insecure_http != self._allow_insecure_http:
            raise ValueError("active target callback endpoint changed")
        with self._lock:
            self.token = access.token

    def close(self) -> None:
        self._client.close()

    def get_target_execution_inputs(
        self,
        job_id: str,
        *,
        continuation: str | None = None,
    ) -> TargetInputPage:
        job = _JOB_ID.validate_python(job_id)
        path = f"/v1/target-executions/{quote(job, safe='')}/inputs"
        return self._request(
            "GET",
            path,
            TargetInputPage,
            params={} if continuation is None else {"continuation": continuation},
        )

    def get_target_execution_outputs(
        self,
        job_id: str,
        *,
        production_sha256: str,
        after_id: str | None = None,
    ) -> TargetOutputPage:
        job = _JOB_ID.validate_python(job_id)
        identity = _JOB_ID.validate_python(production_sha256)
        path = f"/v1/target-executions/{quote(job, safe='')}/outputs"
        params = {"production_sha256": identity}
        if after_id is not None:
            params["after_id"] = after_id
        return self._request("GET", path, TargetOutputPage, params=params)

    def iter_outputs(self, production: TargetProductionAuthority) -> Iterator[OutputArtifact]:
        """Reject missing, replaced, reordered or duplicated sealed declarations."""
        after_id = None
        digest = hashlib.sha256()
        count = 0
        total_bytes = 0
        roles: dict[str, int] = {}
        while True:
            page = self.get_target_execution_outputs(
                production.job_id,
                production_sha256=production.production_sha256,
                after_id=after_id,
            )
            if page.production_sha256 != production.production_sha256 or page.after_id != after_id:
                raise TargetProtocolError(
                    "Stove0 target-output authority or continuation changed",
                    failure_kind="invalid_response",
                )
            for artifact in page.artifacts:
                update_output_artifact_commitment(digest, ordinal=count, artifact=artifact)
                count += 1
                total_bytes += artifact.bytes
                roles[artifact.role] = roles.get(artifact.role, 0) + 1
                if count > production.outputs.artifact_count:
                    raise TargetProtocolError(
                        "Stove0 target-output traversal exceeds sealed coverage",
                        failure_kind="invalid_response",
                    )
                yield artifact
            if page.complete:
                actual = OutputArtifactSetIdentity.model_validate(
                    {
                        "artifact_count": count,
                        "total_bytes": str(total_bytes),
                        "roles": tuple(
                            OutputArtifactRoleCount(role=role, count=count)
                            for role, count in sorted(roles.items())
                        ),
                        "sha256": digest.hexdigest(),
                    }
                )
                if actual != production.outputs:
                    raise TargetProtocolError(
                        "Stove0 target-output traversal differs from sealed production",
                        failure_kind="invalid_response",
                    )
                return
            after_id = page.next_after_id

    def declare_target_execution_output(
        self, job_id: str, output: OutputArtifact
    ) -> TargetCallbackAcknowledgement:
        job = _JOB_ID.validate_python(job_id)
        path = f"/v1/target-executions/{quote(job, safe='')}/outputs/{quote(output.id, safe='')}"
        return self._request("PUT", path, TargetCallbackAcknowledgement, payload=output)

    def declare_target_execution_disposition(
        self,
        job_id: str,
        disposition: InputDispositionDeclaration,
    ) -> TargetCallbackAcknowledgement:
        job = _JOB_ID.validate_python(job_id)
        path = (
            f"/v1/target-executions/{quote(job, safe='')}/dispositions/"
            f"{quote(disposition.input_id, safe='')}"
        )
        return self._request("PUT", path, TargetCallbackAcknowledgement, payload=disposition)

    def declare_target_execution_source_edge(
        self, job_id: str, edge: OutputSourceEdge
    ) -> TargetCallbackAcknowledgement:
        job = _JOB_ID.validate_python(job_id)
        path = (
            f"/v1/target-executions/{quote(job, safe='')}/source-edges/"
            f"{quote(edge.output_id, safe='')}/{quote(edge.input_id, safe='')}"
        )
        return self._request("PUT", path, TargetCallbackAcknowledgement, payload=edge)

    def seal_target_execution_production(self, job_id: str) -> TargetProductionSealResponse:
        job = _JOB_ID.validate_python(job_id)
        path = f"/v1/target-executions/{quote(job, safe='')}/production/seal"
        return self._request("POST", path, TargetProductionSealResponse)

    def iter_inputs(self, job_id: str) -> Iterator[InputArtifact]:
        continuation: str | None = None
        authority = None
        while True:
            page = self.get_target_execution_inputs(job_id, continuation=continuation)
            if authority is None:
                authority = page.authority
            elif page.authority != authority:
                raise TargetProtocolError(
                    "Stove0 target-input authority changed during traversal",
                    failure_kind="invalid_response",
                )
            yield from page.artifacts
            if page.complete:
                return
            continuation = page.next_continuation

    def _request(
        self,
        method: str,
        path: str,
        model: type[ModelT],
        *,
        payload: BaseModel | None = None,
        params: Mapping[str, str] | None = None,
    ) -> ModelT:
        operation = http_operation_for_request(TARGET_CALLBACK_HTTP_OPERATIONS, method, path)
        if operation is None:
            raise ValueError("target callback request is absent from its HTTP contract")
        with self._lock:
            token = self.token
        response = self._client.request(
            method,
            f"{self.base_url}{path}",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
            params=params,
            json=(
                payload.model_dump(mode="json", by_alias=True, exclude_none=True)
                if payload is not None
                else None
            ),
        )
        if response.status_code >= 400:
            try:
                code, message, details = parse_declared_error_payload(
                    operation,
                    status=response.status_code,
                    payload=response.json(),
                )
            except (TypeError, ValueError) as exc:
                raise TargetProtocolError(
                    "Stove0 returned an undeclared callback error response",
                    failure_kind="invalid_response",
                ) from exc
            raise TargetProtocolError(
                message,
                failure_kind="remote_rejection",
                code=code,
                observed_status=response.status_code,
                details=details,
            )
        try:
            return model.model_validate(response.json())
        except (ValueError, ValidationError) as exc:
            raise TargetProtocolError(
                "Stove0 returned an invalid target-callback response",
                failure_kind="invalid_response",
            ) from exc


def _job_id(value: str) -> str:
    try:
        return _JOB_ID.validate_python(value, strict=True)
    except ValidationError as exc:
        raise ValueError("target job ID must be a lowercase SHA-256") from exc


__all__ = ["TargetCallbackClient", "TargetClient", "TargetProtocolError"]
