"""Framework-neutral HTTP binding for independently maintained observers."""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path

from http_api_contracts.metadata_binding import MetadataHttpBinding
from pydantic import BaseModel, ValidationError
from riverhog_canonical_json import parse_identity_json
from stove0_observer_protocol import (
    OBSERVER_HTTP_OPERATIONS,
    AcceptedObservationJob,
    ContentObservationInvocation,
)

from stove0_observer_support.persistent import ObserverServiceError, PersistentObserverService

_JSON_CONTENT_TYPE = "application/json"
_LOG = logging.getLogger(__name__)
_DEFAULT_MAX_REQUEST_BYTES = 4 * 1024 * 1024
_OBSERVER_HTTP_ERROR_STATUS = {
    "bad_request": 400,
    "invalid_observation_request": 400,
    "method_not_allowed": 405,
    "not_found": 404,
    "observer_failed": 500,
    "request_too_large": 413,
    "unauthorized": 401,
    "job_identity_mismatch": 409,
    "job_request_mismatch": 409,
    "observer_runtime_mismatch": 409,
    "admission_unavailable": 503,
    "job_not_found": 404,
}


@dataclass(frozen=True, slots=True)
class ObserverHttpResponse:
    status: int
    headers: tuple[tuple[str, str], ...]
    body: bytes


class ObserverHttpBinding:
    """Translate bounded control calls into a component-owned observer service.

    The binding is deliberately independent of any web framework. External
    maintainers may adapt :meth:`handle` to ASGI, WSGI, aiohttp, Flask, FastAPI,
    or another server without importing stove0 core.
    """

    def __init__(
        self,
        service: PersistentObserverService,
        *,
        maximum_request_bytes: int = _DEFAULT_MAX_REQUEST_BYTES,
        metadata_root: Path | None = None,
    ) -> None:
        if maximum_request_bytes < 1:
            raise ValueError("observer HTTP request limit must be positive")
        self.service = service
        self.maximum_request_bytes = maximum_request_bytes
        self.metadata = MetadataHttpBinding(
            owner=service,
            root=metadata_root,
            operations=OBSERVER_HTTP_OPERATIONS,
            execute=lambda method, path, body: self._handle_inline(method, path, body, staged=True),
            response=ObserverHttpResponse,
            execute_model=lambda method, path, model: self._handle_inline(
                method, path, b"", staged=True, validated=model
            ),
        )

    def handle(self, method: str, path: str, body: bytes = b"") -> ObserverHttpResponse:
        if path.startswith("/v1/metadata/"):
            return self.metadata.handle(method, path, body)
        return self._handle_inline(method, path, body)

    def _handle_inline(
        self,
        method: str,
        path: str,
        body: bytes,
        *,
        staged: bool = False,
        validated: BaseModel | None = None,
    ) -> ObserverHttpResponse:
        normalized_method = method.upper()
        if normalized_method == "GET" and path == "/v1/observer":
            if body:
                return _error(400, "bad_request", "GET /v1/observer must not include a body")
            try:
                return _model_response(self.service.descriptor())
            except Exception:
                _LOG.exception("content observer descriptor failed")
                return _error(500, "observer_failed", "content observer descriptor failed")
        match = re.fullmatch(r"/v1/observations/([a-f0-9]{64})(/cancel)?", path)
        if match:
            job_id, suffix = match.groups()
            if normalized_method == "GET" and suffix is None:
                if body:
                    return _error(400, "bad_request", "observer status GET must not include a body")
                try:
                    return _model_response(self.service.get_job(job_id))
                except ObserverServiceError as exc:
                    return _error(exc.status, exc.code, exc.message)
                except Exception:
                    _LOG.exception("observer status failed")
                    return _error(500, "observer_failed", "observer status failed")
            if (normalized_method, suffix) not in {("PUT", None), ("POST", "/cancel")}:
                return _error(405, "method_not_allowed", "observer endpoint method is not allowed")
            if not staged and len(body) > self.maximum_request_bytes:
                return _error(413, "request_too_large", "observer request exceeds its size limit")
            try:
                model = AcceptedObservationJob if suffix else ContentObservationInvocation
                if validated is not None:
                    if not staged or not isinstance(validated, model):
                        raise TypeError(
                            "metadata handoff differs from the observation request type"
                        )
                    request = validated
                else:
                    request = model.model_validate(parse_identity_json(body))
            except (ValidationError, ValueError) as exc:
                return _error(400, "invalid_observation_request", str(exc))
            try:
                if request.job_id != job_id:
                    return _error(
                        409, "job_identity_mismatch", "observer path differs from its invocation"
                    )
                if isinstance(request, AcceptedObservationJob):
                    status = self.service.cancel_job(request)
                else:
                    status = self.service.put_job(request)
                return _model_response(status)
            except ObserverServiceError as exc:
                return _error(exc.status, exc.code, exc.message)
            except Exception:
                _LOG.exception("content observer control failed")
                return _error(500, "observer_failed", "content observer control failed")
        if path == "/v1/observer":
            return _error(405, "method_not_allowed", "observer endpoint method is not allowed")
        return _error(404, "not_found", "observer endpoint not found")


def _model_response(model: BaseModel) -> ObserverHttpResponse:
    dump = getattr(model, "model_dump_json", None)
    if not callable(dump):
        raise TypeError("observer HTTP response is not a protocol model")
    return ObserverHttpResponse(
        status=200,
        headers=(("Content-Type", _JSON_CONTENT_TYPE),),
        body=str(dump(by_alias=True, exclude_none=True)).encode("utf-8"),
    )


def _error(status: int, code: str, message: str) -> ObserverHttpResponse:
    if _OBSERVER_HTTP_ERROR_STATUS.get(code) != status:
        raise ValueError("observer HTTP binding emitted an undeclared error code/status")
    body = json.dumps(
        {"error": {"code": code, "message": message}},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return ObserverHttpResponse(
        status=status,
        headers=(("Content-Type", _JSON_CONTENT_TYPE),),
        body=body,
    )


__all__ = ["OBSERVER_HTTP_OPERATIONS", "ObserverHttpBinding", "ObserverHttpResponse"]
