"""HTTP client for artifact-free departure effect targets."""

from __future__ import annotations

from typing import Any, TypeVar

import httpx
from http_api_contracts import (
    http_operation_for_request,
    parse_declared_error_payload,
    safe_http_base_url,
)
from pydantic import BaseModel
from stove0_target_protocol import (
    DEPARTURE_EFFECT_HTTP_OPERATIONS,
    DepartureEffectIntent,
    DepartureEffectReceipt,
    DepartureEffectTargetDescriptor,
)

from stove0_target_client.client import TargetProtocolError

ModelT = TypeVar("ModelT", bound=BaseModel)


class DepartureEffectClient:
    def __init__(
        self,
        base_url: str,
        *,
        token: str | None = None,
        timeout: float = 300.0,
        allow_insecure_http: bool = False,
    ) -> None:
        self.base_url = safe_http_base_url(
            base_url,
            setting="departure effect target base URL",
            allow_insecure_http=allow_insecure_http,
        )
        self.token = token.strip() if token and token.strip() else None
        self.timeout = timeout

    def put_effect(self, intent: DepartureEffectIntent) -> DepartureEffectReceipt:
        descriptor = self.descriptor()
        if descriptor.target_identity != intent.target_identity:
            raise TargetProtocolError(
                "departure target descriptor differs from the sealed intent",
                failure_kind="invalid_response",
            )
        path = f"/v1/departure-effects/{intent.departure_id}"
        receipt = self._request("PUT", path, DepartureEffectReceipt, payload=intent)
        if (
            receipt.departure_id != intent.departure_id
            or receipt.target_identity != intent.target_identity
        ):
            raise TargetProtocolError(
                "departure target receipt differs from the request",
                failure_kind="invalid_response",
            )
        return receipt

    def descriptor(self) -> DepartureEffectTargetDescriptor:
        return self._request("GET", "/v1/departure-target", DepartureEffectTargetDescriptor)

    def _request(
        self,
        method: str,
        path: str,
        model: type[ModelT],
        *,
        payload: BaseModel | None = None,
    ) -> ModelT:
        operation = http_operation_for_request(DEPARTURE_EFFECT_HTTP_OPERATIONS, method, path)
        if operation is None:
            raise RuntimeError("departure effect request is absent from its HTTP contract")
        kwargs: dict[str, Any] = (
            {} if payload is None else {"json": payload.model_dump(mode="json")}
        )
        try:
            with httpx.Client(timeout=self.timeout) as client:
                response = client.request(
                    method,
                    f"{self.base_url}{path}",
                    headers=({"Authorization": f"Bearer {self.token}"} if self.token else None),
                    **kwargs,
                )
        except httpx.HTTPError as exc:
            raise TargetProtocolError(
                f"departure effect request failed: {exc}", failure_kind="transport"
            ) from exc
        if response.status_code >= 400:
            try:
                code, message, details = parse_declared_error_payload(
                    operation, status=response.status_code, payload=response.json()
                )
            except (TypeError, ValueError) as exc:
                raise TargetProtocolError(
                    "departure target returned an undeclared error",
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
                "departure target returned an invalid response",
                failure_kind="invalid_response",
            ) from exc


__all__ = ["DepartureEffectClient"]
