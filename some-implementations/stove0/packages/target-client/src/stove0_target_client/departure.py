"""HTTP client for artifact-free departure effect targets."""

from __future__ import annotations

from typing import Any, TypeVar

import httpx
from http_api_contracts import (
    http_operation_for_request,
    parse_declared_error_payload,
    safe_http_base_url,
)
from http_api_contracts.control import check_control_budget, control_timeout, finite_control_seconds
from http_api_contracts.metadata_contact import metadata_contact
from http_api_contracts.metadata_exchange import NativeMetadataTransport
from pydantic import BaseModel
from stove0_target_protocol import (
    DEPARTURE_EFFECT_HTTP_OPERATIONS,
    DepartureEffectIntent,
    DepartureEffectStatus,
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
        timeout: float = 5.0,
        allow_insecure_http: bool = False,
        staged_metadata: bool = False,
    ) -> None:
        self.base_url = safe_http_base_url(
            base_url,
            setting="departure effect target base URL",
            allow_insecure_http=allow_insecure_http,
        )
        self.token = token.strip() if token and token.strip() else None
        self.timeout = finite_control_seconds(timeout)
        self._metadata = (
            NativeMetadataTransport(
                wire=lambda method, path, model, payload=None: self._wire(
                    method, path, model, payload=payload
                ),
                operations=DEPARTURE_EFFECT_HTTP_OPERATIONS,
                protocol_error=TargetProtocolError,
                timeout=self.timeout,
            )
            if staged_metadata
            else None
        )

    def put_effect(self, intent: DepartureEffectIntent) -> DepartureEffectStatus:
        descriptor = self.descriptor()
        if descriptor.target_identity != intent.target_identity:
            raise TargetProtocolError(
                "departure target descriptor differs from the sealed intent",
                failure_kind="invalid_response",
            )
        path = f"/v1/departure-effects/{intent.departure_id}"
        receipt = self._request("PUT", path, DepartureEffectStatus, payload=intent)
        if (
            receipt.departure_id != intent.departure_id
            or receipt.target_identity != intent.target_identity
        ):
            raise TargetProtocolError(
                "departure target receipt differs from the request",
                failure_kind="invalid_response",
            )
        return receipt

    def status(self, departure_id: str) -> DepartureEffectStatus:
        return self._request("GET", f"/v1/departure-effects/{departure_id}", DepartureEffectStatus)

    def cancel(self, intent: DepartureEffectIntent) -> DepartureEffectStatus:
        status = self._request(
            "POST",
            f"/v1/departure-effects/{intent.departure_id}/cancel",
            DepartureEffectStatus,
            payload=intent,
        )
        if (
            status.departure_id != intent.departure_id
            or status.target_identity != intent.target_identity
        ):
            raise TargetProtocolError(
                "departure cancellation status differs from intent", failure_kind="invalid_response"
            )
        return status

    def descriptor(self) -> DepartureEffectTargetDescriptor:
        return self._request("GET", "/v1/departure-target", DepartureEffectTargetDescriptor)

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
            with httpx.Client(timeout=control_timeout(self.timeout)) as client:
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
        check_control_budget()
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
