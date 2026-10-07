"""Stove0's bounded metadata-only use of the public Riverhog client."""

from __future__ import annotations

from typing import Any

import httpx
from http_api_contracts.control import check_control_budget, control_timeout
from http_api_contracts.metadata_contact import metadata_contact
from riverhog_client import ApiClient


class ControlApiClient(ApiClient):
    @metadata_contact
    def _request(
        self, /, operation_id: str, method: str, path: str, **kwargs: Any
    ) -> httpx.Response:
        kwargs["timeout"] = control_timeout()
        response = super()._request(operation_id, method, path, **kwargs)
        check_control_budget()
        return response
