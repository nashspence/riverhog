from __future__ import annotations

import httpx

from arc_cli.client import ApiClient
from contracts.operator import copy as operator_copy


class ApiUnreachable(RuntimeError):
    def __init__(self) -> None:
        super().__init__(operator_copy.api_unreachable())
        self.copy_text = operator_copy.api_unreachable()


def check_api_reachable(*, base_url: str | None = None) -> None:
    base_url = base_url or ApiClient().base_url
    try:
        with httpx.Client(base_url=base_url, timeout=2.0) as client:
            response = client.get("/healthz")
    except httpx.HTTPError as exc:
        raise ApiUnreachable() from exc
    if response.is_success:
        return
    if response.status_code >= 500:
        raise ApiUnreachable()
