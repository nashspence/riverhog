from __future__ import annotations

import httpx
import pytest
from riverhog_client import ApiClient
from riverhog_protocol import ArtifactDiscoveryRequest


def test_client_sends_closed_discovery_request_and_checks_query_identity() -> None:
    request = ArtifactDiscoveryRequest.model_validate(
        {"provenance_all": [{"values": [{"operator": "equals", "value": "camera"}]}]}
    )
    observed: list[httpx.Request] = []

    def respond(current: httpx.Request) -> httpx.Response:
        observed.append(current)
        return httpx.Response(200, json={"query_identity": request.identity(), "artifacts": []})

    api = ApiClient(base_url="http://localhost:8000", token="test", allow_insecure_http=True)
    api._request_client = httpx.Client(  # type: ignore[attr-defined]
        base_url=api.base_url, transport=httpx.MockTransport(respond)
    )
    try:
        assert api.discover_artifacts(request, page_token="next")["artifacts"] == []
        assert observed[0].method == "POST"
        assert observed[0].url.path == "/v1/artifacts/discover"
        assert observed[0].url.params["page_token"] == "next"
        assert b"riverhog-artifact-discovery/v1" in observed[0].content
    finally:
        api.close()


def test_client_rejects_a_page_from_another_query() -> None:
    api = ApiClient(base_url="http://localhost:8000", token="test", allow_insecure_http=True)
    api._request_client = httpx.Client(  # type: ignore[attr-defined]
        base_url=api.base_url,
        transport=httpx.MockTransport(
            lambda _: httpx.Response(200, json={"query_identity": "0" * 64, "artifacts": []})
        ),
    )
    try:
        with pytest.raises(RuntimeError, match="another query"):
            api.discover_artifacts({})
    finally:
        api.close()
