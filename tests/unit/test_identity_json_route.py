from __future__ import annotations

from fastapi.routing import APIRoute
from fastapi.testclient import TestClient
from riverhog_api.app import create_app
from riverhog_api.routing import ExactJsonRoute


def test_public_json_routes_admit_exact_values_before_model_validation() -> None:
    app = create_app()
    public_routes = [
        route
        for included in app.routes
        if getattr(getattr(included, "include_context", None), "prefix", None) == "/v1"
        for route in included.original_router.routes
        if isinstance(route, APIRoute)
    ]
    assert public_routes
    assert all(isinstance(route, ExactJsonRoute) for route in public_routes)

    client = TestClient(app)
    for body, reason, headers in (
        (b'{"work_id":"one","work_id":"two"}', "duplicate", {"content-type": "application/json"}),
        (b'{"work_document":{"value":0.1}}', "numeric", {"content-type": "application/json"}),
        (
            b'{"work_document":{"value":9007199254740993}}',
            "numeric",
            {"content-type": "application/json"},
        ),
        (b'{"work_id":"one","work_id":"two"}', "duplicate", {}),
    ):
        response = client.post(
            "/v1/collection-processing-claims",
            content=body,
            headers=headers,
        )
        assert response.status_code == 400
        assert reason in response.json()["error"]["message"]
