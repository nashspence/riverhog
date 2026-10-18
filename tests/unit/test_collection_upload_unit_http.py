from __future__ import annotations

import hashlib

import pytest
from fastapi.testclient import TestClient
from riverhog_core.app_permissions import (
    ALL_RESOURCES,
    COLLECTIONS_CREATE,
    ApplicationAccess,
    Principal,
)
from riverhog_protocol.errors import Forbidden

from tests.support.upload_api import UploadServiceApi


class _IssuedUnit:
    def __init__(self, expected_bytes: int, *, denied: bool = False) -> None:
        self.expected_bytes = expected_bytes
        self.denied = denied
        self.access_checks = 0
        self.uploads = []

    def require_access(self, collection_id, principal):
        self.access_checks += 1
        if self.denied:
            raise Forbidden("upload session belongs to another principal")

    def get_unit(self, collection_id, volume_id, unit):
        return {
            "unit": str(unit),
            "payload_bytes": str(self.expected_bytes),
            "plaintext_bytes": str(self.expected_bytes),
            "sources": [
                {
                    "artifact_id": "a" * 64,
                    "offset": "0",
                    "bytes": str(self.expected_bytes),
                    "artifact_sha256": hashlib.sha256(b"x" * self.expected_bytes).hexdigest(),
                }
            ],
            "state": "pending",
        }

    def upload_unit(self, collection_id, volume_id, unit, *, plan_sha256, content):
        self.uploads.append((collection_id, volume_id, unit, plan_sha256, content))
        return {**self.get_unit(collection_id, volume_id, unit), "state": "committed"}


def _request(service, *, content, declared, plan=True):
    principal = Principal(
        id="fixture-target",
        key_id="fixture-key",
        access=frozenset({ApplicationAccess(COLLECTIONS_CREATE, ALL_RESOURCES)}),
    )
    api = UploadServiceApi(service, principal)
    client = TestClient(api._app)
    try:
        request = client.build_request(
            "PUT",
            f"/v1/collection-upload-sessions/1/volumes/segment-{'0' * 64}/units/0",
            content=content,
            headers={
                "Content-Type": "application/octet-stream",
                "Authorization": "Bearer fixture-upload-token",
                **({"If-Match": '"' + "b" * 64 + '"'} if plan else {}),
            },
        )
        if declared is None:
            request.headers.pop("Content-Length", None)
        else:
            request.headers["Content-Length"] = declared
        return client.send(request)
    finally:
        client.close()
        api.close()


@pytest.mark.parametrize("size", [0, 1])
def test_binary_http_accepts_exact_issued_unit_including_empty(size):
    service = _IssuedUnit(size)
    response = _request(service, content=b"x" * size, declared=str(size))
    assert response.status_code == 200 and response.json()["state"] == "committed"
    assert service.access_checks == 1
    assert service.uploads == [(1, "segment-" + "0" * 64, 0, "b" * 64, b"x" * size)]


@pytest.mark.parametrize(
    "size, content, declared, status",
    [
        (0, b"x", "0", 400),
        (1, b"", "1", 400),
        (1, b"xx", "1", 400),
        (2, b"x", "1", 400),
        (1, b"x", "invalid", 411),
        (1, b"x", None, 411),
    ],
)
def test_binary_http_rejects_wrong_declared_or_actual_length_before_upload(
    size, content, declared, status
):
    service = _IssuedUnit(size)
    response = _request(service, content=content, declared=declared)
    assert response.status_code == status
    assert service.access_checks == 1 and service.uploads == []


def test_empty_binary_http_still_requires_session_access_and_plan_header():
    service = _IssuedUnit(0, denied=True)
    assert _request(service, content=b"", declared="0").status_code == 403
    assert service.access_checks == 1 and service.uploads == []
    service = _IssuedUnit(0)
    assert _request(service, content=b"", declared="0", plan=False).status_code == 400
    assert service.uploads == []
