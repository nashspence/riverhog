"""Actual SDK and HTTP upload contract over a test-owned service container."""

from __future__ import annotations

from types import SimpleNamespace

import httpx
from fastapi import FastAPI
from fastapi.testclient import TestClient
from http_api_contracts.browse import BrowseTokenCodec
from riverhog_api.app import create_app
from riverhog_client import ApiClient
from riverhog_core.app_permissions import Principal
from riverhog_core.services.collection_uploads import SqlAlchemyCollectionUploadService
from riverhog_protocol import CollectionUploadWorkBatchDocument


class _SyncASGITransport(httpx.BaseTransport):
    def __init__(self, app: FastAPI) -> None:
        self.client = TestClient(app)

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        response = self.client.request(
            request.method,
            str(request.url),
            content=request.read(),
            headers=request.headers,
        )
        return httpx.Response(
            response.status_code, headers=response.headers, content=response.content
        )

    def close(self) -> None:
        self.client.close()


class UploadServiceApi(ApiClient):
    def __init__(
        self,
        service: SqlAlchemyCollectionUploadService,
        principal: Principal,
        *,
        app: FastAPI | None = None,
        work_count: list[int] | None = None,
    ) -> None:
        super().__init__(base_url="https://testserver", token="fixture-upload-token")
        self.service = service
        self.principal = principal
        self._work_count = work_count if work_count is not None else [0]

        class Keys:
            def authenticate(self, token: str) -> Principal | None:
                return principal if token == "fixture-upload-token" else None

        self._app = (
            app
            if app is not None
            else create_app(
                container=SimpleNamespace(
                    app_keys=Keys(),
                    collection_uploads=service,
                    browse_tokens=BrowseTokenCodec(
                        b"fixture-upload-browse-key-0000000000", lifetime_seconds=3600
                    ),
                )
            )
        )

    @property
    def work_calls(self) -> int:
        return self._work_count[0]

    def _make_client(self, *, timeout_seconds: float) -> httpx.Client:
        return httpx.Client(
            transport=_SyncASGITransport(self._app),
            base_url=self.base_url,
            headers={"Authorization": f"Bearer {self.token}"},
            timeout=timeout_seconds,
        )

    def spawn(self) -> UploadServiceApi:
        return UploadServiceApi(
            self.service, self.principal, app=self._app, work_count=self._work_count
        )

    def acquire_collection_upload_session_work(
        self, collection_id: int, *, limit: int = 16
    ) -> CollectionUploadWorkBatchDocument:
        self._work_count[0] += 1
        return super().acquire_collection_upload_session_work(collection_id, limit=limit)

    def get_collection_upload_session(self, collection_id: int) -> dict[str, object]:
        self.service.process_due_finalizations(limit=1)
        return super().get_collection_upload_session(collection_id)
