from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from typing import Any

import httpx
import piggity.main
import pytest
from piggity.main import app as cli_app
from pydantic import TypeAdapter
from riverhog_api.app import create_app
from riverhog_api.schemas.collections import CollectionTagSelectorBatch
from riverhog_client.client import ApiClient
from riverhog_core.app_permissions import CATALOG_READ, ApplicationAccess, ApplicationPrincipal
from riverhog_core.domain.models import CollectionListPage
from typer.testing import CliRunner


@pytest.fixture(
    params=[
        ["x" * 65_536],
        ["é" * 32_768],
        [f"{index:03d}-" + "x" * 700 for index in range(100)],
    ]
)
def valid_large_tags(request: pytest.FixtureRequest) -> list[str]:
    tags = request.param
    assert TypeAdapter(CollectionTagSelectorBatch).validate_python(tags) == tags
    return tags


def test_client_and_cli_send_large_valid_selectors_in_a_json_body(
    monkeypatch: pytest.MonkeyPatch,
    valid_large_tags: list[str],
) -> None:
    received: list[list[str]] = []

    def handle(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.path == "/v1/collections:search"
        assert "tags=" not in str(request.url)
        tags = json.loads(request.content)["tags"]
        received.append(tags)
        return httpx.Response(200, json={"collections": []})

    api = ApiClient(base_url="https://riverhog.test")
    api._request_client = httpx.Client(
        base_url=api.base_url,
        transport=httpx.MockTransport(handle),
    )
    monkeypatch.setattr(piggity.main, "client", lambda: api)
    try:
        assert api.list_collections(tags=valid_large_tags) == {"collections": []}
        args = ["collection", "list", "--json"]
        for tag in valid_large_tags:
            args.extend(("--tag", tag))
        result = CliRunner().invoke(cli_app, args)
        assert result.exit_code == 0, result.output
        assert json.loads(result.stdout) == {"collections": []}
    finally:
        api.close()
    assert received == [valid_large_tags, valid_large_tags]


def test_http_search_accepts_the_full_selector_domain(
    valid_large_tags: list[str],
) -> None:
    principal = ApplicationPrincipal(
        app="reader",
        key_id="reader-key",
        access=frozenset({ApplicationAccess(CATALOG_READ)}),
    )

    class Keys:
        def authenticate(self, token: str) -> ApplicationPrincipal | None:
            return principal if token == "reader-token" else None

    class Collections:
        def __init__(self) -> None:
            self.calls: list[dict[str, Any]] = []

        def list(self, **kwargs: Any) -> CollectionListPage:
            self.calls.append(kwargs)
            return CollectionListPage(
                page_size=kwargs["page_size"],
                next_position=None,
                sort=kwargs["sort"],
                order=kwargs["order"],
                query=kwargs["q"],
                encryption_format=kwargs["encryption_format"],
                passphrase_id=kwargs["passphrase_id"],
                tags=tuple(kwargs["tags"]),
                collections=[],
            )

    collections = Collections()
    app = create_app(container=SimpleNamespace(app_keys=Keys(), collections=collections))

    async def exercise() -> None:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://testserver",
        ) as client:
            response = await client.post(
                "/v1/collections:search",
                headers={"Authorization": "Bearer reader-token"},
                json={"tags": valid_large_tags},
            )
            assert response.status_code == 200, response.text
            assert response.json()["tags"] == valid_large_tags

    asyncio.run(exercise())
    assert len(collections.calls) == 1
    assert collections.calls[0]["tags"] == valid_large_tags


@pytest.mark.parametrize("tag", ["x" * 65_536, "é" * 32_768])
def test_tag_membership_selector_uses_json_through_client_cli_and_http(
    monkeypatch: pytest.MonkeyPatch,
    tag: str,
) -> None:
    identity = "a" * 64
    received: list[str] = []

    def handle(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.path == "/v1/collections/42/tags:contains"
        assert "tag=" not in str(request.url)
        received.append(json.loads(request.content)["tag"])
        return httpx.Response(
            200,
            json={
                "collection_id": 42,
                "revision": 1,
                "tag_set_identity": identity,
                "tag": tag,
                "present": False,
            },
        )

    api = ApiClient(base_url="https://riverhog.test")
    api._request_client = httpx.Client(
        base_url=api.base_url,
        transport=httpx.MockTransport(handle),
    )
    monkeypatch.setattr(piggity.main, "client", lambda: api)
    try:
        assert (
            api.collection_contains_tag(42, tag=tag, revision=1, tag_set_identity=identity)["tag"]
            == tag
        )
        result = CliRunner().invoke(
            cli_app,
            [
                "collection",
                "tag",
                "contains",
                "42",
                tag,
                "--revision",
                "1",
                "--tag-set-identity",
                identity,
                "--json",
            ],
        )
        assert result.exit_code == 0, result.output
        assert json.loads(result.stdout)["tag"] == tag
    finally:
        api.close()
    assert received == [tag, tag]

    principal = ApplicationPrincipal(
        app="reader",
        key_id="reader-key",
        access=frozenset({ApplicationAccess(CATALOG_READ)}),
    )

    class Keys:
        def authenticate(self, token: str) -> ApplicationPrincipal | None:
            return principal if token == "reader-token" else None

    class Tags:
        def contains(self, collection_id: int, **kwargs: Any) -> dict[str, Any]:
            assert collection_id == 42
            assert kwargs["tag"] == tag
            return {
                "collection_id": collection_id,
                "revision": kwargs["revision"],
                "tag_set_identity": kwargs["tag_set_identity"],
                "tag": kwargs["tag"],
                "present": False,
            }

    app = create_app(container=SimpleNamespace(app_keys=Keys(), collection_tags=Tags()))

    async def exercise() -> None:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://testserver",
        ) as client:
            response = await client.post(
                "/v1/collections/42/tags:contains",
                params={"revision": 1, "tag_set_identity": identity},
                headers={"Authorization": "Bearer reader-token"},
                json={"tag": tag},
            )
            assert response.status_code == 200, response.text
            assert response.json()["tag"] == tag

    asyncio.run(exercise())
