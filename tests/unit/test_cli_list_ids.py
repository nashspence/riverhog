from __future__ import annotations

import json
from typing import Any

import a_riverhog_cli.main
import httpx
import pytest
from a_riverhog_cli.main import app
from riverhog_api.schemas.collections import ListCollectionsResponse
from riverhog_client import ApiClient
from riverhog_protocol import COLLECTION_TAG_REQUEST_MEMBERS_MAX
from riverhog_protocol.errors import BadRequest
from typer.testing import CliRunner

runner = CliRunner()


@pytest.mark.parametrize(
    "count", [COLLECTION_TAG_REQUEST_MEMBERS_MAX, COLLECTION_TAG_REQUEST_MEMBERS_MAX + 1]
)
def test_collection_list_tag_batch_preserves_the_client_acceptance_boundary(
    monkeypatch, count: int
) -> None:
    requests: list[httpx.Request] = []
    tags = [f"fixture-{index}" for index in range(count)]

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.method == "POST"
        assert request.url.path == "/v1/collections:search"
        assert json.loads(request.content)["tags"] == tags
        payload = ListCollectionsResponse(
            collections=[],
            page_size=25,
            next_page_token=None,
            sort="id",
            order="asc",
            query=None,
            encryption_format=None,
            passphrase_id=None,
            tags=tags,
        )
        return httpx.Response(200, json=payload.model_dump(mode="json"))

    with ApiClient(base_url="https://riverhog.invalid", token="fixture") as api:
        with httpx.Client(
            base_url=api.base_url,
            transport=httpx.MockTransport(handle),
        ) as transport:
            monkeypatch.setattr(api, "_persistent_client", lambda: transport)
            monkeypatch.setattr(a_riverhog_cli.main, "client", lambda: api)
            result = runner.invoke(
                app,
                ["collection", "list", "--json", *(arg for tag in tags for arg in ("--tag", tag))],
            )
    if count == COLLECTION_TAG_REQUEST_MEMBERS_MAX:
        assert result.exit_code == 0, result.output
        assert len(requests) == 1
        assert json.loads(result.stdout)["tags"] == tags
    else:
        assert result.exit_code != 0
        assert isinstance(result.exception, BadRequest)
        assert "invalid cardinality" in str(result.exception)
        assert requests == []


def test_collection_list_ids_emits_one_pipeable_bounded_page(monkeypatch) -> None:
    class FakeClient:
        def list_collections(self, **kwargs: Any) -> dict[str, object]:
            assert kwargs["q"] == "camera"
            assert kwargs["encryption_format"] == "age-v1-scrypt"
            assert kwargs["passphrase_id"] == "fixture-archive-key-v2"
            assert kwargs["page_size"] == 10
            assert kwargs["page_token"] == "next-page"
            return {
                "collections": [{"id": 41}, {"id": 42}],
                "page_size": 10,
                "next_page_token": "later-page",
            }

    monkeypatch.setattr(a_riverhog_cli.main, "client", FakeClient)

    result = runner.invoke(
        app,
        [
            "collection",
            "list",
            "--query",
            "camera",
            "--encryption-format",
            "age-v1-scrypt",
            "--passphrase-id",
            "fixture-archive-key-v2",
            "--page-token",
            "next-page",
            "--page-size",
            "10",
            "--ids",
        ],
    )

    assert result.exit_code == 0
    assert result.stdout == "41\n42\n"


def test_collection_upload_list_ids_forwards_bounded_page_and_filters(monkeypatch) -> None:
    class FakeClient:
        def list_collection_upload_sessions(self, **kwargs: Any) -> dict[str, object]:
            assert kwargs == {
                "page_size": 25,
                "page_token": None,
                "q": "camera",
                "state": "uploading",
                "sort": "created_at",
                "order": "desc",
            }
            return {
                "uploads": [{"collection_id": 41}, {"collection_id": 42}],
                "page_size": 25,
                "next_page_token": None,
            }

    monkeypatch.setattr(a_riverhog_cli.main, "client", FakeClient)

    result = runner.invoke(
        app,
        [
            "collection",
            "upload",
            "list",
            "--query",
            "camera",
            "--state",
            "uploading",
            "--ids",
        ],
    )

    assert result.exit_code == 0
    assert result.stdout == "41\n42\n"


def test_find_selectors_emits_pipeable_file_identities_from_one_page(monkeypatch) -> None:
    class FakeClient:
        def search(self, query: str | None, **kwargs: Any) -> dict[str, object]:
            assert query == "invoice"
            return {
                "files": [
                    {
                        "collection_id": 41,
                        "path": "tax/invoice.pdf",
                        "file_ref": "41/tax/invoice.pdf",
                    },
                    {
                        "collection_id": 42,
                        "path": "tax/invoice.pdf",
                        "file_ref": "42/tax/invoice.pdf",
                    },
                ],
                "page_size": 25,
                "next_page_token": None,
            }

    monkeypatch.setattr(a_riverhog_cli.main, "client", FakeClient)

    result = runner.invoke(app, ["find", "-q", "invoice", "--selectors"])

    assert result.exit_code == 0
    assert result.stdout == ("41::tax/invoice.pdf\n42::tax/invoice.pdf\n")
    human = runner.invoke(app, ["find", "-q", "invoice"])
    structured = runner.invoke(app, ["find", "-q", "invoice", "--json"])
    assert human.exit_code == structured.exit_code == 0
    assert "tax/invoice.pdf" in human.stdout
    assert json.loads(structured.stdout)["files"][0]["collection_id"] == 41


def test_riverhog_closes_its_shared_api_client(monkeypatch) -> None:
    closed: list[bool] = []

    class FakeClient:
        def list_collections(self, **_kwargs: Any) -> dict[str, object]:
            return {"collections": [], "page_size": 25, "next_page_token": None}

        def close(self) -> None:
            closed.append(True)

    monkeypatch.setattr(a_riverhog_cli.main, "_API_CLIENT", FakeClient())

    result = runner.invoke(app, ["collection", "list"])

    assert result.exit_code == 0
    assert closed == [True]
    assert a_riverhog_cli.main._API_CLIENT is None
