from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import httpx
import pytest
from sqlalchemy import delete, select

from arc_api.app import create_app
from arc_core.catalog_models import ActivePinRecord, CollectionFileRecord, FileCopyRecord
from arc_core.domain.enums import FetchState
from arc_core.domain.errors import NotFound
from arc_core.domain.models import CollectionSummary, FetchCopyHint, FetchSummary
from arc_core.domain.selectors import parse_target
from arc_core.domain.types import CollectionId, CopyId, FetchId, TargetStr
from arc_core.fs_paths import normalize_collection_id
from arc_core.sqlite_db import make_session_factory, session_scope
from tests.fixtures.acceptance import REPO_ROOT, SRC_ROOT, _LiveServerHandle, _reserve_local_port
from tests.fixtures.data import DOCS_COLLECTION_ID, DOCS_FILES, PHOTOS_2024_FILES, write_tree


class ProductionCollectionsClient:
    def __init__(self, system: ProductionSystem) -> None:
        self._system = system

    def get(self, collection_id: str) -> CollectionSummary:
        response = self._system.request("GET", f"/v1/collections/{collection_id}")
        payload = response.json()
        return CollectionSummary(
            id=CollectionId(payload["id"]),
            files=payload["files"],
            bytes=payload["bytes"],
            hot_bytes=payload["hot_bytes"],
            archived_bytes=payload["archived_bytes"],
        )


class ProductionFetchesClient:
    def __init__(self, system: ProductionSystem) -> None:
        self._system = system

    def get(self, fetch_id: str) -> FetchSummary:
        pins = self._system.request("GET", "/v1/pins").json()["pins"]
        for pin in pins:
            fetch = pin["fetch"]
            if fetch["id"] != fetch_id:
                continue
            return FetchSummary(
                id=FetchId(fetch["id"]),
                target=TargetStr(pin["target"]),
                state=FetchState(fetch["state"]),
                files=0,
                bytes=0,
                copies=[
                    FetchCopyHint(
                        id=CopyId(copy["id"]),
                        volume_id=copy["volume_id"],
                        location=copy["location"],
                    )
                    for copy in fetch["copies"]
                ],
            )
        raise NotFound(f"fetch not found: {fetch_id}")


@dataclass(frozen=True, slots=True)
class ProductionStoredFile:
    collection_id: str
    path: str
    hot: bool
    archived: bool


class ProductionExactPinsProxy:
    def __init__(self, system: ProductionSystem) -> None:
        self._system = system

    def discard(self, canonical: str) -> None:
        with session_scope(make_session_factory(str(self._system.db_path))) as session:
            record = session.get(ActivePinRecord, canonical)
            if record is not None:
                session.delete(record)


class ProductionStateClient:
    def __init__(self, system: ProductionSystem) -> None:
        self._system = system
        self.exact_pins = ProductionExactPinsProxy(system)

    def collection_files(self, collection_id: str) -> list[ProductionStoredFile]:
        with session_scope(make_session_factory(str(self._system.db_path))) as session:
            records = session.scalars(
                select(CollectionFileRecord).where(
                    CollectionFileRecord.collection_id == collection_id
                )
            ).all()
        return [
            ProductionStoredFile(
                collection_id=record.collection_id,
                path=record.path,
                hot=record.hot,
                archived=record.archived,
            )
            for record in sorted(records, key=lambda item: item.path)
        ]

    def selected_files(
        self,
        raw_target: str,
        *,
        missing_ok: bool = False,
    ) -> list[ProductionStoredFile]:
        target = parse_target(raw_target)
        with session_scope(make_session_factory(str(self._system.db_path))) as session:
            records = session.scalars(select(CollectionFileRecord)).all()
        selected = [
            ProductionStoredFile(
                collection_id=record.collection_id,
                path=record.path,
                hot=record.hot,
                archived=record.archived,
            )
            for record in records
            if (
                f"{record.collection_id}/{record.path}".startswith(target.canonical)
                if target.is_dir
                else f"{record.collection_id}/{record.path}" == target.canonical
            )
        ]
        if not selected and not missing_ok:
            raise AssertionError(f"target not found: {raw_target}")
        return selected

    def is_hot(self, raw_target: str) -> bool:
        selected = self.selected_files(raw_target, missing_ok=True)
        return bool(selected) and all(record.hot for record in selected)

    @property
    def fetches(self) -> list[str]:
        with session_scope(make_session_factory(str(self._system.db_path))) as session:
            records = session.scalars(
                select(ActivePinRecord).order_by(ActivePinRecord.fetch_order)
            ).all()
        return [record.fetch_id for record in records]


@dataclass(slots=True)
class ProductionSystem:
    workspace: Path
    server: _LiveServerHandle
    base_url: str
    previous_arc_staging_root: str | None
    previous_arc_db_path: str | None
    collections: ProductionCollectionsClient
    fetches: ProductionFetchesClient
    state: ProductionStateClient

    @property
    def db_path(self) -> Path:
        return (self.workspace / ".arc" / "state.sqlite3").resolve()

    @classmethod
    def create(cls, workspace: Path) -> ProductionSystem:
        previous_arc_staging_root = os.environ.get("ARC_STAGING_ROOT")
        previous_arc_db_path = os.environ.get("ARC_DB_PATH")
        os.environ["ARC_STAGING_ROOT"] = str((workspace / "staging").resolve())
        os.environ["ARC_DB_PATH"] = str((workspace / ".arc" / "state.sqlite3").resolve())
        app = create_app()
        with _reserve_local_port() as reserved:
            server = _LiveServerHandle(app, host="127.0.0.1", port=reserved.port)
        server.start()
        system = cls(
            workspace=workspace,
            server=server,
            base_url=server.base_url,
            previous_arc_staging_root=previous_arc_staging_root,
            previous_arc_db_path=previous_arc_db_path,
            collections=cast(ProductionCollectionsClient, None),
            fetches=cast(ProductionFetchesClient, None),
            state=cast(ProductionStateClient, None),
        )
        system.collections = ProductionCollectionsClient(system)
        system.fetches = ProductionFetchesClient(system)
        system.state = ProductionStateClient(system)
        return system

    def close(self) -> None:
        self.server.close()
        self._restore_environment()

    def restart(self) -> None:
        self.server.close()
        app = create_app()
        with _reserve_local_port() as reserved:
            server = _LiveServerHandle(app, host="127.0.0.1", port=reserved.port)
        server.start()
        self.server = server
        self.base_url = server.base_url

    def request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, object] | None = None,
        json_body: Mapping[str, object] | None = None,
        headers: Mapping[str, str] | None = None,
        content: bytes | None = None,
    ) -> httpx.Response:
        with httpx.Client(base_url=self.base_url, timeout=5.0) as client:
            return client.request(
                method,
                path,
                params=params,
                json=json_body,
                headers=headers,
                content=content,
            )

    def run_arc(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, "-m", "arc_cli.main", *args],
            cwd=REPO_ROOT,
            env=self._subprocess_env(),
            capture_output=True,
            text=True,
            check=False,
        )

    def run_arc_disc(
        self, *args: str, input_text: str = "\n" * 16
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, "-m", "arc_disc.main", *args],
            cwd=REPO_ROOT,
            env=self._subprocess_env(),
            input=input_text,
            capture_output=True,
            text=True,
            check=False,
        )

    def seed_staged_collection(
        self, collection_id: str, files: Mapping[str, bytes] | None = None
    ) -> None:
        normalized_collection_id = normalize_collection_id(collection_id)
        write_tree(
            self.workspace / "staging" / normalized_collection_id, files or PHOTOS_2024_FILES
        )

    def seed_collection_closed(
        self, collection_id: str, files: Mapping[str, bytes]
    ) -> None:
        normalized_collection_id = normalize_collection_id(collection_id)
        self.seed_staged_collection(normalized_collection_id, files)
        response = self.request(
            "POST",
            "/v1/collections/close",
            json_body={"path": f"/staging/{normalized_collection_id}"},
        )
        assert response.status_code == 200, response.text

    def seed_photos_hot(self) -> None:
        self.seed_collection_closed("photos-2024", PHOTOS_2024_FILES)

    def seed_nested_photos_hot(self) -> None:
        self.seed_collection_closed("photos/2024", PHOTOS_2024_FILES)

    def seed_parent_photos_hot(self) -> None:
        self.seed_collection_closed("photos", PHOTOS_2024_FILES)

    def seed_docs_hot(self) -> None:
        if not self._collection_exists("docs"):
            self.seed_collection_closed("docs", DOCS_FILES)

    def seed_docs_archive(self) -> None:
        self.seed_docs_hot()
        self._update_file_hot_and_archive(
            DOCS_COLLECTION_ID,
            "tax/2022/invoice-123.pdf",
            hot=False,
            archived=True,
            copies=[
                {
                    "copy_id": "copy-docs-1",
                    "volume_id": "20260419T230001Z",
                    "location": "vault-a/shelf-01",
                }
            ],
        )
        self._update_file_hot_and_archive(
            DOCS_COLLECTION_ID,
            "tax/2022/receipt-456.pdf",
            hot=True,
            archived=True,
            copies=[
                {
                    "copy_id": "copy-docs-2",
                    "volume_id": "20260419T230002Z",
                    "location": "vault-a/shelf-02",
                }
            ],
        )

    def seed_search_fixtures(self) -> None:
        self.seed_docs_archive()
        self.seed_photos_hot()

    def seed_pin(self, target: str) -> None:
        response = self.request("POST", "/v1/pin", json_body={"target": target})
        assert response.status_code == 200, response.text

    def seed_fetch(self, fetch_id: str, target: str) -> None:
        pins = self.request("GET", "/v1/pins").json()["pins"]
        for pin in pins:
            if pin["target"] == target:
                assert pin["fetch"]["id"] == fetch_id
                return

        target_record = parse_target(target)
        selected = self.state.selected_files(target, missing_ok=False)
        missing_bytes = sum(
            self._selected_file_bytes(record.collection_id, record.path)
            for record in selected
            if not record.hot
        )
        with session_scope(make_session_factory(str(self.db_path))) as session:
            next_order = (
                session.scalar(select(ActivePinRecord.fetch_order).order_by(
                    ActivePinRecord.fetch_order.desc()
                ).limit(1))
                or 0
            ) + 1
            session.merge(
                ActivePinRecord(
                    target=target_record.canonical,
                    fetch_id=fetch_id,
                    fetch_order=next_order,
                    fetch_state=(
                        FetchState.DONE.value
                        if missing_bytes == 0
                        else FetchState.WAITING_MEDIA.value
                    ),
                )
            )

    def pins_list(self) -> list[str]:
        return [item["target"] for item in self.request("GET", "/v1/pins").json()["pins"]]

    def _subprocess_env(self) -> dict[str, str]:
        env = os.environ.copy()
        pythonpath_parts = [str(ROOT) for ROOT in (SRC_ROOT, REPO_ROOT)]
        existing = env.get("PYTHONPATH")
        if existing:
            pythonpath_parts.append(existing)
        env["PYTHONPATH"] = os.pathsep.join(pythonpath_parts)
        env["ARC_BASE_URL"] = self.base_url
        env["ARC_STAGING_ROOT"] = str((self.workspace / "staging").resolve())
        env["ARC_DB_PATH"] = str(self.db_path)
        return env

    def _update_file_hot_and_archive(
        self,
        collection_id: str,
        path: str,
        *,
        hot: bool,
        archived: bool,
        copies: list[dict[str, str]],
    ) -> None:
        with session_scope(make_session_factory(str(self.db_path))) as session:
            record = session.get(
                CollectionFileRecord,
                {
                    "collection_id": collection_id,
                    "path": path,
                },
            )
            assert record is not None
            record.hot = hot
            record.archived = archived
            session.execute(
                delete(FileCopyRecord).where(
                    FileCopyRecord.collection_id == collection_id,
                    FileCopyRecord.path == path,
                )
            )
            for copy in copies:
                session.add(
                    FileCopyRecord(
                        collection_id=collection_id,
                        path=path,
                        copy_id=copy["copy_id"],
                        volume_id=copy["volume_id"],
                        location=copy["location"],
                    )
                )

    def _selected_file_bytes(self, collection_id: str, path: str) -> int:
        with session_scope(make_session_factory(str(self.db_path))) as session:
            record = session.get(
                CollectionFileRecord,
                {
                    "collection_id": collection_id,
                    "path": path,
                },
            )
            assert record is not None
            return record.bytes

    def _restore_environment(self) -> None:
        if self.previous_arc_staging_root is None:
            os.environ.pop("ARC_STAGING_ROOT", None)
        else:
            os.environ["ARC_STAGING_ROOT"] = self.previous_arc_staging_root

        if self.previous_arc_db_path is None:
            os.environ.pop("ARC_DB_PATH", None)
        else:
            os.environ["ARC_DB_PATH"] = self.previous_arc_db_path

    def _collection_exists(self, collection_id: str) -> bool:
        response = self.request("GET", f"/v1/collections/{collection_id}")
        if response.status_code == 200:
            return True
        if response.status_code == 404:
            return False
        raise AssertionError(response.text)

    def __getattr__(self, name: str) -> object:
        raise NotImplementedError(
            f"production acceptance suite does not provide fixture-backed state for {name}"
        )


@pytest.fixture
def acceptance_system(tmp_path: Path) -> Iterator[ProductionSystem]:
    system = ProductionSystem.create(tmp_path)
    try:
        yield system
    finally:
        system.close()
