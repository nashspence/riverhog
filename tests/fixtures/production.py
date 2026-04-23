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

from arc_api.app import create_app
from arc_core.domain.models import CollectionSummary
from arc_core.domain.types import CollectionId
from arc_core.fs_paths import normalize_collection_id
from tests.fixtures.acceptance import REPO_ROOT, SRC_ROOT, _LiveServerHandle, _reserve_local_port
from tests.fixtures.data import DOCS_FILES, PHOTOS_2024_FILES, write_tree


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


@dataclass(slots=True)
class ProductionSystem:
    workspace: Path
    server: _LiveServerHandle
    base_url: str
    previous_arc_staging_root: str | None
    previous_arc_db_path: str | None
    collections: ProductionCollectionsClient

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
        )
        system.collections = ProductionCollectionsClient(system)
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
        self.seed_collection_closed("docs", DOCS_FILES)

    def _subprocess_env(self) -> dict[str, str]:
        env = os.environ.copy()
        pythonpath_parts = [str(ROOT) for ROOT in (SRC_ROOT, REPO_ROOT)]
        existing = env.get("PYTHONPATH")
        if existing:
            pythonpath_parts.append(existing)
        env["PYTHONPATH"] = os.pathsep.join(pythonpath_parts)
        env["ARC_BASE_URL"] = self.base_url
        env["ARC_STAGING_ROOT"] = str((self.workspace / "staging").resolve())
        env["ARC_DB_PATH"] = str((self.workspace / ".arc" / "state.sqlite3").resolve())
        return env

    def _restore_environment(self) -> None:
        if self.previous_arc_staging_root is None:
            os.environ.pop("ARC_STAGING_ROOT", None)
        else:
            os.environ["ARC_STAGING_ROOT"] = self.previous_arc_staging_root

        if self.previous_arc_db_path is None:
            os.environ.pop("ARC_DB_PATH", None)
        else:
            os.environ["ARC_DB_PATH"] = self.previous_arc_db_path

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
