"""Local artifact materialization from exact selected canonical provenance."""

from __future__ import annotations

import hashlib
import json
import uuid
from pathlib import Path
from typing import Any

import pytest
from a_riverhog_cli import local
from riverhog_client.canonical_production import ProducerAttribution, build_member_journal
from riverhog_materialization import (
    DestinationRules,
    primary_sidecar_components,
    shared_journal_components,
)
from riverhog_protocol import (
    ArtifactMemberIdentityDocument,
    PortableCollectionArtifact,
    PortableCollectionHeader,
    PortableCollectionInventoryAuthority,
    PortableCollectionInventoryPage,
    portable_collection_inventory_identity,
)
from riverhog_provenance import BoundedSourceObserver, BytesSource
from typer.testing import CliRunner

ROOT = "a" * 64
ARTIFACT_SET = "b" * 64
PROVENANCE = "c" * 64
CREATED_AT = "2026-07-19T20:55:09.123456000Z"


class FakeApi:
    def __init__(
        self,
        payloads: dict[str, bytes],
        hints: dict[str, tuple[str, ...] | None],
    ) -> None:
        self.payloads = payloads
        self.journals = {}
        self.bindings = {}
        self.members = []
        for artifact_id, content in sorted(payloads.items()):
            member = ArtifactMemberIdentityDocument.model_validate(
                {
                    "artifact_id": artifact_id,
                    "bytes": str(len(content)),
                    "sha256": hashlib.sha256(content).hexdigest(),
                }
            )
            produced = build_member_journal(
                member=member,
                observation=BoundedSourceObserver().observe(BytesSource(content)),
                delivery_context_id=f"urn:uuid:{uuid.uuid4()}",
                attribution=ProducerAttribution(
                    "test-cli", "test-bytes", "v1", "event-1", "test", {}, "d" * 64
                ),
                materialization_hint=hints.get(artifact_id),
            )
            self.members.append(member)
            self.journals[produced.journal_id] = produced.content
            self.bindings[artifact_id] = produced.binding.model_dump(mode="json")
        self.header = PortableCollectionHeader(
            collection="1",
            artifact_set_identity=ARTIFACT_SET,
            provenance_identity=PROVENANCE,
            encryption_format="age-v1-scrypt",
            passphrase_id="fixture-archive-key-v1",
        )
        self.inventory_identity = portable_collection_inventory_identity(
            self.header,
            [
                PortableCollectionArtifact.from_mapping(member.model_dump(mode="json"))
                for member in self.members
            ],
        )
        self.selected: list[tuple[int, str]] = []
        self.acknowledged: list[str] = []

    def __enter__(self) -> FakeApi:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def spawn(self) -> FakeApi:
        return self

    def get_collection(self, collection_id: int) -> dict[str, Any]:
        assert collection_id == 1
        return {
            "id": 1,
            "created_at": CREATED_AT,
            "archive_root_sha256": ROOT,
            "artifact_set_identity": ARTIFACT_SET,
            "tag_revision": 1,
            "tag_set_identity": "e" * 64,
        }

    def get_portable_collection_inventory(
        self, collection_id: int, **kwargs: object
    ) -> PortableCollectionInventoryPage:
        assert collection_id == 1
        assert kwargs["cursor"] is None
        assert kwargs["inventory_identity"] is None
        return PortableCollectionInventoryPage(
            authority=PortableCollectionInventoryAuthority(
                header=self.header,
                inventory_identity=self.inventory_identity,
                artifact_count=str(len(self.members)),
                artifact_bytes=str(sum(int(member.bytes) for member in self.members)),
            ),
            artifacts=self.members,
            complete=True,
        )

    def list_collection_provenance_journals(
        self, collection_id: int, **_kwargs: object
    ) -> dict[str, Any]:
        assert collection_id == 1
        return {
            "archive_root_sha256": ROOT,
            "journals": [
                {
                    "journal_id": journal_id,
                    "bytes": str(len(raw)),
                    "sha256": hashlib.sha256(raw).hexdigest(),
                }
                for journal_id, raw in sorted(self.journals.items())
            ],
            "next_journal_id": None,
        }

    def download_collection_provenance_journal(
        self, collection_id: int, journal_id: str, *, output: Path
    ) -> tuple[int, str]:
        assert collection_id == 1
        raw = self.journals[journal_id]
        output.write_bytes(raw)
        return len(raw), hashlib.sha256(raw).hexdigest()

    def get_collection_artifact_provenance(
        self, collection_id: int, artifact_id: str
    ) -> dict[str, Any]:
        assert collection_id == 1
        member = next(item for item in self.members if item.artifact_id == artifact_id)
        return {
            "archive_root_sha256": ROOT,
            "artifact": member.model_dump(mode="json"),
            "binding": self.bindings[artifact_id],
        }

    def list_collection_tags(self, collection_id: int, **_kwargs: object) -> dict[str, Any]:
        assert collection_id == 1
        return {
            "collection_id": "1",
            "revision": 1,
            "tag_set_identity": "e" * 64,
            "tags": ["source:test"],
            "next_page_token": None,
        }

    def plan_retrieval(self, artifacts: list[tuple[int, str]], **_kwargs: object) -> dict[str, Any]:
        self.selected = sorted(artifacts)
        return {
            "id": "plan-1",
            "etag": "f" * 64,
            "artifact_count": len(artifacts),
            "requires_restore": False,
        }

    def list_retrieval_plan_artifacts(self, plan_id: str, **_kwargs: object) -> dict[str, Any]:
        assert plan_id == "plan-1"
        return {
            "plan_id": plan_id,
            "etag": "f" * 64,
            "start_ordinal": 0,
            "complete": True,
            "next_ordinal": None,
            "artifacts": [
                {
                    "collection_id": "1",
                    "artifact_id": artifact_id,
                    "bytes": str(len(self.payloads[artifact_id])),
                    "sha256": hashlib.sha256(self.payloads[artifact_id]).hexdigest(),
                    "requires_restore": False,
                }
                for _, artifact_id in self.selected
            ],
        }

    def create_retrieval_job(self, plan_id: str, *, plan_etag: str) -> dict[str, Any]:
        assert plan_id == "plan-1" and plan_etag == "f" * 64
        return {"id": "job-1", "state": "ready", "lease_seconds": 60}

    def get_retrieval_job(self, job_id: str) -> dict[str, Any]:
        assert job_id == "job-1"
        return {"id": job_id, "state": "ready", "lease_seconds": 60}

    def renew_retrieval_job(self, job_id: str, *, lease_seconds: int) -> dict[str, Any]:
        assert job_id == "job-1" and lease_seconds == 60
        return self.get_retrieval_job(job_id)

    def download_retrieval_artifact(
        self,
        job_id: str,
        *,
        collection_id: int,
        artifact_id: str,
        output: Path,
        expected_bytes: int,
        expected_sha256: str,
    ) -> int:
        assert job_id == "job-1" and collection_id == 1
        content = self.payloads[artifact_id]
        assert expected_bytes == len(content)
        assert expected_sha256 == hashlib.sha256(content).hexdigest()
        output.write_bytes(content)
        return len(content)

    def acknowledge_retrieval_job(self, job_id: str) -> dict[str, Any]:
        self.acknowledged.append(job_id)
        return {"id": job_id, "state": "completed"}

    def cancel_retrieval_job(self, job_id: str) -> dict[str, Any]:
        return {"id": job_id, "state": "canceled"}


@pytest.fixture
def local_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "local"
    root.mkdir()
    monkeypatch.setenv("A_RIVERHOG_CLI_LOCAL_ROOT", str(root))
    monkeypatch.setattr(
        local,
        "_destination_rules",
        lambda _target: DestinationRules(False, True, "exact", 255, 4096),
    )
    assert CliRunner().invoke(local.local_app, ["state", "upgrade"]).exit_code == 0
    return root


def test_local_state_baseline_rejects_previous_path_schema(
    local_root: Path,
) -> None:
    assert CliRunner().invoke(local.local_app, ["state", "verify"]).exit_code == 0
    with local._connect(local_root) as db:
        columns = {row["name"] for row in db.execute("PRAGMA table_info(desired_artifacts)")}
        assert "artifact_id" in columns
        assert "path" not in columns


def test_local_sync_materializes_exact_hint_and_complete_provenance(
    local_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    hinted = "1" * 64
    nameless = "2" * 64
    api = FakeApi(
        {hinted: b"hinted payload", nameless: b"nameless payload"},
        {hinted: ("Album", "clip.mov"), nameless: None},
    )
    monkeypatch.setattr(local, "ApiClient", lambda: api)
    runner = CliRunner()
    added = runner.invoke(local.local_app, ["add", "1", "--json"])
    assert added.exit_code == 0, added.exception
    assert json.loads(added.stdout)["collection"]["artifacts"] == 2
    assert runner.invoke(local.local_app, ["sync", "--json"]).exit_code == 0
    assert (local_root / "1/files/Album/clip.mov").read_bytes() == b"hinted payload"
    fallback = local_root / f"1/artifacts/{nameless[:2]}/{nameless}"
    assert fallback.read_bytes() == b"nameless payload"
    for artifact_id in (hinted, nameless):
        binding = api.bindings[artifact_id]
        journal = api.journals[binding["journal"]["journal_id"]]
        sidecar = local_root / "1" / Path(*primary_sidecar_components(artifact_id))
        assert sidecar.read_bytes() == journal[: int(binding["journal"]["prefix_bytes"])]
        shared = (
            local_root / "1" / Path(*shared_journal_components(hashlib.sha256(journal).hexdigest()))
        )
        assert shared.read_bytes() == journal
    assert runner.invoke(local.local_app, ["audit"]).exit_code == 0
    assert api.acknowledged == ["job-1"]

    hinted_path = local_root / "1/files/Album/clip.mov"
    hinted_path.write_bytes(b"local edit")
    ordinary = runner.invoke(local.local_app, ["sync", "--json"])
    assert ordinary.exit_code == 0
    assert hinted_path.read_bytes() == b"local edit"
    assert runner.invoke(local.local_app, ["audit"]).exit_code == 1
    repaired = runner.invoke(local.local_app, ["repair", "--json"])
    assert repaired.exit_code == 0, repaired.exception
    assert hinted_path.read_bytes() == b"hinted payload"
    assert any((local_root / ".a-riverhog-cli-quarantine").iterdir())
    assert runner.invoke(local.local_app, ["audit"]).exit_code == 0


def test_local_id_layout_and_all_member_collision_fallback(
    local_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    first = "3" * 64
    second = "4" * 64
    api = FakeApi(
        {first: b"first", second: b"second"},
        {first: ("Clip.mov",), second: ("clip.mov",)},
    )
    monkeypatch.setattr(local, "ApiClient", lambda: api)
    monkeypatch.setattr(
        local,
        "_destination_rules",
        lambda _target: DestinationRules(False, False, "exact", 255, 4096),
    )
    runner = CliRunner()
    added = runner.invoke(local.local_app, ["add", "1"])
    assert added.exit_code == 0, added.exception
    with local._connect(local_root) as db:
        rows = list(
            db.execute(
                "SELECT artifact_id, destination_json, reason, hint_json "
                "FROM desired_artifacts ORDER BY artifact_id"
            )
        )
    assert len(rows) == 2
    assert all(row["reason"] == "destination-collision" for row in rows)
    assert all(json.loads(row["destination_json"])[0] == "artifacts" for row in rows)
    assert all(row["hint_json"] is not None for row in rows)


def test_local_rejects_wrong_primary_anchor_without_freezing_state(
    local_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifact_id = "5" * 64
    api = FakeApi({artifact_id: b"content"}, {artifact_id: ("name",)})
    api.bindings[artifact_id]["journal"]["prefix_sha256"] = "0" * 64
    monkeypatch.setattr(local, "ApiClient", lambda: api)
    result = CliRunner().invoke(local.local_app, ["add", "1"])
    assert result.exit_code != 0
    with local._connect(local_root) as db:
        assert db.execute("SELECT COUNT(*) FROM desired_collections").fetchone()[0] == 0
