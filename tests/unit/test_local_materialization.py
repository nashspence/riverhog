"""Local artifact materialization from exact selected canonical provenance."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import pytest
from a_riverhog_cli import local
from riverhog_archive_contracts import (
    BOUND_HISTORY_EXTENT,
    RETAINED_HISTORY_EXTENT,
    MemberHistoryStore,
    ProvenanceRootDocument,
    SourceMemberHistoryBindingProof,
    binding_tree_commitment,
    member_history_object_path,
    provenance_structure_object_path,
)
from riverhog_canonical_json import require_canonical_json
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
from typer.testing import CliRunner

from tests.support.qualification.recovery_archive import FixtureArchive, write_archive

ROOT = "a" * 64
ARTIFACT_SET = "b" * 64
PROVENANCE = "c" * 64
CREATED_AT = "2026-07-19T20:55:09.123456000Z"
_NATIVE_DESTINATION_RULES = local._destination_rules


class FakeApi:
    def __init__(
        self,
        payloads: dict[str, bytes],
        hints: dict[str, tuple[str, ...] | None],
        *,
        archive: FixtureArchive | None = None,
    ) -> None:
        self.payloads = payloads
        if archive is None:
            with TemporaryDirectory() as root:
                archive = write_archive(Path(root), members=payloads, hints=hints)
        self.archive = archive
        self.root = archive.archive_root_sha256
        self.journals = dict(archive.journals)
        self.history_bindings = {row.artifact_id: row for row in archive.history_bindings}
        self.history_documents = {
            artifact_id: json.loads(
                archive.history_objects[member_history_object_path(row.history_sha256)]
            )
            for artifact_id, row in self.history_bindings.items()
        }
        self.bindings = {
            artifact_id: {
                "artifact_id": artifact_id,
                **document["primary"],
            }
            for artifact_id, document in self.history_documents.items()
        }
        self.members = [
            ArtifactMemberIdentityDocument.model_validate(
                {
                    "artifact_id": artifact_id,
                    "bytes": str(len(content)),
                    "sha256": hashlib.sha256(content).hexdigest(),
                }
            )
            for artifact_id, content in sorted(payloads.items())
        ]
        provenance = ProvenanceRootDocument.from_json_bytes(archive.provenance_root)
        self.header = PortableCollectionHeader(
            collection="1",
            artifact_set_identity=provenance.artifact_set_sha256,
            provenance_identity=hashlib.sha256(archive.provenance_root).hexdigest(),
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
        self.plan_options: list[dict[str, object]] = []
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
            "archive_root_sha256": self.root,
            "artifact_set_identity": self.header.artifact_set_identity,
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
            "archive_root_sha256": self.root,
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
            "archive_root_sha256": self.root,
            "artifact": member.model_dump(mode="json"),
            "binding": self.bindings[artifact_id],
            "history_binding": self.history_bindings[artifact_id].to_mapping(),
            "member_history": self.history_documents[artifact_id],
        }

    def get_collection_provenance_structure(
        self, collection_id: int, object_id: str, *, archive_root_sha256: str
    ) -> bytes:
        assert collection_id == 1 and archive_root_sha256 == self.root
        return self.archive.history_objects[provenance_structure_object_path(object_id)]

    def get_collection_artifact_history_binding_proof(
        self, collection_id: int, artifact_id: str, *, archive_root_sha256: str
    ) -> SourceMemberHistoryBindingProof:
        assert collection_id == 1 and archive_root_sha256 == self.root
        tree = binding_tree_commitment(
            self.archive.history_bindings, target_artifact_id=artifact_id
        )
        assert tree.target_index is not None
        return SourceMemberHistoryBindingProof(
            source_identity="d0" * 32,
            collection_id=collection_id,
            archive_root=self.archive.archive_root,
            provenance_root=self.archive.provenance_root,
            binding=self.history_bindings[artifact_id],
            index=tree.target_index,
            siblings=tree.target_siblings,
        )

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
        self.plan_options.append(_kwargs)
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


def test_local_sync_and_repair_forward_archive_source_for_new_plans(
    local_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact = "1" * 64
    api = FakeApi({artifact: b"payload"}, {artifact: None})
    monkeypatch.setattr(local, "ApiClient", lambda: api)
    runner = CliRunner()
    assert runner.invoke(local.local_app, ["add", "1"]).exit_code == 0
    synced = runner.invoke(
        local.local_app,
        [
            "sync",
            "--source-store",
            "cold",
            "--restore-policy",
            "never",
            "--json",
        ],
    )
    assert synced.exit_code == 0, synced.exception
    assert api.plan_options[-1]["source_store"] == "cold"
    assert api.plan_options[-1]["restore_policy"] == "never"
    destination = local_root / f"1/artifacts/{artifact[:2]}/{artifact}"
    destination.write_bytes(b"modified")
    repaired = runner.invoke(local.local_app, ["repair", "--source-store", "warm", "--json"])
    assert repaired.exit_code == 0, repaired.exception
    assert api.plan_options[-1]["source_store"] == "warm"
    assert destination.read_bytes() == b"payload"


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


@pytest.mark.parametrize("name,windows_names", [("é" * 120, False), (":" * 80, True)])
def test_repair_preserves_long_materialization_edits_with_opaque_quarantine_names(
    local_root: Path, monkeypatch: pytest.MonkeyPatch, name: str, windows_names: bool
) -> None:
    artifact_id = "1" * 64
    archived = b"archived payload"
    api = FakeApi({artifact_id: archived}, {artifact_id: (name,)})
    monkeypatch.setattr(local, "ApiClient", lambda: api)
    monkeypatch.setattr(
        local,
        "_destination_rules",
        lambda target: replace(_NATIVE_DESTINATION_RULES(target), windows_names=windows_names),
    )
    runner = CliRunner()
    added = runner.invoke(local.local_app, ["add", "1"])
    assert added.exit_code == 0, added.exception
    synced = runner.invoke(local.local_app, ["sync"])
    assert synced.exit_code == 0, synced.exception
    output = next((local_root / "1/files").iterdir())
    assert len(output.name.encode("utf-8")) == 240
    edit = b"local edit to retain"
    output.write_bytes(edit)
    repaired = runner.invoke(local.local_app, ["repair"])
    assert repaired.exit_code == 0, repaired.exception
    assert output.read_bytes() == archived
    quarantine = local_root / ".a-riverhog-cli-quarantine"
    retained = list(quarantine.glob("*.data"))
    assert len(retained) == 1 and retained[0].read_bytes() == edit
    association = require_canonical_json(retained[0].with_suffix(".json").read_bytes())
    assert association == {
        "collection_id": "1",
        "artifact_id": artifact_id,
        "original_destination": list(output.relative_to(local_root).parts),
    }
    assert len(retained[0].name.encode("utf-8")) < 64
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


def test_local_list_filters_sorts_and_fences_continuation(local_root: Path) -> None:
    with local._connect(local_root) as db:
        for collection_id in (1, 2, 3):
            db.execute(
                "INSERT INTO desired_collections "
                "(collection_id,archive_root_sha256,inventory_identity,artifact_set_identity,"
                "provenance_identity,created_at,layout_mode,rules_json,remote_unavailable) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    collection_id,
                    ROOT,
                    "d" * 64,
                    ARTIFACT_SET,
                    PROVENANCE,
                    CREATED_AT,
                    "id-layout",
                    "{}",
                    int(collection_id == 2),
                ),
            )
            db.execute(
                "INSERT INTO desired_collection_tags (collection_id,tag) VALUES (?,?)",
                (collection_id, "camera" if collection_id != 3 else "delivery"),
            )
    runner = CliRunner()
    options = [
        "list",
        "--page-size",
        "1",
        "--sort",
        "collection_id",
        "--order",
        "desc",
        "--query",
        "camera",
        "--json",
    ]
    first = runner.invoke(local.local_app, options)
    assert first.exit_code == 0, first.exception
    first_page = json.loads(first.stdout)
    assert [row["collection_id"] for row in first_page["collections"]] == [2]
    assert first_page["collections"][0]["status"] == "remote-unavailable"
    token = first_page["next_page_token"]
    assert token
    second = runner.invoke(local.local_app, [*options, "--page-token", token])
    assert second.exit_code == 0, second.exception
    second_page = json.loads(second.stdout)
    assert [row["collection_id"] for row in second_page["collections"]] == [1]
    assert second_page["next_page_token"] is None
    changed = runner.invoke(
        local.local_app, [*options, "--query", "delivery", "--page-token", token]
    )
    assert changed.exit_code != 0
    assert "page token" in changed.stderr
    by_status = runner.invoke(local.local_app, ["list", "--query", "remote-unavailable", "--json"])
    assert by_status.exit_code == 0, by_status.exception
    assert [row["collection_id"] for row in json.loads(by_status.stdout)["collections"]] == [2]


@pytest.mark.parametrize("field", ["history_binding", "member_history"])
def test_local_requires_final_member_history_at_add_and_sync(
    local_root: Path, monkeypatch: pytest.MonkeyPatch, field: str
) -> None:
    artifact_id = "6" * 64
    api = FakeApi({artifact_id: b"content"}, {artifact_id: None})
    monkeypatch.setattr(local, "ApiClient", lambda: api)
    original = api.get_collection_artifact_provenance

    def primary_only(*args):
        detail = original(*args)
        del detail[field]
        return detail

    monkeypatch.setattr(api, "get_collection_artifact_provenance", primary_only)
    runner = CliRunner()
    result = runner.invoke(local.local_app, ["add", "1"])
    assert result.exit_code != 0
    with local._connect(local_root) as db:
        assert db.execute("SELECT COUNT(*) FROM desired_collections").fetchone()[0] == 0
    monkeypatch.setattr(api, "get_collection_artifact_provenance", original)
    assert runner.invoke(local.local_app, ["add", "1"]).exit_code == 0
    monkeypatch.setattr(api, "get_collection_artifact_provenance", primary_only)
    assert runner.invoke(local.local_app, ["sync"]).exit_code != 0
    assert not (local_root / f"1/artifacts/{artifact_id[:2]}/{artifact_id}").exists()


@pytest.mark.parametrize("extent", [BOUND_HISTORY_EXTENT, RETAINED_HISTORY_EXTENT])
def test_local_retains_late_and_inherited_selection_for_offline_audit_and_repair(
    local_root: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, extent: str
) -> None:
    source = write_archive(
        tmp_path / "source", late_shared_history=True, late_shared_scope="retained"
    )
    archive = write_archive(
        tmp_path / "derived",
        inherited_history=source,
        inherited_extent=extent,
        late_shared_history=True,
    )
    api = FakeApi(dict(archive.members), dict(archive.hints), archive=archive)
    monkeypatch.setattr(local, "ApiClient", lambda: api)
    runner = CliRunner()
    added = runner.invoke(local.local_app, ["add", "1"])
    assert added.exit_code == 0, added.exception
    synced = runner.invoke(local.local_app, ["sync"])
    assert synced.exit_code == 0, synced.exception
    with local._connect(local_root) as db:
        rows = list(db.execute("SELECT * FROM desired_artifacts ORDER BY artifact_id"))
        objects = list(db.execute("SELECT * FROM desired_history_objects ORDER BY object_id"))
    assert len(rows) == len(archive.members)
    assert len(objects) >= 5
    for row in rows:
        assert row["history_extent"] == RETAINED_HISTORY_EXTENT
        binding, proof = local._frozen_member_history(row, archive.archive_root_sha256)
        assert proof.binding == binding
        assert (
            json.loads(row["history_binding_json"])
            == api.history_bindings[row["artifact_id"]].to_mapping()
        )
        history = MemberHistoryStore(lambda path: (archive.history_objects[path],)).descriptor(
            binding
        )
        assert int(history.roots.record_count) == 2
        assert (
            local_root / "1" / Path(*local._history_file_components(binding.artifact_id, ".json"))
        ).read_bytes() == history.to_json_bytes()
        for components, raw in local._history_control_files(binding, proof):
            assert (local_root / "1" / Path(*components)).read_bytes() == raw
    first_history = api.history_documents[rows[0]["artifact_id"]]
    assert first_history["imports"]["record_count"] == "1"
    monkeypatch.setattr(
        local,
        "ApiClient",
        lambda: (_ for _ in ()).throw(AssertionError("offline audit must not use Riverhog")),
    )
    assert runner.invoke(local.local_app, ["audit"]).exit_code == 0
    first = local_root / "1" / Path(*local._structure_components(objects[0]["object_id"]))
    original_bytes = first.read_bytes()
    first.write_bytes(b"local edit")
    assert runner.invoke(local.local_app, ["audit"]).exit_code == 1
    monkeypatch.setattr(local, "ApiClient", lambda: api)
    assert runner.invoke(local.local_app, ["sync"]).exit_code != 0
    assert first.read_bytes() == b"local edit"
    repaired = runner.invoke(local.local_app, ["repair"])
    assert repaired.exit_code == 0, repaired.exception
    assert first.read_bytes() == original_bytes
    first.unlink()
    assert runner.invoke(local.local_app, ["audit"]).exit_code == 1
    synced = runner.invoke(local.local_app, ["sync"])
    assert synced.exit_code == 0, synced.exception
    assert first.read_bytes() == original_bytes
    assert runner.invoke(local.local_app, ["audit"]).exit_code == 0
    evicted = runner.invoke(local.local_app, ["evict", "1", "--confirm"])
    assert evicted.exit_code == 0, evicted.exception
    assert not list((local_root / "1" / "structure").rglob("*.json"))
    assert not list((local_root / "1" / "provenance" / "history").rglob("*.json"))


@pytest.mark.parametrize("damage", ["descriptor", "proof", "extent"])
def test_local_audit_rejects_modified_history_selection(
    local_root: Path, monkeypatch: pytest.MonkeyPatch, damage: str
) -> None:
    artifact_id = "7" * 64
    api = FakeApi({artifact_id: b"content"}, {artifact_id: None})
    monkeypatch.setattr(local, "ApiClient", lambda: api)
    runner = CliRunner()
    assert runner.invoke(local.local_app, ["add", "1"]).exit_code == 0
    assert runner.invoke(local.local_app, ["sync"]).exit_code == 0
    if damage == "descriptor":
        path = local_root / "1" / Path(*local._history_file_components(artifact_id, ".json"))
        path.write_bytes(b"{}")
    elif damage == "proof":
        with local._connect(local_root) as db:
            proof = json.loads(
                db.execute("SELECT history_proof_json FROM desired_artifacts").fetchone()[0]
            )
            proof["collection_id"] = "2"
            db.execute(
                "UPDATE desired_artifacts SET history_proof_json = ?",
                (local.canonical_json_bytes(proof).decode(),),
            )
            db.commit()
    else:
        with local._connect(local_root) as db:
            db.execute("PRAGMA ignore_check_constraints = ON")
            db.execute("UPDATE desired_artifacts SET history_extent = ?", (BOUND_HISTORY_EXTENT,))
            db.commit()
    result = runner.invoke(local.local_app, ["audit", "--json"])
    assert result.exit_code == 1, result.exception
    if damage == "extent":
        assert "CHECK constraint failed" in str(result.exception)
    else:
        assert json.loads(result.stdout)["problems"] > 0


@pytest.mark.skipif(
    os.name != "posix", reason="real PATH_MAX boundary uses POSIX filesystem limits"
)
def test_local_long_valid_hint_falls_back_using_the_actual_collection_prefix(
    local_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    deep_root = local_root / ("deep-" + "p" * 180)
    deep_root.mkdir()
    monkeypatch.setenv("A_RIVERHOG_CLI_LOCAL_ROOT", str(deep_root))
    assert CliRunner().invoke(local.local_app, ["state", "upgrade"]).exit_code == 0
    local_root = deep_root
    hint = ("x" * 128,) * 31
    artifact_id = "8" * 64
    api = FakeApi({artifact_id: b"content"}, {artifact_id: hint})
    monkeypatch.setattr(local, "ApiClient", lambda: api)
    monkeypatch.setattr(local, "_destination_rules", _NATIVE_DESTINATION_RULES)
    collection_root = local_root / "1"
    collection_root.mkdir()
    rules = _NATIVE_DESTINATION_RULES(collection_root)
    assert (
        rules.relative_path_bytes
        == os.pathconf(collection_root, "PC_PATH_MAX")
        - len(os.fsencode(collection_root.absolute()))
        - 2
    )
    runner = CliRunner()
    result = runner.invoke(local.local_app, ["add", "1"])
    assert result.exit_code == 0, result.exception
    with local._connect(local_root) as db:
        row = db.execute(
            "SELECT destination_json, reason, hint_json FROM desired_artifacts"
        ).fetchone()
    assert row["reason"] == "destination-limits"
    assert json.loads(row["hint_json"]) == list(hint)
    assert json.loads(row["destination_json"]) == ["artifacts", artifact_id[:2], artifact_id]
    result = runner.invoke(local.local_app, ["sync"])
    assert result.exit_code == 0, result.exception
    assert (
        collection_root / "artifacts" / artifact_id[:2] / artifact_id
    ).read_bytes() == b"content"
