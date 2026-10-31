from __future__ import annotations

import hashlib
import json
import os
from contextlib import closing, contextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from a_riverhog_cli import main as riverhog_main
from a_riverhog_cli.directory_upload import prepare_upload, preview_upload
from typer.testing import CliRunner

from tests.support.cli_upload import read_upload_sources

RUNNER = CliRunner()


def test_local_upload_allocates_distinct_stable_artifact_ids_before_registration(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    root = tmp_path / "collection"
    root.mkdir()
    (root / "one.bin").write_bytes(b"same bytes")
    (root / "two.bin").write_bytes(b"same bytes")

    first = prepare_upload(root, "same-request", ordering_bytes=64 * 1024)
    second = prepare_upload(root, "same-request", ordering_bytes=64 * 1024)
    assert first == second
    assert len(read_upload_sources(first)) == 2
    assert len({source.artifact_id for source in read_upload_sources(first)}) == 2
    assert {source.sha256 for source in read_upload_sources(first)} == {
        hashlib.sha256(b"same bytes").hexdigest()
    }
    assert all(len(source.artifact_id) == 64 for source in read_upload_sources(first))
    assert first.source_naming_view_id.startswith("urn:uuid:")

    (root / "two.bin").write_bytes(b"changed bytes")
    with pytest.raises(ValueError, match="saved upload identity differs"):
        prepare_upload(root, "same-request", ordering_bytes=64 * 1024)


def test_upload_preview_streams_hashes_without_allocating_ids_or_opening_api(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    root = tmp_path / "collection"
    (root / "nested").mkdir(parents=True)
    (root / "nested" / "clip.bin").write_bytes(b"video")

    def forbidden_read_bytes(self: Path) -> bytes:
        raise AssertionError(f"preview must stream source bytes: {self}")

    def forbidden_client() -> object:
        raise AssertionError("dry-run must not open an API client")

    monkeypatch.setattr(Path, "read_bytes", forbidden_read_bytes)
    monkeypatch.setattr(riverhog_main, "client", forbidden_client)
    result = RUNNER.invoke(
        riverhog_main.app,
        [
            "collection",
            "upload",
            "start",
            str(root),
            "--idempotency-key",
            "preview",
            "--dry-run",
            "--json",
        ],
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.stdout)
    assert payload == {
        "format": "a-riverhog-cli-upload-preview/v1",
        "idempotency_key": "preview",
        "artifact_count": 1,
        "total_bytes": 5,
        "sources": [
            {
                "relative_components": ["nested", "clip.bin"],
                "bytes": 5,
                "sha256": hashlib.sha256(b"video").hexdigest(),
            }
        ],
    }
    assert not list(tmp_path.rglob("*.json"))


def test_local_upload_rejects_symlink_sources_before_allocation(tmp_path: Path) -> None:
    root = tmp_path / "collection"
    root.mkdir()
    target = tmp_path / "outside.bin"
    target.write_bytes(b"outside")
    (root / "linked.bin").symlink_to(target)
    with pytest.raises(ValueError, match="symbolic link"):
        preview_upload(root)


def test_upload_cli_forwards_persisted_ids_and_source_advice_to_producer(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    root = tmp_path / "collection"
    root.mkdir()
    (root / "clip.bin").write_bytes(b"video")
    calls: dict[str, Any] = {}

    class FakeProducer:
        constraints = object()

        def __init__(self, api: object, **kwargs: Any) -> None:
            calls["api"] = api
            calls["constructor"] = kwargs

        def append_inputs(self, inputs: object, *, expected_identities: object) -> None:
            calls["inputs"] = inputs
            calls["expected_identities"] = expected_identities

        def finish(self) -> SimpleNamespace:
            calls["finished"] = True
            return SimpleNamespace(receipt={"collection_id": 41, "state": "finalized"})

        def stop(self) -> None:
            calls["stopped"] = True

    api = object()
    monkeypatch.setenv(
        "A_RIVERHOG_CLI_PRIMARY_STATE",
        json.dumps({"scope": {"service": "fixture-service", "account": "fixture-account"}}),
    )
    monkeypatch.setattr(riverhog_main, "client", lambda: api)
    monkeypatch.setattr(riverhog_main, "IncrementalCollectionProducer", FakeProducer)
    arguments = [
        "collection",
        "upload",
        "start",
        str(root),
        "--idempotency-key",
        "upload-once",
        "--json",
    ]
    first = RUNNER.invoke(riverhog_main.app, [*arguments, "--initialize-state"])
    assert first.exit_code == 0, first.output
    identity = next(iter(calls["expected_identities"].values()))
    assert len(identity.artifact_id) == 64
    source = calls["inputs"][0]
    assert source.artifact_id == identity.artifact_id
    assert source.materialization_hint == ("clip.bin",)
    assert source.allow_missing_materialization_hint is False
    assert source.source == (root / "clip.bin").resolve()
    assert calls["api"] is api
    assert calls["finished"] and calls["stopped"]

    second = RUNNER.invoke(riverhog_main.app, arguments)
    assert second.exit_code == 0, second.output
    assert next(iter(calls["expected_identities"].values())).artifact_id == identity.artifact_id


def test_upload_artifact_list_and_discard_controls_have_human_json_parity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact_id = "a" * 64
    challenge = "discard-upload:fixture"

    class Api:
        def list_collection_upload_session_artifacts(
            self, collection_id: int, **_kwargs: object
        ) -> dict[str, object]:
            assert collection_id == 41
            return {
                "page_size": 25,
                "next_page_token": None,
                "artifacts": [
                    {
                        "artifact_id": artifact_id,
                        "bytes": 10,
                        "sha256": "b" * 64,
                        "custody_receipt": {"receipt_sha256": "c" * 64},
                    }
                ],
            }

        def plan_collection_upload_discard(self, collection_id: int) -> dict[str, object]:
            assert collection_id == 41
            return {
                "status": "ready",
                "collection_id": collection_id,
                "warning": "This permanently destroys Riverhog-custodied artifacts.",
                "blockers": [],
                "challenge": challenge,
            }

        def discard_collection_upload(
            self, collection_id: int, *, challenge: str
        ) -> dict[str, object]:
            assert (collection_id, challenge) == (41, "discard-upload:fixture")
            return {"status": "discarded", "collection_id": collection_id}

    monkeypatch.setattr(riverhog_main, "client", Api)
    cases = (
        (["collection", "upload", "artifacts", "41"], artifact_id, "artifacts"),
        (["collection", "upload", "discard", "41", "--dry-run"], "permanently destroys", "status"),
        (["collection", "upload", "discard", "41", "--confirm", challenge], "discarded", "status"),
    )
    for arguments, expected_human, structured_key in cases:
        human = RUNNER.invoke(riverhog_main.app, arguments)
        structured = RUNNER.invoke(riverhog_main.app, [*arguments, "--json"])
        assert human.exit_code == structured.exit_code == 0
        assert expected_human in human.stdout
        assert structured_key in json.loads(structured.stdout)


def test_cli_reuses_declared_native_observation_and_rejects_changed_finalized_inputs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from riverhog_client import PrimaryStateError
    from riverhog_provenance import BoundedSourceObserver, BytesSource
    from riverhog_provenance_contracts import core_contract

    from tests.unit.test_incremental_collection_producer import _CustodyApi

    root = tmp_path / "sources"
    root.mkdir()
    (root / "clip.bin").write_bytes(b"exact CLI member")
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    monkeypatch.setenv(
        "A_RIVERHOG_CLI_PRIMARY_STATE",
        json.dumps({"scope": {"service": "fixture-service", "account": "fixture-account"}}),
    )
    observed = []

    class Observer:
        contract = core_contract()

        def observe_native_file(self, path, *, host_id, naming_view_id):
            observed.append((host_id, naming_view_id))
            return BoundedSourceObserver().observe(BytesSource(path.read_bytes()))

    api = _CustodyApi()
    from a_riverhog_cli.upload_sqlite import UploadSqliteResources

    resource_owners = []

    def application_resources(policy):
        owner = UploadSqliteResources(policy)
        resource_owners.append(owner)
        return owner

    monkeypatch.setenv(
        "A_RIVERHOG_CLI_SQLITE_RESOURCES",
        json.dumps({"connections": 8, "cursors": 32, "operations": 4}),
    )
    monkeypatch.setattr(riverhog_main, "UploadSqliteResources", application_resources)
    monkeypatch.setattr(riverhog_main, "client", lambda: api)
    monkeypatch.setattr(riverhog_main, "resolve_provenance_observer", lambda _name: Observer())
    arguments = [
        "collection",
        "upload",
        "start",
        str(root),
        "--idempotency-key",
        "native-cli",
        "--provenance-observer",
        "fixture-native",
        "--source-host-id",
        "urn:uuid:11111111-1111-4111-8111-111111111111",
        "--json",
    ]
    first = RUNNER.invoke(riverhog_main.app, [*arguments, "--initialize-state"])
    assert first.exit_code == 0, first.output
    second = RUNNER.invoke(riverhog_main.app, arguments)
    assert second.exit_code == 0, second.output
    assert len(observed) == 1 and len(api.journals) == 1 and api.session_calls == 1
    assert resource_owners[0].status.connection_peak > 0
    assert resource_owners[0].status.cursor_peak > 0
    assert resource_owners[0].status.operation_peak > 0
    assert all(
        owner.status.connections == 0 and owner.pending_directories == ()
        for owner in resource_owners
    )
    changed = list(arguments)
    changed[-2] = "urn:uuid:22222222-2222-4222-8222-222222222222"
    rejected = RUNNER.invoke(riverhog_main.app, changed)
    assert rejected.exit_code != 0 and isinstance(rejected.exception, PrimaryStateError)
    assert len(observed) == 1 and len(api.journals) == 1 and api.session_calls == 1


@pytest.mark.skipif(os.name != "posix", reason="actual POSIX CLI allocation")
@pytest.mark.parametrize("failure", ["none", "append", "inventory"])
def test_upload_cli_owns_one_source_traversal_per_batch_pass_and_closes_on_failure(
    monkeypatch, tmp_path, failure
):
    from a_riverhog_cli.upload_sources import LocalUpload

    root = tmp_path / "sources"
    root.mkdir()
    for index in range(5):
        (root / f"member-{index}").write_bytes(b"" if index == 0 else b"equal bytes")
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "state"))
    monkeypatch.setenv(
        "A_RIVERHOG_CLI_PRIMARY_STATE",
        json.dumps({"scope": {"service": "fixture-service", "account": "fixture-account"}}),
    )
    active = opened = stopped = 0
    batches = []
    actual = LocalUpload.open_sources

    @contextmanager
    def tracked(self):
        nonlocal active, opened
        with actual(self) as sources:
            active += 1
            opened += 1
            try:
                yield sources
            finally:
                active -= 1

    class Producer:
        constraints = object()

        def __init__(self, _api, **options):
            self.inventory = options["member_inventory"]

        def append_inputs(self, inputs, *, expected_identities):
            assert active == 1 and 0 < len(inputs) <= 2
            assert set(expected_identities) == {value.artifact_id for value in inputs}
            batches.append(tuple(value.artifact_id for value in inputs))
            if failure == "append":
                raise OSError("append interrupted")

        def finish(self):
            assert active == 0
            with closing(self.inventory()) as inventory:
                first = next(inventory)
                assert active == 1
                if failure == "inventory":
                    raise OSError("inventory interrupted")
                observed = [first, *inventory]
                assert len(observed) == 5
                assert {value.artifact_id for value in observed} == {
                    value for batch in batches for value in batch
                }
            assert active == 0
            return SimpleNamespace(receipt={"collection_id": 41, "state": "finalized"})

        def stop(self):
            nonlocal stopped
            assert active == 0
            stopped += 1

    monkeypatch.setattr(LocalUpload, "open_sources", tracked)
    monkeypatch.setattr(riverhog_main, "COLLECTION_UPLOAD_REGISTRATION_BATCH_FILES", 2)
    monkeypatch.setattr(riverhog_main, "IncrementalCollectionProducer", Producer)
    monkeypatch.setattr(riverhog_main, "client", object)
    result = RUNNER.invoke(
        riverhog_main.app,
        [
            "collection",
            "upload",
            "start",
            str(root),
            "--idempotency-key",
            "scoped",
            "--initialize-state",
            "--json",
        ],
    )
    assert (result.exit_code == 0) is (failure == "none")
    if failure != "none":
        assert isinstance(result.exception, OSError)
    assert active == 0 and stopped == 1
    assert opened == (1 if failure == "append" else 2)
    assert [len(batch) for batch in batches] == ([2] if failure == "append" else [2, 2, 1])
