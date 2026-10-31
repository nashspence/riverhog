from __future__ import annotations

import copy
import json
import os
import subprocess
import sys
from contextlib import closing, contextmanager
from dataclasses import asdict
from pathlib import Path

import pytest
from a_riverhog_cli import directory_upload, main, upload_windows
from a_riverhog_cli.upload_windows import WindowsUploadPolicy
from pydantic import ValidationError
from rich.text import Text
from riverhog_client import (
    PrimaryStateCapacity,
    SqliteOperationBank,
    SqliteOperationGrants,
    SqlitePrimaryLimits,
    SqliteReadinessPlan,
)
from typer.testing import CliRunner

from tests.support.cli_upload import read_upload_sources
from tests.unit.test_cli_upload_windows_binding import _SqlOnlyProcess, selection
from tests.unit.test_incremental_collection_producer import _CustodyApi


def policy_payload(parent):
    chosen = selection(parent)
    return {
        "parent": str(parent),
        "heap_bytes": 64 * 1024**2,
        "native_limits": [1, 2],
        "limits": asdict(chosen.limits),
        "operations": asdict(chosen.operations),
        "retry_profile": list(chosen.retry_profile),
        "readiness": {
            **asdict(chosen.readiness),
            "prefix_lengths": list(chosen.readiness.prefix_lengths),
        },
        "identity_bytes": chosen.identity_bytes,
        "preparation_steps": chosen.preparation_steps,
    }


def forbidden(*_args, **_kwargs):
    pytest.fail("policy parsing or selection opened a runtime or state")


def test_windows_command_policy_parses_exact_public_grants_without_opening_state(
    tmp_path, monkeypatch
):
    parent = tmp_path / "uncreated-parent"
    payload = policy_payload(parent)
    monkeypatch.setattr(upload_windows, "WindowsSqliteProcess", forbidden)
    monkeypatch.setattr(upload_windows, "UploadSqliteResources", forbidden)
    policy = WindowsUploadPolicy.model_validate_json(json.dumps(payload))
    assert policy.model_dump(mode="json") == payload
    assert type(policy.limits) is SqlitePrimaryLimits
    assert type(policy.operations) is SqliteOperationGrants
    assert type(policy.operations.forward) is SqliteOperationBank
    assert type(policy.readiness) is SqliteReadinessPlan
    assert not parent.exists()


@pytest.mark.parametrize(
    "field",
    [
        "parent",
        "heap_bytes",
        "native_limits",
        "limits",
        "operations",
        "retry_profile",
        "readiness",
        "identity_bytes",
        "preparation_steps",
    ],
)
def test_windows_command_policy_requires_every_resource_grant(tmp_path, field):
    payload = policy_payload(tmp_path / "uncreated-parent")
    del payload[field]
    with pytest.raises(ValidationError):
        WindowsUploadPolicy.model_validate_json(json.dumps(payload))
    assert not (tmp_path / "uncreated-parent").exists()


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("parent",), "relative"),
        (("heap_bytes",), True),
        (("heap_bytes",), 0),
        (("heap_bytes",), -1),
        (("heap_bytes",), 1.5),
        (("heap_bytes",), "67108864"),
        (("heap_bytes",), 1 << 63),
        (("native_limits", 0), True),
        (("native_limits", 0), 0),
        (("native_limits", 1), -1),
        (("identity_bytes",), False),
        (("identity_bytes",), 0),
        (("identity_bytes",), 1.5),
        (("preparation_steps",), True),
        (("preparation_steps",), 0),
        (("preparation_steps",), "512"),
        (("retry_profile", 0), True),
        (("retry_profile", 0), -1),
        (("retry_profile", 1), -1),
        (("limits", "database_bytes"), 1),
        (("limits", "cache_bytes"), True),
        (("operations", "forward", "buffer_bytes"), True),
        (("operations", "recovery", "read_calls"), -1),
        (("readiness", "free_pages"), True),
        (("readiness", "prefix_lengths", 0), -1),
    ],
)
def test_windows_command_policy_rejects_invalid_grants_before_runtime_or_state(
    tmp_path, monkeypatch, path, value
):
    payload = copy.deepcopy(policy_payload(tmp_path / "uncreated-parent"))
    target = payload
    for component in path[:-1]:
        target = target[component]
    target[path[-1]] = value
    monkeypatch.setattr(upload_windows, "WindowsSqliteProcess", forbidden)
    monkeypatch.setattr(upload_windows, "UploadSqliteResources", forbidden)
    with pytest.raises(ValidationError):
        WindowsUploadPolicy.model_validate_json(json.dumps(payload))
    assert not (tmp_path / "uncreated-parent").exists()


@pytest.mark.parametrize(
    "path", [(), ("limits",), ("operations",), ("operations", "forward"), ("readiness",)]
)
def test_windows_command_policy_refuses_unknown_fields_in_nested_public_grants(tmp_path, path):
    payload = policy_payload(tmp_path / "uncreated-parent")
    target = payload
    for component in path:
        target = target[component]
    target["unknown_grant"] = 1
    with pytest.raises(ValidationError):
        WindowsUploadPolicy.model_validate_json(json.dumps(payload))


def test_windows_command_policy_is_immutable_and_selects_the_exact_admitted_owners(tmp_path):
    policy = WindowsUploadPolicy.model_validate_json(
        json.dumps(policy_payload(tmp_path / "uncreated-parent"))
    )
    with pytest.raises(ValidationError, match="frozen"):
        policy.heap_bytes = 1

    class Owner:
        def __bool__(self):
            return False

    process, resources = Owner(), Owner()
    chosen = policy.selection(process, resources)
    assert chosen.process is process and chosen.resources is resources
    assert chosen.parent == policy.parent
    assert chosen.limits is policy.limits and chosen.operations is policy.operations
    assert chosen.readiness is policy.readiness
    assert chosen.retry_profile == policy.retry_profile
    assert chosen.identity_bytes == policy.identity_bytes
    assert chosen.preparation_steps == policy.preparation_steps
    assert not chosen.parent.exists()


def command_fixture(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    parent = tmp_path / "retained"
    monkeypatch.setenv(
        "A_RIVERHOG_CLI_PRIMARY_STATE",
        json.dumps(
            {
                "scope": {"service": "fixture-service", "account": "fixture-account"},
                "control_bytes": 8192,
                "inventory_order_bytes": 65536,
            }
        ),
    )
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "unused-posix"))
    return source, parent


def test_invalid_windows_command_policy_precedes_process_state_and_remote_work(
    tmp_path, monkeypatch
):
    source, parent = command_fixture(tmp_path, monkeypatch)
    payload = policy_payload(parent)
    del payload["heap_bytes"]
    for name in [
        "WindowsSqliteProcess",
        "UploadSqliteResources",
        "prepare_primary_upload",
        "client",
    ]:
        monkeypatch.setattr(main, name, forbidden)
    result = CliRunner().invoke(
        main.app,
        ["collection", "upload", "start", str(source), "--windows-primary", json.dumps(payload)],
    )
    assert result.exit_code == 2
    assert "--windows-primary" in Text.from_ansi(result.output).plain
    assert not parent.exists()


def test_windows_process_refusal_precedes_consumer_state_and_remote_work(tmp_path, monkeypatch):
    source, parent = command_fixture(tmp_path, monkeypatch)
    refusal = RuntimeError("process admission refused")

    def rejected(**grants):
        assert grants == {"heap_bytes": 64 * 1024**2, "native_limits": (1, 2)}
        raise refusal

    monkeypatch.setattr(main, "WindowsSqliteProcess", rejected)
    for name in ["UploadSqliteResources", "prepare_primary_upload", "client"]:
        monkeypatch.setattr(main, name, forbidden)
    result = CliRunner().invoke(
        main.app,
        [
            "collection",
            "upload",
            "start",
            str(source),
            "--windows-primary",
            json.dumps(policy_payload(parent)),
        ],
    )
    assert result.exception is refusal
    assert not parent.exists()


def test_windows_command_dry_run_opens_no_process_or_state_even_with_unparsed_policy(
    tmp_path, monkeypatch
):
    source, parent = command_fixture(tmp_path, monkeypatch)
    (source / "empty").write_bytes(b"")
    for name in [
        "WindowsSqliteProcess",
        "UploadSqliteResources",
        "prepare_primary_upload",
        "client",
    ]:
        monkeypatch.setattr(main, name, forbidden)
    result = CliRunner().invoke(
        main.app,
        [
            "collection",
            "upload",
            "start",
            str(source),
            "--windows-primary",
            "unparsed fixture",
            "--dry-run",
            "--json",
        ],
    )
    assert result.exit_code == 0, result.exception
    payload = json.loads(result.stdout)
    assert payload["artifact_count"] == 1 and payload["total_bytes"] == 0
    assert not parent.exists()


def exercise_selected_command(tmp_path, monkeypatch, member_count, native):
    source, parent = command_fixture(tmp_path, monkeypatch)
    if native:
        import importlib.util

        spec = importlib.util.spec_from_file_location(
            "command_parent",
            Path(__file__).resolve().parents[1] / "platform/windows_primary_process.py",
        )
        parents = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = parents
        spec.loader.exec_module(parents)
        parents._parent(parent)
    else:
        parent.mkdir()
    contents = [b"" if index % 3 == 0 else b"equal bytes" for index in range(member_count)]
    for index, content in enumerate(contents):
        (source / f"member-{index}").write_bytes(content)
    payload = policy_payload(parent)
    payload["readiness"]["prefix_lengths"] = [8201, 16]
    api = _CustodyApi()
    first_registration_sizes = []
    actual_register = api.register_collection_upload_session_artifacts

    def register(collection_id, artifacts, **options):
        new_members = sum(row["artifact_id"] not in api.rows for row in artifacts)
        if new_members:
            first_registration_sizes.append(new_members)
        return actual_register(collection_id, artifacts, **options)

    monkeypatch.setattr(api, "register_collection_upload_session_artifacts", register)
    events, processes, resources, projections, identities = [], [], [], [], []
    actual_process = main.WindowsSqliteProcess
    actual_prepare = main.prepare_primary_upload

    def selected_process(**grants):
        assert grants == {"heap_bytes": payload["heap_bytes"], "native_limits": (1, 2)}
        events.append("process")
        if not processes:
            processes.append(actual_process(**grants) if native else _SqlOnlyProcess())
        # A cold state reopen retains the one admitted process policy.
        return processes[0]

    class Resources(main.UploadSqliteResources):
        def __init__(self, policy, **options):
            assert events[-1] == "process"
            assert options["process"] is processes[0]
            assert options["temporary_storage"] == options["projection_storage"] == "MEMORY"
            super().__init__(policy, **options)
            resources.append(self)
            events.append("resources")

        def connect(self, database, **options):
            connection = super().connect(database, **options)
            with closing(connection.execute("PRAGMA main.database_list")) as cursor:
                files = [row[2] for row in cursor]
            projections.append(files)
            return connection

    @contextmanager
    def admitted(*args, **options):
        assert events[-1] == "resources"
        assert options["windows"].process is processes[0]
        assert options["windows"].resources is resources[-1]
        with actual_prepare(*args, **options) as (upload, store):
            identities.append(tuple(read_upload_sources(upload)))
            events.append("state")
            yield upload, store

    def client():
        assert events[-1] == "state"
        events.append("api")
        return api

    monkeypatch.setattr(main, "WindowsSqliteProcess", selected_process)
    monkeypatch.setattr(main, "UploadSqliteResources", Resources)
    monkeypatch.setattr(main, "prepare_primary_upload", admitted)
    monkeypatch.setattr(main, "client", client)
    monkeypatch.setattr(directory_upload, "_state_path", forbidden)
    args = [
        "collection",
        "upload",
        "start",
        str(source),
        "--idempotency-key",
        "retained",
        "--tag",
        "z",
        "--tag",
        "a",
        "--sqlite-resources",
        json.dumps({"connections": 2, "cursors": 2, "operations": 1, "blobs": 1}),
        "--json",
    ]
    if member_count > 1:
        args += ["--windows-primary", json.dumps(payload)]
    else:
        monkeypatch.setenv("A_RIVERHOG_CLI_WINDOWS_PRIMARY", json.dumps(payload))
    first = CliRunner().invoke(main.app, [*args, "--initialize-state"])
    if member_count >= 16:
        assert isinstance(first.exception, PrimaryStateCapacity)
        assert "payload would consume progress headroom" in str(first.exception)
        assert api.session_calls == 1 and api.rows == {} and api.completed is None
        assert len(identities) == 1
        monkeypatch.setattr(directory_upload.secrets, "token_hex", forbidden)
        # Replenish physical backing within the original database/journal grants.
        # Keep the progress floor and all logical, heap and operation limits.
        for target_pages in (256, 384, 512):
            payload["readiness"]["target_pages"] = target_pages
            args[args.index("--windows-primary") + 1] = json.dumps(payload)
            first = CliRunner().invoke(main.app, args)
            if first.exit_code == 0:
                break
            assert isinstance(first.exception, PrimaryStateCapacity)
            assert "payload would consume progress headroom" in str(first.exception)
            assert api.session_calls == 1 and api.completed is None
    assert first.exit_code == 0, first.exception
    original = tuple(api.rows.items())
    assert len(original) == member_count
    assert len({row["artifact_id"] for _, row in original}) == member_count
    assert sorted(int(row["bytes"]) for _, row in original) == sorted(map(len, contents))
    if member_count > 1:
        equal = [row for _, row in original if row["bytes"] != "0"]
        assert len({row["sha256"] for row in equal}) == 1
        assert len({row["artifact_id"] for row in equal}) == len(equal)
    assert first_registration_sizes == [16] * (member_count // 16) + (
        [member_count % 16] if member_count % 16 else []
    )
    assert api.initial_tags == ("a", "z")
    monkeypatch.setattr(directory_upload.secrets, "token_hex", forbidden)
    resumed = CliRunner().invoke(main.app, args)
    assert resumed.exit_code == 0, resumed.exception
    first_receipt, resumed_receipt = json.loads(first.stdout), json.loads(resumed.stdout)
    for key in ("state", "artifact_set_identity", "archive_root_sha256", "collection"):
        assert resumed_receipt[key] == first_receipt[key]
    assert all(value == identities[0] for value in identities)
    assert tuple(api.rows.items()) == original and api.session_calls == 1
    monkeypatch.setattr(directory_upload, "_verified_upload_sources", forbidden)
    monkeypatch.setattr(directory_upload, "_verified_ordered_upload_sources", forbidden)
    conflict = CliRunner().invoke(main.app, [*args, "--initialize-state"])
    assert isinstance(conflict.exception, FileExistsError)
    assert api.session_calls == 1
    assert len(resources) == len(identities) + 1
    for index, owner in enumerate(resources):
        status = owner.status
        expected_peak = 0 if index == len(resources) - 1 else 2
        assert status.connection_limit == status.cursor_limit == 2
        assert status.operation_limit == status.blob_limit == 1
        assert status.connection_peak == status.cursor_peak == expected_peak
        assert status.operation_peak == status.blob_peak == bool(expected_peak)
        assert status.connections == status.cursors == status.operations == status.blobs == 0
        assert status.unreturned_connections == status.closing_connections == 0
        assert status.unreturned_blobs == 0
    assert all(owner.pending_directories == () for owner in resources)
    assert projections and all(all(name == "" for name in files) for files in projections)
    assert list(parent.glob("*.sqlite3"))
    assert not (tmp_path / "unused-posix").exists()
    assert [(source / f"member-{index}").read_bytes() for index in range(member_count)] == contents
    if native:
        heap = processes[0].report()
        assert (
            0 < heap["sqlite_heap_peak_bytes"] <= heap["heap_limit_bytes"] == payload["heap_bytes"]
        )
        assert processes[0].native_report()["owners"] == processes[0].native_report()["files"] == 0


@pytest.mark.skipif(os.name != "posix", reason="actual Unix-SQL command integration fixture")
@pytest.mark.parametrize("member_count", [1, 3, 16, 17])
def test_selected_cli_command_completes_and_cold_reopens_original_state(
    tmp_path, monkeypatch, member_count
):
    exercise_selected_command(tmp_path, monkeypatch, member_count, False)


@pytest.mark.skipif(os.name != "nt", reason="qualified Windows engine and private local NTFS")
@pytest.mark.parametrize("member_count", [1, 3, 16, 17])
def test_native_windows_cli_command_completes_and_cold_reopens_original_state(
    tmp_path, member_count
):
    code = """
import json,sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
import pytest
from tests.unit.test_cli_upload_windows_policy import exercise_selected_command
with pytest.MonkeyPatch.context() as patch:
    exercise_selected_command(Path(sys.argv[2]), patch, int(sys.argv[3]), True)
print(json.dumps({"passed": True, "actual_windows_sqlite": True}))
"""
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            code,
            str(Path(__file__).resolve().parents[2]),
            str(tmp_path),
            str(member_count),
        ],
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(result.stdout)["actual_windows_sqlite"]
