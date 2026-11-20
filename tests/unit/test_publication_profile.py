from __future__ import annotations

import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest
from riverhog_archive_contracts import BOUND_HISTORY_EXTENT, RETAINED_HISTORY_EXTENT

from scripts import performance_measurement as performance
from scripts import transfer_profile, validation_identity, validation_process
from tests.support.qualification import canonical_history_scale, publication_profile
from tests.support.qualification.publication_geometry import (
    WORKLOAD_FORMAT,
    PublicationGeometry,
    read_workload,
)
from tests.unit import test_operation_lifecycle_api as local_api_fixture


def document() -> dict[str, object]:
    return {
        "format": WORKLOAD_FORMAT,
        "payload_sizes": [0, 17],
        "shared_root_members": 2,
        "shared_claim_members": 1,
        "retained_journals": 2,
        "required_journals": 0,
    }


@pytest.mark.parametrize(
    "fault",
    ["unknown-field", "format", "payload-bool", "empty", "fanout", "required-pool", "documentary"],
)
def test_workload_rejects_invalid_geometry_before_starting_a_fixture(
    tmp_path: Path,
    fault: str,
) -> None:
    value = document()
    if fault == "unknown-field":
        value["unknown"] = 1
    elif fault == "format":
        value["format"] = "invalid"
    elif fault == "payload-bool":
        value["payload_sizes"] = [True, 17]
    elif fault == "empty":
        value["payload_sizes"] = []
    elif fault == "fanout":
        value["shared_root_members"] = 3
    elif fault == "required-pool":
        value["required_journals"] = 3
    else:
        value["shared_claim_members"] = 3
    path = tmp_path / "workload.json"
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError):
        read_workload(path)


@pytest.mark.parametrize("refusal", ["receipt", "api", "container"])
def test_profile_preserves_the_original_refusal_and_attempts_both_resource_receivers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    refusal: str,
) -> None:
    original = RuntimeError("receipt receiver refused")
    close_refusal = RuntimeError("resource receiver refused")
    received = []

    def close(name):
        received.append(name)
        if name == refusal:
            raise close_refusal

    fixture = SimpleNamespace(
        artifact_ids=("a" * 64,),
        archive_root_sha256="b" * 64,
        api=SimpleNamespace(close=lambda: close("api")),
        container=SimpleNamespace(close=lambda: close("container")),
    )
    monkeypatch.setattr(publication_profile, "publish_shared_history", lambda *_a, **_k: fixture)
    phases = []

    def phase(value):
        phases.append(value["phase"])
        if value["phase"] == "publication-received":
            raise original

    previous = os.environ.get("RIVERHOG_CONFIG")
    workspace = tmp_path / "owned-workspace"
    with pytest.raises(RuntimeError) as caught:
        publication_profile.run_publication_profile(
            PublicationGeometry((17,), 1),
            workspace,
            record_phase=phase,
        )
    assert caught.value is (original if refusal == "receipt" else close_refusal)
    if refusal != "receipt":
        assert caught.value.__context__ is original
    assert received == ["api", "container"]
    assert "cleanup-received" not in phases
    assert workspace.is_dir(), "workspace belongs to the caller receiving the fixture process"
    assert os.environ.get("RIVERHOG_CONFIG") == previous


@pytest.mark.parametrize(
    "fault",
    ["exit", "run-id", "unverified", "geometry", "source", "receipt-shape", "deadline"],
)
def test_parent_rejects_invalid_or_interrupted_completion_after_receiving_the_helper(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    source = {"source_sha": "a" * 40, "source_clean": True}
    identity_calls = 0

    def identity(_root):
        nonlocal identity_calls
        identity_calls += 1
        if fault == "source" and identity_calls > 1:
            return {"source_sha": "b" * 40, "source_clean": True}
        return source

    monkeypatch.setattr(validation_identity, "source_identity", identity)
    workspaces = []

    def run(command, *, cwd, env):
        assert command[2] == "tests.support.qualification.publication_profile"
        workspace = Path(command[4]).parent
        assert workspace.is_dir()
        assert env["RIVERHOG_CONFIG"] == str(Path(command[4]) / "riverhog.yaml")
        workspaces.append(workspace)
        selected = json.loads(Path(command[3]).read_text())
        selected.pop("format")
        observed = {
            "completion_verified": True,
            "workload": selected,
            "conditions": {"finalization_timeout_seconds": 900},
        }
        if fault == "deadline":
            assert command[9] == "3600"
        receipt = {
            "format": "riverhog-publication-completion/v1",
            "run_id": command[7],
            "source": source,
            "observed": observed,
        }
        if fault == "exit":
            return 9
        if fault == "run-id":
            receipt["run_id"] = "different"
        elif fault == "unverified":
            observed["completion_verified"] = False
        elif fault == "geometry":
            observed["workload"] = {**selected, "payload_sizes": [17]}
        elif fault == "receipt-shape":
            receipt["observed"] = []
        Path(command[5]).write_text(json.dumps(receipt))
        return 0

    monkeypatch.setattr(validation_process, "run_validation_child", run)
    workload = tmp_path / "workload.json"
    workload.write_text(json.dumps(document()))
    output = tmp_path / "profiles"
    with pytest.raises(performance.PerformanceError):
        transfer_profile._profile_collection_publication(
            workload,
            output,
            finalization_timeout_seconds=3600 if fault == "deadline" else None,
        )
    assert workspaces and all(not path.exists() for path in workspaces)
    assert not list(output.glob("*/profile.json"))
    phases = list(output.glob("*/phase.json"))
    assert len(phases) == 1
    failed = json.loads(phases[0].read_text())
    assert failed["phase"] == "failed"
    assert failed["child_received"] is True
    assert failed["run_id"] == phases[0].parent.name


def test_real_native_profile_verifies_one_collection_through_explicit_reader_cache_states(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sentinel = str(tmp_path / "unavailable-deployed-config")
    monkeypatch.setenv("RIVERHOG_CONFIG", sentinel)
    phases = []
    received_deadlines = []
    original_finish = canonical_history_scale.IncrementalCollectionProducer.finish
    original_api = canonical_history_scale._api

    def spawned_api(*args, **kwargs):
        api = original_api(*args, **kwargs)
        spawned = api.spawn()
        api.close()
        return spawned

    def unexpected_worker_wait(*_args, **_kwargs):
        raise AssertionError("publication status polling must not join archive workers")

    monkeypatch.setattr(canonical_history_scale, "_api", spawned_api)
    monkeypatch.setattr(local_api_fixture, "drive_custody_turn", unexpected_worker_wait)
    monkeypatch.setattr(local_api_fixture, "drive_finalization_turn", unexpected_worker_wait)

    def finish(producer, *args, **kwargs):
        received_deadlines.append(kwargs["timeout_seconds"])
        return original_finish(producer, *args, **kwargs)

    monkeypatch.setattr(canonical_history_scale.IncrementalCollectionProducer, "finish", finish)
    result = publication_profile.run_publication_profile(
        PublicationGeometry((17,), 1, 1, 1, shared_claim_members=1),
        tmp_path / "profile-workspace",
        record_phase=phases.append,
        finalization_timeout_seconds=3600,
    )
    assert received_deadlines == [3600]
    assert result["conditions"]["finalization_timeout_seconds"] == 3600
    assert result["conditions"]["archive_worker_progress"] == "bounded-coordinator-polls"
    assert os.environ["RIVERHOG_CONFIG"] == sentinel
    assert result["completion_verified"] is True
    assert phases[-1]["phase"] == "cleanup-received"
    assert any(p["phase"] == "append-inputs" and p.get("completed") == 1 for p in phases)
    assert any(p["phase"] == "member-histories-staged" for p in phases)
    measurements = result["measurements"]
    assert [m["native_cache_state"] for m in measurements] == [
        "fresh-native-reader",
        "warm-native-reader",
        "evicted-native-reader",
    ]
    roots = set()
    for measurement in measurements:
        verified = measurement["verified"]
        roots.add(verified["archive_root_sha256"])
        assert verified["members"] == 1 and verified["payload_bytes"] == 17
        assert verified["physical_journals"] == 3
        assert verified["shared_documentary_primaries"] == 1
        for extent in (BOUND_HISTORY_EXTENT, RETAINED_HISTORY_EXTENT):
            assert verified["extents"][extent]["unique_journals"] == 3
        assert measurement["actual_stored_read_calls"] >= measurement["distinct_stored_objects"] > 0
        assert measurement["actual_stored_octets"] > 0
        assert measurement["actual_stored_octets_per_second"] == pytest.approx(
            measurement["actual_stored_octets"] / verified["verification_seconds"]
        )
        assert measurement["verified_payload_bytes_per_second"] == pytest.approx(
            verified["payload_bytes"] / verified["verification_seconds"]
        )
        assert verified["stored_publication"]["part_checks"] > 0
    assert len(roots) == 1
    assert result["conditions"]["production_memory_qualification"] is False
    assert result["conditions"]["independent_installed_recovery"] is False
    assert result["publication_verified_payload_bytes_per_second"] == pytest.approx(
        17 / result["publication_seconds"]
    )
    assert result["publication_verified_members_per_second"] == pytest.approx(
        1 / result["publication_seconds"]
    )


@pytest.mark.parametrize("seconds", [0, -1, True])
def test_profile_rejects_invalid_resource_deadlines_before_creating_workspace(
    tmp_path: Path,
    seconds: int,
) -> None:
    workspace = tmp_path / "profile"
    with pytest.raises(ValueError):
        publication_profile.run_publication_profile(
            PublicationGeometry((17,), 1),
            workspace,
            record_phase=lambda _value: None,
            finalization_timeout_seconds=seconds,
        )
    assert not workspace.exists()


@pytest.mark.parametrize(
    "argv",
    [
        [
            "--scenario",
            "collection-publication",
            "--publication-workload",
            "workload.json",
            "--finalization-timeout-seconds",
            "0",
        ],
        ["--scenario", "stove0-scale", "--finalization-timeout-seconds", "3600"],
        ["--scenario", "riverhog-ingress", "--finalization-timeout-seconds", "3600"],
    ],
)
def test_resource_deadline_argument_requires_a_positive_publication_scenario(
    argv: list[str],
) -> None:
    parser = transfer_profile._parser()
    with pytest.raises(SystemExit) as caught:
        args = parser.parse_args(argv)
        transfer_profile._validate_args(args, parser)
    assert caught.value.code == 2
