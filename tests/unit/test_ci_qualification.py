from __future__ import annotations

import json
import re
import subprocess
from concurrent.futures import ThreadPoolExecutor
from threading import Event, Lock
from types import SimpleNamespace

import pytest
import yaml

from scripts import ci_qualification as qualification
from scripts import qualify_stove0_processing as processing


@pytest.fixture
def graph() -> dict:
    dockerfiles = {
        "riverhog": "riverhog/Dockerfile",
        "stove0": "some-implementations/stove0/application/server/Dockerfile",
        "observer": "some-implementations/stove0/observers/example/Dockerfile",
        "target": "some-implementations/stove0/targets/example/Dockerfile",
        "companion": "some-implementations/riverhog/storage/example/Dockerfile",
    }
    return {
        "group": {"default": {"targets": list(dockerfiles)}},
        "target": {
            name: {"dockerfile": path, "tags": [f"{name}:dev"]}
            for name, path in dockerfiles.items()
        },
    }


def test_coarse_image_groups_exhaust_the_native_bake_default(graph: dict) -> None:
    groups = qualification.image_groups(graph)
    observed = [target for targets in groups.values() for target in targets]
    assert all(groups.values())
    assert sorted(observed) == sorted(graph["group"]["default"]["targets"])
    assert len(observed) == len(set(observed))
    graph["target"]["new"] = {"dockerfile": "some-implementations/new/Dockerfile"}
    graph["group"]["default"]["targets"].append("new")
    with pytest.raises(ValueError, match="no qualification group"):
        qualification.image_groups(graph)


@pytest.mark.parametrize("targets", [[], ["riverhog", "riverhog"]])
def test_image_groups_reject_an_empty_or_duplicate_native_definition(
    graph: dict, targets: list[str]
) -> None:
    graph["group"]["default"]["targets"] = targets
    with pytest.raises(ValueError, match="nonempty and unique"):
        qualification.image_groups(graph)


@pytest.mark.parametrize("corruption", [None, "revision", "created", "version", "composition"])
def test_loaded_images_must_match_source_and_build_configuration(
    monkeypatch: pytest.MonkeyPatch, graph: dict, corruption: str | None
) -> None:
    monkeypatch.setattr(
        qualification,
        "build_settings",
        lambda *args, **kwargs: ["*.args.SOURCE_REVISION=current", "*.args.BUILD_CREATED=created"],
    )
    labels = {
        "org.opencontainers.image.revision": "current",
        "org.opencontainers.image.created": "created",
        "org.opencontainers.image.version": "development",
        "io.github.nashspence.riverhog.composition": "bundled-components",
    }
    if corruption:
        key = (
            "io.github.nashspence.riverhog.composition"
            if corruption == "composition"
            else f"org.opencontainers.image.{corruption}"
        )
        labels[key] = "wrong"
    monkeypatch.setattr(
        subprocess,
        "check_output",
        lambda *args, **kwargs: json.dumps(
            [{"Id": "sha256:" + "a" * 64, "Config": {"Labels": labels}}]
        ),
    )
    if corruption:
        with pytest.raises(ValueError, match="source/config inputs"):
            qualification.verify_images(["stove0"], graph, compose=True)
    else:
        qualification.verify_images(["stove0"], graph, compose=True)
        with pytest.raises(ValueError, match="cannot use the composed image"):
            qualification.verify_images(["stove0"], graph)


def test_a_prebuilt_cache_cannot_waive_image_verification(
    monkeypatch: pytest.MonkeyPatch, graph: dict
) -> None:
    monkeypatch.setenv("RIVERHOG_CI_IMAGES_PREBUILT", "1")
    monkeypatch.setattr(qualification, "run", lambda *args, **kwargs: pytest.fail("must stop"))

    def reject(*args, **kwargs):
        raise ValueError("stale image")

    monkeypatch.setattr(qualification, "verify_images", reject)
    with pytest.raises(ValueError, match="stale image"):
        qualification.qualify_images(["riverhog"], graph)


def test_a_cold_build_uses_current_inputs_before_verification(
    monkeypatch: pytest.MonkeyPatch, graph: dict
) -> None:
    monkeypatch.delenv("RIVERHOG_CI_IMAGES_PREBUILT", raising=False)
    events = []
    monkeypatch.setattr(
        qualification, "build_settings", lambda *args, **kwargs: ["*.args.SOURCE_REVISION=current"]
    )
    monkeypatch.setattr(qualification, "run", lambda command: events.append(command))
    monkeypatch.setattr(
        qualification, "verify_images", lambda *args, **kwargs: events.append("verified")
    )
    qualification.prepare_images(["riverhog"], graph)
    assert events == [
        [
            "docker",
            "buildx",
            "bake",
            "--file",
            "docker-bake.hcl",
            "--load",
            "--set",
            "*.args.SOURCE_REVISION=current",
            "riverhog",
        ],
        "verified",
    ]


def test_local_linux_qualification_delegates_every_shared_semantic_lane(
    monkeypatch: pytest.MonkeyPatch,
    graph: dict,
) -> None:
    commands = []
    monkeypatch.setattr(qualification, "run", lambda command, **kwargs: commands.append(command))
    monkeypatch.setattr(qualification.sys, "platform", "linux")
    monkeypatch.setattr(qualification, "bake_graph", lambda: graph)
    monkeypatch.setattr(qualification, "compose_targets", lambda *args: ["stove0"])
    monkeypatch.setattr(qualification, "prepare_images", lambda *args, **kwargs: None)
    qualification.linux_qualification()
    assert commands[0] == ["make", "client-platform-qualification"]
    assert {tuple(command) for command in commands} == {
        *(("make", target) for target in qualification.REPOSITORY_TARGETS),
        ("make", "unit-shards-check"),
        *(("make", "unit-shard", f"UNIT_SHARD={shard}") for shard in qualification.SHARDS),
        *(
            ("make", "image-qualification", f"IMAGE_GROUP={group}")
            for group in qualification.IMAGE_GROUPS
        ),
        *(
            ("make", "compose-shard", f"COMPOSE_LANE={lane}")
            for lane in qualification.COMPOSE_LANES
        ),
        ("make", "client-platform-qualification"),
    }
    assert len(commands) == len({tuple(command) for command in commands})


def test_linux_aggregate_propagates_failure_without_claiming_remaining_proof(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    commands = []

    def fail(command):
        commands.append(command)
        raise subprocess.CalledProcessError(7, command)

    monkeypatch.setattr(qualification, "run", fail)
    monkeypatch.setattr(qualification.sys, "platform", "linux")
    assert qualification.main(["linux"]) == 1
    assert len(commands) == 1
    assert "qualification failed" in capsys.readouterr().err


def test_independent_local_proofs_obey_the_host_bound_and_run_exactly_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release = Event()
    two_started = Event()
    lock = Lock()
    active = 0
    peak = 0
    observed = []

    def bounded(command, **kwargs):
        nonlocal active, peak
        with lock:
            observed.append(command)
            active += 1
            peak = max(active, peak)
            if active == 2:
                two_started.set()
        assert release.wait(5)
        with lock:
            active -= 1

    monkeypatch.setattr(qualification, "run", bounded)
    commands = [["make", str(index)] for index in range(5)]
    with ThreadPoolExecutor(max_workers=1) as caller:
        result = caller.submit(qualification.independent_proofs, commands, jobs=2)
        try:
            assert two_started.wait(5)
            assert not result.done()
        finally:
            release.set()
        result.result(timeout=5)
    assert peak == 2
    assert active == 0
    assert sorted(observed) == commands


def test_local_proof_failure_waits_for_started_fixture_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = Event()
    cleanup = Event()
    finished = Event()

    def proof(command, **kwargs):
        if command == ["make", "failed"]:
            assert started.wait(5)
            raise subprocess.CalledProcessError(7, command)
        started.set()
        assert cleanup.wait(5)
        finished.set()

    monkeypatch.setattr(qualification, "run", proof)
    with ThreadPoolExecutor(max_workers=1) as caller:
        result = caller.submit(
            qualification.independent_proofs, [["make", "failed"], ["make", "fixture"]], jobs=2
        )
        try:
            assert started.wait(5)
            assert not result.done()
        finally:
            cleanup.set()
        with pytest.raises(subprocess.CalledProcessError):
            result.result(timeout=5)
    assert finished.is_set()


def test_invalid_local_parallelism_stops_before_fixture_setup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(qualification.sys, "platform", "linux")
    monkeypatch.setattr(qualification, "run", lambda command: pytest.fail("must stop"))
    with pytest.raises(ValueError, match="must be positive"):
        qualification.linux_qualification(jobs=0)


def test_native_qualification_preserves_staged_install_and_bounded_failure_evidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    commands = []
    monkeypatch.delenv("RIVERHOG_QUALIFICATION_MOUNT_FIXTURE", raising=False)
    monkeypatch.delenv("RIVERHOG_QUALIFICATION_MOUNT_LOCK", raising=False)
    monkeypatch.setenv("GOGURT_FAILURE_EVIDENCE_DIR", "/tmp/qualification-evidence")
    monkeypatch.setattr(qualification, "run", lambda command: commands.append(command))
    qualification.native_platform()
    assert commands[0][3:] == ["-q", *qualification.NATIVE_TESTS]
    assert commands[1][-4:] == [
        "--listener-lifecycle-repetitions",
        "12",
        "--gogurt-evidence-dir",
        "/tmp/qualification-evidence",
    ]


def test_native_qualification_forwards_operator_fixture_without_changing_proof(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    commands = []
    monkeypatch.setenv("RIVERHOG_QUALIFICATION_MOUNT_FIXTURE", "/operator/fixture.json")
    monkeypatch.delenv("RIVERHOG_QUALIFICATION_MOUNT_LOCK", raising=False)
    monkeypatch.setattr(qualification, "run", lambda command: commands.append(command))
    qualification.native_platform()
    assert commands[1][-2:] == ["--listener-mount-fixture", "/operator/fixture.json"]
    assert commands[1][4:7] == ["--listener-lifecycle", "--listener-lifecycle-repetitions", "12"]


def test_native_qualification_forwards_the_shared_operator_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    commands = []
    monkeypatch.setenv("RIVERHOG_QUALIFICATION_MOUNT_FIXTURE", "/operator/fixture.json")
    monkeypatch.setenv("RIVERHOG_QUALIFICATION_MOUNT_LOCK", "/operator/shared.lock")
    monkeypatch.setattr(qualification, "run", lambda command: commands.append(command))
    qualification.native_platform()
    assert commands[1][-4:] == [
        "--listener-mount-fixture",
        "/operator/fixture.json",
        "--listener-mount-lock",
        "/operator/shared.lock",
    ]


def test_compose_lane_rejects_unknown_ownership_before_reading_declarations(graph: dict) -> None:
    with pytest.raises(ValueError, match="unknown Compose qualification"):
        qualification.compose_targets("unknown", graph)


def test_required_processing_lanes_select_only_their_canonical_runtime_images() -> None:
    filenames = (
        "riverhog/compose.yaml",
        "some-implementations/stove0/application/compose.yaml",
        "some-implementations/riverhog/ingress/ftp/compose.yaml",
        "some-implementations/riverhog/applications/a-riverhog-minisign-witness/compose.yaml",
        "some-implementations/riverhog/applications/a-riverhog-opentimestamps-witness/compose.yaml",
    )
    graph = {"target": {}}
    for filename in filenames:
        document = yaml.safe_load((qualification.ROOT / filename).read_text())
        for definition in document["services"].values():
            if "build" in definition:
                dockerfile = definition["build"]["dockerfile"]
                graph["target"][dockerfile] = {"dockerfile": dockerfile}
    selected = {
        lane: set(qualification.compose_targets(lane, graph))
        for lane in (*qualification.COMPOSE_LANES, "processing-scale", "all")
    }
    assert qualification.COMPOSE_LANES == (
        "storage",
        "ingress-custody",
        "processing-admission",
        "processing-e2e",
        "processing-overlap",
        "review-delivery",
        "witnesses",
    )
    assert selected["all"] == set().union(*(selected[lane] for lane in qualification.COMPOSE_LANES))
    media = selected["processing-admission"]
    assert all(selected[lane] == media for lane in qualification.PROCESSING_LANES)
    assert "some-implementations/stove0/application/server/Dockerfile" in media
    assert "some-implementations/stove0/targets/opus/Dockerfile" in media
    assert not any("review0" in path or "rclone" in path or "witness" in path for path in media)
    assert not any("stove0" in path for path in selected["ingress-custody"])
    assert selected["storage"] < selected["ingress-custody"] < media
    assert "processing-scale" not in qualification.COMPOSE_LANES


def test_composed_stove0_has_its_own_cache_scope_without_merging_target_inputs() -> None:
    settings = qualification.cache_settings(["stove0", "riverhog"], compose=True)
    assert settings == [
        "stove0.cache-from=type=gha,scope=stove0-compose",
        "stove0.cache-to=type=gha,scope=stove0-compose,mode=max,ignore-error=true",
        "riverhog.cache-from=type=gha,scope=riverhog",
        "riverhog.cache-to=type=gha,scope=riverhog,mode=max,ignore-error=true",
    ]


def test_required_compose_plan_owns_every_lifecycle_in_the_shared_harness() -> None:
    source = (qualification.ROOT / "scripts/test_compose_smoke.sh").read_text()
    selectors = set(re.findall(r"owns_qualification ([a-z0-9-]+)", source))
    assert selectors == set(qualification.COMPOSE_LANES) | {"processing-scale"}
    assert 'qualification_lane="${1:-all}"' in source
    for path in qualification.NATIVE_TESTS:
        assert (qualification.ROOT / path).exists()


def test_processing_wait_retries_a_timed_out_read_before_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("STOVE0_SMOKE_COMPLETION_TIMEOUT", "30")
    sleeps = []
    monkeypatch.setattr(
        processing, "time", SimpleNamespace(monotonic=lambda: 0, sleep=sleeps.append)
    )
    calls = []

    def read(path):
        calls.append(path)
        if len(calls) == 1:
            raise TimeoutError("status read timed out")
        return {"work": [{"phase": "complete", "output": {"collection_id": "1"}}]}

    monkeypatch.setattr(processing, "stove", read)
    processing.wait()
    assert len(calls) == 2
    assert calls[0] == calls[1]
    assert sleeps == [0.5]


@pytest.mark.parametrize("phase", ["failed", "canceled", "inapplicable", "abandon_pending"])
def test_processing_wait_rejects_terminal_work_after_a_successful_read(
    monkeypatch: pytest.MonkeyPatch, phase: str
) -> None:
    monkeypatch.setenv("STOVE0_SMOKE_COMPLETION_TIMEOUT", "30")
    monkeypatch.setattr(processing, "time", SimpleNamespace(monotonic=lambda: 0))
    monkeypatch.setattr(
        processing, "stove", lambda path: {"work": [{"work_id": "work", "phase": phase}]}
    )
    with pytest.raises(RuntimeError, match=phase):
        processing.wait()


def test_processing_wait_still_fails_at_its_global_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("STOVE0_SMOKE_COMPLETION_TIMEOUT", "1")
    ticks = iter([0.0, 0.0, 2.0])
    monkeypatch.setattr(
        processing, "time", SimpleNamespace(monotonic=lambda: next(ticks), sleep=lambda _: None)
    )
    calls = []

    def unavailable(path, payload=None):
        calls.append((path, payload))
        if payload is None:
            raise TimeoutError("status read timed out")
        return {"status": "observed"}

    monkeypatch.setattr(processing, "stove", unavailable)
    with pytest.raises(TimeoutError, match="observed"):
        processing.wait()
    assert len(calls) == 2
    assert calls[1] == ("/v1/admin/scheduler/run", {"role": "controller", "work_limit": 25})


@pytest.mark.parametrize("initiation", [False, True])
def test_processing_planning_drives_pollable_control_until_exact_completion(
    monkeypatch: pytest.MonkeyPatch, initiation: bool
) -> None:
    monkeypatch.setenv("STOVE0_SMOKE_COMPLETION_TIMEOUT", "30")
    monkeypatch.setattr(
        processing, "time", SimpleNamespace(monotonic=lambda: 0, sleep=lambda _: None)
    )
    path = "/v1/work-initiations/job" if initiation else "/v1/workflow-previews/job"
    identity = {"job_id": "a" * 64, "work_id": "b" * 64}
    observing = {**identity, "state": "observing"}
    completed = {**identity, "state": "completed", "exact_result": "unchanged"}
    if initiation:
        observing = {"state": "pending", "job": observing}
        completed = {
            "state": "completed",
            "job": {**identity, "state": "admitting"},
            "exact_result": "unchanged",
        }
    states = iter([observing, completed])
    calls = []

    def read(operation, payload=None):
        calls.append((operation, payload))
        return {"progressed": []} if payload is not None else next(states)

    monkeypatch.setattr(processing, "stove", read)
    assert processing.await_planning(path, initiation=initiation) == completed
    assert calls == [
        ("/v1/admin/scheduler/run", {"role": "controller", "work_limit": 25}),
        (path, None),
        ("/v1/admin/scheduler/run", {"role": "controller", "work_limit": 25}),
        (path, None),
    ]


def test_processing_initiation_rejection_does_not_become_success(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(processing, "time", SimpleNamespace(monotonic=lambda: 0))
    monkeypatch.setattr(
        processing,
        "stove",
        lambda *_: {
            "state": "rejected",
            "job": {"job_id": "a" * 64, "work_id": "b" * 64, "state": "completed"},
        },
    )
    with pytest.raises(RuntimeError, match="rejected"):
        processing.await_planning("/v1/work-initiations/job", initiation=True)


def test_processing_progress_reports_durable_phases_during_long_waits(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    monkeypatch.setenv("STOVE0_SMOKE_COMPLETION_TIMEOUT", "300")
    ticks = iter([0, 1, 2, 62, 63])
    monkeypatch.setattr(
        processing, "time", SimpleNamespace(monotonic=lambda: next(ticks), sleep=lambda _: None)
    )
    rows = iter(
        [
            {"work_id": "work", "phase": "output_finalizing"},
            {"work_id": "work", "phase": "output_finalizing"},
            {"work_id": "work", "phase": "output_finalizing"},
            {"work_id": "work", "phase": "complete", "output": {"collection_id": "1"}},
        ]
    )
    monkeypatch.setattr(processing, "stove", lambda _: {"work": [next(rows)]})
    processing.wait()
    progress = [json.loads(line) for line in capsys.readouterr().err.splitlines()]
    assert len(progress) == 2
    assert [row["elapsed_seconds"] for row in progress] == [1, 62]
    assert all(row["work"][0]["phase"] == "output_finalizing" for row in progress)


@pytest.mark.parametrize("ready_at", [902, None])
def test_admission_wait_retains_pending_work_past_the_old_timeout_and_reports_planning(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture, ready_at: int | None
) -> None:
    monkeypatch.setenv("INPUT_COLLECTION_ID", "1")
    monkeypatch.setenv("RIVERHOG_SMOKE_SCHEDULER_STEP", "previewed")
    monkeypatch.setenv("STOVE0_SMOKE_ADMISSION_TIMEOUT", "1860")
    ticks = iter([0, 1, ready_at if ready_at is not None else 1861])
    monkeypatch.setattr(
        processing, "time", SimpleNamespace(monotonic=lambda: next(ticks), sleep=lambda _: None)
    )
    contacts = []
    scans = 0

    def read(path, payload=None, *, timeout=30):
        nonlocal scans
        contacts.append((path, timeout))
        if payload is not None:
            return {
                "admission": {"failures": []},
                "previews": {"progressed": ["job"], "failures": []},
            }
        if path.startswith("/v1/workflow-previews/"):
            return {"job_id": "job", "state": "observing", "result": None}
        scans += 1
        return {
            "admissions": [
                {
                    "intent": {"collection": {"collection_id": "1"}, "admission_id": "admission"},
                    "state": "intent" if scans == 1 else "previewed",
                    "attempt_count": 0,
                    "failure": None,
                }
            ]
        }

    monkeypatch.setattr(processing, "stove", read)
    if ready_at is not None:
        processing.await_admission()
        assert scans == 2
    else:
        with pytest.raises(AssertionError) as error:
            processing.await_admission()
        diagnostic = json.loads(str(error.value))
        assert diagnostic["expected"] == "previewed"
        assert diagnostic["planning"] == [{"job_id": "job", "state": "observing", "outcome": None}]
        assert diagnostic["state"] == "intent"
    progress = [json.loads(line) for line in capsys.readouterr().err.splitlines()]
    assert progress[0]["planning"][0]["state"] == "observing"
    assert progress[0]["attempt_count"] == 0 and progress[0]["failure"] is None
    assert all(timeout == 5 for _, timeout in contacts)


def test_admission_wait_does_not_retry_a_reported_admission_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("INPUT_COLLECTION_ID", "1")
    monkeypatch.setenv("RIVERHOG_SMOKE_SCHEDULER_STEP", "previewed")
    monkeypatch.setenv("STOVE0_SMOKE_ADMISSION_TIMEOUT", "1860")
    monkeypatch.setattr(processing, "time", SimpleNamespace(monotonic=lambda: 0))
    contacts = []

    def read(path, payload=None, *, timeout=30):
        contacts.append((path, timeout))
        return {"admission": {"failures": [{"error": "backend-failure"}]}}

    monkeypatch.setattr(processing, "stove", read)
    with pytest.raises(AssertionError, match="backend-failure"):
        processing.await_admission()
    assert contacts == [("/v1/admin/scheduler/run", 5)]


@pytest.mark.parametrize("state", ["intent", "previewed", "work_bound"])
def test_automatic_execution_requires_the_exact_durable_admission_binding(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture, state: str
) -> None:
    monkeypatch.setenv("INPUT_COLLECTION_ID", "7")
    row = {
        "intent": {"collection": {"collection_id": "7"}, "policy_id": "conformance-media"},
        "state": state,
        "work_id": "automatic-work",
    }
    calls = []

    def read(path, *, timeout=30):
        calls.append(path)
        if path.startswith("/v1/admissions"):
            return {"admissions": [row]}
        return {"work_id": "automatic-work"}

    monkeypatch.setattr(processing, "stove", read)
    if state != "work_bound":
        with pytest.raises(AssertionError, match="durably work_bound"):
            processing.admission_work()
        assert len(calls) == 1
    else:
        processing.admission_work()
        assert calls[-1] == "/v1/work/automatic-work"
        assert capsys.readouterr().out.strip() == "automatic-work"


@pytest.mark.parametrize("matches", [[], [{"intent": {"collection": {"collection_id": "8"}}}]])
def test_automatic_execution_does_not_substitute_another_input(
    monkeypatch: pytest.MonkeyPatch, matches: list
) -> None:
    monkeypatch.setenv("INPUT_COLLECTION_ID", "7")
    monkeypatch.setattr(processing, "stove", lambda *args, **kwargs: {"admissions": matches})
    with pytest.raises(AssertionError, match="exact input"):
        processing.admission_work()
