from __future__ import annotations

import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event, Lock

import pytest

from scripts import ci_qualification as qualification


def test_docker_bound_does_not_occupy_slots_needed_by_independent_unit_proofs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release, saturated = Event(), Event()
    lock = Lock()
    active = heavy = peak = heavy_peak = 0
    observed = []
    projects, temporaries = [], []
    monkeypatch.setenv("COMPOSE_PROJECT_NAME", "inherited-project")
    monkeypatch.setenv("RIVERHOG_API_PORT", "31337")

    def proof(command, *, env):
        nonlocal active, heavy, peak, heavy_peak
        docker = command[1] == "compose-shard"
        with lock:
            observed.append(tuple(command))
            active += 1
            heavy += docker
            peak, heavy_peak = max(peak, active), max(heavy_peak, heavy)
            if docker:
                assert env["RIVERHOG_CI_IMAGES_PREBUILT"] == "1"
                assert env["RIVERHOG_API_PORT"] == "0"
                assert env["COMPOSE_PROJECT_NAME"] != "inherited-project"
                assert env["TEST_COMPOSE_PROJECT_NAME"] == env["COMPOSE_PROJECT_NAME"]
                projects.append(env["COMPOSE_PROJECT_NAME"])
                temporary = Path(env["TMPDIR"])
                assert temporary.is_dir()
                (temporary / "private-proof").write_bytes(b"owned")
                temporaries.append(temporary)
            if active == 3:
                assert heavy == 2
                saturated.set()
        assert release.wait(5)
        with lock:
            active -= 1
            heavy -= docker

    monkeypatch.setattr(qualification, "run", proof)
    commands = [
        *(
            ["make", "compose-shard", f"COMPOSE_LANE={lane}"]
            for lane in qualification.COMPOSE_LANES
        ),
        ["make", "unit-shard", "UNIT_SHARD=unit"],
    ]
    with ThreadPoolExecutor(max_workers=1) as caller:
        result = caller.submit(
            qualification.independent_proofs,
            commands,
            jobs=3,
            docker_jobs=2,
            images_prepared=True,
        )
        try:
            assert saturated.wait(5)
        finally:
            release.set()
        result.result(timeout=5)
    assert peak == 3 and heavy_peak == 2
    assert active == heavy == 0
    assert sorted(observed) == sorted(map(tuple, commands))
    assert len(projects) == len(set(projects)) == len(qualification.COMPOSE_LANES)
    assert len(temporaries) == len(set(temporaries))
    assert all(not directory.exists() for directory in temporaries)


def test_compose_aggregate_prepares_one_image_barrier_before_isolated_scenarios(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    graph = {"prepared": False}
    observed, barriers = [], []
    monkeypatch.setattr(qualification, "bake_graph", lambda: graph)
    monkeypatch.setattr(qualification, "compose_targets", lambda lane, _: [lane])

    def prepare(targets, declaration, *, compose):
        assert targets == ["all"] and declaration is graph and compose is True
        barriers.append(targets)
        graph["prepared"] = True

    def proof(command, *, env):
        assert graph["prepared"]
        assert env["RIVERHOG_CI_IMAGES_PREBUILT"] == "1"
        observed.append(command[2].removeprefix("COMPOSE_LANE="))

    monkeypatch.setattr(qualification, "prepare_images", prepare)
    monkeypatch.setattr(qualification, "run", proof)
    qualification.compose_qualification(jobs=3, docker_jobs=2)
    assert barriers == [["all"]]
    assert sorted(observed) == sorted(qualification.COMPOSE_LANES)


def test_parallel_compose_cannot_bypass_image_preparation(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(qualification, "run", lambda *a, **k: pytest.fail("must stop"))
    with pytest.raises(ValueError, match="image barrier"):
        qualification.independent_proofs(
            [["make", "compose-shard", "COMPOSE_LANE=storage"]], jobs=2
        )


@pytest.mark.parametrize("jobs,docker_jobs", [(0, 2), (2, 0), (-1, 2), (2, -1)])
def test_invalid_resource_budget_stops_before_build_or_fixture_setup(
    monkeypatch: pytest.MonkeyPatch, jobs: int, docker_jobs: int
) -> None:
    monkeypatch.setattr(qualification.sys, "platform", "linux")
    monkeypatch.setattr(qualification, "bake_graph", lambda: pytest.fail("must stop"))
    monkeypatch.setattr(qualification, "run", lambda *a, **k: pytest.fail("must stop"))
    with pytest.raises(ValueError, match="must be positive"):
        qualification.linux_qualification(jobs=jobs, docker_jobs=docker_jobs)
    with pytest.raises(ValueError, match="must be positive"):
        qualification.compose_qualification(jobs=jobs, docker_jobs=docker_jobs)


def test_failed_compose_proof_cleans_its_private_scratch(monkeypatch: pytest.MonkeyPatch) -> None:
    owned = []

    def fail(command, *, env):
        temporary = Path(env["TMPDIR"])
        owned.append(temporary)
        (temporary / "partially-created").write_bytes(b"private")
        raise subprocess.CalledProcessError(7, command)

    monkeypatch.setattr(qualification, "run", fail)
    with pytest.raises(subprocess.CalledProcessError):
        qualification.independent_proofs(
            [["make", "compose-shard", "COMPOSE_LANE=storage"]],
            jobs=2,
            docker_jobs=1,
            images_prepared=True,
        )
    assert owned and all(not path.exists() for path in owned)
