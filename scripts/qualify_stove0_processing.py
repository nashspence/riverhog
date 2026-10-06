"""Assert exact processing plans and settled identities in the built Compose services.

The Compose harness executes this source inside the API or Opus container.
Only maintained contract models and the published operator API establish proof.
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.request
from pathlib import Path
from typing import Any

from riverhog_protocol import canonical_json_bytes, canonical_json_sha256
from stove0_protocol import CollectionRootIdentityRef, WorkflowPreview
from stove0_target_protocol import AcceptedTargetJob, TargetJobStatus, TransformPlan


def stove(path: str, payload: Any = None) -> dict[str, Any]:
    request = urllib.request.Request(
        "http://127.0.0.1:8080" + path,
        data=None if payload is None else canonical_json_bytes(payload),
        headers={
            "Authorization": "Bearer stove0-compose-smoke-token",
            "Content-Type": "application/json",
        },
    )
    # Large scale fixtures perform the same complete bulk operation; their
    # finite request budget grows without changing ordinary CI fixture budgets.
    timeout = (
        300 + 30 * max(0, int(os.environ["STOVE0_SMOKE_FILE_COUNT"]) - 16)
        if payload is not None
        else 30
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        document = json.load(response)
    assert isinstance(document, dict)
    return document


def recipe_id() -> str:
    return os.environ.get("STOVE0_SMOKE_RECIPE_ID", "stove0.conformance-media/v1")


def route_ids() -> set[str]:
    if recipe_id() == "stove0.audio-archive/v1":
        return {"archive-audio"}
    assert recipe_id() == "stove0.conformance-media/v1"
    return {"archive-audio", "archive-audio-overlap"}


def work_diagnostic(row: dict[str, Any]) -> dict[str, Any]:
    target = row.get("target_status") or {}
    return {
        "work_id": row.get("work_id"),
        "phase": row.get("phase"),
        "revision": row.get("revision"),
        "failure": row.get("failure"),
        "inapplicable": row.get("inapplicable"),
        "abandon_outcome": row.get("abandon_outcome"),
        "target_state": target.get("state"),
        "target_attempt": target.get("attempt"),
        "target_failure": target.get("failure"),
        "target_inapplicable": target.get("inapplicable"),
    }


def wait() -> None:
    # A slow status read does not establish a work outcome. Retry only timed-out
    # reads within the same finite fixture deadline; terminal work still fails.
    deadline = time.monotonic() + int(os.environ["STOVE0_SMOKE_COMPLETION_TIMEOUT"])
    rows: list[dict[str, Any]] = []
    while time.monotonic() < deadline:
        try:
            rows = stove("/v1/work?page_size=100&sort=updated_at&order=asc")["work"]
        except TimeoutError:
            time.sleep(0.5)
            continue
        terminal_failure = next(
            (
                row
                for row in rows
                if row["phase"] in {"failed", "canceled", "inapplicable", "abandon_pending"}
            ),
            None,
        )
        if terminal_failure is not None:
            raise RuntimeError(canonical_json_bytes(work_diagnostic(terminal_failure)).decode())
        if rows and all(row["phase"] == "complete" for row in rows):
            assert any(int((row.get("output") or {}).get("collection_id") or 0) > 0 for row in rows)
            return
        time.sleep(0.5)
    scheduler = stove("/v1/admin/scheduler/run", {"role": "controller", "work_limit": 25})
    raise TimeoutError(
        canonical_json_bytes(
            {"work": [work_diagnostic(row) for row in rows], "scheduler": scheduler}
        ).decode()
    )


def declared_values(actual: Any, declared: Any) -> None:
    """Check every explicit route value while allowing typed contract defaults."""

    if isinstance(declared, dict):
        assert isinstance(actual, dict)
        for name, value in declared.items():
            declared_values(actual[name], value)
    else:
        assert actual == declared


def invoke() -> None:
    from stove0_operator_contracts import (
        OperatorWorkflowPreviewRequest,
        RecipeCatalogView,
        WorkCreateRequest,
        WorkView,
    )

    receipt = json.loads(os.environ["RIVERHOG_INPUT_RECEIPT"])
    root = CollectionRootIdentityRef.model_validate(
        {
            name: receipt[name]
            for name in ("collection_id", "archive_root_sha256", "artifact_set_identity")
        }
    )
    catalog = RecipeCatalogView.model_validate(stove("/v1/recipes"))
    recipe = next(row for row in catalog.recipes if row.definition.id == recipe_id())
    preview = WorkflowPreview.model_validate(
        stove(
            "/v1/workflow-previews",
            OperatorWorkflowPreviewRequest(recipe_id=recipe_id(), inputs=(root,)).model_dump(
                mode="json", exclude_none=True
            ),
        )
    )
    assert preview.state == "ready", preview.state
    assert preview.work.recipe.id == recipe.definition.id
    assert preview.work.recipe.revision == recipe.definition.revision
    assert preview.work.recipe.sha256 == recipe.sha256
    assert preview.work.inputs == (root,)
    assert preview.branch_set_plan is not None
    branches = {row.branch_id: row for row in preview.branch_set_plan.branches}
    assert set(branches) == route_ids(), set(branches)
    targets = {row.branch_id: row for row in preview.target_plans}
    assert set(targets) == route_ids(), set(targets)
    assert len({row.work_id for row in targets.values()}) == len(targets)
    selections = {row.selection_sha256: row for row in preview.selections}
    expected_audio = int(os.environ["STOVE0_SMOKE_FILE_COUNT"])
    expected_sidecars = int(os.environ.get("STOVE0_SMOKE_SIDECAR_COUNT", "1"))
    plans = {}
    for route in recipe.definition.routes:
        if route.id not in targets:
            continue
        assert route.kind == "operation"
        branch = branches[route.id]
        assert branch.kind == "leaf"
        selection = selections[branch.artifact_selection.selection_sha256]
        assert selection.artifact_count == expected_audio + expected_sidecars
        assert all(row.collection == root for row in selection.artifacts)
        roles = [row.role for row in selection.artifacts]
        assert roles.count("stove0.media.source/v1") == expected_audio
        assert roles.count("stove0.media.xmp-source/v1") == expected_sidecars
        target = targets[route.id]
        assert target.work_id == branch.workflow_plan.work.work_id
        assert target.workflow_plan_sha256 == branch.workflow_plan.workflow_plan_sha256
        plan = TransformPlan.model_validate(
            {**target.target_plan.plan, "plan_sha256": target.target_plan.plan_sha256}
        )
        assert plan.operation_id == route.operation_id
        assert plan.inputs.selection == selection.ref()
        assert plan.intent == branch.workflow_plan.work.effective_intent
        assert plan.input_groups == branch.workflow_plan.input_groups
        assert len(plan.input_groups) == expected_audio
        assert sum(len(group.associated_ids) for group in plan.input_groups) == expected_sidecars
        assert {group.primary_id for group in plan.input_groups} == {
            row.id for row in selection.artifacts if row.role == "stove0.media.source/v1"
        }
        assert {subject for group in plan.input_groups for subject in group.associated_ids} == {
            row.id for row in selection.artifacts if row.role == "stove0.media.xmp-source/v1"
        }
        assert branch.workflow_plan.input_retrieval_policy == route.input_retrieval_policy
        for name, value in route.intent.items():
            declared_values(plan.intent[name], value)
        projection = plan.execution_parameters["media_projection"]
        assert isinstance(projection, dict)
        assert isinstance(projection["items"], list)
        assert isinstance(projection["retained_xmp_sidecars"], list)
        assert len(projection["items"]) == expected_audio
        assert len(projection["retained_xmp_sidecars"]) == expected_sidecars
        plans[route.id] = {
            "work_id": target.work_id,
            "plan_sha256": plan.plan_sha256,
            "bitrate_kbps": plan.intent["bitrate_kbps"],
        }
    if len(plans) == 2:
        assert plans["archive-audio"]["bitrate_kbps"] == 128
        assert plans["archive-audio-overlap"]["bitrate_kbps"] == 96
        assert len({row["plan_sha256"] for row in plans.values()}) == 2
    work = WorkView.model_validate(
        stove(
            "/v1/work",
            WorkCreateRequest(
                recipe_id=recipe_id(), inputs=(root,), preview_sha256=preview.preview_sha256
            ).model_dump(mode="json", exclude_none=True),
        )
    )
    expected_work = os.environ.get("EXPECTED_WORK_ID")
    if expected_work:
        assert work.work_id == expected_work
    assert work.work == preview.work
    assert work.preview_acceptance is not None
    assert work.preview_acceptance.preview_sha256 == preview.preview_sha256
    assert {
        row.branch_id: (row.work_id, row.plan_sha256)
        for row in work.preview_acceptance.target_plans
    } == {name: (row["work_id"], row["plan_sha256"]) for name, row in plans.items()}
    print(
        canonical_json_bytes(
            {
                "proof": "accepted-processing-plan",
                "recipe": preview.work.recipe.model_dump(mode="json"),
                "audio_inputs": expected_audio,
                "sidecars": expected_sidecars,
                "targets": plans,
            }
        ).decode(),
        file=sys.stderr,
    )
    print(work.work_id)


def snapshot() -> None:
    from stove0_operator_contracts import WorkView

    parents = os.environ["STOVE0_WORK_IDS"].split(",")
    assert len(parents) == len(set(parents))
    result: dict[str, dict[str, Any]] = {"parents": {}, "children": {}}
    for work_id in parents:
        parent = WorkView.model_validate(stove("/v1/work/" + work_id))
        assert parent.phase == "complete"
        assert parent.work.recipe.id == recipe_id()
        assert parent.coordination_settlement is not None
        assert parent.preview_acceptance is not None
        assert {row.branch_id for row in parent.preview_acceptance.target_plans} == route_ids()
        result["parents"][work_id] = parent.coordination_settlement.model_dump(mode="json")
        for target in parent.preview_acceptance.target_plans:
            child = WorkView.model_validate(stove("/v1/work/" + target.work_id))
            assert child.phase == "complete"
            assert child.target_plan is not None
            assert child.target_plan.plan_sha256 == target.plan_sha256
            assert child.target_settlement is not None
            assert child.target_request is not None
            assert child.target_status is not None and child.target_status.state == "succeeded"
            assert child.output is not None
            assert child.work_id not in result["children"]
            result["children"][child.work_id] = {
                "branch_id": target.branch_id,
                "job_id": child.target_request.declaration.job_id,
                "request_sha256": child.target_request.request_sha256,
                "plan_sha256": target.plan_sha256,
                "target_status_sha256": canonical_json_sha256(
                    child.target_status.model_dump(mode="json", exclude_none=True)
                ),
                "output": child.output.model_dump(mode="json"),
                "settlement": child.target_settlement.model_dump(mode="json"),
            }
    assert len(result["children"]) == len(parents) * len(route_ids())
    assert len({row["job_id"] for row in result["children"].values()}) == len(result["children"])
    assert len({row["output"]["collection_id"] for row in result["children"].values()}) == len(
        result["children"]
    )
    # An extra work/target would reveal an accidentally active admission policy
    # or replay duplication, even if the intended parents still complete.
    page = stove("/v1/work?page_size=100&sort=work_id&order=asc")
    assert page.get("next_page_token") is None
    assert {row["work_id"] for row in page["work"]} == set(parents) | set(result["children"])
    print(canonical_json_bytes(result).decode())


def target_records() -> None:
    state = Path("/var/lib/a-stove0-opus-target")
    accepted = {
        path.name.removesuffix(".accepted.json"): AcceptedTargetJob.model_validate_json(
            path.read_bytes()
        )
        for path in state.glob("*.accepted.json")
    }
    statuses = {
        path.name.removesuffix(".status.json"): TargetJobStatus.model_validate_json(
            path.read_bytes()
        )
        for path in state.glob("*.status.json")
    }
    snapshot_json = os.environ.get("STOVE0_SETTLED_SNAPSHOT")
    if snapshot_json is None:
        assert not accepted and not statuses, (tuple(accepted), tuple(statuses))
        print("preflight-only: zero accepted target jobs")
        return
    expected = json.loads(snapshot_json)["children"]
    jobs = {row["job_id"]: row for row in expected.values()}
    assert set(accepted) == set(statuses) == set(jobs)
    for job_id, row in jobs.items():
        assert accepted[job_id].declaration.job_id == job_id
        assert accepted[job_id].request_sha256 == row["request_sha256"]
        status = statuses[job_id]
        assert status.job_id == job_id and status.state == "succeeded"
        assert status.request_sha256 == row["request_sha256"]
        assert status.plan_sha256 == row["plan_sha256"]
        assert (
            canonical_json_sha256(status.model_dump(mode="json", exclude_none=True))
            == row["target_status_sha256"]
        )
    print(f"settled target queue: {len(jobs)} distinct successful jobs")


def main() -> None:
    {"invoke": invoke, "wait": wait, "snapshot": snapshot, "target-records": target_records}[
        sys.argv[1]
    ]()


if __name__ == "__main__":
    main()
