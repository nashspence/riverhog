"""Operator inspection retains exact original testimony and owner-bound pages."""

from dataclasses import replace
from typing import Any, cast

import pytest
from compiled_program_fixture import run_program
from fastapi.testclient import TestClient
from http_api_contracts.browse import BrowseTokenCodec
from sqlalchemy import select
from stove0_api.app import create_app
from stove0_api_client import Stove0ApiClient, Stove0ApiError
from stove0_core import SqlAlchemyStateStore, WorkflowPreviewService, WorkRecord
from stove0_core.persistence import _PLANNING_CONTEXTS
from stove0_core.preview_state import PreviewRecord
from stove0_protocol import PlanningJobPayload, PlanningJobRequest
from test_compiled_program import _catalog, _member, _source
from test_stove0_api_parity import _composition, _operator_operations

from tests.operation_observer import OperationObserver, TimeoutNeutralTestClient


@pytest.mark.parametrize("owner_kind", ("work", "preview"))
def test_current_client_inspects_named_tasks_and_exact_accepted_evidence(tmp_path, owner_kind):
    planner, progress, work = run_program(
        tmp_path,
        _source(observe={"probe": {"use": "facts"}, "second": {"use": "facts"}}),
        _catalog(),
        (_member("one"), _member("two")),
        facts_by_task={
            name: {
                "one": {"kind": "media", "discard": True},
                "two": {"kind": "media", "discard": False},
            }
            for name in ("probe", "second")
        },
        owner_kind=owner_kind,
    )
    assert progress.state == "ready"
    owner_id = planner.state.planning_owner[1]
    # Reopen independently of the planner and its hot caches.
    state = SqlAlchemyStateStore(f"sqlite:///{tmp_path / 'state.db'}")
    state.create(WorkRecord(work=work))
    preview = WorkflowPreviewService(
        store=state,
        deliveries=state,
        riverhog=object(),
        planning=planner,
        observers=planner.observers,
        targets=planner.targets,
    )
    other = PlanningJobRequest.seal(PlanningJobPayload(work=work, invocation_id="f" * 64))
    state.create_preview(PreviewRecord(job=other))
    with state.engine.connect() as connection:
        before = tuple(connection.execute(select(_PLANNING_CONTEXTS)).all())
    now = [1000.0]
    composition = _composition()
    application = create_app(
        replace(
            composition,
            state=state,
            preview=preview,
            browse_tokens=BrowseTokenCodec(
                composition.config.browse_token_signing_key,
                lifetime_seconds=600,
                clock=lambda: now[0],
            ),
        )
    )
    observer = OperationObserver.install(application, application="stove0")
    with TestClient(application) as transport:
        client = Stove0ApiClient("http://testserver", "stove0-test-token", allow_insecure_http=True)
        client._client = cast(Any, TimeoutNeutralTestClient(transport, observer=observer))
        kwargs = {"owner_kind": owner_kind, "owner_id": owner_id}
        first = client.list_observation_tasks(**kwargs, page_size=1)
        assert len(first.tasks) == 1 and first.next_page_token
        second = client.list_observation_tasks(
            **kwargs, page_size=1, page_token=first.next_page_token
        )
        assert second.next_page_token is None
        assert [task.task_id for task in (*first.tasks, *second.tasks)] == ["probe", "second"]
        assert all(task.state == "complete" for task in (*first.tasks, *second.tasks))
        now[0] += 1
        repeated = client.list_observation_tasks(**kwargs, page_size=1)
        assert repeated.next_page_token != first.next_page_token
        assert repeated.model_dump(exclude={"next_page_token"}) == first.model_dump(
            exclude={"next_page_token"}
        )
        assert (
            client.list_observation_tasks(
                **kwargs, page_size=1, page_token=repeated.next_page_token
            )
            == second
        )
        task = client.get_observation_task(work.work_id, "probe", **kwargs)
        assert task.question.task_id == "probe" and task.accepted.question == task.question
        accepted = task.accepted
        result_page = client.get_observation_results(
            work.work_id,
            "probe",
            **kwargs,
            evidence_set_sha256=accepted.evidence_set_sha256,
            limit=1,
        )
        assert result_page.authority == accepted
        original = client.get_observation_result(result_page.results[0].request_id, **kwargs)
        assert original == state.read_planning_context(
            owner_kind, owner_id
        ).accepted_observations.original_evidence(
            original.request.request_id, authorize=lambda scope: None
        )
        assert original.request.task_id == "probe"
        assert original.result.result_sha256 == result_page.results[0].result_sha256
        view = task.views["artifacts"]
        records, ordinal = [], 0
        while True:
            page = client.get_observation_view(
                work.work_id,
                "probe",
                "artifacts",
                **kwargs,
                view_sha256=view.view_sha256,
                start_ordinal=ordinal,
                limit=1,
            )
            assert page.authority == view
            records.extend(page.records)
            ordinal += len(page.records)
            if page.complete:
                break
        assert len(records) == view.record_count
        assert {row.subject_id for row in records if row.kind == "subject"} == {"one", "two"}
        assert all(result_page.results[0] in row.support for row in records)
        for invalid in (
            {"owner_kind": "preview", "owner_id": other.job_id},
            {"owner_kind": owner_kind, "owner_id": "9" * 64},
        ):
            with pytest.raises(Stove0ApiError):
                client.list_observation_tasks(**invalid, page_token=first.next_page_token)
            with pytest.raises(Stove0ApiError) as error:
                client.get_observation_result(original.request.request_id, **invalid)
            assert error.value.observed_status == 404
        assert (
            client.list_observation_tasks(owner_kind="preview", owner_id=other.job_id).tasks == ()
        )
        for method, extra in (
            (client.get_observation_results, {"evidence_set_sha256": "9" * 64}),
            (client.get_observation_view, {"view_id": "artifacts", "view_sha256": "9" * 64}),
        ):
            with pytest.raises(Stove0ApiError) as error:
                method(work.work_id, "probe", **kwargs, **extra)
            assert error.value.observed_status == 400
        assert transport.get("/v1/observation-tasks", params=kwargs).status_code == 401
        client._client = None
    observer.require(
        {
            key: route
            for key, route in _operator_operations().items()
            if route.startswith("GET /v1/observation-")
        }
    )
    with state.engine.connect() as connection:
        assert tuple(connection.execute(select(_PLANNING_CONTEXTS)).all()) == before
    state.engine.dispose()
    planner.state.engine.dispose()
