from __future__ import annotations

import pytest
from pydantic import ValidationError
from riverhog_storage_adapter_protocol import (
    CleanupCandidate,
    CleanupExecutionResult,
    CleanupInventoryPage,
    CleanupInventoryProgress,
    CleanupInventoryRequest,
    CleanupInventoryStepRequest,
    CleanupInvocation,
    CleanupItemOutcome,
    CleanupStepRequest,
    WriteCallIdentity,
    cleanup_execution_result,
    cleanup_inventory_execution_result,
    cleanup_step_sha256,
    unknown_cleanup_execution,
    validate_cleanup_execution,
    validated_storage_adapter,
)

_INCARNATION = "00000000-0000-4000-8000-000000000001"


def _invocation(*, control_bytes: int = 1024 * 1024) -> CleanupInvocation:
    request = CleanupStepRequest(
        selected_terminal_sha256="1" * 64,
        inventory=CleanupInventoryPage(
            object_prefix="archive/",
            target_incarnation=_INCARNATION,
            candidates=(
                CleanupCandidate(object_path="archive/a", kind="object_revision", token="a"),
                CleanupCandidate(object_path="archive/b", kind="open_write", token="b"),
            ),
            scanned_items=2,
            exhausted=False,
            continuation="next",
        ),
        maximum_provider_requests=4,
        maximum_control_bytes=control_bytes,
        maximum_in_flight=2,
    )
    return CleanupInvocation(
        call=WriteCallIdentity(
            call_id="2" * 64,
            exact_request_sha256=cleanup_step_sha256(request),
            target_incarnation=_INCARNATION,
            dispatch_owner="3" * 64,
        ),
        request=request,
    )


def test_cleanup_progress_commits_the_exact_selection_without_inventing_closure() -> None:
    invocation = _invocation()
    result = cleanup_execution_result(
        invocation,
        tuple(
            CleanupItemOutcome(candidate=item, outcome="disposed")
            for item in invocation.request.inventory.candidates
        ),
        provider_requests=4,
        execution="closed",
    )
    validate_cleanup_execution(invocation, result)
    assert result.result is not None
    assert result.result.selection_complete and not result.result.exhausted
    assert result.result.continuation == "next"
    assert CleanupExecutionResult.model_validate_json(result.model_dump_json()) == result
    partial = cleanup_execution_result(
        invocation,
        (
            CleanupItemOutcome(
                candidate=invocation.request.inventory.candidates[0], outcome="disposed"
            ),
            CleanupItemOutcome(
                candidate=invocation.request.inventory.candidates[1], outcome="unknown"
            ),
        ),
        provider_requests=3,
        execution="unknown",
    )
    assert partial.result is not None
    assert not partial.result.selection_complete and not partial.result.exhausted
    assert partial.result.continuation is None


@pytest.mark.parametrize(
    "change",
    ["correlation", "item", "advance", "requests", "closure", "invalid-outcome", "integer-flag"],
)
def test_cleanup_rejects_changed_results_and_unaccounted_resources(change: str) -> None:
    invocation = _invocation()
    response = cleanup_execution_result(
        invocation,
        tuple(
            CleanupItemOutcome(candidate=item, outcome="unknown")
            for item in invocation.request.inventory.candidates
        ),
        provider_requests=3,
        execution="unknown",
    )
    assert response.result is not None
    if change == "correlation":
        response = response.model_copy(
            update={"call": response.call.model_copy(update={"call_id": "4" * 64})}
        )
    elif change == "item":
        first = response.result.items[0]
        items = (
            first.model_copy(
                update={"candidate": first.candidate.model_copy(update={"token": "other"})}
            ),
            *response.result.items[1:],
        )
        response = response.model_copy(
            update={"result": response.result.model_copy(update={"items": items})}
        )
    elif change == "advance":
        response = response.model_copy(
            update={
                "result": response.result.model_copy(
                    update={"continuation": "next", "exhausted": True}
                )
            }
        )
    elif change == "requests":
        response = response.model_copy(
            update={"result": response.result.model_copy(update={"provider_requests": 5})}
        )
    elif change == "closure":
        response = response.model_copy(update={"execution": "closed"})
    elif change == "invalid-outcome":
        response = response.model_copy(update={"outcome": "invented"})
    else:
        response = response.model_copy(
            update={"result": response.result.model_copy(update={"selection_complete": 0})}
        )
    with pytest.raises(ValueError):
        validate_cleanup_execution(invocation, response)


def test_cleanup_rejects_changed_terminal_selection_and_reserves_control_before_dispatch() -> None:
    invocation = _invocation()
    changed = invocation.request.model_copy(update={"selected_terminal_sha256": "4" * 64})
    with pytest.raises(ValidationError, match="exact selection"):
        CleanupInvocation(call=invocation.call, request=changed)
    with pytest.raises(ValidationError, match="control grant"):
        _invocation(control_bytes=128)


def test_validated_cleanup_has_no_implicit_descriptor_provider_call() -> None:
    invocation = _invocation()

    class Owner:
        def descriptor(self):
            raise AssertionError("cleanup must account for its own live identity check")

        def execute_cleanup(self, selected: CleanupInvocation) -> CleanupExecutionResult:
            return unknown_cleanup_execution(selected)

    port = validated_storage_adapter(Owner())  # type: ignore[arg-type]
    assert port.execute_cleanup(invocation) == unknown_cleanup_execution(invocation)


def _inventory_invocation(*, control_bytes=1024 * 1024, response_bytes=65536):
    request = CleanupInventoryStepRequest(
        selected_terminal_sha256="1" * 64,
        inventory=CleanupInventoryRequest(
            object_prefix="archive/",
            target_incarnation=_INCARNATION,
            maximum_items=2,
            maximum_response_bytes=response_bytes,
        ),
        maximum_provider_requests=3,
        maximum_control_bytes=control_bytes,
        maximum_in_flight=2,
    )
    return CleanupInvocation(
        call=WriteCallIdentity(
            call_id="5" * 64,
            exact_request_sha256=cleanup_step_sha256(request),
            target_incarnation=_INCARNATION,
            dispatch_owner="3" * 64,
        ),
        request=request,
    )


def test_budgeted_inventory_correlates_only_its_own_read_and_preserves_unknown_execution():
    invocation = _inventory_invocation()
    page = _invocation().request.inventory
    response = cleanup_inventory_execution_result(
        invocation, page, provider_requests=3, execution="closed"
    )
    assert response.outcome == "cleanup_inventory"
    assert isinstance(response.result, CleanupInventoryProgress)
    assert response.result.inventory == page
    assert response.result.provider_requests == 3
    assert CleanupInvocation.model_validate_json(invocation.model_dump_json()) == invocation
    assert CleanupExecutionResult.model_validate_json(response.model_dump_json()) == response
    validate_cleanup_execution(invocation, response.model_copy(update={"execution": "unknown"}))
    with pytest.raises(ValueError, match="differs from its operation"):
        validate_cleanup_execution(
            _invocation(), response.model_copy(update={"call": _invocation().call})
        )


@pytest.mark.parametrize("change", ("call", "prefix", "incarnation", "requests", "count", "bytes"))
def test_budgeted_inventory_rejects_changed_observations_and_resource_overruns(change):
    invocation = _inventory_invocation(response_bytes=2048)
    response = cleanup_inventory_execution_result(
        invocation, _invocation().request.inventory, provider_requests=3, execution="closed"
    )
    if change == "call":
        response = response.model_copy(
            update={"call": response.call.model_copy(update={"call_id": "f" * 64})}
        )
    elif change == "requests":
        response = response.model_copy(
            update={"result": response.result.model_copy(update={"provider_requests": 4})}
        )
    else:
        updates = {
            "prefix": {"object_prefix": "elsewhere/"},
            "incarnation": {"target_incarnation": "00000000-0000-4000-8000-000000000099"},
            "count": {"scanned_items": 3},
            "bytes": {"continuation": "x" * 3000},
        }[change]
        page = response.result.inventory.model_copy(update=updates)
        response = response.model_copy(
            update={"result": response.result.model_copy(update={"inventory": page})}
        )
    with pytest.raises(ValueError):
        validate_cleanup_execution(invocation, response)


def test_budgeted_inventory_reserves_complete_response_capacity_before_dispatch():
    with pytest.raises(ValidationError, match="control grant"):
        _inventory_invocation(control_bytes=65536, response_bytes=65536)
    invocation = _inventory_invocation()
    changed = invocation.request.model_copy(update={"selected_terminal_sha256": "f" * 64})
    with pytest.raises(ValidationError, match="exact selection"):
        CleanupInvocation(call=invocation.call, request=changed)
