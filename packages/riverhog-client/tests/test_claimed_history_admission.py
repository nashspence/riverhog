from __future__ import annotations

import shutil
from types import SimpleNamespace

import pytest
from riverhog_archive_contracts import BOUND_HISTORY_EXTENT, RETAINED_HISTORY_EXTENT
from riverhog_client import ApiClient
from riverhog_client.processing import (
    ClaimedCollectionReader,
    ClaimedCollectionRuntime,
    CollectionTransformRuntime,
    DerivedCollectionSpec,
)
from riverhog_client.processing import provenance as owner
from riverhog_protocol.collection_workflows import OperationIdentity, RecipeIdentity
from riverhog_provenance import external_reference
from test_processing_provenance import ProvenanceApi, _add_selected

from tests.support.sqlite_admission import Capacity, Population


@pytest.fixture
def api():
    value = ProvenanceApi()
    yield value
    value.summary.close()


def reader(api, population, handles):
    return ClaimedCollectionReader(
        api,
        inputs=(api.root,),
        work_id="f" * 64,
        claim_id="claim",
        fence=1,
        history_connection_factory=population,
        on_history_storage_open=handles.append,
    )


def test_claimed_history_admission_preserves_exact_extents_and_api_replacement(api):
    late = _add_selected(api, inclusion="retained", retract=True)
    population, handles = Population(limit=2), []
    selected = reader(api, population, handles)
    selected.replace_api(api)
    view = selected.provenance(api.artifact)
    for extent, expected in (
        (BOUND_HISTORY_EXTENT, {api.journal_id}),
        (RETAINED_HISTORY_EXTENT, {api.journal_id, late}),
    ):
        with view.history_closure(extent=extent) as closure:
            assert {anchor.journal_id for anchor in closure.journal_anchors()} == expected
            assert closure.membership_image(max_bytes=1024 * 1024) is not None
            assert population.active == 1
        assert population.active == 0
    assert len(handles) == 2 and len(population.connections) == 4 and population.peak == 2
    assert all(connection.close_calls == 1 for connection in population.connections)
    assert not any(path.parent.exists() for path, _ in population.attempts)


def test_claimed_history_refusal_precedes_structural_reads_or_journal_work(api):
    population, handles = Population(), []
    population.refuse = True
    view = reader(api, population, handles).provenance(api.artifact)
    before = list(api.calls)
    with pytest.raises(Capacity):
        with view.history_closure(extent=BOUND_HISTORY_EXTENT):
            pytest.fail("refused closure returned")
    assert not api.structure_reads and api.calls == before and not handles
    assert population.active == 0
    shutil.rmtree(population.attempts[0][0].parent)


@pytest.mark.parametrize("after", [False, True])
def test_claimed_history_uncertain_worklist_close_retains_explicit_retry(api, after):
    population, handles = Population(), []
    view = reader(api, population, handles).provenance(api.artifact)
    with pytest.raises(OSError, match="close unconfirmed"):
        with view.history_closure(extent=BOUND_HISTORY_EXTENT) as closure:
            db = population.connections[0]
            db.close_failures, db.close_after = 1, after
            closure.close()
    assert handles == [closure] and population.active == 1 and db.close_calls == 1
    assert population.attempts[0][0].is_file()
    closure.close()
    assert population.active == 0 and db.close_calls == 2
    assert not population.attempts[0][0].parent.exists()


@pytest.mark.parametrize("runtime_type", [ClaimedCollectionRuntime, CollectionTransformRuntime])
def test_processing_runtime_preserves_history_admission_across_capability_refresh(
    api, primary_store, monkeypatch, runtime_type
):
    population, handles = Population(), []
    common = dict(
        base_url="https://riverhog.invalid",
        capability_token="fixture-capability",
        claim_id="claim",
        fence=1,
        work_id="f" * 64,
        execution_id="e" * 64,
        history_connection_factory=population,
        on_history_storage_open=handles.append,
    )
    spec = DerivedCollectionSpec(
        inputs=(api.root,),
        recipe=RecipeIdentity("fixture/v1", 1, "a" * 64),
        operation=OperationIdentity("fixture/v1", "b" * 64),
    )

    def claim(_client, _identity):
        return SimpleNamespace(
            plan=SimpleNamespace(
                execution_id="e" * 64,
                output_policy=spec.output_policy,
                inputs=SimpleNamespace(sha256="1" * 64),
                artifacts=SimpleNamespace(sha256="2" * 64),
            )
        )

    monkeypatch.setattr(ApiClient, "get_processing_claim", claim)
    if runtime_type is ClaimedCollectionRuntime:
        runtime = runtime_type.from_capability(inputs=(api.root,), **common)
    else:
        spec = DerivedCollectionSpec(
            inputs=(api.root,),
            recipe=RecipeIdentity("fixture/v1", 1, "a" * 64),
            operation=OperationIdentity("fixture/v1", "b" * 64),
        )
        runtime = runtime_type.from_capability(
            primary_store=primary_store,
            member_inventory=lambda: (),
            spec=spec,
            controller_evidence={"execution_id": "e" * 64},
            producer_app="fixture",
            **common,
        )
    try:
        runtime.refresh_capability("replacement-fixture-capability")
        runtime.reader.replace_api(api)
        with runtime.reader.provenance(api.artifact).history_closure(
            extent=BOUND_HISTORY_EXTENT
        ) as closure:
            assert [a.to_mapping() for a in closure.journal_anchors()] == [
                api.binding.journal.model_dump(mode="json")
            ]
    finally:
        runtime.close()
    assert len(handles) == len(population.connections) == 1 and population.active == 0


@pytest.mark.parametrize("failure", [None, "identity", "fence"])
def test_foreign_reference_lookup_confirms_summary_close_on_return_and_failure(
    api, monkeypatch, failure
):
    population, handles = Population(), []
    view = reader(api, Population(), []).provenance(api.artifact)
    reference = external_reference(api.summary, api.summary.states[0]["id"])
    validate = owner.validate_journal_chunks

    def admitted(chunks, **options):
        summary = validate(
            chunks, connection_factory=population, on_storage_open=handles.append, **options
        )
        if failure == "fence":

            def revoked():
                raise PermissionError("root fence revoked")

            view.verify_root = revoked
        return summary

    monkeypatch.setattr(owner, "validate_journal_chunks", admitted)
    if failure == "identity":
        reference = {**reference, "object_id": "urn:uuid:99999999-9999-4999-8999-999999999999"}
    if failure is None:
        assert view.resolve_external_reference(reference) == {
            key: value for key, value in reference.items() if key != "scope"
        }
    else:
        with pytest.raises(PermissionError if failure == "fence" else ValueError):
            view.resolve_external_reference(reference)
    assert len(population.connections) == 1 and population.active == 0
    assert population.connections[0].close_calls == 1 and len(handles) == 2
    assert not any(path.parent.exists() for path, _ in population.attempts)
