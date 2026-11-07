from __future__ import annotations

import shutil
from types import SimpleNamespace

import pytest
from riverhog_archive_contracts import BOUND_HISTORY_EXTENT, RETAINED_HISTORY_EXTENT
from riverhog_client import ApiClient
from riverhog_client.processing import (
    ClaimedCollectionRuntime,
    CollectionTransformRuntime,
    DerivedCollectionSpec,
)
from riverhog_protocol.collection_workflows import OperationIdentity, RecipeIdentity
from test_claimed_history_admission import api as api
from test_claimed_history_admission import reader
from test_processing_provenance import _add_selected

from tests.support.sqlite_admission import Capacity, Population


def selected(api, heads, handles, **ports):
    return reader(
        api,
        Population(),
        [],
        history_verification_connection_factory=heads,
        on_history_verification_storage_open=handles.append,
        **ports,
    ).provenance(api.artifact)


def run(view, access):
    if access == "descriptor":
        return view.history_store.descriptor(view.history_binding)
    with view.history_closure(extent=BOUND_HISTORY_EXTENT) as closure:
        return tuple(closure.journal_anchors())


@pytest.mark.parametrize("extent", [BOUND_HISTORY_EXTENT, RETAINED_HISTORY_EXTENT])
def test_claimed_verification_ports_preserve_exact_extents_and_fresh_selections(api, extent):
    late = _add_selected(api, inclusion="retained", retract=True)
    heads, handles = Population(limit=1), []
    client = reader(
        api,
        Population(),
        [],
        history_verification_connection_factory=heads,
        on_history_verification_storage_open=handles.append,
    )
    client.replace_api(api)
    view = client.provenance(api.artifact)
    expected = {api.journal_id} if extent == BOUND_HISTORY_EXTENT else {api.journal_id, late}
    roots = tuple(view.history_store.roots(view.history_binding, extent=extent))
    assert {root.journal.journal_id for root in roots} == expected
    for _ in range(2):
        with view.history_closure(extent=extent) as closure:
            assert {anchor.journal_id for anchor in closure.journal_anchors()} == expected
            assert heads.active == 0
    assert handles and len(handles) == len(heads.connections)
    assert all(owner.cleanup_complete for owner in handles)
    assert heads.peak == 1 and heads.active == 0
    assert all(connection.close_calls == 1 for connection in heads.connections)
    assert not any(path.parent.exists() for path, _ in heads.attempts)


@pytest.mark.parametrize("access", ["descriptor", "closure"])
@pytest.mark.parametrize("unreturned", [False, True])
def test_claimed_heads_refusal_stops_before_journal_work_and_retains_factory_files(
    api, access, unreturned
):
    heads, handles = Population(limit=1), []
    view = selected(api, heads, handles)
    before = list(api.calls)
    heads.refuse, heads.unreturned = not unreturned, unreturned
    with pytest.raises(OSError if unreturned else Capacity):
        run(view, access)
    assert api.calls == before and not handles
    assert heads.active == int(unreturned)
    if unreturned:
        assert heads.connections[0].close_calls == 0
        heads.connections[0].close()
    shutil.rmtree(heads.attempts[0][0].parent)
    assert heads.active == 0


@pytest.mark.parametrize("access", ["descriptor", "closure"])
@pytest.mark.parametrize("after", [False, True])
def test_claimed_heads_unknown_close_remains_charged_until_explicit_retry(api, access, after):
    heads, handles = Population(limit=1), []

    def retain(owner):
        handles.append(owner)
        if len(handles) == 1:
            heads.connections[-1].close_failures = 1
            heads.connections[-1].close_after = after

    view = reader(
        api,
        Population(),
        [],
        history_verification_connection_factory=heads,
        on_history_verification_storage_open=retain,
    ).provenance(api.artifact)
    with pytest.raises(OSError):
        run(view, access)
    assert heads.active == 1 and heads.connections[0].close_calls == 1
    assert not handles[0].cleanup_complete
    with pytest.raises(Capacity):
        run(view, access)
    assert heads.connections[0].close_calls == 1 and len(handles) == 1
    shutil.rmtree(heads.attempts[-1][0].parent)
    handles[0].close()
    assert heads.active == 0
    run(view, access)
    assert all(owner.cleanup_complete for owner in handles)
    assert heads.active == 0


@pytest.mark.parametrize("access", ["descriptor", "closure"])
def test_claimed_heads_retention_runs_before_schema_and_keeps_actual_failed_owner(api, access):
    heads, handles = Population(), []

    def retain(owner):
        handles.append(owner)
        assert owner._db is heads.connections[-1]
        cursor = heads.connections[-1].execute("PRAGMA schema_version")
        try:
            assert cursor.fetchone() == (0,)
        finally:
            cursor.close()
        heads.fail_schema = True
        heads.connections[-1].close_failures = 1

    view = reader(
        api,
        Population(),
        [],
        history_verification_connection_factory=heads,
        on_history_verification_storage_open=retain,
    ).provenance(api.artifact)
    with pytest.raises(OSError):
        run(view, access)
    assert heads.active == 1 and len(handles) == 1
    assert heads.connections[-1].close_calls == 1
    handles[-1].close()
    assert heads.active == 0


@pytest.mark.parametrize("runtime_type", [ClaimedCollectionRuntime, CollectionTransformRuntime])
def test_runtime_keeps_verification_admission_after_capability_replacement(
    api, monkeypatch, runtime_type
):
    heads, handles = Population(limit=1), []
    spec = DerivedCollectionSpec(
        inputs=(api.root,),
        recipe=RecipeIdentity("fixture/v1", 1, "a" * 64),
        operation=OperationIdentity("fixture/v1", "b" * 64),
    )
    monkeypatch.setattr(
        ApiClient,
        "get_processing_claim",
        lambda _client, _identity: SimpleNamespace(
            plan=SimpleNamespace(
                execution_id="e" * 64,
                output_policy=spec.output_policy,
                inputs=SimpleNamespace(sha256="1" * 64),
                artifacts=SimpleNamespace(sha256="2" * 64),
            )
        ),
    )
    common = dict(
        base_url="https://riverhog.invalid",
        capability_token="fixture-capability",
        claim_id="claim",
        fence=1,
        work_id="f" * 64,
        execution_id="e" * 64,
        history_verification_connection_factory=heads,
        on_history_verification_storage_open=handles.append,
    )
    if runtime_type is ClaimedCollectionRuntime:
        runtime = runtime_type.from_capability(inputs=(api.root,), **common)
    else:

        class UnusedPrimary:
            def __getattr__(self, name):
                pytest.fail(f"claimed verification accessed primary storage: {name}")

        runtime = runtime_type.from_capability(
            primary_store=UnusedPrimary(),
            member_inventory=lambda: (),
            spec=spec,
            controller_evidence={"execution_id": "e" * 64},
            producer_app="fixture",
            **common,
        )
    try:
        runtime.refresh_capability("replacement-fixture-capability")
        runtime.reader.replace_api(api)
        view = runtime.reader.provenance(api.artifact)
        assert view.history_store.descriptor(view.history_binding) == view.history
        with view.history_closure(extent=BOUND_HISTORY_EXTENT) as closure:
            assert [anchor.to_mapping() for anchor in closure.journal_anchors()] == [
                api.binding.journal.model_dump(mode="json")
            ]
    finally:
        runtime.close()
    assert handles and len(handles) == len(heads.connections)
    assert all(owner.cleanup_complete for owner in handles)
    assert heads.active == 0 and heads.peak == 1


@pytest.mark.parametrize("access", ["descriptor", "closure"])
def test_claimed_root_revocation_cannot_be_bypassed_by_verification_factory(api, access):
    heads, handles = Population(), []
    view = selected(api, heads, handles)

    def revoked():
        raise PermissionError("claimed root revoked")

    view.verify_root = revoked
    with pytest.raises(PermissionError):
        run(view, access)
    # Descriptor bytes are root-fenced before verification admits SQLite.
    assert not heads.attempts and not handles and not api.structure_reads
