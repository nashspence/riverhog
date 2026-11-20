from __future__ import annotations

import gc
import hashlib
import shutil
from contextlib import contextmanager

import pytest
from riverhog_archive_contracts import BOUND_HISTORY_EXTENT, RecordSetCommitment
from riverhog_archive_contracts.member_history import MEMBER_HISTORY_IMPORTS_SCHEMA
from riverhog_client.processing import CollectionTransformRuntime, DerivedCollectionSpec
from riverhog_client.processing.writer import (
    DerivedCollectionWriter,
    IncrementalDerivedCollectionWriter,
)
from riverhog_client.producer import ProducerArtifactIdentity
from test_transform import (
    CONTROLLER_EVIDENCE,
    EXECUTION_ID,
    INPUT_ID,
    WORK_ID,
    InputHistoryApi,
    UploadApi,
    _completion_records,
    _derived_stream,
    _spec,
    inventory_from_rows,
)

from tests.support.sqlite_admission import Capacity, Population


@pytest.fixture
def source():
    value = InputHistoryApi(1, INPUT_ID, b"exact selected input")
    yield value
    value.summary.close()


class Admission:
    def __init__(self):
        self.population = Population(limit=2)
        self.selection, self.verification, self.roles = [], [], []

    def connect(self, role, database, **options):
        self.roles.append(role)
        return self.population(database, **options)

    def retain(self, role, owner):
        assert owner._db is self.population.connections[-1]
        cursor = owner._db.execute("PRAGMA schema_version")
        try:
            assert cursor.fetchone() == (0,)
        finally:
            cursor.close()
        getattr(self, role).append(owner)
        if role == "verification":
            assert self.selection[-1]._db is self.population.connections[-2]
            assert self.population.active == 2

    def ports(self):
        return {
            "history_selection_connection_factory": lambda *a, **kw: self.connect(
                "selection", *a, **kw
            ),
            "on_history_selection_storage_open": lambda owner: self.retain("selection", owner),
            "history_verification_connection_factory": lambda *a, **kw: self.connect(
                "verification", *a, **kw
            ),
            "on_history_verification_storage_open": lambda owner: self.retain(
                "verification", owner
            ),
        }

    def assert_closed(self):
        assert self.population.active == 0
        assert all(not path.parent.exists() for path, _ in self.population.attempts)


@contextmanager
def execution(api, source, primary_store, admission, *, runtime=False, publish=False):
    spec = DerivedCollectionSpec(
        inputs=(source.root,), recipe=_spec().recipe, operation=_spec().operation
    )
    common = dict(
        primary_store=primary_store,
        member_inventory=lambda: inventory_from_rows(api.registered),
        spec=spec,
        claim_id="claim-1",
        fence=1,
        work_id=WORK_ID,
        execution_id=EXECUTION_ID,
        controller_evidence=CONTROLLER_EVIDENCE,
        producer_app="fixture-transform",
        producer_version="1",
        **admission.ports(),
    )
    if runtime:
        owner = CollectionTransformRuntime(api, **common)
        try:
            yield (
                owner
                if publish
                else owner.open_incremental_publication(execution_envelope_sha256="c" * 64)
            )
        finally:
            owner.close()
    elif publish:
        yield DerivedCollectionWriter(api, **common)
    else:
        owner = IncrementalDerivedCollectionWriter(
            api, execution_envelope_sha256="c" * 64, **common
        )
        try:
            yield owner
        finally:
            owner.stop()


def append(writer, source):
    output = _derived_stream(0, b"derived payload")
    return writer.append(
        output,
        identity=ProducerArtifactIdentity(output.artifact_id, output.bytes, output.sha256),
        output_id=output.output_id,
        source_histories=(source.accepted(),),
        history_extent=BOUND_HISTORY_EXTENT,
    )


def accepted_primary(api):
    output = _derived_stream(0, b"derived payload")
    binding = api.bindings[output.artifact_id]
    return binding, api.journals[binding.journal.journal_id], tuple(api.registered)


def configure_disposition(api, source, output):
    from riverhog_canonical_json import canonical_json_bytes, canonical_json_sha256
    from riverhog_protocol.collection_workflows import ArtifactDispositionSetIdentity

    selected = {
        "collection_id": str(source.root.collection_id),
        "archive_root_sha256": source.root.archive_root_sha256,
        "artifact_id": str(source.member.artifact_id),
    }
    api.dispositions = [{"input": selected, "status": "transformed"}]
    api.output_edges = [{"input": selected, "output_artifact_id": str(output.artifact_id)}]
    api.derivation_identity = ArtifactDispositionSetIdentity(
        1,
        1,
        1,
        canonical_json_sha256(
            {
                "format": "riverhog-artifact-disposition-set/v1",
                "disposition_count": "1",
                "dispositions_sha256": hashlib.sha256(
                    canonical_json_bytes(api.dispositions[0]) + b"\n"
                ).hexdigest(),
                "output_edge_count": "1",
                "output_artifact_count": "1",
                "outputs_sha256": hashlib.sha256(
                    canonical_json_bytes(api.output_edges[0]) + b"\n"
                ).hexdigest(),
            }
        ),
    )


@pytest.mark.parametrize("runtime", [False, True])
@pytest.mark.parametrize("publish", [False, True])
def test_derived_history_admits_actual_owners_and_preserves_exact_import(
    source, primary_store, runtime, publish
):
    api, admission = UploadApi(), Admission()
    output = _derived_stream(0, b"derived payload")
    with source.accepted().history_import(extent=BOUND_HISTORY_EXTENT) as (imported, _, _closure):
        commitment = RecordSetCommitment(MEMBER_HISTORY_IMPORTS_SCHEMA)
        commitment.update(imported.key, imported.to_mapping())
        expected = commitment.ref()
    configure_disposition(api, source, output)
    with execution(
        api, source, primary_store, admission, runtime=runtime, publish=publish
    ) as writer:
        if publish:
            records = _completion_records()
            receipt = writer.publish(
                (output,),
                identities={
                    output.artifact_id: ProducerArtifactIdentity(
                        output.artifact_id, output.bytes, output.sha256
                    )
                },
                source_histories={output.artifact_id: (source.accepted(),)},
                history_extent=BOUND_HISTORY_EXTENT,
                completion_records=records,
                execution_envelope_sha256="c" * 64,
                execution_sha256=records[2].sha256,
                disposition_set=api.derivation_identity,
                poll_seconds=0.01,
            )
            assert receipt.collection_id == 7
        else:
            append(writer, source)
    assert api.history_inputs[output.artifact_id] == expected
    assert len(api.registered) == 1
    assert admission.roles == ["selection", "verification"]
    assert len(admission.selection) == len(admission.verification) == 1
    assert admission.population.peak == 2
    assert all(db.close_calls == 1 for db in admission.population.connections)
    admission.assert_closed()


@pytest.mark.parametrize("runtime", [False, True])
@pytest.mark.parametrize("role", ["selection", "verification"])
@pytest.mark.parametrize("unreturned", [False, True])
def test_derived_history_refusal_preserves_primary_and_retries_exactly(
    source, primary_store, runtime, role, unreturned
):
    api, admission = UploadApi(), Admission()
    connect = admission.connect

    def fail(selected, *args, **options):
        admission.population.refuse = selected == role and not unreturned
        admission.population.unreturned = selected == role and unreturned
        return connect(selected, *args, **options)

    admission.connect = fail
    with execution(api, source, primary_store, admission, runtime=runtime) as writer:
        with pytest.raises(OSError if unreturned else Capacity):
            append(writer, source)
        exact = accepted_primary(api)
        assert not api.history_inputs and len(api.registered) == 1
        assert admission.population.active == int(unreturned)
        assert len(admission.selection) == int(role == "verification")
        assert not admission.verification
        if unreturned:
            admission.population.connections[-1].close()
        shutil.rmtree(admission.population.attempts[-1][0].parent)
        admission.connect = connect
        admission.population.refuse = admission.population.unreturned = False
        append(writer, source)
        assert accepted_primary(api) == exact
        assert len(api.history_inputs) == 1
    admission.assert_closed()


@pytest.mark.parametrize("runtime", [False, True])
@pytest.mark.parametrize("role", ["selection", "verification"])
@pytest.mark.parametrize("after", [False, True])
def test_derived_history_uncertain_close_keeps_charge_and_primary_until_explicit_retry(
    source, primary_store, runtime, role, after
):
    api, admission = UploadApi(), Admission()
    retain = admission.retain

    def interrupt(selected, owner):
        retain(selected, owner)
        if selected == role:
            owner._db.close_failures = 1
            owner._db.close_after = after

    admission.retain = interrupt
    with execution(api, source, primary_store, admission, runtime=runtime) as writer:
        with pytest.raises(OSError, match="close unconfirmed"):
            append(writer, source)
        exact = accepted_primary(api)
        gc.collect()
        retained = admission.selection[0]
        assert retained._closing
        assert admission.population.active == (2 if role == "verification" else 1)
        assert all(
            db.close_calls == (0 if role == "verification" and i == 0 else 1)
            for i, db in enumerate(admission.population.connections)
        )
        with pytest.raises(RuntimeError, match="closed"):
            retained.seal()
        retained.close()
        admission.assert_closed()
        admission.retain = retain
        append(writer, source)
        assert accepted_primary(api) == exact
        assert len(api.history_inputs) == 1
    admission.assert_closed()


@pytest.mark.parametrize("role", ["selection", "verification"])
@pytest.mark.parametrize("failure", ["retention", "schema"])
def test_derived_history_initialization_failure_keeps_actual_owner_reachable(
    source, primary_store, role, failure
):
    api, admission = UploadApi(), Admission()
    retain = admission.retain

    def interrupt(selected, owner):
        retain(selected, owner)
        if selected == role:
            owner._db.close_failures = 1
            if failure == "retention":
                raise InterruptedError("retention interrupted")
            admission.population.fail_schema = True

    admission.retain = interrupt
    with execution(api, source, primary_store, admission, runtime=True) as writer:
        with pytest.raises(OSError, match="close unconfirmed"):
            append(writer, source)
        assert admission.population.active > 0
        assert not api.history_inputs and len(api.registered) == 1
        retained = admission.selection[0]
        retained.close()
    admission.assert_closed()


def test_derived_history_unknown_extent_cannot_open_selection_or_register_payload(
    source, primary_store
):
    api, admission = UploadApi(), Admission()
    output = _derived_stream(0, b"derived payload")
    with execution(api, source, primary_store, admission) as writer:
        with pytest.raises(ValueError, match="extent"):
            writer.append(
                output,
                identity=ProducerArtifactIdentity(output.artifact_id, output.bytes, output.sha256),
                output_id=output.output_id,
                source_histories=(source.accepted(),),
                history_extent="unknown",
            )
    assert not api.registered and not admission.roles


def test_derived_history_admission_survives_capability_refresh_before_and_after_open(
    source, primary_store
):
    from riverhog_client import ApiClient

    api, admission = UploadApi(), Admission()
    template = ApiClient(base_url="https://riverhog.invalid", token="fixture-capability")
    try:
        for name in (
            "base_url",
            "allow_insecure_http",
            "host_header",
            "http2",
            "timeout_seconds",
            "upload_timeout_seconds",
        ):
            setattr(api, name, getattr(template, name))
    finally:
        template.close()
    with execution(api, source, primary_store, admission, runtime=True, publish=True) as runtime:
        runtime.refresh_capability("replacement-fixture-capability")
        runtime.api.replace(api)
        writer = runtime.open_incremental_publication(execution_envelope_sha256="c" * 64)
        append(writer, source)
        exact = accepted_primary(api)
        runtime.refresh_capability("second-fixture-capability")
        runtime.api.replace(api)
        assert runtime.open_incremental_publication(execution_envelope_sha256="c" * 64) is writer
        append(writer, source)
        assert accepted_primary(api) == exact
    assert admission.roles == ["selection", "verification"] * 2
    assert len(admission.selection) == len(admission.verification) == 2
    admission.assert_closed()
