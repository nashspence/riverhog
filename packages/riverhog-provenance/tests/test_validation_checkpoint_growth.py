from __future__ import annotations

import shutil
import sqlite3
from contextlib import closing
from copy import deepcopy
from dataclasses import replace

import pytest
from riverhog_growth_contracts import growth_binding
from riverhog_provenance import (
    BoundedSourceObserver,
    BytesSource,
    assertion,
    create_journal,
    parse_journal,
    reference,
)
from riverhog_provenance._journal_prefix import install as install_prefix
from riverhog_provenance._session_checkpoint import StaleValidationCheckpoint, ValidationLedger
from riverhog_provenance._session_checkpoint import install as install_parent
from riverhog_provenance_contracts import ContractCatalog
from test_child_admission_growth import octets
from test_journal_prefix import checkpoint_frame
from test_semantic_edge import WorkConnection, complete_sample, observe_graph_tree, observed_work
from test_semantic_owner import assert_uncommitted_hidden, snapshot
from test_validation_checkpoint import prefix


@pytest.fixture(scope="module")
def checkpoint_histories(tmp_path_factory):
    """Retain real maintained prefix/parent transitions once for independent cold call copies."""
    base = tmp_path_factory.mktemp("checkpoint-histories")
    catalog = ContractCatalog()
    observed = BoundedSourceObserver(catalog=catalog).observe(BytesSource(b"checkpoint payload"))
    who = observed.observer_agent_id
    result = []
    for index, (count, payload, epoch, binding_octets) in enumerate(
        (
            (0, 1, 2, 64),
            (1, 1, 2, 64),
            (17, 1, 2, 64),
            (129, 1, 2, 64),
            (1025, 1, 2, 64),
            (1, 65537, 2, 64),
            (1025, 65537, 2, 64),
            (17, 1, 2**200, 16384),
            (0, 1, 2**200, 16384),
        )
    ):
        path = base / f"history-{index}.sqlite3"
        fragment = deepcopy(observed.graph_fragment())
        fragment["extensions"] = [
            assertion(
                "extension",
                who,
                subject=reference(fragment["artifacts"][0]["id"], "artifact"),
                property="urn:test:checkpoint-width",
                value={"type": "text", "value": "x" * payload},
            )
        ]
        first = parse_journal(create_journal(fragment, recorded_by_agent_id=who, catalog=catalog))[
            0
        ]
        # Setup is kept off the cold pager and outside every measured call.
        binding = b"source-fence:" + b"x" * (binding_octets - len(b"source-fence:"))
        with closing(sqlite3.connect(":memory:")) as db:
            install_prefix(db)
            install_parent(db)
            native = prefix(db)
            native.initialize()
            current = ValidationLedger(db, attempt_id="a" * 32, binding=binding, worker_epoch=2)
            current.initialize()
            for ordinal in range(count):
                candidate = first if ordinal == 0 else checkpoint_frame(native.load(), who, catalog)
                before = current.load()
                publication = native.prepare_publication(native.prepare(candidate, catalog))
                db.execute("BEGIN IMMEDIATE").close()
                current._fence(before)
                accepted = native._apply(publication)
                current._publish(
                    before,
                    replace(
                        before.checkpoint,
                        version=before.checkpoint.version + 1,
                        frame_ordinal=ordinal + 1,
                    ),
                    prefix=(native.attempt_id, native.binding, accepted.to_json()),
                )
                db.commit()
            if epoch > current.worker_epoch:
                current = current.replace_worker(epoch)
            before = current.load()
            # A valid wide control codec models prior owner-local retries without
            # changing the accepted history or reducing a real transition count.
            state = replace(before.checkpoint, version=max(before.checkpoint.version, epoch))
            db.execute("UPDATE journal_validation SET checkpoint=?", (state.to_json(),)).close()
            db.commit()
            with closing(sqlite3.connect(path)) as stored:
                db.backup(stored)
        result.append((path, count, epoch, binding))
    return result


def invoke_checkpoint(current, operation, before, after, epoch):
    if operation == "_fence":
        return current._fence(before)
    if operation == "_publish":
        return current._publish(before, after)
    if operation == "replace_worker":
        return current.replace_worker(epoch + 1)
    return getattr(current, operation)()


def checkpoint_samples(tmp_path, cases, operation, *, samples):
    binding = growth_binding(getattr(ValidationLedger, operation))
    for index, (source, count, epoch, inputs) in enumerate(cases):
        if operation in {"_initialize", "initialize"} and count:
            continue
        path = tmp_path / f"{operation}-{index}.sqlite3"
        shutil.copyfile(source, path)
        with closing(sqlite3.connect(path)) as setup:
            current = ValidationLedger(
                setup, attempt_id="a" * 32, binding=inputs, worker_epoch=epoch
            )
            before = current.load()
            after = replace(before.checkpoint, version=before.checkpoint.version + 1)
            if operation in {"_initialize", "initialize"}:
                setup.execute("DELETE FROM journal_validation").close()
                setup.commit()
            original = snapshot(setup)
            retained = sum(len(rows) for rows in original.values())
            width = 1024 + 2 * octets(
                (
                    inputs,
                    before.checkpoint.to_json(),
                    after.to_json(),
                    before.prefix,
                    str(epoch),
                    str(epoch + 1),
                )
            )
            dimensions = {"retained_rows": retained, "metadata_row_octets": width}
        with closing(sqlite3.connect(path, factory=WorkConnection)) as db:
            db.execute("PRAGMA synchronous=FULL").close()
            db.execute("PRAGMA cache_size=-64").close()
            current = ValidationLedger(db, attempt_id="a" * 32, binding=inputs, worker_epoch=epoch)
            applied = operation in {"_initialize", "_fence", "_publish"}
            if applied:
                db.execute("BEGIN IMMEDIATE").close()

            with observe_graph_tree(
                db,
                binding,
                dimensions,
                child_dimensions=lambda parent, _operation: dict(parent),
                operation_owner=lambda _operation: ValidationLedger,
            ) as frame:
                with db.observe():
                    returned = invoke_checkpoint(current, operation, before, after, epoch)
            primitive = observed_work(db, {key: 0 for key in db.metrics}, 0, 0)
            primitive["changed_rows"] = db.metrics["changed_rows"]
            assert primitive["changed_rows"] == int(
                operation in {"_initialize", "initialize", "_publish", "replace_worker"}
            )
            samples.append(complete_sample(frame, primitive))
            if applied:
                assert db.in_transaction
                assert_uncommitted_hidden(path, original)
                db.rollback()
                assert snapshot(db) == original
                # Reapply outside the observation to establish the exact committed suffix.
                db.execute("BEGIN IMMEDIATE").close()
                returned = invoke_checkpoint(current, operation, before, after, epoch)
                db.commit()
            assert not db.in_transaction
            current_rows = snapshot(db)
            assert {
                name: rows for name, rows in current_rows.items() if name != "journal_validation"
            } == {name: rows for name, rows in original.items() if name != "journal_validation"}
            if operation == "replace_worker":
                with pytest.raises(StaleValidationCheckpoint):
                    current.load()
                assert returned.worker_epoch == epoch + 1 and returned.load().checkpoint == after
            elif operation == "load":
                assert returned == before and current_rows == original
            elif operation == "_fence":
                assert returned is None and current_rows == original
            elif operation == "_publish":
                assert returned.checkpoint == after and current.load() == returned
            else:
                assert returned.checkpoint.version == 0 and returned.checkpoint.frame_ordinal == 0
                assert current.load() == returned
    assert len(samples) == (2 if operation in {"_initialize", "initialize"} else 9)


@pytest.mark.growth_witness(
    "riverhog-validation-checkpoint-load-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._session_checkpoint.ValidationLedger.load",
)
def test_actual_cold_checkpoint_load(tmp_path, checkpoint_histories, record_property):
    samples = []
    record_property("growth_samples", samples)
    checkpoint_samples(tmp_path, checkpoint_histories, "load", samples=samples)


@pytest.mark.growth_witness(
    "riverhog-validation-checkpoint-fence-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._session_checkpoint.ValidationLedger._fence",
)
def test_actual_cold_checkpoint_fence(tmp_path, checkpoint_histories, record_property):
    samples = []
    record_property("growth_samples", samples)
    checkpoint_samples(tmp_path, checkpoint_histories, "_fence", samples=samples)


@pytest.mark.growth_witness(
    "riverhog-validation-checkpoint-publish-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._session_checkpoint.ValidationLedger._publish",
)
def test_actual_cold_checkpoint_publish(tmp_path, checkpoint_histories, record_property):
    samples = []
    record_property("growth_samples", samples)
    checkpoint_samples(tmp_path, checkpoint_histories, "_publish", samples=samples)


@pytest.mark.growth_witness(
    "riverhog-validation-checkpoint-initialization-application-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._session_checkpoint.ValidationLedger._initialize",
)
def test_actual_cold_checkpoint_initialization_application(
    tmp_path, checkpoint_histories, record_property
):
    samples = []
    record_property("growth_samples", samples)
    checkpoint_samples(tmp_path, checkpoint_histories, "_initialize", samples=samples)


@pytest.mark.growth_witness(
    "riverhog-validation-checkpoint-initialize-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._session_checkpoint.ValidationLedger.initialize",
)
def test_actual_cold_checkpoint_initialize(tmp_path, checkpoint_histories, record_property):
    samples = []
    record_property("growth_samples", samples)
    checkpoint_samples(tmp_path, checkpoint_histories, "initialize", samples=samples)


@pytest.mark.growth_witness(
    "riverhog-validation-checkpoint-replace-worker-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._session_checkpoint.ValidationLedger.replace_worker",
)
def test_actual_cold_checkpoint_replace_worker(tmp_path, checkpoint_histories, record_property):
    samples = []
    record_property("growth_samples", samples)
    checkpoint_samples(tmp_path, checkpoint_histories, "replace_worker", samples=samples)
