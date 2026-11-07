from __future__ import annotations

import shutil
import sqlite3
from contextlib import closing
from dataclasses import replace

import pytest
from riverhog_growth_contracts import growth_binding
from riverhog_provenance._session_checkpoint import StaleValidationCheckpoint, ValidationLedger
from test_child_admission_growth import octets
from test_semantic_edge import WorkConnection, WorkCursor, complete_sample, observed_work
from test_semantic_owner import assert_uncommitted_hidden, snapshot
from test_validation_checkpoint_growth import checkpoint_histories as checkpoint_histories


class CheckpointInterrupted(BaseException):
    pass


class InterruptedCursor(WorkCursor):
    def close(self):
        result = super().close()
        if (
            self.connection.recording
            and self.connection.interrupt_publication
            and self.sql.startswith("UPDATE journal_validation")
        ):
            self.connection.interrupt_publication = False
            raise CheckpointInterrupted
        return result


class RejectionConnection(WorkConnection):
    interrupt_publication = False

    def cursor(self, *args, **kwargs):
        if kwargs.get("factory") is WorkCursor:
            kwargs["factory"] = InterruptedCursor
        return super().cursor(*args, **kwargs)


def rejection_samples(root, histories, operation, *, samples):
    kinds = {
        "load": ("worker", "algorithm", "prefix"),
        "_fence": ("proposal", "outside"),
        "_publish": ("proposal", "prefix", "version", "outside", "interrupt"),
    }[operation]
    binding = growth_binding(getattr(ValidationLedger, operation))
    for index, (source, _count, epoch, inputs) in enumerate(histories):
        for kind in kinds:
            path = root / f"{operation}-{index}-{kind}.sqlite3"
            shutil.copyfile(source, path)
            with closing(sqlite3.connect(path, factory=RejectionConnection)) as db:
                db.execute("PRAGMA synchronous=FULL").close()
                db.execute("PRAGMA cache_size=-64").close()
                current = ValidationLedger(
                    db, attempt_id="a" * 32, binding=inputs, worker_epoch=epoch
                )
                before = current.load()
                proposed = before
                if kind == "proposal":
                    proposed = replace(
                        before,
                        checkpoint=replace(
                            before.checkpoint, version=before.checkpoint.version + 1
                        ),
                    )
                after = replace(
                    proposed.checkpoint,
                    version=proposed.checkpoint.version + (2 if kind == "version" else 1),
                )
                if kind == "worker":
                    db.execute(
                        "UPDATE journal_validation SET worker_epoch=?", (str(epoch + 1),)
                    ).close()
                elif kind == "algorithm":
                    db.execute("UPDATE journal_validation SET algorithm=?", ("superseded",)).close()
                elif kind == "prefix":
                    db.execute("UPDATE journal_prefix SET binding=?", (b"changed-prefix",)).close()
                db.commit()
                original = snapshot(db)
                dimensions = {
                    "retained_rows": sum(
                        len(original[name])
                        for name in ("journal_validation", "journal_prefix", "frames")
                    ),
                    "metadata_row_octets": 1024
                    + 2
                    * octets(
                        (
                            original["journal_validation"],
                            original["journal_prefix"],
                            inputs,
                            before,
                            proposed,
                            after,
                            str(epoch),
                        )
                    ),
                }
                applied = operation != "load" and kind != "outside"
                if applied:
                    db.execute("BEGIN IMMEDIATE").close()
                frame = {"binding": binding, "dimensions": dimensions, "delegates": {}}
                expected = (
                    CheckpointInterrupted
                    if kind == "interrupt"
                    else ValueError
                    if kind == "version"
                    else RuntimeError
                    if kind == "outside"
                    else StaleValidationCheckpoint
                )
                db.interrupt_publication = kind == "interrupt"
                with db.observe():
                    with pytest.raises(expected):
                        if operation == "load":
                            current.load()
                        elif operation == "_fence":
                            current._fence(proposed)
                        else:
                            current._publish(proposed, after)
                primitive = observed_work(db, {name: 0 for name in db.metrics}, 0, 0)
                primitive["changed_rows"] = db.metrics["changed_rows"]
                sample = complete_sample(frame, primitive)
                samples.append(sample)
                assert sample["metrics"]["changed_rows"] == int(kind == "interrupt")
                assert sample["metrics"]["source_payload_octets"] == 0
                assert db.in_transaction == applied
                if applied:
                    assert_uncommitted_hidden(path, original)
                if kind == "interrupt":
                    # Physical SQL work remains counted even though rollback removes its result.
                    changed = snapshot(db)
                    assert changed != original
                    assert {
                        name: rows for name, rows in changed.items() if name != "journal_validation"
                    } == {
                        name: rows
                        for name, rows in original.items()
                        if name != "journal_validation"
                    }
                else:
                    assert snapshot(db) == original
                db.rollback()
                assert not db.in_transaction and snapshot(db) == original
                if kind == "interrupt":
                    db.execute("BEGIN IMMEDIATE").close()
                    returned = current._publish(before, after)
                    db.commit()
                    assert current.load() == returned and returned.checkpoint == after


@pytest.mark.growth_witness(
    "riverhog-validation-checkpoint-load-rejection-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._session_checkpoint.ValidationLedger.load",
)
def test_actual_rejected_checkpoint_load_preserves_owned_state(
    tmp_path, checkpoint_histories, record_property
):
    samples = []
    record_property("growth_samples", samples)
    rejection_samples(tmp_path, checkpoint_histories, "load", samples=samples)


@pytest.mark.growth_witness(
    "riverhog-validation-checkpoint-fence-rejection-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._session_checkpoint.ValidationLedger._fence",
)
def test_actual_rejected_checkpoint_fence_preserves_owned_state(
    tmp_path, checkpoint_histories, record_property
):
    samples = []
    record_property("growth_samples", samples)
    rejection_samples(tmp_path, checkpoint_histories, "_fence", samples=samples)


@pytest.mark.growth_witness(
    "riverhog-validation-checkpoint-publication-rejection-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_provenance._session_checkpoint.ValidationLedger._publish",
)
def test_actual_rejected_or_interrupted_publication_preserves_transaction_custody(
    tmp_path, checkpoint_histories, record_property
):
    samples = []
    record_property("growth_samples", samples)
    rejection_samples(tmp_path, checkpoint_histories, "_publish", samples=samples)
