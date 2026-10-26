from __future__ import annotations

import hashlib
import os
from contextlib import contextmanager

import pytest
from riverhog_canonical_json import canonical_json_bytes
from riverhog_core import journal_validation_workspace as kernel
from riverhog_core import journal_validation_workspace_reference as owner
from riverhog_growth_contracts import growth_binding, project_bindings, validate_execution_samples

from tests.unit.test_journal_validation_workspace_reference import CloseFault, held

RETAIN, CLOSE = owner.retain_validation_workspace, owner.ValidationWorkspaceReference.close


@contextmanager
def observe(monkeypatch, custody, *, post_loss=False):
    metrics = {
        bound.metric: 0
        for operation in (RETAIN, CLOSE)
        for bound in growth_binding(operation).declaration.bounds
    }
    actual_current = kernel.ValidationWorkspaceCustody.verify_current
    file_io, duplicate = owner.FileIO, owner.os.dup

    class Stream:
        def __init__(self, stream):
            self.stream = stream

        def close(self):
            metrics["stream_closes"] += 1
            return self.stream.close()

        def read(self, *args, **kwargs):
            metrics["payload_reads"] += 1
            return self.stream.read(*args, **kwargs)

        def write(self, *args, **kwargs):
            metrics["payload_writes"] += 1
            return self.stream.write(*args, **kwargs)

    def current(selected):
        if selected is not custody:
            return actual_current(selected)
        metrics["custody_checks"] += 1
        result = actual_current(selected)
        return result and not (post_loss and metrics["custody_checks"] == 2)

    def construct(path, *args, **kwargs):
        metrics["stream_constructions"] += 1
        metrics["stream_path_octets"] += len(os.fsencode(path))
        return Stream(file_io(path, *args, **kwargs))

    def dup(descriptor):
        metrics["descriptor_duplications"] += 1
        return duplicate(descriptor)

    with monkeypatch.context() as patch:
        patch.setattr(kernel.ValidationWorkspaceCustody, "verify_current", current)
        patch.setattr(owner, "FileIO", construct)
        patch.setattr(owner.os, "dup", dup)
        for name, metric in (
            ("open", "path_opens"),
            ("listdir", "directory_enumerations"),
            ("scandir", "directory_enumerations"),
        ):
            actual = getattr(os, name)

            def call(*args, _actual=actual, _metric=metric, **kwargs):
                metrics[_metric] += 1
                return _actual(*args, **kwargs)

            patch.setattr(os, name, call)
        yield metrics, Stream


def sample(operation, dimensions, metrics):
    binding = growth_binding(operation)
    projection = project_bindings((binding,))
    result = {
        "declaration": binding.declaration.id,
        "declaration_sha256": hashlib.sha256(
            canonical_json_bytes(projection["declarations"][binding.declaration.id])
        ).hexdigest(),
        "dimensions": dimensions,
        "metrics": {bound.metric: metrics[bound.metric] for bound in binding.declaration.bounds},
        "delegates": {},
    }
    validate_execution_samples(projection, binding.declaration.id, [result])
    return result


def exercise(tmp_path, monkeypatch, record_property, operation):
    samples = []
    for siblings, width in ((1, 1), (17, 17), (1025, 17), (1, 101)):
        base = tmp_path / f"{siblings}-{width}"
        base.mkdir(mode=0o700)
        root = base / ("p" * width)
        for number in range(siblings):
            with kernel.lease_journal_validation_workspace(
                root, f"{number + 2:032x}", create=True
            ) as directory:
                assert directory is not None
        with held(root) as custody:
            dimensions = {
                "directory_path_octets": len(os.fsencode(custody.directory)),
                "sibling_workspaces": siblings,
            }
            cases = (
                ("success", "callback", "post-loss")
                if operation is RETAIN
                else ("success", "before", "after", "already-closed")
            )
            for kind in cases:
                references = []
                reference = None
                if operation is CLOSE:
                    reference = RETAIN(custody, on_reference=references.append)
                    if kind == "already-closed":
                        reference.close()
                try:
                    with observe(monkeypatch, custody, post_loss=kind == "post-loss") as (
                        metrics,
                        stream,
                    ):

                        def retain(current, _metrics=metrics, _references=references, _kind=kind):
                            _metrics["retention_callbacks"] += 1
                            _references.append(current)
                            if _kind == "callback":
                                raise InterruptedError("retention interrupted")

                        if operation is RETAIN:
                            if kind == "success":
                                reference = RETAIN(custody, on_reference=retain)
                            elif kind == "callback":
                                with pytest.raises(InterruptedError):
                                    RETAIN(custody, on_reference=retain)
                            else:
                                with pytest.raises(kernel.LostJournalValidationWorkspace):
                                    RETAIN(custody, on_reference=retain)
                        else:
                            if kind != "already-closed":
                                actual = reference._stream
                                if kind in {"before", "after"}:
                                    actual = CloseFault(actual, after=kind == "after")
                                reference._stream = stream(actual)
                            if kind in {"before", "after"}:
                                with pytest.raises(OSError, match="unconfirmed"):
                                    CLOSE(reference)
                            else:
                                CLOSE(reference)
                        observed = dict(metrics)
                    samples.append(sample(operation, dimensions, observed))
                    if operation is RETAIN:
                        assert observed["retention_callbacks"] == 1
                        assert observed["descriptor_duplications"] == int(kind != "callback")
                        assert observed["stream_closes"] == int(kind == "post-loss")
                    else:
                        assert observed["stream_closes"] == int(kind != "already-closed")
                finally:
                    for reference in references:
                        reference.close()
        assert not custody.verify_current()
    record_property("growth_samples", samples)


@pytest.mark.growth_witness(
    "riverhog-validation-workspace-reference-retain-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.journal_validation_workspace_reference.retain_validation_workspace",
)
def test_actual_lease_retention_ports_and_rejected_cleanup_stay_constant(
    tmp_path, monkeypatch, record_property
):
    exercise(tmp_path, monkeypatch, record_property, RETAIN)


@pytest.mark.growth_witness(
    "riverhog-validation-workspace-reference-close-work/v1",
    gates=("unit", "unit-unit", "check-in", "linux-qualification"),
    operation="riverhog_core.journal_validation_workspace_reference.ValidationWorkspaceReference.close",
)
def test_actual_same_stream_close_and_fault_ports_stay_constant(
    tmp_path, monkeypatch, record_property
):
    exercise(tmp_path, monkeypatch, record_property, CLOSE)
