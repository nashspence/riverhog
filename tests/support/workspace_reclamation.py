"""Observe actual extent syscalls and exact delegated reclamation invocations."""

from __future__ import annotations

import builtins
import hashlib
import io
import os
import sqlite3
from contextlib import contextmanager

import pytest
import workspace_reclamation
import workspace_reclamation as receiving
from riverhog_canonical_json import canonical_json_bytes
from riverhog_growth_contracts import growth_binding

_FSENCODE = os.fsencode


class ReclamationMetrics(dict):
    def __init__(self, values):
        super().__init__(values)
        self.attempts = []


@pytest.fixture
def retained_file_receivers():
    originals = []

    def retain():
        owner = workspace_reclamation.FileReclamationOwner()
        originals.append(owner)
        return owner

    yield retain
    for owner in reversed(originals):
        owner.close()


def execution_sample(binding, projection, dimensions, metrics, delegates):
    if binding.operation is workspace_reclamation.reclaim_retired_files:
        assert not delegates and isinstance(metrics, ReclamationMetrics)
        assert len(metrics.attempts) == 1
        attempt = metrics.attempts[0]
        assert attempt["names"] == dimensions["candidate_files"]
        assert attempt["directory_octets"] == dimensions["directory_octets"]
        assert attempt["octet_budget"] == dimensions["octet_budget"]
        child = attempt["binding"]
        delegates = {
            "attempt": [execution_sample(child, projection, dimensions, attempt["metrics"], {})]
        }
    return {
        "declaration": binding.declaration.id,
        "declaration_sha256": hashlib.sha256(
            canonical_json_bytes(projection["declarations"][binding.declaration.id])
        ).hexdigest(),
        "dimensions": dict(dimensions),
        "metrics": dict(metrics),
        "delegates": delegates,
    }


@contextmanager
def observe_reclamation(monkeypatch, operation):
    binding = growth_binding(operation)
    metrics = ReclamationMetrics({bound.metric: 0 for bound in binding.declaration.bounds})
    names = ("stat", "fstat", "getuid", "open", "close", "ftruncate", "unlink", "fsync")
    originals = {name: getattr(os, name) for name in names}
    compare = workspace_reclamation._compare_names
    directory_encoder = receiving._encode_directory
    encoding_directory = False

    def counted(name, metric):
        original = originals[name]

        def observed(*args, **kwargs):
            metrics[metric] += 1
            if name == "ftruncate":
                before = originals["fstat"](args[0]).st_size
                result = original(*args, **kwargs)
                metrics["reclaimed_extent_octets"] += before - originals["fstat"](args[0]).st_size
                return result
            if name == "unlink":
                before = originals["stat"](args[0], follow_symlinks=False, **kwargs).st_size
                result = original(*args, **kwargs)
                metrics["reclaimed_extent_octets"] += before
                return result
            return original(*args, **kwargs)

        return observed

    def encoded(name):
        value = _FSENCODE(name)
        metric = "encoded_directory_octets" if encoding_directory else "encoded_name_octets"
        metrics[metric] += len(value)
        return value

    def encoded_directory(directory):
        nonlocal encoding_directory
        previous = encoding_directory
        encoding_directory = True
        try:
            return directory_encoder(directory)
        finally:
            encoding_directory = previous

    def native_observed(actual):
        source_binding = growth_binding(actual)

        def observed(owner, *args, **kwargs):
            originals = [resource.metrics for resource in (owner._directory, owner._file)]
            before_metrics = dict(metrics)
            try:
                return actual(owner, *args, **kwargs)
            finally:
                for resource, before in zip(
                    (owner._directory, owner._file), originals, strict=True
                ):
                    after = resource.metrics
                    for name, metric in (
                        ("open_calls", "open_calls"),
                        ("close_calls", "close_calls"),
                        ("fstat_calls", "metadata_calls"),
                        ("dup_calls", "dup_calls"),
                        ("control_octets", "native_control_octets"),
                    ):
                        metrics[metric] += after[name] - before[name]
                if actual.__name__ == "reclaim":
                    metrics.attempts.append(
                        {
                            "binding": source_binding,
                            "names": len(kwargs["names"]),
                            "directory_octets": len(owner._path),
                            "octet_budget": kwargs["octet_budget"],
                            "metrics": {
                                key: value - before_metrics[key] for key, value in metrics.items()
                            },
                        }
                    )

        return observed

    def compared(left, right, **kwargs):
        metrics["name_order_comparisons"] += 1
        metrics["name_comparison_octets"] += len(left) + len(right)
        return compare(left, right, **kwargs)

    def forbidden(*_args, **_kwargs):
        pytest.fail("reclamation read payloads, opened SQLite or enumerated a directory")

    with monkeypatch.context() as patch:
        for name, metric in (
            ("stat", "metadata_calls"),
            ("fstat", "metadata_calls"),
            ("getuid", "owner_identity_calls"),
            ("open", "open_calls"),
            ("close", "close_calls"),
            ("ftruncate", "truncate_calls"),
            ("unlink", "unlink_calls"),
            ("fsync", "sync_calls"),
        ):
            patch.setattr(os, name, counted(name, metric))
        patch.setattr(os, "fsencode", encoded)
        patch.setattr(receiving, "_encode_directory", encoded_directory)
        for name in ("reclaim", "close"):
            patch.setattr(
                receiving.FileReclamationOwner,
                name,
                native_observed(getattr(receiving.FileReclamationOwner, name)),
            )
        patch.setattr(workspace_reclamation, "_compare_names", compared)
        for module, name in (
            (builtins, "open"),
            (io, "open"),
            (os, "read"),
            (os, "listdir"),
            (os, "scandir"),
            (sqlite3, "connect"),
        ):
            patch.setattr(module, name, forbidden)
        yield metrics


@contextmanager
def observe_file_delegate(monkeypatch, module, projection, dimensions, metrics):
    actual = module.reclaim_retired_files
    child = growth_binding(actual)
    samples = []

    def observed(directory, *, receiver, names, octet_budget, expected_directory_identity=None):
        before = dict(metrics)
        before_attempts = len(metrics.attempts)
        try:
            return actual(
                directory,
                receiver=receiver,
                names=names,
                octet_budget=octet_budget,
                expected_directory_identity=expected_directory_identity,
            )
        finally:
            observed_metrics = ReclamationMetrics(
                {name: metrics[name] - before[name] for name in metrics}
            )
            observed_metrics.attempts = metrics.attempts[before_attempts:]
            encoded = tuple(_FSENCODE(name) for name in names)
            samples.append(
                execution_sample(
                    child,
                    projection,
                    {
                        "candidate_files": len(names),
                        "name_octets": sum(map(len, encoded)),
                        "maximum_name_octets": max(map(len, encoded), default=0),
                        "directory_octets": len(_FSENCODE(directory)),
                        **dimensions,
                        "octet_budget": octet_budget,
                    },
                    observed_metrics,
                    {},
                )
            )

    with monkeypatch.context() as patch:
        patch.setattr(module, "reclaim_retired_files", observed)
        yield samples
